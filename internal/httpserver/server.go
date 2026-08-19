// Package httpserver 负责提供 DanmuFlow 的 HTTP/WebSocket 入口。
// 它把外部的 HTTP 请求转换成对内部 room 包的调用，是用户与房间模型之间的桥梁。
package httpserver

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/ratelimit"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/1012-Penn/DanmuFlow/internal/sensitive"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const readinessCheckTimeout = time.Second

// Server 把 HTTP Server 与 DanmuFlow 的后台资源生命周期绑定在一起。
// 内嵌标准库 Server，保留 ListenAndServe 等熟悉 API；Shutdown 则额外处理
// WebSocket 和消息消费者，因为它们不属于普通 HTTP 请求生命周期。
type Server struct {
	*http.Server

	stopConsumer func()
	connections  *websocketConnections
	draining     *atomic.Bool
}

// New 创建 DanmuFlow 的 HTTP 入口。
// 后续可以在这里继续添加路由和中间件。
func New(addr string) *Server {
	server, err := NewWithKafka(addr, KafkaConfigFromEnv(), zap.NewNop())
	if err != nil {
		panic(err)
	}
	return server
}

// NewWithLogger 创建带结构化日志的 DanmuFlow HTTP 入口。
// logger 由进程启动入口创建并注入，保证 HTTP、WebSocket 和 Kafka 使用同一个日志出口。
// 默认从 DANMUFLOW_KAFKA_* 环境变量读取 Kafka 配置，生产启动入口也可以使用
// NewWithKafka 显式注入配置。
func NewWithLogger(addr string, logger *zap.Logger) *Server {
	server, err := NewWithKafka(addr, KafkaConfigFromEnv(), logger)
	if err != nil {
		panic(err)
	}
	return server
}

// NewWithKafka 创建使用指定 Kafka 配置的 HTTP/WebSocket 入口。
// 消息总线配置错误会返回给启动方；Kafka 的网络连接错误则会在实际发布或消费时返回。
func NewWithKafka(addr string, kafkaConfig bus.KafkaConfig, logger *zap.Logger) (*Server, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	rooms := room.NewRegistry()
	observability := metrics.New()
	messageBus, cancelConsumer, err := newKafkaMessageBus(rooms, kafkaConfig, observability, logger.Named("message_bus"))
	if err != nil {
		return nil, err
	}

	return newServerWithBus(addr, rooms, messageBus, cancelConsumer, observability, logger), nil
}

// newServerWithBus 构造共享路由和 WebSocket 处理器。
// messageBus 由调用方注入，使生产环境可以使用 Kafka，单元测试可以使用 InMemoryBus。
func newServerWithBus(addr string, rooms *room.Registry, messageBus bus.Bus, cancelConsumer func(), observability *metrics.Metrics, logger *zap.Logger) *Server {
	if logger == nil {
		logger = zap.NewNop()
	}

	// gin.New() 返回一个不带默认中间件的引擎，
	// 下面显式挂上 zap 日志和 Recovery，避免 Gin 默认日志与 zap 产生两套格式。
	router := gin.New()
	router.Use(zapHTTPLogger(logger.Named("http")), zapRecovery(logger.Named("recovery")))

	messageLimiter, err := ratelimit.New(5, 10)
	if err != nil {
		panic(err)
	}
	// 第一版词库先以内存配置注入，后续可以替换为配置文件或 Redis，
	// 而不需要改变 WebSocket 处理器和过滤器的调用方式。
	sensitiveFilter := sensitive.New([]string{"赌博", "诈骗"})
	connections := newWebSocketConnections()
	draining := &atomic.Bool{}
	canPublish := func() bool {
		if draining.Load() {
			return false
		}
		readiness, ok := messageBus.(bus.Readiness)
		return ok && readiness.ConsumerReady()
	}
	isReady := func() bool {
		if !canPublish() {
			return false
		}
		readiness := messageBus.(bus.Readiness)
		checkContext, cancel := context.WithTimeout(context.Background(), readinessCheckTimeout)
		defer cancel()
		ready := readiness.Check(checkContext) == nil
		return ready
	}
	websocketHandler := newWebSocketHandler(rooms, messageLimiter, sensitiveFilter, messageBus, isReady, canPublish, connections, observability, logger.Named("websocket"))

	// 根路径用于快速确认服务已经启动。
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "DanmuFlow is running\n")
	})

	// /healthz 是给负载均衡或容器编排用的健康检查端点。
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok\n")
	})
	// /readyz 表示“此实例现在可以安全承接弹幕”。它比 /healthz 严格：既要
	// 未处于发布下线过程，也要已加入 Kafka 消费组且生产端还能连接 Kafka。
	router.GET("/readyz", func(c *gin.Context) {
		if !isReady() {
			c.String(http.StatusServiceUnavailable, "not ready\n")
			return
		}
		c.String(http.StatusOK, "ready\n")
	})
	// /metrics 供 Prometheus 定期抓取，不参与 WebSocket 升级与业务路由。
	router.GET("/metrics", gin.WrapH(observability.Handler()))

	// /ws 是弹幕客户端的长连接入口。
	// 这里把 Gin 的 ResponseWriter 和 Request 直接交给 WebSocket 处理器，
	// 因为后续升级连接后就不再走普通 HTTP 路由逻辑。
	router.GET("/ws", func(c *gin.Context) {
		websocketHandler.ServeHTTP(c.Writer, c.Request)
	})

	server := &http.Server{
		Addr:    addr,
		Handler: router,
		// WebSocket 升级前先完成请求头读取；这个超时能防慢请求攻击，而升级后
		// 连接由 websocket.go 的 Ping/Pong 心跳接管。
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return &Server{
		Server:       server,
		stopConsumer: cancelConsumer,
		connections:  connections,
		draining:     draining,
	}
}

// Shutdown 按发布下线顺序关闭服务：先让就绪探针失败并停止收新连接，再通知现有
// WebSocket 客户端使用 1012 重连，最后停止消费者和 HTTP listener。调用方提供的
// context（main 中为 10 秒）是整段收尾的最大时间预算。
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.Server == nil {
		return nil
	}
	s.draining.Store(true)
	s.connections.closeForServiceRestart(ctx)
	if s.stopConsumer != nil {
		s.stopConsumer()
	}
	err := s.Server.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
