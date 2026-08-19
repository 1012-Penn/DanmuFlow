// Package httpserver 负责提供 DanmuFlow 的 HTTP/WebSocket 入口。
// 它把外部的 HTTP 请求转换成对内部 room 包的调用，是用户与房间模型之间的桥梁。
package httpserver

import (
	"net/http"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/ratelimit"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/1012-Penn/DanmuFlow/internal/sensitive"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// New 创建 DanmuFlow 的 HTTP 入口。
// 后续可以在这里继续添加路由和中间件。
func New(addr string) *http.Server {
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
func NewWithLogger(addr string, logger *zap.Logger) *http.Server {
	server, err := NewWithKafka(addr, KafkaConfigFromEnv(), logger)
	if err != nil {
		panic(err)
	}
	return server
}

// NewWithKafka 创建使用指定 Kafka 配置的 HTTP/WebSocket 入口。
// 消息总线配置错误会返回给启动方；Kafka 的网络连接错误则会在实际发布或消费时返回。
func NewWithKafka(addr string, kafkaConfig bus.KafkaConfig, logger *zap.Logger) (*http.Server, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	rooms := room.NewRegistry()
	messageBus, cancelConsumer, err := newKafkaMessageBus(rooms, kafkaConfig, logger.Named("message_bus"))
	if err != nil {
		return nil, err
	}

	return newServerWithBus(addr, rooms, messageBus, cancelConsumer, logger), nil
}

// newServerWithBus 构造共享路由和 WebSocket 处理器。
// messageBus 由调用方注入，使生产环境可以使用 Kafka，单元测试可以使用 InMemoryBus。
func newServerWithBus(addr string, rooms *room.Registry, messageBus bus.Bus, cancelConsumer func(), logger *zap.Logger) *http.Server {
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
	websocketHandler := newWebSocketHandler(rooms, messageLimiter, sensitiveFilter, messageBus, logger.Named("websocket"))

	// 根路径用于快速确认服务已经启动。
	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "DanmuFlow is running\n")
	})

	// /healthz 是给负载均衡或容器编排用的健康检查端点。
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok\n")
	})

	// /ws 是弹幕客户端的长连接入口。
	// 这里把 Gin 的 ResponseWriter 和 Request 直接交给 WebSocket 处理器，
	// 因为后续升级连接后就不再走普通 HTTP 路由逻辑。
	router.GET("/ws", func(c *gin.Context) {
		websocketHandler.ServeHTTP(c.Writer, c.Request)
	})

	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}
	server.RegisterOnShutdown(cancelConsumer)
	return server
}
