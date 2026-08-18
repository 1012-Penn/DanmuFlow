// Package httpserver 负责提供 DanmuFlow 的 HTTP/WebSocket 入口。
// 它把外部的 HTTP 请求转换成对内部 room 包的调用，是用户与房间模型之间的桥梁。
package httpserver

import (
	"net/http"

	"github.com/1012-Penn/DanmuFlow/internal/ratelimit"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/1012-Penn/DanmuFlow/internal/sensitive"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// New 创建 DanmuFlow 的 HTTP 入口。
// 后续可以在这里继续添加路由和中间件。
func New(addr string) *http.Server {
	return NewWithLogger(addr, zap.NewNop())
}

// NewWithLogger 创建带结构化日志的 DanmuFlow HTTP 入口。
// logger 由进程启动入口创建并注入，保证 HTTP 和 WebSocket 使用同一个日志出口。
func NewWithLogger(addr string, logger *zap.Logger) *http.Server {
	if logger == nil {
		logger = zap.NewNop()
	}

	// gin.New() 返回一个不带默认中间件的引擎，
	// 下面显式挂上 zap 日志和 Recovery，避免 Gin 默认日志与 zap 产生两套格式。
	router := gin.New()
	router.Use(zapHTTPLogger(logger.Named("http")), zapRecovery(logger.Named("recovery")))

	// rooms 是整个进程共享的房间注册表。
	// 每个 room_id 对应一个独立的 Room，同一个房间内的连接才会互相广播。
	rooms := room.NewRegistry()
	messageLimiter, err := ratelimit.New(5, 10)
	if err != nil {
		panic(err)
	}
	// 第一版词库先以内存配置注入，后续可以替换为配置文件或 Redis，
	// 而不需要改变 WebSocket 处理器和过滤器的调用方式。
	sensitiveFilter := sensitive.New([]string{"赌博", "诈骗"})
	messageBus, cancelConsumer, err := newInMemoryMessageBus(rooms, logger.Named("message_bus"))
	if err != nil {
		panic(err)
	}
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
