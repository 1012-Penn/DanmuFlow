// Package httpserver 负责提供 DanmuFlow 的 HTTP/WebSocket 入口。
// 它把外部的 HTTP 请求转换成对内部 room 包的调用，是用户与房间模型之间的桥梁。
package httpserver

import (
	"net/http"

	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/gin-gonic/gin"
)

// New 创建 DanmuFlow 的 HTTP 入口。
// 后续可以在这里继续添加路由和中间件。
func New(addr string) *http.Server {
	// gin.New() 返回一个不带默认中间件的引擎，
	// 下面显式挂上 Logger 和 Recovery，便于查看请求日志并在 panic 时恢复服务。
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	// rooms 是整个进程共享的房间注册表。
	// 每个 room_id 对应一个独立的 Room，同一个房间内的连接才会互相广播。
	rooms := room.NewRegistry()
	websocketHandler := newWebSocketHandler(rooms)

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

	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
