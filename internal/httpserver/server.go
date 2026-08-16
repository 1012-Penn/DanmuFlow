package httpserver

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// New 创建 DanmuFlow 的 HTTP 入口。
// 后续可以在这里继续添加路由和中间件。
func New(addr string) *http.Server {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "DanmuFlow is running\n")
	})
	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok\n")
	})

	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
