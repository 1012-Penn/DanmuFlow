package httpserver

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// zapHTTPLogger 记录每个 HTTP 请求完成后的结构化摘要。
// 它不记录查询字符串，避免把 token 等敏感参数写入日志。
func zapHTTPLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		}
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		switch {
		case c.Writer.Status() >= http.StatusInternalServerError:
			logger.Error("http_request", fields...)
		case c.Writer.Status() >= http.StatusBadRequest:
			logger.Warn("http_request", fields...)
		default:
			logger.Info("http_request", fields...)
		}
	}
}

// zapRecovery 将 HTTP handler 中的 panic 转换为结构化错误日志和 500 响应。
func zapRecovery(logger *zap.Logger) gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		logger.Error("http_panic",
			zap.Any("panic", recovered),
			zap.Stack("stack"),
		)
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}
