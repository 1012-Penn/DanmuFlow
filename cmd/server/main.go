package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/httpserver"
	"github.com/1012-Penn/DanmuFlow/internal/logging"
	"go.uber.org/zap"
)

// main 是 DanmuFlow 服务的启动入口。
// 它只负责读取启动参数、创建 HTTP 服务并阻塞等待服务运行。
func main() {
	logger, err := logging.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	// 允许通过环境变量修改 HTTP 服务监听地址，便于本地开发和容器部署。
	// 如果没有设置，就使用默认的 :8080，让程序开箱即用。
	addr := os.Getenv("DANMUFLOW_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// Kafka 配置通过环境变量注入，便于本地 Docker、测试环境和生产部署使用不同 Broker。
	// NewWithKafkaAndRouting 创建 Gin 路由、内存房间、Kafka 数据面和可选 Redis 路由控制面，
	// 最终返回一个标准库 *http.Server，方便后续统一管理超时等配置。
	srv, err := httpserver.NewWithKafkaAndRouting(addr, httpserver.KafkaConfigFromEnv(), httpserver.RoutingConfigFromEnv(), logger)
	if err != nil {
		logger.Error("initialize_http_server_failed", zap.Error(err))
		return
	}
	logger.Info("http_server_started", zap.String("addr", addr))

	// 收到容器停止或 Ctrl+C 后，最多用 10 秒执行发布下线：/readyz 先失败，
	// 已连接客户端收到 WebSocket 1012，再停止 Kafka 消费和 HTTP listener。
	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- srv.ListenAndServe()
	}()

	select {
	case <-signalContext.Done():
		logger.Info("http_server_shutdown_started")
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()
		if err := srv.Shutdown(shutdownContext); err != nil {
			logger.Error("http_server_shutdown_failed", zap.Error(err))
		}
	case err := <-serveErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http_server_stopped", zap.Error(err))
		}
	}
}
