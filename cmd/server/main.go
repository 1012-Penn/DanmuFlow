package main

import (
	"fmt"
	"os"

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
	// NewWithKafka 创建 Gin 路由、内存房间和 Kafka 消息总线，
	// 最终返回一个标准库 *http.Server，方便后续统一管理超时等配置。
	srv, err := httpserver.NewWithKafka(addr, httpserver.KafkaConfigFromEnv(), logger)
	if err != nil {
		logger.Error("initialize_http_server_failed", zap.Error(err))
		return
	}
	logger.Info("http_server_started", zap.String("addr", addr))

	// ListenAndServe 会一直阻塞，直到服务被关闭或发生致命错误。
	// 因此这里放在 main 的最后；一旦返回，程序就退出。
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("http_server_stopped", zap.Error(err))
		return
	}
}
