package main

import (
	"log"
	"os"

	"github.com/1012-Penn/DanmuFlow/internal/httpserver"
)

// main 是 DanmuFlow 服务的启动入口。
// 它只负责读取启动参数、创建 HTTP 服务并阻塞等待服务运行。
func main() {
	// 允许通过环境变量修改 HTTP 服务监听地址，便于本地开发和容器部署。
	// 如果没有设置，就使用默认的 :8080，让程序开箱即用。
	addr := os.Getenv("DANMUFLOW_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// New 会创建 Gin 路由、内存房间和 WebSocket 处理器，
	// 最终返回一个标准库 *http.Server，方便后续统一管理超时等配置。
	srv := httpserver.New(addr)
	log.Printf("DanmuFlow HTTP server listening on %s", addr)

	// ListenAndServe 会一直阻塞，直到服务被关闭或发生致命错误。
	// 因此这里放在 main 的最后；一旦返回，程序就退出。
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
