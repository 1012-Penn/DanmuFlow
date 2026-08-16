package main

import (
	"log"
	"os"

	"github.com/1012-Penn/DanmuFlow/internal/httpserver"
)

func main() {
	// 允许通过环境变量修改 HTTP 服务监听地址，便于本地开发和容器部署。
	addr := os.Getenv("DANMUFLOW_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	srv := httpserver.New(addr)
	log.Printf("DanmuFlow HTTP server listening on %s", addr)

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
