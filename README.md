# DanmuFlow

DanmuFlow 是一个使用 Go 构建的高并发直播间弹幕系统。

## 本地运行

```bash
go run ./cmd/server
```

默认监听 `:8080`，也可以通过 `DANMUFLOW_HTTP_ADDR` 修改地址：

```bash
DANMUFLOW_HTTP_ADDR=:9090 go run ./cmd/server
```

可用端点：

- `GET /`：确认服务已启动
- `GET /healthz`：健康检查
