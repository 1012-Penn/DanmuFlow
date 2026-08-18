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
- `GET /ws?room_id=room-a&user_id=alice`：建立指定房间的 WebSocket 弹幕连接

## WebSocket 使用方式

启动服务后，客户端连接：

```text
ws://localhost:8080/ws?room_id=room-a&user_id=alice
```

客户端发送 JSON 弹幕：

```json
{"content":"hello"}
```

服务端会把消息广播给 `room-a` 的在线客户端：

```json
{"sequence":1,"content":"hello"}
```

其中 `sequence` 是当前内存房间内递增的消息序号。

当前版本的限制：

- 房间路由暂时只存在于单个进程内存中，尚未使用 Redis 或其他共享存储；
- 每个 `user_id` 默认每秒允许 5 条弹幕，最多突发 10 条，超限消息会被丢弃；
- 服务重启后连接和消息都会丢失；
- 慢客户端可能丢失消息，但不会阻塞其他客户端；
- 尚未接入 Kafka、Redis、MySQL、登录鉴权和历史消息补偿。

## 验证

```bash
go test -race ./...
```
