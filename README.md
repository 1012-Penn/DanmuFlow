# DanmuFlow

DanmuFlow 是一个使用 Go 构建的高并发直播间弹幕系统。当前处于**单进程内存 MVP** 阶段：已经完整跑通「客户端连接 → 发送弹幕 → 房间广播」的最小闭环，Kafka / Redis / MySQL 等基础设施尚未接入。

## 环境要求

- Go 1.26+

## 本地运行

```bash
go run ./cmd/server
```

默认监听 `:8080`。

### 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DANMUFLOW_HTTP_ADDR` | `:8080` | HTTP 服务监听地址 |
| `DANMUFLOW_LOG_LEVEL` | `info` | 日志等级：`debug` / `info` / `warn` / `error` |

日志默认以 JSON 格式输出到标准输出，便于 Docker / Kubernetes / 日志采集器统一收集：

```bash
DANMUFLOW_HTTP_ADDR=:9090 DANMUFLOW_LOG_LEVEL=debug go run ./cmd/server
```

## 可用端点

- `GET /`：确认服务已启动
- `GET /healthz`：健康检查
- `GET /ws?room_id=room-a&user_id=alice`：建立指定房间的 WebSocket 弹幕连接（`room_id`、`user_id` 缺一不可，缺失时返回 HTTP 400）

## WebSocket 使用方式

启动服务后，客户端连接：

```text
ws://localhost:8080/ws?room_id=room-a&user_id=alice
```

客户端发送 JSON 弹幕：

```json
{"content":"hello"}
```

服务端会把消息广播给 `room-a` 房间内的**所有**在线客户端（包括发送者自己）：

```json
{"sequence":1,"content":"hello"}
```

其中 `sequence` 是当前内存房间内单调递增的消息序号。

## 协议与错误处理

### 客户端 → 服务端

```json
{"content":"hello"}
```

### 服务端 → 客户端

正常广播：

```json
{"sequence":1,"content":"hello"}
```

语义错误（空内容、超长、敏感词、消息总线不可用）以错误 JSON 返回，格式统一为：

```json
{"code":"empty_content","message":"content cannot be empty"}
```

### 错误码

| code | 触发条件 | 连接行为 |
| --- | --- | --- |
| `invalid_json` | JSON 语法错误 | 返回错误后**关闭连接** |
| `empty_content` | 正文为空或全是空白字符 | 返回错误，连接**继续** |
| `content_too_long` | 正文超过 500 个字符 | 返回错误，连接**继续** |
| `sensitive_content` | 命中敏感词 | 返回错误，连接**继续** |
| `message_bus_unavailable` | 消息总线队列持续满，1 秒内无法入队 | 返回错误，连接**继续** |

单条 WebSocket 数据帧超过 4 KiB 时，服务端发送 WebSocket 关闭帧 `1009`（message too big）并结束连接，不返回上面的 JSON 错误码。

超过限流阈值（见下）的消息被**静默丢弃**，不返回错误、不消耗房间序号、连接保持。

## 当前限制

- 房间路由只存在于单个进程内存中，尚未使用 Redis 或其他共享存储；
- 每个 `user_id` 默认每秒允许 5 条弹幕、最多突发 10 条，超限消息被丢弃；
- 单条 WebSocket 数据帧最大 4 KiB，弹幕正文最多 500 个字符；
- 默认拦截 `赌博` 和 `诈骗` 等敏感词，命中后不广播、不消耗房间序号；
- 服务重启后连接和消息都会丢失；
- 慢客户端可能丢失自己的消息，但不会阻塞房间内其他客户端；
- 当前已接入进程内 InMemoryBus，但尚未接入 Kafka、Redis、MySQL、登录鉴权和历史消息补偿。

## 项目结构

```text
cmd/server/              服务启动入口
internal/httpserver/     HTTP/WebSocket 网关层
internal/room/           内存房间模型（房间注册、成员、序号、广播）
internal/message/        跨组件弹幕消息模型
internal/bus/             消息总线抽象和 InMemoryBus
internal/ratelimit/      按 user_id 维度的令牌桶限流器
internal/sensitive/      内存敏感词过滤
internal/logging/        结构化日志（zap）初始化
```

## 验证

```bash
go test -race ./...
```
