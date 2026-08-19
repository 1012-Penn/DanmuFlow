# DanmuFlow

DanmuFlow 是一个使用 Go 构建的高并发直播间弹幕系统。当前已经跑通「客户端连接 → Kafka → 房间广播」的最小闭环；房间和 WebSocket 连接仍然保存在单个进程内存中。

## 环境要求

- Go 1.26+

## 本地运行

先启动 Kafka 和默认 Topic：

```bash
docker compose up -d
docker compose ps
```

确认 Kafka 健康后，再启动 DanmuFlow：

```bash
go run ./cmd/server
```

默认监听 `:8080`。

### 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DANMUFLOW_HTTP_ADDR` | `:8080` | HTTP 服务监听地址 |
| `DANMUFLOW_LOG_LEVEL` | `info` | 日志等级：`debug` / `info` / `warn` / `error` |
| `DANMUFLOW_KAFKA_BROKERS` | `localhost:9092` | Kafka Broker 地址，多个地址用逗号分隔 |
| `DANMUFLOW_KAFKA_TOPIC` | `danmaku` | 弹幕 Topic |
| `DANMUFLOW_KAFKA_GROUP_ID` | `danmuflow-broadcast` | 房间广播消费者组 |

日志默认以 JSON 格式输出到标准输出，便于 Docker / Kubernetes / 日志采集器统一收集：

```bash
DANMUFLOW_HTTP_ADDR=:9090 \
DANMUFLOW_KAFKA_BROKERS=localhost:9092 \
DANMUFLOW_LOG_LEVEL=debug \
go run ./cmd/server
```

启动 Kafka 后，首次运行可以使用默认的 `danmaku` Topic 和 `danmuflow-broadcast` 消费者组。Kafka 客户端使用惰性连接，服务启动时不会主动验证 Broker；发送或消费消息时如果 Kafka 不可用，日志会记录网络错误，WebSocket 发布会返回 `message_bus_unavailable`。

Kafka Producer 使用 `RequireOne` 确认级别和 10ms 批次等待：正常低流量消息不会因为默认的 1 秒攒批窗口卡住，但在 Broker leader 尚未完成副本同步前发生故障时，极少量消息可能丢失。这个取舍符合弹幕场景的低延迟优先目标；需要更强持久性时可改回 `RequireAll`。

停止本地 Kafka：

```bash
docker compose down
```

运行真实 Kafka 集成测试：

```bash
DANMUFLOW_KAFKA_BROKERS=localhost:9092 \
go test -tags=integration ./internal/bus -run TestKafkaBusEndToEndPreservesRoomOrder -v
```

普通 `go test ./...` 不会连接 Kafka；集成测试通过 `integration` build tag 单独运行。

## 可用端点

- `GET /`：确认服务已启动
- `GET /healthz`：进程存活检查
- `GET /readyz`：流量就绪检查；要求已加入 Kafka 消费组、生产端可连接 Kafka，且未处于发布下线过程。负载均衡应使用此端点
- `GET /metrics`：Prometheus 指标抓取端点，包含连接数、消息拒绝原因、Kafka 发布和消费处理耗时、消费者就绪/重启次数、慢客户端丢弃数与 Go 运行时指标
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
- 服务重启时，在线连接会收到 WebSocket `1012`（服务重启）并需要客户端重连；未持久化的在途消息仍可能丢失；
- 慢客户端可能丢失自己的消息，但不会阻塞房间内其他客户端；
- 生产入口已接入 Kafka；房间路由仍只存在于单个进程内存中，尚未使用 Redis 或其他共享存储；
- 单元测试使用 InMemoryBus，不要求测试环境运行 Kafka；
- 尚未接入 MySQL、登录鉴权和历史消息补偿。

## 项目结构

```text
cmd/server/              服务启动入口
internal/httpserver/     HTTP/WebSocket 网关层
internal/room/           内存房间模型（房间注册、成员、序号、广播）
internal/message/        跨组件弹幕消息模型
internal/bus/             消息总线抽象、InMemoryBus 和 KafkaBus
internal/ratelimit/      按 user_id 维度的令牌桶限流器
internal/sensitive/      内存敏感词过滤
internal/logging/        结构化日志（zap）初始化
```

## 验证

```bash
go test -race ./...
```

## 基线压测

`cmd/loadtest` 会建立指定数量的 WebSocket 连接，均匀分配到多个房间，并统计发送者收到自身弹幕回显的端到端延迟。报告中的 `self_delivered` 是按消息 ID 去重后的唯一回显数，`duplicate_self_delivered` 是重复回显数；两者分开后，Kafka 故障恢复时的重复投递不会被误算成送达成功。压测期间可通过 `/metrics` 观察服务端的发布、消费和慢客户端指标：

```bash
go run ./cmd/loadtest \
  -url ws://localhost:8080/ws \
  -connections 100 \
  -rooms 10 \
  -rate 100 \
  -duration 5m
```

压测工具的 `rate` 是所有连接合计的每秒发送数。应让单个用户的发送速率低于当前每秒 5 条的限流阈值，除非实验目的就是测试限流行为。
