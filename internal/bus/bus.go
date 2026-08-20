// Package bus 定义 DanmuFlow 内部消息总线的抽象边界。
package bus

import (
	"context"

	"github.com/1012-Penn/DanmuFlow/internal/message"
)

// Handler 处理消息消费者收到的一条弹幕。
// handler 返回错误时，具体总线实现应停止当前消费流程并把错误交给调用方；
// 重试和死信策略属于后续 Kafka 消费者阶段，不在当前接口中偷偷决定。
type Handler func(ctx context.Context, msg message.Danmaku) error

// Bus 是弹幕消息生产者和消费者共同依赖的抽象。
// Publish 应遵守调用方的 context 生命周期；Consume 会阻塞到 context 被取消、
// handler 返回错误或底层消息基础设施发生不可恢复错误。
type Bus interface {
	// Publish 将一条弹幕交给消息总线。
	Publish(ctx context.Context, msg message.Danmaku) error
	// Consume 持续消费消息并交给 handler 处理。
	Consume(ctx context.Context, handler Handler) error
}

// Readiness 是消息总线可选提供的运行状态接口。
// ConsumerReady 表示消费者已经成功加入消费组并拿到当前 generation；Check
// 用于确认生产端仍能与 Kafka 建立连接。HTTP 网关同时依赖两者，避免把“进程还在”
// 错当成“可以安全接收新弹幕”。
type Readiness interface {
	ConsumerReady() bool
	Check(ctx context.Context) error
}

// RoomOwnership 暴露 Kafka 分区与当前消费者实例之间的只读归属关系。
// HTTP 网关只依赖这个抽象判断房间是否属于本机，不直接操作 consumer-group 状态。
type RoomOwnership interface {
	// PartitionForRoom 返回 roomID 按生产端相同 hash 规则进入的 Kafka 分区。
	// 元数据尚未就绪或 roomID 无效时，第二个返回值为 false。
	PartitionForRoom(roomID string) (int, bool)
	// OwnsRoom 报告当前 consumer-group generation 是否把该房间分区分配给本机。
	OwnsRoom(roomID string) bool
	// AssignedPartitions 返回当前 generation 分配给本机的有序快照。
	// 调用方可以修改返回切片，不会影响 KafkaBus 内部状态。
	AssignedPartitions() []int
}
