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
