package bus

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/1012-Penn/DanmuFlow/internal/message"
)

var (
	// ErrInvalidBuffer 表示创建总线时提供的队列容量不是正数。
	ErrInvalidBuffer = errors.New("bus buffer size must be greater than zero")
	// ErrNilHandler 表示消费者没有提供处理每条消息的函数。
	ErrNilHandler = errors.New("bus handler cannot be nil")
	// ErrConsumerAlreadyRunning 表示同一个 InMemoryBus 已经有一个活跃消费者。
	ErrConsumerAlreadyRunning = errors.New("bus consumer is already running")
)

// InMemoryBus 是只用于单进程开发和测试的异步消息总线。
// messages 是整个进程共享的有界队列；当前实现只允许一个消费者，
// 以便用最小模型验证“发布后按入队顺序消费”的数据流。
type InMemoryBus struct {
	messages chan message.Danmaku

	// consuming 标识是否已有 Consume 调用正在读取 messages。
	// 多个消费者会竞争同一个 channel，破坏本阶段要验证的顺序模型，
	// 因此使用原子状态拒绝第二个消费者，而不是让它悄悄抢走消息。
	consuming atomic.Bool
}

// NewInMemory 创建一个带固定容量的进程内消息总线。
// bufferSize 决定突发消息最多可在消费者处理前暂存多少条；队列满后，
// Publish 会等待消费者腾出空间或等待调用方取消 context。
func NewInMemory(bufferSize int) (*InMemoryBus, error) {
	if bufferSize <= 0 {
		return nil, ErrInvalidBuffer
	}

	return &InMemoryBus{
		messages: make(chan message.Danmaku, bufferSize),
	}, nil
}

// Publish 把一条弹幕放入总线队列。
// 当队列已满时，它不会丢弃消息或无限制创建 goroutine，而是阻塞等待；
// 调用方可以通过 context 取消这次等待，从而把背压决定权留在接入层。
func (b *InMemoryBus) Publish(ctx context.Context, msg message.Danmaku) error {
	select {
	case b.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Consume 持续按消息进入队列的顺序调用 handler。
// 它会在 context 取消、handler 返回错误或发现已有消费者运行时结束；
// 总线不关闭 messages channel，因为生产者可能仍在并发发布，生命周期由 context 管理。
func (b *InMemoryBus) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return ErrNilHandler
	}
	if !b.consuming.CompareAndSwap(false, true) {
		return ErrConsumerAlreadyRunning
	}
	defer b.consuming.Store(false)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-b.messages:
			if err := handler(ctx, msg); err != nil {
				return err
			}
		}
	}
}

// Compile-time check: InMemoryBus 必须始终满足后续 KafkaBus 也会使用的 Bus 接口。
var _ Bus = (*InMemoryBus)(nil)
