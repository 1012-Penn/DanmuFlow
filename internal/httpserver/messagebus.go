package httpserver

import (
	"context"
	"errors"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"go.uber.org/zap"
)

const inMemoryBusBufferSize = 1024

// newInMemoryMessageBus 创建测试用的进程内消息总线，并启动房间广播消费者。
func newInMemoryMessageBus(rooms *room.Registry, logger *zap.Logger) (bus.Bus, context.CancelFunc, error) {
	messageBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		return nil, nil, err
	}

	return messageBus, startMessageBusConsumer(rooms, messageBus, logger), nil
}

// newKafkaMessageBus 创建生产环境使用的 Kafka 消息总线，并启动房间广播消费者。
func newKafkaMessageBus(rooms *room.Registry, config bus.KafkaConfig, logger *zap.Logger) (bus.Bus, context.CancelFunc, error) {
	messageBus, err := bus.NewKafka(config)
	if err != nil {
		return nil, nil, err
	}

	return messageBus, startMessageBusConsumer(rooms, messageBus, logger), nil
}

// startMessageBusConsumer 启动唯一的房间广播消费者，并返回完整的清理函数。
// 清理时先取消 Consume，再关闭 Kafka Reader/Writer，确保网络资源和 goroutine
// 都有明确的退出路径；InMemoryBus 没有 Close 方法，只需要取消 context。
func startMessageBusConsumer(rooms *room.Registry, messageBus bus.Bus, logger *zap.Logger) context.CancelFunc {
	consumerContext, cancelConsumer := context.WithCancel(context.Background())
	go func() {
		err := messageBus.Consume(consumerContext, func(_ context.Context, msg message.Danmaku) error {
			chatRoom, err := rooms.GetOrCreate(msg.RoomID)
			if err != nil {
				logger.Error("message_consume_room_lookup_failed",
					zap.String("room_id", msg.RoomID),
					zap.String("message_id", msg.MessageID),
					zap.Error(err),
				)
				return err
			}

			if err := chatRoom.Publish(msg.Content); err != nil {
				// 消息已经入队后，最后一个客户端可能先断开。
				// 这种消息只能丢弃，不能让一个空房间错误杀死整个消费者。
				if errors.Is(err, room.ErrNoClient) {
					logger.Debug("message_dropped",
						zap.String("reason", "room_has_no_clients"),
						zap.String("room_id", msg.RoomID),
						zap.String("message_id", msg.MessageID),
					)
					return nil
				}
				logger.Error("message_consume_publish_failed",
					zap.String("room_id", msg.RoomID),
					zap.String("message_id", msg.MessageID),
					zap.Error(err),
				)
				return err
			}
			return nil
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("message_consumer_stopped", zap.Error(err))
		}
	}()

	return func() {
		cancelConsumer()
		if closer, ok := messageBus.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				logger.Error("message_bus_close_failed", zap.Error(err))
			}
		}
	}
}
