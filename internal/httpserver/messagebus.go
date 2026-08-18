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

// newInMemoryMessageBus 创建服务器级消息总线，并启动唯一的房间广播消费者。
// 消费者把跨组件消息转换回本机 Room.Publish；未来替换为 Kafka 时，
// WebSocket 接入层只需要替换 Bus 实现，不需要改变房间广播逻辑。
func newInMemoryMessageBus(rooms *room.Registry, logger *zap.Logger) (bus.Bus, context.CancelFunc, error) {
	messageBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		return nil, nil, err
	}

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

	return messageBus, cancelConsumer, nil
}
