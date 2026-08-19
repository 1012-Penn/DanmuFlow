package httpserver

import (
	"context"
	"errors"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"go.uber.org/zap"
)

const inMemoryBusBufferSize = 1024

const (
	consumerRetryInitialDelay = 100 * time.Millisecond
	consumerRetryMaxDelay     = 5 * time.Second
)

// newInMemoryMessageBus 创建测试用的进程内消息总线，并启动房间广播消费者。
func newInMemoryMessageBus(rooms *room.Registry, observability *metrics.Metrics, logger *zap.Logger) (bus.Bus, context.CancelFunc, error) {
	messageBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		return nil, nil, err
	}

	return messageBus, startMessageBusConsumer(rooms, messageBus, observability, logger), nil
}

// newKafkaMessageBus 创建生产环境使用的 Kafka 消息总线，并启动房间广播消费者。
func newKafkaMessageBus(rooms *room.Registry, config bus.KafkaConfig, observability *metrics.Metrics, logger *zap.Logger) (bus.Bus, context.CancelFunc, error) {
	config.Logger = logger
	messageBus, err := bus.NewKafka(config)
	if err != nil {
		return nil, nil, err
	}

	return messageBus, startMessageBusConsumer(rooms, messageBus, observability, logger), nil
}

// startMessageBusConsumer 启动房间广播消费者，并返回完整的清理函数。
// 瞬时 Kafka 错误不会永久杀死消费链：监督循环会指数退避后重新 Consume。错误消息
// 是否提交由 Bus 决定；KafkaBus 会跳过不可解析的 JSON，其他错误保留 offset 等待重试。
func startMessageBusConsumer(rooms *room.Registry, messageBus bus.Bus, observability *metrics.Metrics, logger *zap.Logger) context.CancelFunc {
	consumerContext, cancelConsumer := context.WithCancel(context.Background())
	if readiness, ok := messageBus.(bus.Readiness); ok {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			defer observability.ConsumerReady.Set(0)
			for {
				if readiness.ConsumerReady() {
					observability.ConsumerReady.Set(1)
				} else {
					observability.ConsumerReady.Set(0)
				}
				select {
				case <-consumerContext.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	go func() {
		observability.ConsumerRunning.Set(1)
		defer func() {
			observability.ConsumerRunning.Set(0)
			observability.ConsumerReady.Set(0)
		}()

		retryDelay := consumerRetryInitialDelay
		for consumerContext.Err() == nil {
			err := messageBus.Consume(consumerContext, func(_ context.Context, msg message.Danmaku) error {
				startedAt := time.Now()
				defer func() {
					observability.ConsumerHandlerDuration.Observe(time.Since(startedAt).Seconds())
				}()

				chatRoom, err := rooms.GetOrCreate(msg.RoomID)
				if err != nil {
					logger.Error("message_consume_room_lookup_failed",
						zap.String("room_id", msg.RoomID),
						zap.String("message_id", msg.MessageID),
						zap.Error(err),
					)
					return err
				}

				result, err := chatRoom.Publish(msg.Content)
				if err != nil {
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
				observability.RoomClientMessagesDropped.Add(float64(result.DroppedClients))
				return nil
			})

			if consumerContext.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			observability.ConsumerReady.Set(0)
			observability.ConsumerRestarts.Inc()
			logger.Error("message_consumer_failed_will_retry",
				zap.Error(err),
				zap.Duration("retry_after", retryDelay),
			)

			timer := time.NewTimer(retryDelay)
			select {
			case <-consumerContext.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			retryDelay = min(retryDelay*2, consumerRetryMaxDelay)
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
