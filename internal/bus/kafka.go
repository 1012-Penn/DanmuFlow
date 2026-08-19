package bus

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/segmentio/kafka-go"
)

var (
	// ErrInvalidKafkaConfig 表示 Kafka 总线缺少启动所需的配置。
	ErrInvalidKafkaConfig = errors.New("invalid kafka bus config")
	// ErrKafkaBusClosed 表示 Kafka 总线已经关闭，不能继续发送消息。
	ErrKafkaBusClosed = errors.New("kafka bus is closed")
)

// KafkaConfig 描述 KafkaBus 连接 Kafka 所需的最小配置。
// Brokers、Topic 和 GroupID 分别决定连接地址、消息主题和消费者组身份。
type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// KafkaBus 使用 Kafka Writer 发布消息，使用带消费者组的 Reader 消费消息。
// 同一个房间的 RoomID 会作为 Kafka key，因此 kafka.Hash 会把同一房间的
// 消息稳定地路由到同一个 partition；同一 partition 内仍保持追加顺序。
type KafkaBus struct {
	writer *kafka.Writer
	reader *kafka.Reader

	closeOnce sync.Once
	closeErr  error
}

// NewKafka 创建 Kafka 消息总线。
// 构造过程只创建客户端对象，不主动发送消息；Kafka 不可用时，Publish 或 Consume
// 会返回对应的网络错误，便于启动流程决定是否继续运行。
func NewKafka(config KafkaConfig) (*KafkaBus, error) {
	if len(config.Brokers) == 0 || config.Topic == "" || config.GroupID == "" {
		return nil, ErrInvalidKafkaConfig
	}
	for _, broker := range config.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, ErrInvalidKafkaConfig
		}
	}

	return &KafkaBus{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.Brokers...),
			Topic:    config.Topic,
			Balancer: &kafka.Hash{},
			// 弹幕优先低延迟；leader 确认写入后即可返回，不等待所有副本。
			// 代价是 leader 在复制完成前故障时，极少量消息可能丢失。
			RequiredAcks: kafka.RequireOne,
			// 低流量时也要尽快发送，不能让默认的 1 秒攒批窗口
			// 与 WebSocket 的 1 秒 Publish context 同时到期。
			BatchTimeout: 10 * time.Millisecond,
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: config.Brokers,
			Topic:   config.Topic,
			GroupID: config.GroupID,
		}),
	}, nil
}

// Publish 将弹幕序列化为 JSON 写入 Kafka。
// RoomID 同时作为 key，Kafka 会使用它选择 partition，从而保持同一房间的消息顺序。
func (b *KafkaBus) Publish(ctx context.Context, msg message.Danmaku) error {
	if b == nil || b.writer == nil {
		return ErrKafkaBusClosed
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return b.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.RoomID),
		Value: value,
	})
}

// Consume 从 Kafka 读取消息，交给 handler 成功处理后才提交 offset。
// handler 返回错误时当前消息不会提交，函数会结束并把错误交给上层消费者生命周期管理。
func (b *KafkaBus) Consume(ctx context.Context, handler Handler) error {
	if b == nil || b.reader == nil {
		return ErrKafkaBusClosed
	}
	if handler == nil {
		return ErrNilHandler
	}

	for {
		kafkaMessage, err := b.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		var danmaku message.Danmaku
		if err := json.Unmarshal(kafkaMessage.Value, &danmaku); err != nil {
			return err
		}

		if err := handler(ctx, danmaku); err != nil {
			return err
		}
		if err := b.reader.CommitMessages(ctx, kafkaMessage); err != nil {
			return err
		}
	}
}

// Close 释放 Kafka Writer 和 Reader 持有的网络资源。
// Consume 应由调用方先取消 context，再调用 Close，让正在阻塞的读取自然退出。
func (b *KafkaBus) Close() error {
	if b == nil {
		return nil
	}

	b.closeOnce.Do(func() {
		writerErr := b.writer.Close()
		readerErr := b.reader.Close()
		b.closeErr = errors.Join(writerErr, readerErr)
	})
	return b.closeErr
}

// Compile-time check: KafkaBus 与 InMemoryBus 共享同一消息总线接口。
var _ Bus = (*KafkaBus)(nil)
