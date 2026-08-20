//go:build integration

package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/segmentio/kafka-go"
)

func TestKafkaBusEndToEndPreservesRoomOrder(t *testing.T) {
	brokers := os.Getenv("DANMUFLOW_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set DANMUFLOW_KAFKA_BROKERS to run the Kafka integration test")
	}

	config := KafkaConfig{
		Brokers: strings.Split(brokers, ","),
		Topic:   "danmuflow-integration-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		GroupID: "danmuflow-integration-" + strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	connection, err := kafka.Dial("tcp", config.Brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.CreateTopics(kafka.TopicConfig{
		Topic:             config.Topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	metadataContext, cancelMetadata := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetadata()
	if err := waitForTopicLeaders(metadataContext, config.Brokers[0], config.Topic, 3); err != nil {
		t.Fatal(err)
	}

	messageBus, err := NewKafka(config)
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	consumeContext, cancelConsume := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelConsume()

	received := make(chan message.Danmaku, 3)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- messageBus.Consume(consumeContext, func(_ context.Context, msg message.Danmaku) error {
			received <- msg
			return nil
		})
	}()
	readyDeadline := time.Now().Add(10 * time.Second)
	for !messageBus.ConsumerReady() {
		if time.Now().After(readyDeadline) {
			t.Fatal("consumer did not become ready after joining the Kafka consumer group")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 语法损坏和缺少业务字段的 JSON 都是永久性毒消息。它们与后面的合法消息
	// 使用相同 key，确保进入同一分区，从而验证消费者会提交并越过这两个 offset。
	poisonWriter := &kafka.Writer{
		Addr:     kafka.TCP(config.Brokers...),
		Topic:    config.Topic,
		Balancer: &kafka.Hash{},
	}
	poisonWriteContext, cancelPoisonWrite := context.WithTimeout(consumeContext, 5*time.Second)
	defer cancelPoisonWrite()
	if err := writeWhenTopicReady(poisonWriteContext, poisonWriter,
		kafka.Message{Key: []byte("room-order"), Value: []byte("not-json")},
		kafka.Message{Key: []byte("room-order"), Value: []byte(`{"message_id":"poison","room_id":"room-order"}`)},
	); err != nil {
		_ = poisonWriter.Close()
		t.Fatal(err)
	}
	if err := poisonWriter.Close(); err != nil {
		t.Fatal(err)
	}

	for sequence := uint64(1); sequence <= 3; sequence++ {
		if err := messageBus.Publish(consumeContext, message.Danmaku{
			MessageID: fmt.Sprintf("integration-%d", sequence),
			RoomID:    "room-order",
			UserID:    "integration-user",
			Content:   fmt.Sprintf("message-%d", sequence),
			Sequence:  sequence,
			CreatedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for sequence := uint64(1); sequence <= 3; sequence++ {
		select {
		case msg := <-received:
			if msg.RoomID != "room-order" || msg.Sequence != sequence || msg.Content != fmt.Sprintf("message-%d", sequence) {
				t.Fatalf("received message %d = %+v", sequence, msg)
			}
		case <-consumeContext.Done():
			t.Fatalf("timed out waiting for message %d: %v", sequence, consumeContext.Err())
		}
	}

	cancelConsume()
	if err := <-consumeDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume error = %v", err)
	}
}

func writeWhenTopicReady(ctx context.Context, writer *kafka.Writer, messages ...kafka.Message) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := writer.WriteMessages(ctx, messages...)
		if err == nil {
			return nil
		}
		if !errors.Is(err, kafka.UnknownTopicOrPartition) {
			return err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for topic data plane: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForTopicLeaders(ctx context.Context, broker, topic string, wantPartitions int) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		partitions, err := kafka.LookupPartitions(ctx, "tcp", broker, topic)
		if err == nil && len(partitions) == wantPartitions {
			allReady := true
			for _, partition := range partitions {
				if partition.Error != nil || partition.Leader.Host == "" {
					allReady = false
					break
				}
			}
			if allReady {
				return nil
			}
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %d ready partitions for topic %q: %w (last metadata error: %v)", wantPartitions, topic, ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}
