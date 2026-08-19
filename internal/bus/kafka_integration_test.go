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

	// 不可解析的 JSON 是永久性坏消息：消费者应记录、提交并跳过它，随后继续
	// 处理同一个 Topic 上的合法弹幕，而不是反复在同一个 offset 崩溃。
	poisonWriter := &kafka.Writer{Addr: kafka.TCP(config.Brokers...), Topic: config.Topic}
	if err := poisonWriter.WriteMessages(consumeContext, kafka.Message{Value: []byte("not-json")}); err != nil {
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
			Content:   fmt.Sprintf("message-%d", sequence),
			Sequence:  sequence,
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
