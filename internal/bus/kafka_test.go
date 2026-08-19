package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/segmentio/kafka-go"
)

func TestNewKafkaRejectsInvalidConfig(t *testing.T) {
	tests := []KafkaConfig{
		{},
		{Brokers: []string{"localhost:9092"}},
		{Brokers: []string{"localhost:9092"}, Topic: "danmaku"},
		{Brokers: []string{"  "}, Topic: "danmaku", GroupID: "broadcast"},
	}

	for _, config := range tests {
		if _, err := NewKafka(config); !errors.Is(err, ErrInvalidKafkaConfig) {
			t.Fatalf("NewKafka(%+v) error = %v, want %v", config, err, ErrInvalidKafkaConfig)
		}
	}
}

func TestNewKafkaUsesLowLatencyWriterConfig(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	if messageBus.writer.BatchTimeout != 10*time.Millisecond {
		t.Fatalf("BatchTimeout = %v, want %v", messageBus.writer.BatchTimeout, 10*time.Millisecond)
	}
	if messageBus.writer.RequiredAcks != kafka.RequireOne {
		t.Fatalf("RequiredAcks = %v, want %v", messageBus.writer.RequiredAcks, kafka.RequireOne)
	}
}

func TestKafkaBusPublishRequiresBroker(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = messageBus.Publish(ctx, message.Danmaku{RoomID: "room-a", Content: "hello"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish error = %v, want context.Canceled", err)
	}
}

func TestKafkaConsumeRejectsNilHandler(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	if err := messageBus.Consume(context.Background(), nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("Consume error = %v, want %v", err, ErrNilHandler)
	}
}
