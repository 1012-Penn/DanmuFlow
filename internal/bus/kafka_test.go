package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
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

func TestConsumerGroupTransitionClassification(t *testing.T) {
	for _, transitionErr := range []error{
		kafka.UnknownMemberId,
		kafka.IllegalGeneration,
		kafka.RebalanceInProgress,
		fmt.Errorf("wrapped: %w", kafka.UnknownMemberId),
	} {
		if !isConsumerGroupTransition(transitionErr) {
			t.Fatalf("isConsumerGroupTransition(%v) = false, want true", transitionErr)
		}
	}
	for _, fatalErr := range []error{
		context.DeadlineExceeded,
		kafka.GroupAuthorizationFailed,
		kafka.NotCoordinatorForGroup,
	} {
		if isConsumerGroupTransition(fatalErr) {
			t.Fatalf("isConsumerGroupTransition(%v) = true, want false", fatalErr)
		}
	}
}

func TestKafkaBusRoomOwnershipMatchesProducerPartitioner(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	assignments := []kafka.PartitionAssignment{{ID: 0}, {ID: 2}}
	if err := messageBus.setPartitionOwnership([]int{0, 1, 2}, assignments); err != nil {
		t.Fatal(err)
	}

	for _, roomID := range []string{"room-a", "room-b", "hot-room", "room-1000"} {
		want := (&kafka.Hash{}).Balance(kafka.Message{Key: []byte(roomID)}, 0, 1, 2)
		got, ok := messageBus.PartitionForRoom(roomID)
		if !ok || got != want {
			t.Fatalf("PartitionForRoom(%q) = (%d, %t), want (%d, true)", roomID, got, ok, want)
		}
		wantOwned := want == 0 || want == 2
		if gotOwned := messageBus.OwnsRoom(roomID); gotOwned != wantOwned {
			t.Fatalf("OwnsRoom(%q) = %t, want %t", roomID, gotOwned, wantOwned)
		}
	}

	gotAssignments := messageBus.AssignedPartitions()
	if !reflect.DeepEqual(gotAssignments, []int{0, 2}) {
		t.Fatalf("AssignedPartitions() = %v, want [0 2]", gotAssignments)
	}
	gotAssignments[0] = 99
	if got := messageBus.AssignedPartitions(); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("mutating assignment snapshot changed internal state: %v", got)
	}

	messageBus.clearAssignedPartitions()
	if messageBus.OwnsRoom("room-a") || len(messageBus.AssignedPartitions()) != 0 {
		t.Fatal("cleared generation still reports owned rooms or partitions")
	}
	if _, ok := messageBus.PartitionForRoom("room-a"); !ok {
		t.Fatal("clearing assignments also removed stable topic partition metadata")
	}
}

func TestKafkaBusRejectsAssignmentMissingFromMetadata(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()

	err = messageBus.setPartitionOwnership(
		[]int{0, 1},
		[]kafka.PartitionAssignment{{ID: 2}},
	)
	if err == nil {
		t.Fatal("setPartitionOwnership() error = nil, want inconsistent metadata error")
	}
	if _, ok := messageBus.PartitionForRoom("room-a"); ok {
		t.Fatal("failed ownership update published partial topic metadata")
	}
}

func TestKafkaBusOwnershipIsSafeDuringConcurrentRebalance(t *testing.T) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer messageBus.Close()
	if err := messageBus.setPartitionOwnership(
		[]int{0, 1, 2},
		[]kafka.PartitionAssignment{{ID: 0}},
	); err != nil {
		t.Fatal(err)
	}

	var readers sync.WaitGroup
	for reader := range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for iteration := range 1_000 {
				roomID := fmt.Sprintf("room-%d-%d", reader, iteration)
				_, _ = messageBus.PartitionForRoom(roomID)
				_ = messageBus.OwnsRoom(roomID)
				_ = messageBus.AssignedPartitions()
			}
		}()
	}
	for iteration := range 1_000 {
		assignment := iteration % 3
		if err := messageBus.setPartitionOwnership(
			[]int{0, 1, 2},
			[]kafka.PartitionAssignment{{ID: assignment}},
		); err != nil {
			t.Fatal(err)
		}
		if iteration%10 == 0 {
			messageBus.clearAssignedPartitions()
		}
	}
	readers.Wait()
}

func BenchmarkKafkaBusOwnsRoom(b *testing.B) {
	messageBus, err := NewKafka(KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "danmaku",
		GroupID: "broadcast",
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = messageBus.Close() })
	if err := messageBus.setPartitionOwnership(
		[]int{0, 1, 2},
		[]kafka.PartitionAssignment{{ID: 0}, {ID: 1}},
	); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_ = messageBus.OwnsRoom("benchmark-hot-room")
		}
	})
}
