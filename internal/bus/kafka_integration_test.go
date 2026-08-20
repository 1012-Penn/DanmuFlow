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
	"go.uber.org/zap/zaptest"
)

func TestKafkaBusEndToEndPreservesRoomOrder(t *testing.T) {
	config := newKafkaIntegrationConfig(t, 3)

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

func TestKafkaBusTracksExclusiveOwnershipAcrossRebalance(t *testing.T) {
	config := newKafkaIntegrationConfig(t, 3)
	firstConfig := config
	firstConfig.Logger = zaptest.NewLogger(t).Named("first")
	first, err := NewKafka(firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	secondConfig := config
	secondConfig.Logger = zaptest.NewLogger(t).Named("second")
	second, err := NewKafka(secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	testContext, cancelTest := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTest()
	firstContext, cancelFirst := context.WithCancel(testContext)
	secondContext, cancelSecond := context.WithCancel(testContext)
	defer cancelFirst()
	defer cancelSecond()

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- first.Consume(firstContext, func(context.Context, message.Danmaku) error { return nil })
	}()
	go func() {
		secondDone <- second.Consume(secondContext, func(context.Context, message.Danmaku) error { return nil })
	}()

	waitForIntegrationCondition(t, 15*time.Second, "two consumers to own all partitions exclusively", func() bool {
		firstAssignments := first.AssignedPartitions()
		secondAssignments := second.AssignedPartitions()
		if !first.ConsumerReady() || !second.ConsumerReady() || len(firstAssignments) == 0 || len(secondAssignments) == 0 {
			return false
		}
		seen := make(map[int]struct{}, 3)
		for _, partition := range append(firstAssignments, secondAssignments...) {
			if _, duplicate := seen[partition]; duplicate {
				return false
			}
			seen[partition] = struct{}{}
		}
		return len(seen) == 3
	})

	roomsByPartition := make(map[int]string, 3)
	for candidate := 0; candidate < 10_000 && len(roomsByPartition) < 3; candidate++ {
		roomID := fmt.Sprintf("ownership-room-%d", candidate)
		firstPartition, firstOK := first.PartitionForRoom(roomID)
		secondPartition, secondOK := second.PartitionForRoom(roomID)
		if !firstOK || !secondOK || firstPartition != secondPartition {
			t.Fatalf("room %q partition differs between consumers: first=(%d,%t) second=(%d,%t)",
				roomID, firstPartition, firstOK, secondPartition, secondOK)
		}
		roomsByPartition[firstPartition] = roomID
	}
	if len(roomsByPartition) != 3 {
		t.Fatalf("found rooms for %d partitions, want 3", len(roomsByPartition))
	}
	for partition, roomID := range roomsByPartition {
		ownerCount := 0
		if first.OwnsRoom(roomID) {
			ownerCount++
		}
		if second.OwnsRoom(roomID) {
			ownerCount++
		}
		if ownerCount != 1 {
			t.Fatalf("partition %d room %q owner count = %d, want 1", partition, roomID, ownerCount)
		}
	}

	// 一个消费者退出后，它必须先清空旧 generation；存活消费者随后通过
	// Kafka rebalance 接管全部分区，不能出现旧 owner 继续宣称归属的双主状态。
	cancelFirst()
	requireIntegrationConsumerStop(t, firstDone)
	waitForIntegrationConditionOrConsumerStop(t, 15*time.Second, "remaining consumer to take all partitions", secondDone, func() bool {
		return len(first.AssignedPartitions()) == 0 && second.ConsumerReady() && len(second.AssignedPartitions()) == 3
	})
	for _, roomID := range roomsByPartition {
		if first.OwnsRoom(roomID) || !second.OwnsRoom(roomID) {
			t.Fatalf("room %q ownership did not move exclusively to the remaining consumer", roomID)
		}
	}

	cancelSecond()
	requireIntegrationConsumerStop(t, secondDone)
	if len(second.AssignedPartitions()) != 0 {
		t.Fatalf("stopped consumer assignments = %v, want empty", second.AssignedPartitions())
	}
}

func newKafkaIntegrationConfig(t *testing.T, partitionCount int) KafkaConfig {
	t.Helper()
	brokers := os.Getenv("DANMUFLOW_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set DANMUFLOW_KAFKA_BROKERS to run the Kafka integration test")
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	config := KafkaConfig{
		Brokers: strings.Split(brokers, ","),
		Topic:   "danmuflow-integration-" + suffix,
		GroupID: "danmuflow-integration-" + suffix,
	}
	connection, err := kafka.Dial("tcp", config.Brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.CreateTopics(kafka.TopicConfig{
		Topic:             config.Topic,
		NumPartitions:     partitionCount,
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
	if err := waitForTopicLeaders(metadataContext, config.Brokers[0], config.Topic, partitionCount); err != nil {
		t.Fatal(err)
	}
	return config
}

func waitForIntegrationCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForIntegrationConditionOrConsumerStop(t *testing.T, timeout time.Duration, description string, consumerDone <-chan error, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		select {
		case err := <-consumerDone:
			t.Fatalf("consumer stopped while waiting for %s: %v", description, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func requireIntegrationConsumerStop(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume() error = %v, want context cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
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
