package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	// ErrInvalidKafkaConfig 表示 Kafka 总线缺少启动所需的配置。
	ErrInvalidKafkaConfig = errors.New("invalid kafka bus config")
	// ErrKafkaBusClosed 表示 Kafka 总线已经关闭，不能继续发送消息。
	ErrKafkaBusClosed = errors.New("kafka bus is closed")
)

const (
	consumerGroupJoinTimeout      = 5 * time.Second
	consumerGroupRequestTimeout   = 3 * time.Second
	consumerGroupRebalanceTimeout = 5 * time.Second
	consumerGroupSessionTimeout   = 10 * time.Second
	consumerGroupJoinBackoff      = 100 * time.Millisecond
)

// KafkaConfig 描述 KafkaBus 连接 Kafka 所需的最小配置。
// Brokers、Topic 和 GroupID 分别决定连接地址、消息主题和消费者组身份。
type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
	// Logger 用于记录被安全跳过的损坏 Kafka 消息等消费链路事件。
	Logger *zap.Logger
}

// KafkaBus 使用 Kafka Writer 发布消息，并通过 Kafka 消费者组消费消息。
// 同一个房间的 RoomID 会作为 Kafka key，因此 kafka.Hash 会把同一房间的
// 消息稳定地路由到同一个 partition；同一 partition 内仍保持追加顺序。
type KafkaBus struct {
	writer      *kafka.Writer
	partitioner *kafka.Hash
	config      KafkaConfig
	logger      *zap.Logger

	// ConsumerGroup 每次 Consume 调用独享。发生瞬时错误后，上层监督循环会创建
	// 新的一次 Consume，重新加入组；activeGroup 让 Close 能及时打断正在阻塞的读取。
	groupMu       sync.Mutex
	activeGroup   *kafka.ConsumerGroup
	consumerReady atomic.Bool

	// ownershipMu 让 Topic 完整分区集合与本机 assignments 作为一个快照更新。
	// 重平衡期间读取方要么看到旧 generation，要么看到已清空/新 generation，
	// 不能观察到只更新了一半的归属状态。
	ownershipMu       sync.RWMutex
	topicPartitions   []int
	assignedPartition map[int]struct{}
	ownershipRevision atomic.Uint64

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

	logger := config.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	partitioner := &kafka.Hash{}
	return &KafkaBus{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.Brokers...),
			Topic:    config.Topic,
			Balancer: partitioner,
			// 弹幕优先低延迟；leader 确认写入后即可返回，不等待所有副本。
			// 代价是 leader 在复制完成前故障时，极少量消息可能丢失。
			RequiredAcks: kafka.RequireOne,
			// 低流量时也要尽快发送，不能让默认的 1 秒攒批窗口
			// 与 WebSocket 的 1 秒 Publish context 同时到期。
			BatchTimeout: 10 * time.Millisecond,
		},
		partitioner:       partitioner,
		config:            config,
		logger:            logger,
		assignedPartition: make(map[int]struct{}),
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

// Consume 从 Kafka 消费组读取消息，交给 handler 成功处理后才提交 offset。
// 成功取得 generation 才会把 ConsumerReady 置为 true；这代表实例已完成 Kafka
// 消费组协调，而不是仅仅启动了一个 goroutine。每个分区都有独立读取循环，同一分区
// 内仍严格串行，因此不会破坏房间 key 路由带来的顺序。
func (b *KafkaBus) Consume(ctx context.Context, handler Handler) error {
	if b == nil || b.writer == nil {
		return ErrKafkaBusClosed
	}
	if handler == nil {
		return ErrNilHandler
	}

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:               b.config.GroupID,
		Brokers:          b.config.Brokers,
		Topics:           []string{b.config.Topic},
		Timeout:          consumerGroupRequestTimeout,
		RebalanceTimeout: consumerGroupRebalanceTimeout,
		SessionTimeout:   consumerGroupSessionTimeout,
		JoinGroupBackoff: consumerGroupJoinBackoff,
	})
	if err != nil {
		return err
	}
	b.setActiveGroup(group)
	defer func() {
		b.consumerReady.Store(false)
		b.clearAssignedPartitions()
		b.clearActiveGroup(group)
		_ = group.Close()
	}()

	// Generation 的 context 只会在重平衡时取消；外层关闭时还需要主动关闭
	// ConsumerGroup，才能让分区 Reader 不必等到下一条 Kafka 消息才退出。
	groupStopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = group.Close()
		case <-groupStopped:
		}
	}()
	defer close(groupStopped)

	for {
		b.consumerReady.Store(false)
		b.clearAssignedPartitions()
		// ConsumerGroup.Next 在 Kafka 协调器异常时可能一直等不到下一代。若让它
		// 使用整个服务生命周期的 ctx，就会把上层的指数退避监督循环永久卡住。
		// 每次加入或重加入组最多等待 5 秒；超时后返回给监督循环，由它关闭本次
		// group 并创建新的连接尝试，而不是假装消费者仍在恢复。
		joinContext, cancelJoin := context.WithTimeout(ctx, consumerGroupJoinTimeout)
		generation, err := group.Next(joinContext)
		cancelJoin()
		if err != nil {
			if isConsumerGroupTransition(err) && ctx.Err() == nil {
				// kafka-go 的 ConsumerGroup 内部状态机会在这些 generation 错误后
				// 重新入组。关闭整个 group 反而会丢掉恢复进度，并额外等待一次
				// session/join 周期；保持当前 group，下一轮 Next 获取新 assignment。
				b.logger.Warn("kafka_consumer_group_transition_retrying",
					zap.String("topic", b.config.Topic),
					zap.String("group_id", b.config.GroupID),
					zap.Error(err),
				)
				continue
			}
			return err
		}
		metadataContext, cancelMetadata := context.WithTimeout(ctx, consumerGroupRequestTimeout)
		partitions, err := b.lookupTopicPartitions(metadataContext)
		cancelMetadata()
		if err != nil {
			return err
		}
		if err := b.setPartitionOwnership(partitions, generation.Assignments[b.config.Topic]); err != nil {
			return err
		}

		// 即便当前实例暂未分到 partition（消费者数多于分区数），它也已经是
		// 一个有效成员；Kafka 故障或重平衡会让该状态立即回到 false。
		b.consumerReady.Store(true)
		err = b.consumeGeneration(ctx, generation, handler)
		b.consumerReady.Store(false)
		b.clearAssignedPartitions()
		if err != nil {
			return err
		}
	}
}

func isConsumerGroupTransition(err error) bool {
	return errors.Is(err, kafka.UnknownMemberId) ||
		errors.Is(err, kafka.IllegalGeneration) ||
		errors.Is(err, kafka.RebalanceInProgress)
}

func (b *KafkaBus) lookupTopicPartitions(ctx context.Context) ([]int, error) {
	var lookupErrors []error
	for _, broker := range b.config.Brokers {
		partitions, err := kafka.LookupPartitions(ctx, "tcp", broker, b.config.Topic)
		if err != nil {
			lookupErrors = append(lookupErrors, fmt.Errorf("broker %s: %w", broker, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}

		ids := make([]int, 0, len(partitions))
		for _, partition := range partitions {
			if partition.Error != nil {
				lookupErrors = append(lookupErrors,
					fmt.Errorf("broker %s topic %s partition %d: %w", broker, b.config.Topic, partition.ID, partition.Error))
				ids = nil
				break
			}
			ids = append(ids, partition.ID)
		}
		if len(ids) == 0 {
			if len(partitions) == 0 {
				lookupErrors = append(lookupErrors, fmt.Errorf("broker %s topic %s has no partitions", broker, b.config.Topic))
			}
			continue
		}
		sort.Ints(ids)
		return ids, nil
	}
	return nil, fmt.Errorf("lookup Kafka topic partitions: %w", errors.Join(lookupErrors...))
}

func (b *KafkaBus) setPartitionOwnership(partitions []int, assignments []kafka.PartitionAssignment) error {
	available := make(map[int]struct{}, len(partitions))
	partitionSnapshot := append([]int(nil), partitions...)
	for _, partition := range partitionSnapshot {
		available[partition] = struct{}{}
	}
	assigned := make(map[int]struct{}, len(assignments))
	assignedSnapshot := make([]int, 0, len(assignments))
	for _, assignment := range assignments {
		if _, ok := available[assignment.ID]; !ok {
			return fmt.Errorf("assigned Kafka partition %d is missing from topic metadata", assignment.ID)
		}
		assigned[assignment.ID] = struct{}{}
		assignedSnapshot = append(assignedSnapshot, assignment.ID)
	}
	sort.Ints(assignedSnapshot)

	b.ownershipMu.Lock()
	b.topicPartitions = partitionSnapshot
	b.assignedPartition = assigned
	b.ownershipRevision.Add(1)
	b.ownershipMu.Unlock()
	b.logger.Info("kafka_partition_ownership_updated",
		zap.String("topic", b.config.Topic),
		zap.String("group_id", b.config.GroupID),
		zap.Ints("assigned_partitions", assignedSnapshot),
	)
	return nil
}

func (b *KafkaBus) clearAssignedPartitions() {
	if b == nil {
		return
	}
	b.ownershipMu.Lock()
	hadAssignments := len(b.assignedPartition) > 0
	b.assignedPartition = make(map[int]struct{})
	b.ownershipRevision.Add(1)
	b.ownershipMu.Unlock()
	if hadAssignments {
		b.logger.Info("kafka_partition_ownership_cleared",
			zap.String("topic", b.config.Topic),
			zap.String("group_id", b.config.GroupID),
		)
	}
}

// PartitionForRoom 返回 roomID 按 Kafka Producer 相同规则选择的分区。
func (b *KafkaBus) PartitionForRoom(roomID string) (int, bool) {
	if b == nil || b.partitioner == nil || strings.TrimSpace(roomID) == "" {
		return 0, false
	}
	b.ownershipMu.RLock()
	defer b.ownershipMu.RUnlock()
	return b.partitionForRoomLocked(roomID)
}

// OwnsRoom 报告当前 consumer-group generation 是否把 roomID 的分区分配给本机。
func (b *KafkaBus) OwnsRoom(roomID string) bool {
	if b == nil || b.partitioner == nil || strings.TrimSpace(roomID) == "" {
		return false
	}
	b.ownershipMu.RLock()
	defer b.ownershipMu.RUnlock()
	partition, ok := b.partitionForRoomLocked(roomID)
	if !ok {
		return false
	}
	_, owned := b.assignedPartition[partition]
	return owned
}

// partitionForRoomLocked 要求调用方已经持有 ownershipMu 的读锁或写锁。
func (b *KafkaBus) partitionForRoomLocked(roomID string) (int, bool) {
	if len(b.topicPartitions) == 0 {
		return 0, false
	}
	partition := b.partitioner.Balance(kafka.Message{Key: []byte(roomID)}, b.topicPartitions...)
	for _, available := range b.topicPartitions {
		if partition == available {
			return partition, true
		}
	}
	return 0, false
}

// AssignedPartitions 返回当前 generation 分配给本机的有序副本。
func (b *KafkaBus) AssignedPartitions() []int {
	if b == nil {
		return nil
	}
	b.ownershipMu.RLock()
	partitions := make([]int, 0, len(b.assignedPartition))
	for partition := range b.assignedPartition {
		partitions = append(partitions, partition)
	}
	b.ownershipMu.RUnlock()
	sort.Ints(partitions)
	return partitions
}

// OwnershipRevision 返回所有权状态的单调递增版本。
// 版本在分区分配和清空时都递增，因此“失去后又拿回同一分区”也不会被连接层漏掉。
func (b *KafkaBus) OwnershipRevision() uint64 {
	if b == nil {
		return 0
	}
	return b.ownershipRevision.Load()
}

func (b *KafkaBus) consumeGeneration(ctx context.Context, generation *kafka.Generation, handler Handler) error {
	assignments := generation.Assignments[b.config.Topic]
	var workers sync.WaitGroup
	errs := make(chan error, len(assignments))

	// 没有分区时也要绑定一个等待协程，否则 ConsumerGroup 会立即开始下一代，
	// 造成无意义的重平衡循环。
	if len(assignments) == 0 {
		workers.Add(1)
		generation.Start(func(generationContext context.Context) {
			defer workers.Done()
			<-generationContext.Done()
		})
	} else {
		for _, assignment := range assignments {
			assignment := assignment
			workers.Add(1)
			generation.Start(func(generationContext context.Context) {
				defer workers.Done()
				if err := b.consumePartition(generationContext, generation, assignment, handler); err != nil && !errors.Is(err, kafka.ErrGenerationEnded) {
					select {
					case errs <- err:
					default:
					}
				}
			})
		}
	}

	workers.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	select {
	case err := <-errs:
		return err
	default:
		// 所有 reader 因正常重平衡退出，继续等待 ConsumerGroup 给出下一代。
		return nil
	}
}

func (b *KafkaBus) consumePartition(ctx context.Context, generation *kafka.Generation, assignment kafka.PartitionAssignment, handler Handler) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   b.config.Brokers,
		Topic:     b.config.Topic,
		Partition: assignment.ID,
	})
	defer reader.Close()
	if err := reader.SetOffset(assignment.Offset); err != nil {
		return err
	}

	for {
		kafkaMessage, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		var danmaku message.Danmaku
		if err := json.Unmarshal(kafkaMessage.Value, &danmaku); err != nil {
			// JSON 已经无法恢复；不提交会在重启后无限读到同一条坏消息，进而让
			// 整个消费者永远不可用。因此记录证据、跳过并提交当前 offset。
			b.logger.Warn("malformed_kafka_message_skipped",
				zap.String("topic", kafkaMessage.Topic),
				zap.Int("partition", kafkaMessage.Partition),
				zap.Int64("offset", kafkaMessage.Offset),
				zap.Error(err),
			)
			if err := generation.CommitOffsets(offsetsAfter(kafkaMessage)); err != nil {
				return fmt.Errorf("commit malformed message offset: %w", err)
			}
			continue
		}
		if err := danmaku.Validate(); err != nil {
			// JSON 语法正确但缺少必填业务字段，同样无法靠重试恢复。提交该
			// offset 可隔离毒消息，避免它永久阻塞同一分区后续的合法弹幕。
			b.logger.Warn("invalid_kafka_message_skipped",
				zap.String("topic", kafkaMessage.Topic),
				zap.Int("partition", kafkaMessage.Partition),
				zap.Int64("offset", kafkaMessage.Offset),
				zap.String("message_id", danmaku.MessageID),
				zap.String("room_id", danmaku.RoomID),
				zap.Error(err),
			)
			if err := generation.CommitOffsets(offsetsAfter(kafkaMessage)); err != nil {
				return fmt.Errorf("commit invalid message offset: %w", err)
			}
			continue
		}

		if err := handler(ctx, danmaku); err != nil {
			// 房间广播等暂时性错误不能提交 offset；上层会退避重启，保证消息
			// 至少会重试一次，而不会悄悄丢失。
			return fmt.Errorf("handle kafka message: %w", err)
		}
		if err := generation.CommitOffsets(offsetsAfter(kafkaMessage)); err != nil {
			return fmt.Errorf("commit kafka message offset: %w", err)
		}
	}
}

func offsetsAfter(kafkaMessage kafka.Message) map[string]map[int]int64 {
	return map[string]map[int]int64{
		kafkaMessage.Topic: {kafkaMessage.Partition: kafkaMessage.Offset + 1},
	}
}

// ConsumerReady 报告该实例是否已加入 Kafka 消费组的当前 generation。
func (b *KafkaBus) ConsumerReady() bool {
	return b != nil && b.consumerReady.Load()
}

// Check 使用短生命周期连接确认 Kafka 仍可访问。它刻意不复用消费者连接，避免
// 健康探针卡住正常的消费心跳。
func (b *KafkaBus) Check(ctx context.Context) error {
	if b == nil || len(b.config.Brokers) == 0 {
		return ErrKafkaBusClosed
	}
	conn, err := kafka.DialContext(ctx, "tcp", b.config.Brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func (b *KafkaBus) setActiveGroup(group *kafka.ConsumerGroup) {
	b.groupMu.Lock()
	b.activeGroup = group
	b.groupMu.Unlock()
}

func (b *KafkaBus) clearActiveGroup(group *kafka.ConsumerGroup) {
	b.groupMu.Lock()
	if b.activeGroup == group {
		b.activeGroup = nil
	}
	b.groupMu.Unlock()
}

// Close 释放 Kafka Writer 和 Reader 持有的网络资源。
// Consume 应由调用方先取消 context，再调用 Close，让正在阻塞的读取自然退出。
func (b *KafkaBus) Close() error {
	if b == nil {
		return nil
	}

	b.closeOnce.Do(func() {
		b.consumerReady.Store(false)
		b.clearAssignedPartitions()
		b.groupMu.Lock()
		group := b.activeGroup
		b.groupMu.Unlock()
		var groupErr error
		if group != nil {
			groupErr = group.Close()
		}
		writerErr := b.writer.Close()
		b.closeErr = errors.Join(writerErr, groupErr)
	})
	return b.closeErr
}

// Compile-time check: KafkaBus 与 InMemoryBus 共享同一消息总线接口。
var _ Bus = (*KafkaBus)(nil)
var _ Readiness = (*KafkaBus)(nil)
var _ RoomOwnership = (*KafkaBus)(nil)
