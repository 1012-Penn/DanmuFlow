package routing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"go.uber.org/zap"
)

const publisherTokenBytes = 16

// PublisherConfig 描述当前网关发布到路由目录的身份和租约时长。
type PublisherConfig struct {
	GatewayID    string
	WebSocketURL string
	LeaseTTL     time.Duration
	PollInterval time.Duration
}

// Publisher 根据 Kafka assignments 注册并续租当前实例拥有的分区。
type Publisher struct {
	registry  Registry
	ownership bus.RoomOwnership
	lease     Lease
	config    PublisherConfig
	logger    *zap.Logger

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// StartPublisher 启动路由租约循环。稳定期只按 TTL 续租；所有权版本变化会立即重建租约集合。
func StartPublisher(registry Registry, ownership bus.RoomOwnership, config PublisherConfig, logger *zap.Logger) (*Publisher, error) {
	if registry == nil || ownership == nil || config.GatewayID == "" || config.WebSocketURL == "" ||
		config.LeaseTTL <= 0 || config.PollInterval <= 0 || config.PollInterval >= config.LeaseTTL {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	token, err := newPublisherToken()
	if err != nil {
		return nil, fmt.Errorf("create routing publisher token: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	publisher := &Publisher{
		registry:  registry,
		ownership: ownership,
		lease: Lease{
			GatewayID:    config.GatewayID,
			WebSocketURL: config.WebSocketURL,
			Token:        token,
		},
		config: config,
		logger: logger,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go publisher.run(ctx)
	return publisher, nil
}

func (publisher *Publisher) run(ctx context.Context) {
	defer close(publisher.done)
	ticker := time.NewTicker(publisher.config.PollInterval)
	defer ticker.Stop()

	lastRevision := ^uint64(0)
	lastRefresh := time.Time{}
	published := make(map[int]struct{})
	for {
		now := time.Now()
		revision := publisher.ownership.OwnershipRevision()
		refreshDue := now.Sub(lastRefresh) >= publisher.config.LeaseTTL/3
		if revision != lastRevision || refreshDue {
			published = publisher.reconcile(ctx, published)
			lastRevision = revision
			lastRefresh = now
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			publisher.releaseAll(context.Background(), published)
			return
		}
	}
}

func (publisher *Publisher) reconcile(ctx context.Context, published map[int]struct{}) map[int]struct{} {
	assigned := publisher.ownership.AssignedPartitions()
	next := make(map[int]struct{}, len(assigned))
	for _, partition := range assigned {
		next[partition] = struct{}{}
		if err := publisher.registry.Register(ctx, partition, publisher.lease, publisher.config.LeaseTTL); err != nil {
			publisher.logger.Warn("gateway_route_register_failed", zap.Int("partition", partition), zap.Error(err))
			continue
		}
	}
	for partition := range published {
		if _, retained := next[partition]; retained {
			continue
		}
		if err := publisher.registry.Release(ctx, partition, publisher.lease.Token); err != nil {
			publisher.logger.Warn("gateway_route_release_failed", zap.Int("partition", partition), zap.Error(err))
		}
	}
	publisher.logger.Info("gateway_routes_reconciled",
		zap.String("gateway_id", publisher.lease.GatewayID),
		zap.Ints("assigned_partitions", assigned),
	)
	return next
}

func (publisher *Publisher) releaseAll(ctx context.Context, published map[int]struct{}) {
	releaseContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for partition := range published {
		if err := publisher.registry.Release(releaseContext, partition, publisher.lease.Token); err != nil && !errors.Is(err, context.Canceled) {
			publisher.logger.Warn("gateway_route_release_failed", zap.Int("partition", partition), zap.Error(err))
		}
	}
}

// Stop 停止续租、条件删除当前实例发布的租约，并等待后台 goroutine 退出。
func (publisher *Publisher) Stop(ctx context.Context) error {
	if publisher == nil {
		return nil
	}
	publisher.once.Do(publisher.cancel)
	select {
	case <-publisher.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newPublisherToken() (string, error) {
	var token [publisherTokenBytes]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}
