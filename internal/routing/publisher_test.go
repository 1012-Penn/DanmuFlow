package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeOwnership struct {
	mu         sync.RWMutex
	partitions []int
	revision   atomic.Uint64
}

func (ownership *fakeOwnership) PartitionForRoom(string) (int, bool) { return 0, true }
func (ownership *fakeOwnership) OwnsRoom(string) bool                { return true }
func (ownership *fakeOwnership) AssignedPartitions() []int {
	ownership.mu.RLock()
	defer ownership.mu.RUnlock()
	return append([]int(nil), ownership.partitions...)
}
func (ownership *fakeOwnership) OwnershipRevision() uint64 { return ownership.revision.Load() }
func (ownership *fakeOwnership) assign(partitions ...int) {
	ownership.mu.Lock()
	ownership.partitions = append([]int(nil), partitions...)
	ownership.mu.Unlock()
	ownership.revision.Add(1)
}

type fakeRegistry struct {
	mu     sync.Mutex
	leases map[int]Lease
}

func newFakeRegistry() *fakeRegistry { return &fakeRegistry{leases: make(map[int]Lease)} }
func (registry *fakeRegistry) Register(_ context.Context, partition int, lease Lease, _ time.Duration) error {
	registry.mu.Lock()
	registry.leases[partition] = lease
	registry.mu.Unlock()
	return nil
}
func (registry *fakeRegistry) Resolve(_ context.Context, partition int) (Lease, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	lease, ok := registry.leases[partition]
	if !ok {
		return Lease{}, ErrRouteNotFound
	}
	return lease, nil
}
func (registry *fakeRegistry) Release(_ context.Context, partition int, token string) error {
	registry.mu.Lock()
	if lease, ok := registry.leases[partition]; ok && lease.Token == token {
		delete(registry.leases, partition)
	}
	registry.mu.Unlock()
	return nil
}
func (*fakeRegistry) Close() error { return nil }

func TestPublisherFollowsOwnershipAndReleasesOnStop(t *testing.T) {
	registry := newFakeRegistry()
	ownership := &fakeOwnership{}
	ownership.assign(0, 2)
	publisher, err := StartPublisher(registry, ownership, PublisherConfig{
		GatewayID:    "gateway-a",
		WebSocketURL: "wss://a.example/ws",
		LeaseTTL:     300 * time.Millisecond,
		PollInterval: 10 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForRoute(t, registry, 0, true)
	waitForRoute(t, registry, 2, true)
	ownership.assign(1)
	waitForRoute(t, registry, 0, false)
	waitForRoute(t, registry, 1, true)
	waitForRoute(t, registry, 2, false)

	stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := publisher.Stop(stopContext); err != nil {
		t.Fatal(err)
	}
	waitForRoute(t, registry, 1, false)
}

func TestLeaseRequiresSafeWebSocketURL(t *testing.T) {
	valid := Lease{GatewayID: "gateway-a", WebSocketURL: "wss://gateway.example/ws", Token: "token"}
	if !validLease(valid) {
		t.Fatal("valid lease was rejected")
	}
	for _, unsafeURL := range []string{
		"https://gateway.example/ws",
		"wss://user:password@gateway.example/ws",
		"wss://gateway.example/ws?token=embedded",
		"javascript:alert(1)",
	} {
		lease := valid
		lease.WebSocketURL = unsafeURL
		if validLease(lease) {
			t.Fatalf("unsafe WebSocket URL %q was accepted", unsafeURL)
		}
	}
}

func waitForRoute(t *testing.T, registry *fakeRegistry, partition int, present bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, err := registry.Resolve(context.Background(), partition)
		if (err == nil) == present {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("partition %d presence did not become %t", partition, present)
}
