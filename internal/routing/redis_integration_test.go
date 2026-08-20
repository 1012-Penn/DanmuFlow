//go:build integration

package routing

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestRedisRegistryLeaseLifecycle(t *testing.T) {
	address := os.Getenv("DANMUFLOW_REDIS_ADDR")
	if address == "" {
		t.Skip("set DANMUFLOW_REDIS_ADDR to run the Redis integration test")
	}
	registry, err := NewRedisRegistry(RedisConfig{
		Address:   address,
		KeyPrefix: "danmuflow:integration:" + time.Now().Format("150405.000000000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()

	ctx := context.Background()
	first := Lease{GatewayID: "gateway-a", WebSocketURL: "wss://a.example/ws", Token: "token-a"}
	second := Lease{GatewayID: "gateway-b", WebSocketURL: "wss://b.example/ws", Token: "token-b"}
	if err := registry.Register(ctx, 2, first, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, 2, second, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := registry.Release(ctx, 2, first.Token); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.Resolve(ctx, 2); err != nil || got != second {
		t.Fatalf("route after stale release = (%+v, %v), want second lease", got, err)
	}
	if err := registry.Release(ctx, 2, second.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(ctx, 2); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("Resolve() after owner release error = %v, want %v", err, ErrRouteNotFound)
	}

	if err := registry.Register(ctx, 3, first, 80*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if _, err := registry.Resolve(ctx, 3); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("Resolve() after TTL error = %v, want %v", err, ErrRouteNotFound)
	}
}

func BenchmarkRedisRegistryResolve(b *testing.B) {
	address := os.Getenv("DANMUFLOW_REDIS_ADDR")
	if address == "" {
		b.Skip("set DANMUFLOW_REDIS_ADDR to run the Redis integration benchmark")
	}
	registry, err := NewRedisRegistry(RedisConfig{
		Address:   address,
		KeyPrefix: "danmuflow:benchmark:" + time.Now().Format("150405.000000000"),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = registry.Close() })
	lease := Lease{GatewayID: "gateway-a", WebSocketURL: "wss://a.example/ws", Token: "benchmark-token"}
	if err := registry.Register(context.Background(), 1, lease, time.Minute); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = registry.Release(context.Background(), 1, lease.Token) })

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := registry.Resolve(context.Background(), 1); err != nil {
			b.Fatal(err)
		}
	}
}
