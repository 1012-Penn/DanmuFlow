package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/1012-Penn/DanmuFlow/internal/routing"
	"go.uber.org/zap"
)

type routeRegistryStub struct {
	mu     sync.Mutex
	leases map[int]routing.Lease
	err    error
}

func (registry *routeRegistryStub) Register(_ context.Context, partition int, lease routing.Lease, _ time.Duration) error {
	registry.mu.Lock()
	registry.leases[partition] = lease
	registry.mu.Unlock()
	return nil
}
func (registry *routeRegistryStub) Resolve(_ context.Context, partition int) (routing.Lease, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.err != nil {
		return routing.Lease{}, registry.err
	}
	lease, ok := registry.leases[partition]
	if !ok {
		return routing.Lease{}, routing.ErrRouteNotFound
	}
	return lease, nil
}
func (registry *routeRegistryStub) Release(_ context.Context, partition int, token string) error {
	registry.mu.Lock()
	if lease, ok := registry.leases[partition]; ok && lease.Token == token {
		delete(registry.leases, partition)
	}
	registry.mu.Unlock()
	return nil
}
func (*routeRegistryStub) Close() error { return nil }

func TestRouteReturnsGatewayForRoomPartition(t *testing.T) {
	messageBus := newControlledOwnershipBus(t, true)
	registry := &routeRegistryStub{leases: map[int]routing.Lease{
		0: {GatewayID: "gateway-a", WebSocketURL: "wss://gateway-a.example/ws", Token: "secret-token"},
	}}
	observability := metrics.New()
	rooms := room.NewRegistry()
	server := newServerWithBusAndRouting(":0", rooms, messageBus,
		startMessageBusConsumer(rooms, messageBus, observability, zap.NewNop()), observability, registry, nil, zap.NewNop())
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	recorder := httptest.NewRecorder()
	server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/route?room_id=room-a", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response struct {
		RoomID       string `json:"room_id"`
		Partition    int    `json:"partition"`
		GatewayID    string `json:"gateway_id"`
		WebSocketURL string `json:"websocket_url"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.RoomID != "room-a" || response.Partition != 0 || response.GatewayID != "gateway-a" || response.WebSocketURL != "wss://gateway-a.example/ws" {
		t.Fatalf("route response = %+v", response)
	}
	if strings.Contains(recorder.Body.String(), "secret-token") {
		t.Fatal("internal lease token leaked to route response")
	}
}

func TestRouteReturnsServiceUnavailableWithoutLease(t *testing.T) {
	messageBus := newControlledOwnershipBus(t, true)
	registry := &routeRegistryStub{leases: make(map[int]routing.Lease)}
	observability := metrics.New()
	rooms := room.NewRegistry()
	server := newServerWithBusAndRouting(":0", rooms, messageBus,
		startMessageBusConsumer(rooms, messageBus, observability, zap.NewNop()), observability, registry, nil, zap.NewNop())
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	recorder := httptest.NewRecorder()
	server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/route?room_id=room-a", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}
