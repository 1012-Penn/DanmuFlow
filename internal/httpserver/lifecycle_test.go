package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// unavailableBus 模拟“进程仍活着，但 Kafka 消费或生产链已不可用”。
// 它只用于验证网关不会把这种状态错误暴露成健康服务。
type unavailableBus struct{}

func (unavailableBus) Publish(context.Context, message.Danmaku) error {
	return errors.New("unavailable")
}

func (unavailableBus) Consume(ctx context.Context, _ bus.Handler) error {
	<-ctx.Done()
	return ctx.Err()
}

func (unavailableBus) ConsumerReady() bool         { return false }
func (unavailableBus) Check(context.Context) error { return errors.New("unavailable") }

var _ bus.Bus = unavailableBus{}
var _ bus.Readiness = unavailableBus{}

// controlledOwnershipBus 让测试主动推进 Kafka 所有权版本，而不依赖真实 broker。
// 内存消息链路仍保持可用，因此测试只观察 HTTP 连接生命周期。
type controlledOwnershipBus struct {
	*bus.InMemoryBus
	owned    atomic.Bool
	revision atomic.Uint64
}

func newControlledOwnershipBus(t *testing.T, owned bool) *controlledOwnershipBus {
	t.Helper()
	memoryBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		t.Fatal(err)
	}
	messageBus := &controlledOwnershipBus{InMemoryBus: memoryBus}
	messageBus.owned.Store(owned)
	messageBus.revision.Store(1)
	return messageBus
}

func (b *controlledOwnershipBus) PartitionForRoom(string) (int, bool) { return 0, true }
func (b *controlledOwnershipBus) OwnsRoom(string) bool                { return b.owned.Load() }
func (b *controlledOwnershipBus) AssignedPartitions() []int {
	if b.owned.Load() {
		return []int{0}
	}
	return nil
}
func (b *controlledOwnershipBus) OwnershipRevision() uint64 { return b.revision.Load() }
func (b *controlledOwnershipBus) setOwned(owned bool) {
	b.owned.Store(owned)
	b.revision.Add(1)
}

// handshakeRaceBus 在第一次房间校验时推进版本，但仍报告本机拥有该分区，稳定复现
// “握手期间失去后又拿回相同分区”的 generation 竞态窗口。
type handshakeRaceBus struct {
	*bus.InMemoryBus
	transitioned atomic.Bool
	revision     atomic.Uint64
}

func newHandshakeRaceBus(t *testing.T) *handshakeRaceBus {
	t.Helper()
	memoryBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		t.Fatal(err)
	}
	messageBus := &handshakeRaceBus{InMemoryBus: memoryBus}
	messageBus.revision.Store(1)
	return messageBus
}

func (b *handshakeRaceBus) PartitionForRoom(string) (int, bool) { return 0, true }
func (b *handshakeRaceBus) OwnsRoom(string) bool {
	if b.transitioned.CompareAndSwap(false, true) {
		b.revision.Add(1)
	}
	return true
}
func (b *handshakeRaceBus) AssignedPartitions() []int { return []int{0} }
func (b *handshakeRaceBus) OwnershipRevision() uint64 { return b.revision.Load() }

func newServerWithOwnershipBus(messageBus bus.Bus) *Server {
	observability := metrics.New()
	rooms := room.NewRegistry()
	return newServerWithBus(":0", rooms, messageBus,
		startMessageBusConsumer(rooms, messageBus, observability, zap.NewNop()), observability, zap.NewNop())
}

func websocketTestURL(httpURL, query string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + "/ws?" + query
}

func TestHealthzAndReadyzDescribeDifferentStates(t *testing.T) {
	messageBus := unavailableBus{}
	observability := metrics.New()
	rooms := room.NewRegistry()
	server := newServerWithBus(":0", rooms, messageBus,
		startMessageBusConsumer(rooms, messageBus, observability, zap.NewNop()), observability, zap.NewNop())
	defer server.Shutdown(context.Background())

	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusOK},
		{path: "/readyz", want: http.StatusServiceUnavailable},
		{path: "/ws?room_id=room-a&user_id=alice", want: http.StatusServiceUnavailable},
	} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		if recorder.Code != test.want {
			t.Fatalf("%s status = %d, want %d", test.path, recorder.Code, test.want)
		}
	}
}

func TestShutdownNotifiesWebSocketClientToReconnect(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}
	defer listener.Close()

	server := newTestServer()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	wsURL := "ws://" + listener.Addr().String() + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	closeError, ok := err.(*websocket.CloseError)
	if !ok || closeError.Code != websocket.CloseServiceRestart {
		t.Fatalf("close error = %v, want WebSocket close code %d", err, websocket.CloseServiceRestart)
	}

	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve() error = %v, want %v", err, http.ErrServerClosed)
	}
}

func TestWebSocketRejectsRoomOwnedByAnotherInstance(t *testing.T) {
	messageBus := newControlledOwnershipBus(t, false)
	server := newServerWithOwnershipBus(messageBus)
	httpServer := httptest.NewServer(server.Handler)
	t.Cleanup(func() {
		httpServer.Close()
		_ = server.Shutdown(context.Background())
	})

	conn, response, err := websocket.DefaultDialer.Dial(
		websocketTestURL(httpServer.URL, "room_id=room-a&user_id=alice"), nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("WebSocket dial succeeded on an instance that does not own the room")
	}
	if response == nil || response.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("handshake response = %v, want HTTP %d", response, http.StatusMisdirectedRequest)
	}
	response.Body.Close()
}

func TestOwnershipGenerationChangeClosesExistingWebSocketEvenWhenPartitionReturns(t *testing.T) {
	messageBus := newControlledOwnershipBus(t, true)
	server := newServerWithOwnershipBus(messageBus)
	httpServer := httptest.NewServer(server.Handler)
	t.Cleanup(func() {
		httpServer.Close()
		_ = server.Shutdown(context.Background())
	})

	conn, _, err := websocket.DefaultDialer.Dial(
		websocketTestURL(httpServer.URL, "room_id=room-a&user_id=alice"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	messageBus.setOwned(false)
	// 在监视器下一次轮询前拿回同一分区，模拟一次很短的再均衡窗口。
	// 连接仍必须断开，因为旧 generation 期间可能已经漏过消息。
	messageBus.setOwned(true)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	closeError, ok := err.(*websocket.CloseError)
	if !ok || closeError.Code != websocket.CloseServiceRestart || closeError.Text != "room ownership changed" {
		t.Fatalf("close error = %v, want WebSocket 1012 ownership change", err)
	}
}

func TestOwnershipIsRecheckedAfterWebSocketRegistration(t *testing.T) {
	messageBus := newHandshakeRaceBus(t)
	server := newServerWithOwnershipBus(messageBus)
	httpServer := httptest.NewServer(server.Handler)
	t.Cleanup(func() {
		httpServer.Close()
		_ = server.Shutdown(context.Background())
	})

	conn, _, err := websocket.DefaultDialer.Dial(
		websocketTestURL(httpServer.URL, "room_id=room-a&user_id=alice"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	closeError, ok := err.(*websocket.CloseError)
	if !ok || closeError.Code != websocket.CloseServiceRestart || closeError.Text != "room ownership changed" {
		t.Fatalf("close error = %v, want WebSocket 1012 ownership change", err)
	}
}

func TestConnectionRegisteredAfterShutdownSnapshotIsRejected(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}
	defer listener.Close()

	server := newTestServer()
	// 模拟发布下线已经取得空连接快照，但某个请求此前已经通过 readiness 检查、
	// 尚在完成 WebSocket Upgrade 的窗口。draining 保持 false，让测试稳定进入
	// add 的迟到注册分支，而不依赖难以控制的真实调度时序。
	server.connections.closeForServiceRestart(context.Background())

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	wsURL := "ws://" + listener.Addr().String() + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	closeError, ok := err.(*websocket.CloseError)
	if !ok || closeError.Code != websocket.CloseServiceRestart {
		t.Fatalf("close error = %v, want WebSocket close code %d", err, websocket.CloseServiceRestart)
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve() error = %v, want %v", err, http.ErrServerClosed)
	}
}
