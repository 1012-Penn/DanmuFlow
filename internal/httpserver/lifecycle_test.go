package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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
