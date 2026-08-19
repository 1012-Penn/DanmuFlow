package httpserver

import (
	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"go.uber.org/zap"
)

// newTestServer 为 HTTP/WebSocket 单元测试注入 InMemoryBus，
// 让测试只验证网关和房间行为，不依赖开发机上是否运行 Kafka。
func newTestServer() *Server {
	messageBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		panic(err)
	}

	rooms := room.NewRegistry()
	observability := metrics.New()
	return newServerWithBus(":0", rooms, messageBus, startMessageBusConsumer(rooms, messageBus, observability, zap.NewNop()), observability, zap.NewNop())
}
