package httpserver

import (
	"net/http"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"go.uber.org/zap"
)

// newTestServer 为 HTTP/WebSocket 单元测试注入 InMemoryBus，
// 让测试只验证网关和房间行为，不依赖开发机上是否运行 Kafka。
func newTestServer() *http.Server {
	messageBus, err := bus.NewInMemory(inMemoryBusBufferSize)
	if err != nil {
		panic(err)
	}

	rooms := room.NewRegistry()
	return newServerWithBus(":0", rooms, messageBus, startMessageBusConsumer(rooms, messageBus, zap.NewNop()), zap.NewNop())
}
