package httpserver

import (
	"context"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"go.uber.org/zap"
)

func TestInMemoryMessageBusRoutesMessageToRoom(t *testing.T) {
	rooms := room.NewRegistry()
	chatRoom, err := rooms.GetOrCreate("room-a")
	if err != nil {
		t.Fatal(err)
	}
	client, err := chatRoom.Join("alice")
	if err != nil {
		t.Fatal(err)
	}

	messageBus, cancelConsumer, err := newInMemoryMessageBus(rooms, metrics.New(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer cancelConsumer()

	if err := messageBus.Publish(context.Background(), message.Danmaku{
		MessageID: "msg-1",
		RoomID:    "room-a",
		UserID:    "alice",
		Content:   "hello through bus",
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-client.Messages:
		if received.Sequence != 1 || received.Content != "hello through bus" {
			t.Fatalf("received = %+v", received)
		}
	case <-time.After(time.Second):
		t.Fatal("room did not receive message from in-memory bus")
	}
}
