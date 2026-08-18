package message

import (
	"testing"
	"time"
)

func TestDanmakuCarriesCrossComponentFields(t *testing.T) {
	createdAt := time.Date(2026, time.August, 18, 23, 0, 0, 0, time.UTC)
	msg := Danmaku{
		MessageID: "msg-1",
		RoomID:    "room-a",
		UserID:    "alice",
		Content:   "hello",
		Sequence:  7,
		CreatedAt: createdAt,
	}

	if msg.MessageID != "msg-1" || msg.RoomID != "room-a" || msg.UserID != "alice" || msg.Content != "hello" || msg.Sequence != 7 || !msg.CreatedAt.Equal(createdAt) {
		t.Fatalf("Danmaku = %+v", msg)
	}
}
