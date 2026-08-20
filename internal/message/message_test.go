package message

import (
	"errors"
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

func TestDanmakuValidateRejectsMissingRequiredFields(t *testing.T) {
	valid := Danmaku{
		MessageID: "msg-1",
		RoomID:    "room-a",
		UserID:    "alice",
		Content:   "hello",
		CreatedAt: time.Now(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid Danmaku.Validate() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Danmaku)
		want   error
	}{
		{name: "message ID", mutate: func(msg *Danmaku) { msg.MessageID = " " }, want: ErrEmptyMessageID},
		{name: "room ID", mutate: func(msg *Danmaku) { msg.RoomID = " " }, want: ErrEmptyRoomID},
		{name: "user ID", mutate: func(msg *Danmaku) { msg.UserID = " " }, want: ErrEmptyUserID},
		{name: "content", mutate: func(msg *Danmaku) { msg.Content = "\t" }, want: ErrEmptyContent},
		{name: "created at", mutate: func(msg *Danmaku) { msg.CreatedAt = time.Time{} }, want: ErrEmptyCreatedAt},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := valid
			test.mutate(&msg)
			if err := msg.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Danmaku.Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}
