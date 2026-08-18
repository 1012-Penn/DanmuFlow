package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/message"
)

func TestInMemoryBusConsumesMessagesInPublishOrder(t *testing.T) {
	b, err := NewInMemory(2)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumeDone := make(chan error, 1)
	received := make([]string, 0, 2)
	go func() {
		consumeDone <- b.Consume(ctx, func(_ context.Context, msg message.Danmaku) error {
			received = append(received, msg.MessageID)
			if len(received) == 2 {
				cancel()
			}
			return nil
		})
	}()

	for _, id := range []string{"msg-1", "msg-2"} {
		if err := b.Publish(context.Background(), message.Danmaku{MessageID: id}); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-consumeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume() error = %v, want context canceled", err)
	}
	if want := []string{"msg-1", "msg-2"}; !reflect.DeepEqual(received, want) {
		t.Fatalf("received = %v, want %v", received, want)
	}
}

func TestInMemoryBusPublishStopsWhenFullQueueContextIsCanceled(t *testing.T) {
	b, err := NewInMemory(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), message.Danmaku{MessageID: "msg-1"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Publish(ctx, message.Danmaku{MessageID: "msg-2"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context canceled", err)
	}
}

func TestInMemoryBusReturnsHandlerError(t *testing.T) {
	b, err := NewInMemory(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), message.Danmaku{MessageID: "msg-1"}); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("handler failed")
	if err := b.Consume(context.Background(), func(context.Context, message.Danmaku) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Consume() error = %v, want %v", err, wantErr)
	}
}

func TestInMemoryBusAllowsOnlyOneConsumer(t *testing.T) {
	b, err := NewInMemory(1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- b.Consume(ctx, func(context.Context, message.Danmaku) error {
			close(handlerStarted)
			<-releaseHandler
			return nil
		})
	}()

	if err := b.Publish(context.Background(), message.Danmaku{MessageID: "msg-1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("first consumer did not receive the message")
	}

	if err := b.Consume(context.Background(), func(context.Context, message.Danmaku) error {
		return nil
	}); !errors.Is(err, ErrConsumerAlreadyRunning) {
		t.Fatalf("second Consume() error = %v, want %v", err, ErrConsumerAlreadyRunning)
	}

	close(releaseHandler)
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Consume() error = %v, want context canceled", err)
	}
}

func TestInMemoryBusRejectsInvalidArguments(t *testing.T) {
	if _, err := NewInMemory(0); !errors.Is(err, ErrInvalidBuffer) {
		t.Fatalf("NewInMemory() error = %v, want %v", err, ErrInvalidBuffer)
	}

	b, err := NewInMemory(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Consume(context.Background(), nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrNilHandler)
	}
}
