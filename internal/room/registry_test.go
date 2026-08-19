package room

import (
	"sync"
	"testing"
)

func TestRegistryReusesRoomForSameID(t *testing.T) {
	registry := NewRegistry()

	first, err := registry.GetOrCreate("room-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.GetOrCreate(" room-a ")
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("same room id returned different Room instances")
	}
}

func TestRegistryKeepsDifferentRoomsIndependent(t *testing.T) {
	registry := NewRegistry()

	roomA, err := registry.GetOrCreate("room-a")
	if err != nil {
		t.Fatal(err)
	}
	roomB, err := registry.GetOrCreate("room-b")
	if err != nil {
		t.Fatal(err)
	}
	if roomA == roomB {
		t.Fatal("different room ids share the same Room instance")
	}

	clientA, err := roomA.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := roomB.Join("bob")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roomA.Publish("hello"); err != nil {
		t.Fatal(err)
	}

	if message := <-clientA.Messages; message.Content != "hello" {
		t.Fatalf("room-a client received %q, want %q", message.Content, "hello")
	}
	select {
	case message := <-clientB.Messages:
		t.Fatalf("room-b client received message: %+v", message)
	default:
	}
}

func TestRegistryIsSafeForConcurrentGetOrCreate(t *testing.T) {
	registry := NewRegistry()
	rooms := make(chan *Room, 32)
	var group sync.WaitGroup

	for i := 0; i < cap(rooms); i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			got, err := registry.GetOrCreate("room-a")
			if err != nil {
				t.Errorf("GetOrCreate error = %v", err)
				return
			}
			rooms <- got
		}()
	}

	group.Wait()
	first := <-rooms
	for i := 1; i < cap(rooms); i++ {
		if got := <-rooms; got != first {
			t.Fatal("concurrent GetOrCreate returned different Room instances")
		}
	}
}

func TestRegistryRejectsEmptyRoomID(t *testing.T) {
	if _, err := NewRegistry().GetOrCreate(" \t"); err != ErrEmptyRoomID {
		t.Fatalf("GetOrCreate error = %v, want %v", err, ErrEmptyRoomID)
	}
}
