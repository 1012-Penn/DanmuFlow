package room

import "testing"

func TestOneClientReceivesPublishedMessage(t *testing.T) {
	r := New()
	client, err := r.Join()
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Publish("hello"); err != nil {
		t.Fatal(err)
	}

	message := <-client.Messages
	if message.Content != "hello" {
		t.Fatalf("message content = %q, want %q", message.Content, "hello")
	}
}
