package room

import "testing"

// TestClientsInSameRoomReceivePublishedMessage 验证同一个房间里的多个客户端都能收到同一条广播消息。
func TestClientsInSameRoomReceivePublishedMessage(t *testing.T) {
	r := New()

	// 两个客户端加入同一个房间，分别得到自己的消息 channel。
	alice, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := r.Join("bob")
	if err != nil {
		t.Fatal(err)
	}

	// 发布第一条消息后，alice 和 bob 都应该收到，且序号为 1。
	if _, err := r.Publish("hello"); err != nil {
		t.Fatal(err)
	}

	for _, client := range []*Client{alice, bob} {
		message := <-client.Messages
		if message.Sequence != 1 || message.Content != "hello" {
			t.Fatalf("client %s received %+v", client.ID, message)
		}
	}

	// 再发布第二条消息，验证序号继续递增到 2。
	if _, err := r.Publish("world"); err != nil {
		t.Fatal(err)
	}
	for _, client := range []*Client{alice, bob} {
		message := <-client.Messages
		if message.Sequence != 2 || message.Content != "world" {
			t.Fatalf("client %s received %+v", client.ID, message)
		}
	}
}

// TestLeaveStopsClientFromReceivingMessages 验证客户端离开后，它的消息 channel 会被关闭。
func TestLeaveStopsClientFromReceivingMessages(t *testing.T) {
	r := New()

	alice, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := r.Join("bob")
	if err != nil {
		t.Fatal(err)
	}

	// alice 离开房间，之后广播不应该再发给 alice。
	if err := r.Leave("alice"); err != nil {
		t.Fatal(err)
	}

	// 广播一条消息，只有仍在房间里的 bob 能收到。
	if _, err := r.Publish("hello"); err != nil {
		t.Fatal(err)
	}

	message := <-bob.Messages
	if message.Content != "hello" {
		t.Fatalf("bob received %q, want %q", message.Content, "hello")
	}

	// alice 的 channel 已被关闭，读取时 open 应为 false。
	if _, open := <-alice.Messages; open {
		t.Fatal("alice message channel is still open")
	}
}

func TestRoomRejectsEmptyInput(t *testing.T) {
	// 空白客户端 ID 不能作为房间成员的唯一标识。
	r := New()
	if _, err := r.Join(" "); err != ErrEmptyClientID {
		t.Fatalf("Join error = %v, want %v", err, ErrEmptyClientID)
	}

	// 空白弹幕没有内容，不应该进入广播流程。
	client, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish("\t"); err != ErrEmptyContent {
		t.Fatalf("Publish error = %v, want %v", err, ErrEmptyContent)
	}
	select {
	case message := <-client.Messages:
		t.Fatalf("received invalid message: %+v", message)
	default:
	}
}

func TestSlowClientDoesNotBlockOtherClients(t *testing.T) {
	// Alice 和 Bob 都加入房间，但 Alice 暂时不读取自己的消息 channel，模拟慢客户端。
	r := New()
	alice, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := r.Join("bob")
	if err != nil {
		t.Fatal(err)
	}

	// 第一条消息先填满两个客户端各自容量为 1 的 channel。
	if _, err := r.Publish("first"); err != nil {
		t.Fatal(err)
	}

	// Bob 及时读取第一条消息，为第二条消息腾出空间；Alice 仍然保持未读取状态。
	if message := <-bob.Messages; message.Content != "first" {
		t.Fatalf("bob received %q, want %q", message.Content, "first")
	}

	// 第二次 Publish 不应被 Alice 的满 channel 卡住，Bob 仍然应该收到第二条消息。
	result, err := r.Publish("second")
	if err != nil {
		t.Fatal(err)
	}
	if result.DroppedClients != 1 {
		t.Fatalf("dropped clients = %d, want 1", result.DroppedClients)
	}
	if message := <-bob.Messages; message.Content != "second" {
		t.Fatalf("bob received %q, want %q", message.Content, "second")
	}

	// Alice 的 channel 中仍只有第一条消息，第二条已经因为她太慢而被丢弃。
	if message := <-alice.Messages; message.Content != "first" {
		t.Fatalf("alice received %q, want %q", message.Content, "first")
	}
	select {
	case message := <-alice.Messages:
		t.Fatalf("alice unexpectedly received a second message: %+v", message)
	default:
	}
}
