package httpserver

import (
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestWebSocketBroadcastsToTwoClients 验证一条 WebSocket 消息会被广播给同一个房间里的多个客户端。
func TestWebSocketBroadcastsToTwoClients(t *testing.T) {
	// 监听本机随机端口，避免测试并行时发生端口冲突。
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	// 使用 httptest 启动一个真实的 HTTP 服务，这样 WebSocket 的升级过程也会被完整覆盖。
	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	// connect 是测试内部的辅助函数：根据 userID 建立一条 WebSocket 连接。
	connect := func(roomID, userID string) *websocket.Conn {
		t.Helper()

		// 把 http:// 地址改写成 ws:// 地址，并拼接 /ws、room_id 与 user_id 参数。
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=" + roomID + "&user_id=" + userID
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}

	// 两个客户端分别以 alice 和 bob 的身份加入同一个房间。
	alice := connect("room-a", "alice")
	defer alice.Close()
	bob := connect("room-a", "bob")
	defer bob.Close()
	carol := connect("room-b", "carol")
	defer carol.Close()

	// alice 发送一条弹幕，服务端应该把它广播给房间里的所有客户端。
	if err := alice.WriteJSON(websocketRequest{Content: "hello"}); err != nil {
		t.Fatal(err)
	}

	// alice 和 bob 都应该收到同一条消息，并且序号从 1 开始。
	for _, client := range []*websocket.Conn{alice, bob} {
		var response websocketResponse
		if err := client.ReadJSON(&response); err != nil {
			t.Fatal(err)
		}
		if response.Sequence != 1 || response.Content != "hello" {
			t.Fatalf("response = %+v", response)
		}
	}

	// carol 在另一个房间，不应该收到 room-a 的消息。
	_ = carol.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var response websocketResponse
	if err := carol.ReadJSON(&response); err == nil {
		t.Fatalf("different room received response: %+v", response)
	}
}

func TestWebSocketRequiresRoomID(t *testing.T) {
	request := httptest.NewRequest("GET", "/ws?user_id=alice", nil)
	recorder := httptest.NewRecorder()

	New(":0").Handler.ServeHTTP(recorder, request)

	if recorder.Code != 400 {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestWebSocketRejectsInvalidContentAndKeepsConnection(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(websocketRequest{Content: "   "}); err != nil {
		t.Fatal(err)
	}
	var protocolError websocketErrorResponse
	if err := conn.ReadJSON(&protocolError); err != nil {
		t.Fatal(err)
	}
	if protocolError.Code != emptyContentCode {
		t.Fatalf("error = %+v, want code %q", protocolError, emptyContentCode)
	}

	if err := conn.WriteJSON(websocketRequest{Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	var response websocketResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Sequence != 1 || response.Content != "hello" {
		t.Fatalf("response = %+v", response)
	}
}

func TestWebSocketRejectsOverlongContent(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(websocketRequest{Content: strings.Repeat("a", maxContentRunes+1)}); err != nil {
		t.Fatal(err)
	}
	var protocolError websocketErrorResponse
	if err := conn.ReadJSON(&protocolError); err != nil {
		t.Fatal(err)
	}
	if protocolError.Code != contentTooLongCode {
		t.Fatalf("error = %+v, want code %q", protocolError, contentTooLongCode)
	}
}

func TestWebSocketRejectsSensitiveContent(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(websocketRequest{Content: "这是一条赌博消息"}); err != nil {
		t.Fatal(err)
	}
	var protocolError websocketErrorResponse
	if err := conn.ReadJSON(&protocolError); err != nil {
		t.Fatal(err)
	}
	if protocolError.Code != sensitiveContentCode {
		t.Fatalf("error = %+v, want code %q", protocolError, sensitiveContentCode)
	}

	if err := conn.WriteJSON(websocketRequest{Content: "正常消息"}); err != nil {
		t.Fatal(err)
	}
	var response websocketResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Sequence != 1 || response.Content != "正常消息" {
		t.Fatalf("response = %+v", response)
	}
}

func TestWebSocketRejectsOversizedFrame(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(strings.Repeat("a", maxWebSocketFrameSize+1))); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.ReadMessage()
	if closeErr, ok := err.(*websocket.CloseError); !ok || closeErr.Code != websocket.CloseMessageTooBig {
		t.Fatalf("read error = %v, want close code %d", err, websocket.CloseMessageTooBig)
	}
}

func TestWebSocketDropsMessagesOverUserRateLimit(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(New(":0").Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 10; i++ {
		if err := conn.WriteJSON(websocketRequest{Content: "message"}); err != nil {
			t.Fatal(err)
		}
		var response websocketResponse
		if err := conn.ReadJSON(&response); err != nil {
			t.Fatalf("reading allowed message %d: %v", i+1, err)
		}
	}

	// 第 11 条消息仍在突发额度之外，应被限流器丢弃。
	if err := conn.WriteJSON(websocketRequest{Content: "message"}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var response websocketResponse
	if err := conn.ReadJSON(&response); err == nil {
		t.Fatalf("rate-limited message was broadcast: %+v", response)
	}
}
