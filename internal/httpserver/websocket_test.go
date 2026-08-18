package httpserver

import (
	"net"
	"net/http/httptest"
	"strings"
	"testing"

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
	connect := func(userID string) *websocket.Conn {
		t.Helper()

		// 把 http:// 地址改写成 ws:// 地址，并拼接 /ws 与 user_id 参数。
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?user_id=" + userID
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}

	// 两个客户端分别以 alice 和 bob 的身份加入同一个房间。
	alice := connect("alice")
	defer alice.Close()
	bob := connect("bob")
	defer bob.Close()

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
}
