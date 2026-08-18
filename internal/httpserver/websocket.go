package httpserver

import (
	"net/http"
	"strings"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/gorilla/websocket"
)

const (
	// pongWait 是服务端允许客户端没有回应 Pong 的最长时间。
	pongWait = 60 * time.Second
	// pingPeriod 要小于 pongWait，保证服务端会在读超时前主动发出 Ping。
	pingPeriod = (pongWait * 9) / 10
	// writeWait 限制一次控制帧写入最多等待多久。
	writeWait = 10 * time.Second
)

// websocketRequest 是客户端通过 WebSocket 发送给服务端的消息格式。
// 当前只包含弹幕正文，后续可以扩展消息类型、时间戳等字段。
type websocketRequest struct {
	Content string `json:"content"`
}

// websocketResponse 是服务端推送给客户端的消息格式。
// Sequence 是房间内单调递增的序号，Content 是弹幕正文。
type websocketResponse struct {
	Sequence uint64 `json:"sequence"`
	Content  string `json:"content"`
}

// newWebSocketHandler 把一条 WebSocket 连接接到 room_id 对应的内存房间。
// 每条连接有两个方向：当前 handler 读取客户端发送的消息，写协程负责把房间消息推回客户端。
func newWebSocketHandler(rooms *room.Registry) http.Handler {
	// Upgrader 负责把普通 HTTP 请求升级为 WebSocket 长连接。
	// 这里使用零值配置，Gorilla 会执行默认的 Origin 检查，避免无意中接受任意来源的浏览器请求。
	var upgrader websocket.Upgrader

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. 在 Upgrade 之前校验 user_id。
		//    Upgrade 成功后 HTTP 响应已经变成 WebSocket 帧，不能再用 http.Error 返回 400。
		userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
		if userID == "" {
			http.Error(w, "user_id is required\n", http.StatusBadRequest)
			return
		}
		roomID := strings.TrimSpace(r.URL.Query().Get("room_id"))
		if roomID == "" {
			http.Error(w, "room_id is required\n", http.StatusBadRequest)
			return
		}

		// Upgrade 把普通 HTTP 连接升级成 WebSocket 连接。
		// 如果握手失败（例如不是 WebSocket 请求），直接返回即可，不需要继续处理。
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// 这里先注册一个基础清理，确保即使后续初始化失败，连接也会被关闭。
		// 后面还有一个更完整的清理 defer，负责 Leave、再次 Close 和等待写协程退出。
		defer conn.Close()

		// 根据 room_id 找到对应的内存房间，再让当前用户加入这个房间。
		// Join 会为这个连接创建独立的消息 channel，之后房间广播的消息都会写入这个 channel。
		chatRoom, err := rooms.GetOrCreate(roomID)
		if err != nil {
			return
		}
		client, err := chatRoom.Join(userID)
		if err != nil {
			// 加入失败（例如重复 user_id）时无法继续收发消息，直接关闭连接。
			return
		}

		// 5. 设置心跳读超时，并在收到客户端 Pong 时延长超时时间。
		//    如果客户端长期没有回应，ReadJSON 最终会返回超时错误，触发连接清理。
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		heartbeatDone := make(chan struct{})
		go func() {
			// 6. 心跳 goroutine 定期发送 Ping，确认底层网络连接仍然可用。
			ticker := time.NewTicker(pingPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
						return
					}
				case <-heartbeatDone:
					return
				}
			}
		}()

		// writeDone 用于通知当前 goroutine：后台写协程已经退出。
		// 这样在连接关闭时，可以等待写协程安全结束，避免它还在使用已关闭的连接。
		writeDone := make(chan struct{})

		// 启动一个新的 goroutine 来异步处理消息写入。
		// 读循环只负责接收客户端消息，写协程只负责把房间消息推回客户端，
		// 两者可以并行，避免某一方阻塞影响另一方。
		go func() {
			// 使用 defer 确保函数退出时关闭 writeDone 通道。
			// 这通常用于通知其他协程写入操作已完成。
			defer close(writeDone)

			// 从 client.Messages 通道中循环读取消息。
			// 当通道被关闭时，循环自动结束（例如客户端 Leave 后房间关闭了这个 channel）。
			for message := range client.Messages {
				// 构造 WebSocket 响应对象。
				// 将消息的序列号和内容包装成响应格式，供客户端识别顺序和展示内容。
				response := websocketResponse{
					Sequence: message.Sequence,
					Content:  message.Content,
				}

				// 将响应以 JSON 格式写入 WebSocket 连接。
				// 如果写入失败（例如客户端断开），直接返回结束当前 goroutine；
				// 此时会触发上面的 defer 关闭 writeDone 通道。
				if err := conn.WriteJSON(response); err != nil {
					return
				}
			}
		}()

		// 7. 主 goroutine 退出前统一清理心跳、房间成员、网络连接和写协程。
		defer func() {
			close(heartbeatDone)
			_ = chatRoom.Leave(userID)
			_ = conn.Close()
			<-writeDone
		}()

		// 主循环负责读取客户端发来的弹幕。
		// 每读到一条，就调用 Publish 广播给房间里的所有客户端（包括发送者自己）。
		for {
			var request websocketRequest
			if err := conn.ReadJSON(&request); err != nil {
				// 客户端关闭、网络异常或消息格式错误都会让 ReadJSON 返回错误。
				// 此时不需要继续读，直接返回触发上面的清理逻辑。
				return
			}

			// 发布弹幕。如果房间为空等业务错误导致发布失败，当前连接也无法继续正常工作。
			if err := chatRoom.Publish(request.Content); err != nil {
				return
			}
		}
	})
}
