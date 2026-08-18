package httpserver

import (
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/1012-Penn/DanmuFlow/internal/ratelimit"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/gorilla/websocket"
)

const (
	// maxWebSocketFrameSize 限制单条 WebSocket 消息的大小，避免异常客户端
	// 通过超大帧占用过多内存。这个限制作用于 JSON 帧的完整字节数。
	maxWebSocketFrameSize = 4 * 1024
	// maxContentRunes 限制一条弹幕正文的字符数。使用字符数而不是字节数，
	// 这样中文和英文用户看到的是一致的长度规则。
	maxContentRunes = 500

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

// websocketErrorResponse 是服务端返回给客户端的协议错误格式。
// 错误只描述当前请求失败，不代表 WebSocket 一定会关闭。
type websocketErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	invalidJSONCode    = "invalid_json"
	emptyContentCode   = "empty_content"
	contentTooLongCode = "content_too_long"
)

// protocolError 是写协程处理的一条错误响应。
// done 用来让读协程确认错误已经写入，避免读协程立刻关闭连接导致错误帧丢失。
type protocolError struct {
	response websocketErrorResponse
	done     chan error
}

// newWebSocketHandler 把一条 WebSocket 连接接到 room_id 对应的内存房间。
// 每条连接有两个方向：当前 handler 读取客户端发送的消息，写协程负责把房间消息推回客户端。
func newWebSocketHandler(rooms *room.Registry, messageLimiter *ratelimit.Limiter) http.Handler {
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
		// 限制每个 WebSocket 数据帧的大小。超过限制时 Gorilla 会发送
		// 1009 (message too big) 关闭帧，随后读循环退出。
		conn.SetReadLimit(maxWebSocketFrameSize)

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
		protocolErrors := make(chan protocolError, 1)

		// 启动一个新的 goroutine 来异步处理消息写入。
		// 读循环只负责接收客户端消息，写协程只负责把房间消息推回客户端，
		// 两者可以并行，避免某一方阻塞影响另一方。
		go func() {
			// 使用 defer 确保函数退出时关闭 writeDone 通道。
			// 这通常用于通知其他协程写入操作已完成。
			defer close(writeDone)

			// 房间消息和协议错误都必须经过同一个写协程。
			// Gorilla WebSocket 不允许多个 goroutine 同时写数据帧；统一出口
			// 可以避免“广播写入”和“错误响应写入”并发操作同一连接。
			for {
				select {
				case message, open := <-client.Messages:
					if !open {
						return
					}
					response := websocketResponse{
						Sequence: message.Sequence,
						Content:  message.Content,
					}
					if err := writeWebSocketJSON(conn, response); err != nil {
						return
					}
				case protocolErr := <-protocolErrors:
					err := writeWebSocketJSON(conn, protocolErr.response)
					protocolErr.done <- err
					if err != nil {
						return
					}
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

		// queueProtocolError 把错误交给唯一的写协程，并等待写入结果。
		// 对于语义错误，等待完成后连接仍然可以继续接收下一条弹幕；
		// 对于 JSON 语法错误，调用方会在发送错误后结束当前连接。
		queueProtocolError := func(response websocketErrorResponse) bool {
			protocolErr := protocolError{
				response: response,
				done:     make(chan error, 1),
			}
			select {
			case protocolErrors <- protocolErr:
			case <-writeDone:
				return false
			}
			select {
			case err := <-protocolErr.done:
				return err == nil
			case <-writeDone:
				return false
			}
		}

		// 主循环负责读取客户端发来的弹幕。
		// 每读到一条，就调用 Publish 广播给房间里的所有客户端（包括发送者自己）。
		for {
			var request websocketRequest
			if err := conn.ReadJSON(&request); err != nil {
				// 超大帧由 Gorilla 发送 1009 关闭帧；其他读取错误通常表示
				// 客户端断开或 JSON 语法错误。ReadJSON 出错后不能继续复用读循环。
				if !errors.Is(err, websocket.ErrReadLimit) && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					_ = queueProtocolError(websocketErrorResponse{
						Code:    invalidJSONCode,
						Message: "message must be valid JSON",
					})
				}
				return
			}

			// 空白内容和超长内容在进入限流、房间序号分配及广播之前被拒绝。
			// 这里不修改合法正文本身，因此原有广播内容行为保持不变。
			switch {
			case strings.TrimSpace(request.Content) == "":
				if !queueProtocolError(websocketErrorResponse{
					Code:    emptyContentCode,
					Message: "content cannot be empty",
				}) {
					return
				}
				continue
			case utf8.RuneCountInString(request.Content) > maxContentRunes:
				if !queueProtocolError(websocketErrorResponse{
					Code:    contentTooLongCode,
					Message: "content is too long",
				}) {
					return
				}
				continue
			}

			// 限流发生在 Publish 之前，超限消息不会消耗房间序号，也不会进入广播链路。
			// 限流暂时不返回错误帧，客户端保持连接并继续发送后续消息。
			if !messageLimiter.Allow(userID) {
				continue
			}

			// 发布弹幕。如果房间为空等业务错误导致发布失败，当前连接也无法继续正常工作。
			if err := chatRoom.Publish(request.Content); err != nil {
				return
			}
		}
	})
}

// writeWebSocketJSON 是连接的统一数据帧写出口。
// 所有普通消息和协议错误都由同一个 goroutine 调用，避免并发写入连接。
func writeWebSocketJSON(conn *websocket.Conn, value any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return conn.WriteJSON(value)
}
