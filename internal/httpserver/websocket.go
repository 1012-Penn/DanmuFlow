package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/message"
	"github.com/1012-Penn/DanmuFlow/internal/metrics"
	"github.com/1012-Penn/DanmuFlow/internal/ratelimit"
	"github.com/1012-Penn/DanmuFlow/internal/room"
	"github.com/1012-Penn/DanmuFlow/internal/sensitive"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
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
	// messageBusPublishWait 限制接入层等待消息队列空间的最长时间。
	// 队列持续满时，连接不能无限期卡在 Publish 上。
	messageBusPublishWait = time.Second
	// messageIDBytes 使用 128 位随机数作为消息唯一标识。即使多个网关实例同时
	// 运行或进程重启，它们也不共享计数器，随机 ID 仍具有可忽略的碰撞概率。
	messageIDBytes = 16
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
	invalidJSONCode           = "invalid_json"
	emptyContentCode          = "empty_content"
	contentTooLongCode        = "content_too_long"
	sensitiveContentCode      = "sensitive_content"
	messageBusUnavailableCode = "message_bus_unavailable"
)

// websocketConnections 记录当前实例上已经升级的连接。http.Server.Shutdown
// 不会自动关闭被 WebSocket 接管的连接，因此发布重启时需要它主动发送 1012。
type websocketConnections struct {
	mu      sync.Mutex
	items   map[*websocket.Conn]struct{}
	closing bool
}

func newWebSocketConnections() *websocketConnections {
	return &websocketConnections{items: make(map[*websocket.Conn]struct{})}
}

// add 尝试把已升级连接登记到当前实例。
// closing 与 items 共用一把锁，使“登记连接”和“发布下线取得连接快照”具有明确顺序：
// 要么连接先登记并进入关闭快照，要么下线先开始，迟到连接会被拒绝。
func (connections *websocketConnections) add(conn *websocket.Conn) bool {
	connections.mu.Lock()
	defer connections.mu.Unlock()

	if connections.closing {
		return false
	}
	connections.items[conn] = struct{}{}
	return true
}

func (connections *websocketConnections) remove(conn *websocket.Conn) {
	connections.mu.Lock()
	delete(connections.items, conn)
	connections.mu.Unlock()
}

// closeForServiceRestart 向所有连接发出 1012（服务重启）后关闭底层连接。
// WriteControl 可与单独的数据帧写协程并发调用；同时并发关闭各连接，避免慢客户端
// 让整个实例的下线时间随连接数线性增长。
func (connections *websocketConnections) closeForServiceRestart(ctx context.Context) {
	connections.mu.Lock()
	// closing 一旦变为 true 就不再恢复。Server 进入发布下线后不会重新承接连接；
	// 这样通过握手前检查、但尚未来得及登记的连接也不能漏出本次关闭过程。
	connections.closing = true
	items := make([]*websocket.Conn, 0, len(connections.items))
	for conn := range connections.items {
		items = append(items, conn)
	}
	connections.mu.Unlock()

	var workers sync.WaitGroup
	workers.Add(len(items))
	for _, conn := range items {
		conn := conn
		go func() {
			defer workers.Done()
			closeWebSocketForServiceRestart(ctx, conn)
		}()
	}
	finished := make(chan struct{})
	go func() {
		workers.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-ctx.Done():
	}
}

// closeWebSocketForServiceRestart 尽力发送 1012 后关闭连接。
// 它既用于关闭快照中的连接，也用于拒绝在快照之后才完成升级的迟到连接。
func closeWebSocketForServiceRestart(ctx context.Context, conn *websocket.Conn) {
	deadline := time.Now().Add(time.Second)
	if until, ok := ctx.Deadline(); ok && until.Before(deadline) {
		deadline = until
	}
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseServiceRestart, "service is restarting"), deadline)
	_ = conn.Close()
}

// protocolError 是写协程处理的一条错误响应。
// done 用来让读协程确认错误已经写入，避免读协程立刻关闭连接导致错误帧丢失。
type protocolError struct {
	response websocketErrorResponse
	done     chan error
}

// newWebSocketHandler 把一条 WebSocket 连接接到 room_id 对应的内存房间。
// 每条连接有两个方向：当前 handler 读取客户端发送的消息，写协程负责把房间消息推回客户端。
func newWebSocketHandler(rooms *room.Registry, messageLimiter *ratelimit.Limiter, sensitiveFilter *sensitive.Filter, messageBus bus.Bus, canAcceptConnection func() bool, canPublish func() bool, connections *websocketConnections, observability *metrics.Metrics, logger *zap.Logger) http.Handler {
	if logger == nil {
		logger = zap.NewNop()
	}

	// Upgrader 负责把普通 HTTP 请求升级为 WebSocket 长连接。
	// 这里使用零值配置，Gorilla 会执行默认的 Origin 检查，避免无意中接受任意来源的浏览器请求。
	var upgrader websocket.Upgrader

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 就绪失败时先拒绝升级，避免客户端建立一条“能连上但不能安全发送”的连接。
		if !canAcceptConnection() {
			http.Error(w, "message bus is temporarily unavailable\n", http.StatusServiceUnavailable)
			return
		}
		// 1. 在 Upgrade 之前校验 user_id。
		//    Upgrade 成功后 HTTP 响应已经变成 WebSocket 帧，不能再用 http.Error 返回 400。
		userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
		if userID == "" {
			logger.Debug("websocket_rejected", zap.String("reason", "missing_user_id"))
			http.Error(w, "user_id is required\n", http.StatusBadRequest)
			return
		}
		roomID := strings.TrimSpace(r.URL.Query().Get("room_id"))
		if roomID == "" {
			logger.Debug("websocket_rejected", zap.String("reason", "missing_room_id"))
			http.Error(w, "room_id is required\n", http.StatusBadRequest)
			return
		}

		// Upgrade 把普通 HTTP 连接升级成 WebSocket 连接。
		// 如果握手失败（例如不是 WebSocket 请求），直接返回即可，不需要继续处理。
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Debug("websocket_rejected", zap.String("reason", "upgrade_failed"), zap.Error(err))
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
			logger.Error("websocket_room_lookup_failed", zap.String("room_id", roomID), zap.String("user_id", userID), zap.Error(err))
			return
		}
		client, err := chatRoom.Join(userID)
		if err != nil {
			// 加入失败（例如重复 user_id）时无法继续收发消息，直接关闭连接。
			logger.Debug("websocket_rejected", zap.String("reason", "join_room_failed"), zap.String("room_id", roomID), zap.String("user_id", userID), zap.Error(err))
			return
		}
		if !connections.add(conn) {
			// 连接可能在握手前通过 readiness 检查，但发布下线随后已经取得了
			// 连接快照。此时不能让这条 hijacked 连接漏出 http.Server.Shutdown
			// 的管理范围；撤销房间成员并明确通知客户端重连。
			_ = chatRoom.Leave(userID)
			closeWebSocketForServiceRestart(r.Context(), conn)
			return
		}
		defer connections.remove(conn)
		observability.WebSocketConnections.Inc()
		defer observability.WebSocketConnections.Dec()

		connectedAt := time.Now()
		connectionFields := []zap.Field{
			zap.String("room_id", roomID),
			zap.String("user_id", userID),
			zap.String("remote_addr", r.RemoteAddr),
		}
		logger.Info("websocket_connected", connectionFields...)
		defer func() {
			logger.Info("websocket_disconnected",
				append(connectionFields, zap.Duration("duration", time.Since(connectedAt)))...,
			)
		}()

		// 5. 设置心跳读超时，并在收到客户端 Pong 时延长超时时间。
		//    如果客户端长期没有回应，ReadMessage 最终会返回超时错误，触发连接清理。
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
		// 每读到一条，就先发布到消息总线；后台消费者再调用 Room.Publish 广播给房间里的所有客户端。
		for {
			// 先完整读取一条消息再解析 JSON。ReadMessage 在读取过程中受
			// maxWebSocketFrameSize 限制：一旦累计字节数超过限制，Gorilla
			// 会发送 1009 关闭帧并返回 ErrReadLimit。这样大小限制始终在 JSON
			// 校验之前生效，超大帧无论内容是否合法都会先触发 1009 关闭；
			// 如果直接 ReadJSON，垃圾内容的 JSON 解析错误会先返回，导致
			// 超大帧被当成 invalid_json 处理，而不是按大小被拒绝。
			_, payload, err := conn.ReadMessage()
			if err != nil {
				// 超大帧由 Gorilla 发送 1009 关闭帧；其他读取错误通常表示
				// 客户端断开或 JSON 语法错误。ReadMessage 出错后不能继续复用读循环。
				if errors.Is(err, websocket.ErrReadLimit) {
					observability.MessagesRejected.WithLabelValues("frame_too_large").Inc()
					logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "frame_too_large"))...)
					// Gorilla 已经发出 1009 关闭帧。短暂读取剩余数据，等待
					// 客户端的关闭回应，避免底层 TCP 连接在关闭帧送达前因为
					// 还有未读取的数据而发送 RST，丢弃已写入的关闭帧。
					_ = conn.SetReadDeadline(time.Now().Add(writeWait))
					for {
						if _, _, err := conn.ReadMessage(); err != nil {
							break
						}
					}
				} else if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					// ReadMessage 在底层连接中断或客户端未完成关闭握手时也会返回错误，
					// 但此时没有收到可供 JSON 校验的完整数据帧，不能误记为 invalid_json。
					// 连接已经无法继续读取，记录调试日志后直接清理即可。
					logger.Debug("websocket_read_failed", append(connectionFields, zap.Error(err))...)
				}
				return
			}
			observability.MessagesReceived.Inc()

			var request websocketRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				observability.MessagesRejected.WithLabelValues("invalid_json").Inc()
				logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "invalid_json"))...)
				// JSON 语法错误与 ReadMessage 阶段的读取错误一样会结束连接，
				// 发送错误帧后退出读循环。queueProtocolError 会等待错误帧
				// 真正写入，避免连接在错误帧送达前被关闭。
				_ = queueProtocolError(websocketErrorResponse{
					Code:    invalidJSONCode,
					Message: "message must be valid JSON",
				})
				return
			}

			// 空白内容和超长内容在进入限流、房间序号分配及广播之前被拒绝。
			// 这里不修改合法正文本身，因此原有广播内容行为保持不变。
			switch {
			case strings.TrimSpace(request.Content) == "":
				observability.MessagesRejected.WithLabelValues("empty_content").Inc()
				logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "empty_content"))...)
				if !queueProtocolError(websocketErrorResponse{
					Code:    emptyContentCode,
					Message: "content cannot be empty",
				}) {
					return
				}
				continue
			case utf8.RuneCountInString(request.Content) > maxContentRunes:
				observability.MessagesRejected.WithLabelValues("content_too_long").Inc()
				logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "content_too_long"))...)
				if !queueProtocolError(websocketErrorResponse{
					Code:    contentTooLongCode,
					Message: "content is too long",
				}) {
					return
				}
				continue
			}
			if _, matched := sensitiveFilter.Match(request.Content); matched {
				observability.MessagesRejected.WithLabelValues("sensitive_content").Inc()
				logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "sensitive_content"))...)
				if !queueProtocolError(websocketErrorResponse{
					Code:    sensitiveContentCode,
					Message: "message contains sensitive content",
				}) {
					return
				}
				continue
			}

			// 限流发生在消息总线 Publish 之前，超限消息不会进入异步消息链路，
			// 也不会消耗房间序号。
			// 限流暂时不返回错误帧，客户端保持连接并继续发送后续消息。
			if !messageLimiter.Allow(userID) {
				observability.MessagesRejected.WithLabelValues("rate_limit").Inc()
				logger.Debug("message_rejected", append(connectionFields, zap.String("reason", "rate_limit"))...)
				continue
			}

			// 消费者在连接存续期间失去就绪状态时，不再接收新消息。这样 Kafka
			// 虽可能仍可写入，客户端也不会误以为弹幕已经被正常广播。
			if !canPublish() {
				observability.MessagesRejected.WithLabelValues(messageBusUnavailableCode).Inc()
				if !queueProtocolError(websocketErrorResponse{
					Code:    messageBusUnavailableCode,
					Message: "message queue is temporarily unavailable",
				}) {
					return
				}
				continue
			}

			messageID, err := newMessageID(rand.Reader)
			if err != nil {
				// 无法生成可靠 ID 时不能把消息写入 Kafka，否则未来幂等层无法
				// 区分不同弹幕。该失败极少发生，但必须以明确拒绝代替空 ID。
				observability.MessagesRejected.WithLabelValues("message_id_generation_failed").Inc()
				logger.Error("message_id_generation_failed", append(connectionFields, zap.Error(err))...)
				if !queueProtocolError(websocketErrorResponse{
					Code:    messageBusUnavailableCode,
					Message: "message queue is temporarily unavailable",
				}) {
					return
				}
				continue
			}

			danmaku := message.Danmaku{
				MessageID: messageID,
				RoomID:    roomID,
				UserID:    userID,
				Content:   request.Content,
				CreatedAt: time.Now(),
			}
			publishContext, cancelPublish := context.WithTimeout(r.Context(), messageBusPublishWait)
			publishedAt := time.Now()
			err = messageBus.Publish(publishContext, danmaku)
			observability.KafkaPublishDuration.Observe(time.Since(publishedAt).Seconds())
			cancelPublish()
			if err != nil {
				observability.MessagesRejected.WithLabelValues(messageBusUnavailableCode).Inc()
				logger.Debug("message_publish_rejected", append(connectionFields,
					zap.String("reason", "message_bus_unavailable"),
					zap.String("message_id", danmaku.MessageID),
					zap.Error(err),
				)...)
				if !queueProtocolError(websocketErrorResponse{
					Code:    messageBusUnavailableCode,
					Message: "message queue is temporarily unavailable",
				}) {
					return
				}
			}
		}
	})
}

// newMessageID 从加密安全随机源生成 128 位 ID，并编码为固定 32 位十六进制字符串。
// reader 作为参数传入，使随机源失败路径可以在测试中稳定验证。
func newMessageID(reader io.Reader) (string, error) {
	var value [messageIDBytes]byte
	if _, err := io.ReadFull(reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

// writeWebSocketJSON 是连接的统一数据帧写出口。
// 所有普通消息和协议错误都由同一个 goroutine 调用，避免并发写入连接。
func writeWebSocketJSON(conn *websocket.Conn, value any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return conn.WriteJSON(value)
}
