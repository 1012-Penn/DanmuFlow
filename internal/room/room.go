// Package room 提供一个最小的直播间内存模型。
package room

import (
	"errors"
	"strings"
	"sync"
)

var (
	ErrEmptyClientID = errors.New("client id cannot be empty")
	// 这些错误描述调用方可以处理的业务失败，例如重复加入或客户端不存在。
	ErrClientAlreadyJoined = errors.New("a client has already joined the room")
	ErrClientNotFound      = errors.New("client not found in the room")
	ErrEmptyContent        = errors.New("message content cannot be empty")
	ErrNoClient            = errors.New("no client has joined the room")
)

// Message 是房间里传递的一条弹幕。
type Message struct {
	// Sequence 只在当前 Room 内递增，不代表全局消息顺序。
	Sequence uint64
	// Content 是用户发送的弹幕正文。
	Content string
}

// PublishResult 描述一次房间广播的结果。
// DroppedClients 表示因为各自待发送队列已满而没有收到本条消息的客户端数；它不影响
// 其他客户端接收消息，也不会让房间广播阻塞。
type PublishResult struct {
	DroppedClients int
}

// Client 是加入房间后得到的消息管道。
// 当前最小单元中，Room 负责向 Messages 写入消息，调用方负责从中读取消息。
type Client struct {
	// ID 用于在 Room.clients 中定位这个客户端。
	ID string
	// Messages 是房间发送给客户端的消息管道。
	Messages chan Message
}

// Room 保存同一个房间里的多个客户端，并为消息分配房间内递增的序号。
type Room struct {
	// mu 保护 clients 和 sequence。
	// Join、Leave、Publish 都必须先拿到这把锁，避免并发读写 map 或重复分配序号。
	mu sync.Mutex
	// clients 保存当前仍在房间里的客户端；key 是客户端 ID。
	clients map[string]*Client
	// sequence 保存已经分配到房间消息的最大序号。
	sequence uint64
}

// New 创建一个空房间。
func New() *Room {
	// map 必须先初始化，后续 Join 才能安全地写入 clients。
	return &Room{clients: make(map[string]*Client)}
}

// Join 让一个客户端加入房间，并返回它的消息接收出口。
func (r *Room) Join(clientID string) (*Client, error) {
	// 加锁保护共享的 clients map，防止多个 goroutine 同时加入时发生数据竞争。
	r.mu.Lock()
	defer r.mu.Unlock()

	// 拒绝空白 ID，避免多个没有身份的连接全部使用同一个 map key。
	if strings.TrimSpace(clientID) == "" {
		return nil, ErrEmptyClientID
	}

	// 同一个客户端 ID 只能加入一次；重复加入会让后到的连接无法区分消息该发给谁。
	if _, exists := r.clients[clientID]; exists {
		return nil, ErrClientAlreadyJoined
	}

	// 每个客户端拥有独立的 channel，容量为 1。
	// 这样 Publish 写入时不需要等待读取方立刻取走，能稍微减少同步阻塞；
	// 但当前版本还没有做慢客户端保护，如果消费不及时，Publish 仍可能阻塞。
	messages := make(chan Message, 1)
	client := &Client{
		ID:       clientID,
		Messages: messages,
	}

	// 把客户端登记到 Room 中，之后 Publish 广播时就能遍历到它。
	r.clients[clientID] = client
	return client, nil
}

// Leave 让客户端离开房间，并关闭它的消息 channel。
// 关闭 channel 后，读取方可以通过 ok == false 判断客户端生命周期已经结束。
func (r *Room) Leave(clientID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果客户端本来就不在房间里，没有必要重复关闭 channel。
	client, exists := r.clients[clientID]
	if !exists {
		return ErrClientNotFound
	}

	// 先从房间中删除，再关闭 channel。
	// 先删后关可以保证 Publish 在持有锁的情况下不会向已关闭的 channel 写入。
	delete(r.clients, clientID)
	close(client.Messages)
	return nil
}

// Publish 给房间里的每个客户端发送一条弹幕，并返回本次广播的投递结果。
func (r *Room) Publish(content string) (PublishResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 空白弹幕没有业务意义，先拒绝，避免它进入房间广播链路。
	if strings.TrimSpace(content) == "" {
		return PublishResult{}, ErrEmptyContent
	}

	// 房间没有客户端时没有广播目标，返回错误让调用方感知。
	if len(r.clients) == 0 {
		return PublishResult{}, ErrNoClient
	}

	// 先递增序号，保证同房间内每条消息都有一个单调递增的编号。
	r.sequence++
	message := Message{Sequence: r.sequence, Content: content}

	// 遍历当前所有客户端，把同一条消息尝试写入各自的 channel。
	// 这里在锁内发送，是为了避免 Leave 同时删除并关闭 channel 造成向已关闭 channel 写入。
	result := PublishResult{}
	for _, client := range r.clients {
		select {
		case client.Messages <- message:
			// channel 有空间，消息成功进入该客户端的待发送队列。
		default:
			// channel 已满，说明客户端消费速度跟不上广播速度。
			// 只丢弃这个慢客户端的当前消息，不阻塞其他客户端。
			result.DroppedClients++
		}
	}
	return result, nil
}
