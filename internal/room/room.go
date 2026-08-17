// Package room 提供一个最小的直播间内存模型。
package room

import (
	"errors"
	"sync"
)

var (
	ErrClientAlreadyJoined = errors.New("a client has already joined the room")
	ErrNoClient            = errors.New("no client has joined the room")
)

// Message 是房间里传递的一条弹幕。
type Message struct {
	Content string
}

// Client 是加入房间后得到的消息管道。
// 当前最小单元中，Room 负责向 Messages 写入消息，调用方负责从中读取消息。
type Client struct {
	Messages chan Message
}

// Room 是一个暂时只支持单个客户端的内存房间。
// 先把“发布一条消息并收到它”这个最小闭环跑通，后面再扩展为多客户端广播。
type Room struct {
	mu     sync.Mutex
	client *Client
}

// New 创建一个空房间。
func New() *Room {
	return &Room{}
}

// Join 让一个客户端加入房间，并返回它的消息接收出口。
func (r *Room) Join() (*Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil {
		return nil, ErrClientAlreadyJoined
	}

	messages := make(chan Message, 1)
	client := &Client{
		Messages: messages,
	}
	r.client = client
	return client, nil
}

// Publish 把一条弹幕发送给当前客户端。
func (r *Room) Publish(content string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return ErrNoClient
	}

	r.client.Messages <- Message{Content: content}
	return nil
}
