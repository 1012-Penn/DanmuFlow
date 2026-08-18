// Package message 定义 DanmuFlow 组件之间传递的内部消息模型。
package message

import "time"

// Danmaku 是一条可以在网关、消息总线、消费者和房间广播之间传递的弹幕。
// 它描述业务消息本身，不包含 Kafka offset 或 WebSocket 连接状态。
type Danmaku struct {
	// MessageID 是消息的业务唯一标识，用于后续幂等、排查和审计。
	MessageID string `json:"message_id"`
	// RoomID 决定消息应该进入哪个直播间以及未来使用哪个消息分区。
	RoomID string `json:"room_id"`
	// UserID 标识发送者，用于限流、审计和权限判断。
	UserID string `json:"user_id"`
	// Content 是弹幕正文。
	Content string `json:"content"`
	// Sequence 是同一房间内对外暴露的业务序号；在消息进入广播链路后分配。
	Sequence uint64 `json:"sequence"`
	// CreatedAt 是消息进入系统的时间，不代表消息被消费或广播的时间。
	CreatedAt time.Time `json:"created_at"`
}
