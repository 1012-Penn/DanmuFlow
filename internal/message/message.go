// Package message 定义 DanmuFlow 组件之间传递的内部消息模型。
package message

import (
	"errors"
	"strings"
	"time"
)

var (
	// ErrEmptyMessageID 表示消息缺少跨进程唯一标识，无法安全执行幂等处理。
	ErrEmptyMessageID = errors.New("message id cannot be empty")
	// ErrEmptyRoomID 表示消息无法路由到任何直播间。
	ErrEmptyRoomID = errors.New("room id cannot be empty")
	// ErrEmptyUserID 表示消息缺少可审计的发送者身份。
	ErrEmptyUserID = errors.New("user id cannot be empty")
	// ErrEmptyContent 表示消息没有可广播的正文。
	ErrEmptyContent = errors.New("message content cannot be empty")
	// ErrEmptyCreatedAt 表示消息缺少进入系统的时间。
	ErrEmptyCreatedAt = errors.New("message created_at cannot be empty")
)

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

// Validate 检查跨组件消息执行路由、审计和后续幂等处理所需的必填字段。
// Sequence 在广播处理阶段才分配，因此零值是合法的生产端状态。
func (d Danmaku) Validate() error {
	switch {
	case strings.TrimSpace(d.MessageID) == "":
		return ErrEmptyMessageID
	case strings.TrimSpace(d.RoomID) == "":
		return ErrEmptyRoomID
	case strings.TrimSpace(d.UserID) == "":
		return ErrEmptyUserID
	case strings.TrimSpace(d.Content) == "":
		return ErrEmptyContent
	case d.CreatedAt.IsZero():
		return ErrEmptyCreatedAt
	default:
		return nil
	}
}
