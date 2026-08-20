// Package routing 提供 Kafka 分区到 WebSocket 网关地址的控制面目录。
// 弹幕消息不会经过该包；它只帮助新客户端在建立长连接前找到正确实例。
package routing

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrRouteNotFound 表示分区当前没有仍在租约期内的网关。
	ErrRouteNotFound = errors.New("gateway route not found")
	// ErrInvalidConfig 表示 Redis 路由配置不完整或不安全。
	ErrInvalidConfig = errors.New("invalid routing config")
)

// Lease 是一个分区的短期网关租约。
// Token 每次进程启动都必须不同，防止旧进程在退出时删除新进程写入的租约。
type Lease struct {
	GatewayID    string `json:"gateway_id"`
	WebSocketURL string `json:"websocket_url"`
	Token        string `json:"token"`
}

// Registry 保存和查询分区租约。
type Registry interface {
	// Register 写入或刷新分区租约，TTL 到期后 Redis 会自动删除它。
	Register(ctx context.Context, partition int, lease Lease, ttl time.Duration) error
	// Resolve 查询仍有效的分区租约。
	Resolve(ctx context.Context, partition int) (Lease, error)
	// Release 仅在 Redis 中仍是同一个 token 时删除租约。
	Release(ctx context.Context, partition int, token string) error
	// Close 释放客户端连接池。
	Close() error
}
