package room

import (
	"errors"
	"strings"
	"sync"
)

// ErrEmptyRoomID 表示调用方没有提供有效的房间 ID。
var ErrEmptyRoomID = errors.New("room id cannot be empty")

// Registry 按房间 ID 管理进程内的多个 Room。
// Registry 的锁只保护 rooms 这张表；每个 Room 自己负责保护成员和消息序号。
type Registry struct {
	mu    sync.Mutex
	rooms map[string]*Room
}

// NewRegistry 创建一个空的房间注册表。
func NewRegistry() *Registry {
	return &Registry{rooms: make(map[string]*Room)}
}

// GetOrCreate 返回指定 ID 对应的房间；房间不存在时会创建它。
func (r *Registry) GetOrCreate(roomID string) (*Room, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return nil, ErrEmptyRoomID
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.rooms[roomID]; ok {
		return existing, nil
	}

	newRoom := New()
	r.rooms[roomID] = newRoom
	return newRoom, nil
}
