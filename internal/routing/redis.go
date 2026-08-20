package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultKeyPrefix = "danmuflow:routing"

var releaseLeaseScript = redis.NewScript(`
local value = redis.call("GET", KEYS[1])
if not value then
  return 0
end
local lease = cjson.decode(value)
if lease.token == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// RedisConfig 配置单机或哨兵前端 Redis 地址使用的路由目录。
// 当前只创建普通 Client；Redis Cluster 属于流量和基础设施证据出现后的独立演进。
type RedisConfig struct {
	Address   string
	Password  string
	DB        int
	KeyPrefix string
}

// RedisRegistry 使用带 TTL 的 Redis 字符串保存分区租约。
type RedisRegistry struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedisRegistry 创建 Redis 路由目录。客户端采用惰性连接，网络故障会在具体操作时返回。
func NewRedisRegistry(config RedisConfig) (*RedisRegistry, error) {
	if strings.TrimSpace(config.Address) == "" || config.DB < 0 {
		return nil, ErrInvalidConfig
	}
	prefix := strings.TrimSpace(config.KeyPrefix)
	if prefix == "" {
		prefix = defaultKeyPrefix
	}
	return &RedisRegistry{
		client: redis.NewClient(&redis.Options{
			Addr:     config.Address,
			Password: config.Password,
			DB:       config.DB,
		}),
		keyPrefix: strings.TrimSuffix(prefix, ":"),
	}, nil
}

// Register 写入完整租约并设置 TTL。新 Kafka owner 可以覆盖旧租约，缩短故障转移等待。
func (registry *RedisRegistry) Register(ctx context.Context, partition int, lease Lease, ttl time.Duration) error {
	if registry == nil || registry.client == nil || partition < 0 || ttl <= 0 || !validLease(lease) {
		return ErrInvalidConfig
	}
	payload, err := json.Marshal(lease)
	if err != nil {
		return fmt.Errorf("encode gateway route: %w", err)
	}
	if err := registry.client.Set(ctx, registry.key(partition), payload, ttl).Err(); err != nil {
		return fmt.Errorf("register gateway route: %w", err)
	}
	return nil
}

// Resolve 查询一个分区的当前租约。
func (registry *RedisRegistry) Resolve(ctx context.Context, partition int) (Lease, error) {
	if registry == nil || registry.client == nil || partition < 0 {
		return Lease{}, ErrInvalidConfig
	}
	payload, err := registry.client.Get(ctx, registry.key(partition)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Lease{}, ErrRouteNotFound
	}
	if err != nil {
		return Lease{}, fmt.Errorf("resolve gateway route: %w", err)
	}
	var lease Lease
	if err := json.Unmarshal(payload, &lease); err != nil || !validLease(lease) {
		return Lease{}, fmt.Errorf("decode gateway route: %w", ErrInvalidConfig)
	}
	return lease, nil
}

// Release 使用 Lua 在 Redis 内原子完成“比较 token 后删除”，避免旧 owner 删除新租约。
func (registry *RedisRegistry) Release(ctx context.Context, partition int, token string) error {
	if registry == nil || registry.client == nil || partition < 0 || strings.TrimSpace(token) == "" {
		return ErrInvalidConfig
	}
	if err := releaseLeaseScript.Run(ctx, registry.client, []string{registry.key(partition)}, token).Err(); err != nil {
		return fmt.Errorf("release gateway route: %w", err)
	}
	return nil
}

// Close 关闭 Redis 连接池。
func (registry *RedisRegistry) Close() error {
	if registry == nil || registry.client == nil {
		return nil
	}
	return registry.client.Close()
}

func (registry *RedisRegistry) key(partition int) string {
	return fmt.Sprintf("%s:partition:%d", registry.keyPrefix, partition)
}

func validLease(lease Lease) bool {
	if strings.TrimSpace(lease.GatewayID) == "" || strings.TrimSpace(lease.Token) == "" {
		return false
	}
	parsed, err := url.Parse(lease.WebSocketURL)
	return err == nil && (parsed.Scheme == "ws" || parsed.Scheme == "wss") && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}

var _ Registry = (*RedisRegistry)(nil)
