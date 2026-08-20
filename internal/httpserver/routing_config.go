package httpserver

import (
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/routing"
)

const (
	defaultRedisAddress        = "localhost:6379"
	defaultRoutingLeaseTTL     = 6 * time.Second
	defaultRoutingPollInterval = 100 * time.Millisecond
)

// RoutingConfig 控制 Redis 网关目录。PublicWebSocketURL 为空时保持单实例兼容模式，
// 不连接 Redis，也不发布 /route 可用结果。
type RoutingConfig struct {
	RedisAddress       string
	RedisPassword      string
	RedisDB            int
	RedisKeyPrefix     string
	GatewayID          string
	PublicWebSocketURL string
	LeaseTTL           time.Duration
	PollInterval       time.Duration
}

// RoutingConfigFromEnv 读取网关路由配置。
func RoutingConfigFromEnv() RoutingConfig {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "unknown-gateway"
	}
	return RoutingConfig{
		RedisAddress:       envOrDefault("DANMUFLOW_REDIS_ADDR", defaultRedisAddress),
		RedisPassword:      os.Getenv("DANMUFLOW_REDIS_PASSWORD"),
		RedisKeyPrefix:     envOrDefault("DANMUFLOW_REDIS_ROUTE_PREFIX", "danmuflow:routing"),
		GatewayID:          envOrDefault("DANMUFLOW_GATEWAY_ID", hostname),
		PublicWebSocketURL: strings.TrimSpace(os.Getenv("DANMUFLOW_PUBLIC_WS_URL")),
		LeaseTTL:           defaultRoutingLeaseTTL,
		PollInterval:       defaultRoutingPollInterval,
	}
}

func (config RoutingConfig) enabled() bool {
	return strings.TrimSpace(config.PublicWebSocketURL) != ""
}

func (config RoutingConfig) validate() error {
	if !config.enabled() {
		return nil
	}
	parsed, err := url.Parse(config.PublicWebSocketURL)
	if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return routing.ErrInvalidConfig
	}
	if strings.TrimSpace(config.RedisAddress) == "" || strings.TrimSpace(config.GatewayID) == "" ||
		config.RedisDB < 0 || config.LeaseTTL <= 0 || config.PollInterval <= 0 || config.PollInterval >= config.LeaseTTL {
		return routing.ErrInvalidConfig
	}
	return nil
}
