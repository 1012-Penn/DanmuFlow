package httpserver

import (
	"errors"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/routing"
)

func TestRoutingConfigValidation(t *testing.T) {
	valid := RoutingConfig{
		RedisAddress:       "localhost:6379",
		GatewayID:          "gateway-a",
		PublicWebSocketURL: "wss://gateway-a.example/ws",
		LeaseTTL:           6 * time.Second,
		PollInterval:       100 * time.Millisecond,
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid config error = %v", err)
	}
	disabled := RoutingConfig{}
	if err := disabled.validate(); err != nil {
		t.Fatalf("disabled config error = %v", err)
	}

	for name, mutate := range map[string]func(*RoutingConfig){
		"http URL":      func(config *RoutingConfig) { config.PublicWebSocketURL = "https://gateway-a.example/ws" },
		"URL query":     func(config *RoutingConfig) { config.PublicWebSocketURL += "?token=unsafe" },
		"URL user info": func(config *RoutingConfig) { config.PublicWebSocketURL = "wss://user:password@gateway-a.example/ws" },
		"missing Redis": func(config *RoutingConfig) { config.RedisAddress = "" },
		"invalid TTL":   func(config *RoutingConfig) { config.LeaseTTL = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if err := config.validate(); !errors.Is(err, routing.ErrInvalidConfig) {
				t.Fatalf("validate() error = %v, want %v", err, routing.ErrInvalidConfig)
			}
		})
	}
}
