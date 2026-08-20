//go:build integration

package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
	"github.com/1012-Penn/DanmuFlow/internal/routing"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap/zaptest"
)

func TestKafkaOwnershipIsPublishedToRedisAndResolvedByHTTP(t *testing.T) {
	kafkaAddress := os.Getenv("DANMUFLOW_KAFKA_BROKERS")
	redisAddress := os.Getenv("DANMUFLOW_REDIS_ADDR")
	if kafkaAddress == "" || redisAddress == "" {
		t.Skip("set DANMUFLOW_KAFKA_BROKERS and DANMUFLOW_REDIS_ADDR to run routing integration")
	}
	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "danmuflow-routing-integration-" + unique
	groupID := "danmuflow-routing-integration-" + unique
	prefix := "danmuflow:routing-integration:" + unique
	brokers := strings.Split(kafkaAddress, ",")
	connection, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	server, err := NewWithKafkaAndRouting(":0", bus.KafkaConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	}, RoutingConfig{
		RedisAddress:       redisAddress,
		RedisKeyPrefix:     prefix,
		GatewayID:          "integration-gateway",
		PublicWebSocketURL: "ws://integration.example/ws",
		LeaseTTL:           2 * time.Second,
		PollInterval:       50 * time.Millisecond,
	}, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				t.Errorf("Shutdown() error = %v", err)
			}
		})
	}
	t.Cleanup(shutdown)

	var response struct {
		Partition    int    `json:"partition"`
		GatewayID    string `json:"gateway_id"`
		WebSocketURL string `json:"websocket_url"`
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/route?room_id=room-a", nil))
		if recorder.Code == http.StatusOK {
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if response.GatewayID != "integration-gateway" || response.WebSocketURL != "ws://integration.example/ws" {
		t.Fatalf("route response = %+v", response)
	}

	shutdown()
	registry, err := routing.NewRedisRegistry(routing.RedisConfig{
		Address:   redisAddress,
		KeyPrefix: fmt.Sprintf("%s:%s:%s", prefix, topic, groupID),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if _, err := registry.Resolve(context.Background(), response.Partition); !errors.Is(err, routing.ErrRouteNotFound) {
		t.Fatalf("route after shutdown error = %v, want %v", err, routing.ErrRouteNotFound)
	}
}
