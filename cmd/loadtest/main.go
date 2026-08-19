// Command loadtest 是 DanmuFlow 第一轮基线压测使用的 WebSocket 客户端。
// 它用固定数量的连接和发送速率产生可重复流量，并只统计“发送者是否收回自己的弹幕”
// 的端到端延迟，避免把所有房间成员的重复广播都算作独立发送成功。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const messagePrefix = "loadtest"

type config struct {
	websocketURL string
	connections  int
	rooms        int
	rate         int
	duration     time.Duration
}

type request struct {
	Content string `json:"content"`
}

type response struct {
	Content string `json:"content"`
}

// stats 只记录压测结果；它的锁不保护 WebSocket 连接，也不参与服务端业务链路。
type stats struct {
	mu sync.Mutex

	sent          int
	received      int
	selfDelivered int
	writeErrors   int
	readErrors    int
	latencies     []time.Duration
}

func main() {
	config := parseConfig()
	if err := validateConfig(config); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clients, results, closeClients, err := connectClients(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer closeClients()

	log.Printf("load test started: connections=%d rooms=%d rate=%d messages/s duration=%s", config.connections, config.rooms, config.rate, config.duration)
	startedAt := time.Now()
	runTraffic(ctx, clients, config.rate, config.duration, results)
	// 给已经发布到 Kafka、但尚未广播回来的最后一小批消息一个接收窗口。
	time.Sleep(500 * time.Millisecond)
	printReport(time.Since(startedAt), results)
}

func parseConfig() config {
	var config config
	flag.StringVar(&config.websocketURL, "url", "ws://localhost:8080/ws", "WebSocket endpoint without room_id and user_id query parameters")
	flag.IntVar(&config.connections, "connections", 100, "number of WebSocket connections to establish")
	flag.IntVar(&config.rooms, "rooms", 10, "number of rooms to distribute connections across")
	flag.IntVar(&config.rate, "rate", 100, "total messages to send per second across all connections")
	flag.DurationVar(&config.duration, "duration", 5*time.Minute, "how long to send messages")
	flag.Parse()
	return config
}

func validateConfig(config config) error {
	switch {
	case config.connections <= 0:
		return fmt.Errorf("connections must be greater than zero")
	case config.rooms <= 0:
		return fmt.Errorf("rooms must be greater than zero")
	case config.rooms > config.connections:
		return fmt.Errorf("rooms cannot exceed connections: each room must have a receiving connection")
	case config.rate <= 0:
		return fmt.Errorf("rate must be greater than zero")
	case config.rate > int(time.Second):
		return fmt.Errorf("rate must not exceed %d messages/s", int(time.Second))
	case config.duration <= 0:
		return fmt.Errorf("duration must be greater than zero")
	default:
		return nil
	}
}

func connectClients(ctx context.Context, config config) ([]*websocket.Conn, *stats, func(), error) {
	clients := make([]*websocket.Conn, 0, config.connections)
	results := &stats{latencies: make([]time.Duration, 0, config.rate*int(config.duration/time.Second))}

	for clientIndex := range config.connections {
		roomID := fmt.Sprintf("load-room-%d", clientIndex%config.rooms)
		userID := fmt.Sprintf("load-user-%d", clientIndex)
		url := fmt.Sprintf("%s?room_id=%s&user_id=%s", config.websocketURL, roomID, userID)
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
		if err != nil {
			for _, connectedClient := range clients {
				_ = connectedClient.Close()
			}
			return nil, nil, nil, fmt.Errorf("connect client %d: %w", clientIndex, err)
		}
		clients = append(clients, conn)
		go readMessages(conn, userID, results)
	}

	return clients, results, func() {
		for _, client := range clients {
			// 发送 Close 控制帧，让服务端能把压测结束识别为正常断连；此时发送循环已经结束，
			// 因此不会与其他 goroutine 并发写同一个 WebSocket 连接。
			_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "load test finished"), time.Now().Add(time.Second))
			_ = client.Close()
		}
	}, nil
}

func runTraffic(ctx context.Context, clients []*websocket.Conn, rate int, duration time.Duration, results *stats) {
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timeLimit := time.NewTimer(duration)
	defer timeLimit.Stop()

	clientIndex := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeLimit.C:
			return
		case <-ticker.C:
			userID := fmt.Sprintf("load-user-%d", clientIndex)
			sentAt := time.Now()
			content := fmt.Sprintf("%s:%s:%d", messagePrefix, userID, sentAt.UnixNano())
			if err := clients[clientIndex].WriteJSON(request{Content: content}); err != nil {
				results.mu.Lock()
				results.writeErrors++
				results.mu.Unlock()
			} else {
				results.mu.Lock()
				results.sent++
				results.mu.Unlock()
			}
			clientIndex = (clientIndex + 1) % len(clients)
		}
	}
}

func readMessages(conn *websocket.Conn, userID string, results *stats) {
	for {
		var message response
		if err := conn.ReadJSON(&message); err != nil {
			results.mu.Lock()
			results.readErrors++
			results.mu.Unlock()
			return
		}

		results.mu.Lock()
		results.received++
		results.mu.Unlock()

		parts := strings.Split(message.Content, ":")
		if len(parts) != 3 || parts[0] != messagePrefix || parts[1] != userID {
			continue
		}
		sentAt, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}

		results.mu.Lock()
		results.selfDelivered++
		results.latencies = append(results.latencies, time.Since(time.Unix(0, sentAt)))
		results.mu.Unlock()
	}
}

func printReport(elapsed time.Duration, results *stats) {
	results.mu.Lock()
	defer results.mu.Unlock()

	sort.Slice(results.latencies, func(i, j int) bool {
		return results.latencies[i] < results.latencies[j]
	})
	deliveryRate := 0.0
	if results.sent > 0 {
		deliveryRate = float64(results.selfDelivered) / float64(results.sent) * 100
	}

	log.Printf("load test finished: elapsed=%s sent=%d self_delivered=%d delivery_rate=%.2f%% received_total=%d write_errors=%d read_errors=%d", elapsed.Round(time.Millisecond), results.sent, results.selfDelivered, deliveryRate, results.received, results.writeErrors, results.readErrors)
	if len(results.latencies) == 0 {
		log.Print("latency: no sender echo was received")
		return
	}
	log.Printf("latency: p50=%s p95=%s p99=%s max=%s", percentile(results.latencies, 50), percentile(results.latencies, 95), percentile(results.latencies, 99), results.latencies[len(results.latencies)-1])
}

func percentile(sorted []time.Duration, percentile int) time.Duration {
	index := (len(sorted) - 1) * percentile / 100
	return sorted[index]
}
