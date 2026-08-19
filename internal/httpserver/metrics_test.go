package httpserver

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestMetricsEndpointExposesBaselineMetrics 验证 Prometheus 能抓取第一轮基线所需的指标。
func TestMetricsEndpointExposesBaselineMetrics(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()

	newTestServer().Handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, metricName := range []string{
		"danmuflow_websocket_connections_current",
		"danmuflow_messages_received_total",
		"danmuflow_kafka_publish_duration_seconds",
		"danmuflow_consumer_handler_duration_seconds",
		"danmuflow_room_client_messages_dropped_total",
		"danmuflow_consumer_running",
		"go_goroutines",
	} {
		if !strings.Contains(body, metricName) {
			t.Fatalf("/metrics response does not contain %q:\n%s", metricName, body)
		}
	}
}

// TestMetricsEndpointRecordsWebSocketMessage 验证指标会记录真实的 WebSocket 接入、发布与消费事件。
func TestMetricsEndpointRecordsWebSocketMessage(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(newTestServer().Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(websocketRequest{Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var response websocketResponse
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}

	responseMetrics, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer responseMetrics.Body.Close()
	if responseMetrics.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", responseMetrics.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(responseMetrics.Body)
	if err != nil {
		t.Fatal(err)
	}
	metricsText := string(body)
	for _, expected := range []string{
		"danmuflow_websocket_connections_current 1",
		"danmuflow_messages_received_total 1",
		"danmuflow_kafka_publish_duration_seconds_count 1",
		"danmuflow_consumer_handler_duration_seconds_count 1",
	} {
		if !strings.Contains(metricsText, expected) {
			t.Fatalf("/metrics response does not contain %q:\n%s", expected, metricsText)
		}
	}
}

// TestMetricsDoNotClassifyDisconnectedClientAsInvalidJSON 验证连接中断不是一条待校验的 JSON 消息。
func TestMetricsDoNotClassifyDisconnectedClientAsInvalidJSON(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("environment does not allow local network listeners: %v", err)
	}

	server := httptest.NewUnstartedServer(newTestServer().Handler)
	server.Listener = listener
	server.Start()
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room_id=room-a&user_id=alice"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	connectionDeadline := time.Now().Add(time.Second)
	for {
		responseMetrics, err := server.Client().Get(server.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(responseMetrics.Body)
		responseMetrics.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(body), "danmuflow_websocket_connections_current 1") {
			break
		}
		if time.Now().After(connectionDeadline) {
			t.Fatal("server did not record established WebSocket client")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 直接关闭底层连接，模拟客户端来不及完成 WebSocket Close 握手的场景。
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		responseMetrics, err := server.Client().Get(server.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(responseMetrics.Body)
		responseMetrics.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !strings.Contains(string(body), "danmuflow_websocket_connections_current 1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not clean up disconnected WebSocket client")
		}
		time.Sleep(10 * time.Millisecond)
	}

	responseMetrics, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer responseMetrics.Body.Close()
	body, err := io.ReadAll(responseMetrics.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), `danmuflow_messages_rejected_total{reason="invalid_json"}`) {
		t.Fatalf("disconnected client was classified as invalid JSON:\n%s", body)
	}
}
