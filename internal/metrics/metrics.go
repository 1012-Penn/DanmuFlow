// Package metrics 提供 DanmuFlow 的进程内运行指标。
// 指标只使用有限集合的标签，避免把 room_id、user_id 这类高基数字段写入监控系统。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics 汇总 HTTP/WebSocket 网关和消息消费链路需要记录的运行指标。
// 每个 HTTP 服务实例拥有独立的 Registry，测试可以彼此隔离；生产环境由 Prometheus
// 分别抓取各个实例，再在查询时聚合。
type Metrics struct {
	registry *prometheus.Registry

	WebSocketConnections      prometheus.Gauge
	MessagesReceived          prometheus.Counter
	MessagesRejected          *prometheus.CounterVec
	KafkaPublishDuration      prometheus.Histogram
	ConsumerHandlerDuration   prometheus.Histogram
	RoomClientMessagesDropped prometheus.Counter
	ConsumerRunning           prometheus.Gauge
}

// New 创建一组尚未暴露给外部的指标。
// Registry 不使用 Prometheus 的全局默认实例，避免测试间重复注册，也让每个服务实例的
// 指标生命周期跟随自己的 HTTP Server。
func New() *Metrics {
	registry := prometheus.NewRegistry()

	metrics := &Metrics{
		registry: registry,
		WebSocketConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "danmuflow_websocket_connections_current",
			Help: "Current number of established WebSocket connections.",
		}),
		MessagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "danmuflow_messages_received_total",
			Help: "Total number of WebSocket message frames accepted for validation.",
		}),
		MessagesRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "danmuflow_messages_rejected_total",
			Help: "Total number of rejected messages grouped by a bounded rejection reason.",
		}, []string{"reason"}),
		KafkaPublishDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "danmuflow_kafka_publish_duration_seconds",
			Help:    "Time spent waiting for a WebSocket message to be published to Kafka.",
			Buckets: prometheus.DefBuckets,
		}),
		ConsumerHandlerDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "danmuflow_consumer_handler_duration_seconds",
			Help:    "Time spent broadcasting one consumed message to the local room.",
			Buckets: prometheus.DefBuckets,
		}),
		RoomClientMessagesDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "danmuflow_room_client_messages_dropped_total",
			Help: "Total broadcasts dropped because an individual client queue was full.",
		}),
		ConsumerRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "danmuflow_consumer_running",
			Help: "Whether this instance currently has a running message bus consumer (1=true, 0=false).",
		}),
	}

	registry.MustRegister(
		metrics.WebSocketConnections,
		metrics.MessagesReceived,
		metrics.MessagesRejected,
		metrics.KafkaPublishDuration,
		metrics.ConsumerHandlerDuration,
		metrics.RoomClientMessagesDropped,
		metrics.ConsumerRunning,
		prometheus.NewGoCollector(),
	)

	return metrics
}

// Handler 返回供 Prometheus 抓取的 HTTP Handler。
// 它在请求发生时读取当前指标，不会把业务数据持久化到磁盘或外部服务。
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
