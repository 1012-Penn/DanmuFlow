package httpserver

import (
	"os"
	"strings"

	"github.com/1012-Penn/DanmuFlow/internal/bus"
)

const (
	defaultKafkaBroker = "localhost:9092"
	defaultKafkaTopic  = "danmaku"
	defaultKafkaGroup  = "danmuflow-broadcast"
)

// KafkaConfigFromEnv 从环境变量读取 HTTP 服务使用的 Kafka 配置。
// 这样 New、NewWithLogger 和命令行启动入口共享同一套默认值，避免调用方
// 在无 Kafka 环境下因为不同构造函数得到不一致的连接地址。
func KafkaConfigFromEnv() bus.KafkaConfig {
	return bus.KafkaConfig{
		Brokers: splitCSVEnv("DANMUFLOW_KAFKA_BROKERS", defaultKafkaBroker),
		Topic:   envOrDefault("DANMUFLOW_KAFKA_TOPIC", defaultKafkaTopic),
		GroupID: envOrDefault("DANMUFLOW_KAFKA_GROUP_ID", defaultKafkaGroup),
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitCSVEnv(name, fallback string) []string {
	parts := strings.Split(envOrDefault(name, fallback), ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
