package httpserver

import "testing"

func TestKafkaConfigFromEnvUsesDefaults(t *testing.T) {
	t.Setenv("DANMUFLOW_KAFKA_BROKERS", "")
	t.Setenv("DANMUFLOW_KAFKA_TOPIC", "")
	t.Setenv("DANMUFLOW_KAFKA_GROUP_ID", "")

	config := KafkaConfigFromEnv()
	if len(config.Brokers) != 1 || config.Brokers[0] != defaultKafkaBroker {
		t.Fatalf("Brokers = %+v, want [%q]", config.Brokers, defaultKafkaBroker)
	}
	if config.Topic != defaultKafkaTopic || config.GroupID != defaultKafkaGroup {
		t.Fatalf("config = %+v, want topic=%q group=%q", config, defaultKafkaTopic, defaultKafkaGroup)
	}
}

func TestKafkaConfigFromEnvSplitsBrokers(t *testing.T) {
	t.Setenv("DANMUFLOW_KAFKA_BROKERS", "broker-a:9092, broker-b:9092")
	t.Setenv("DANMUFLOW_KAFKA_TOPIC", "custom-topic")
	t.Setenv("DANMUFLOW_KAFKA_GROUP_ID", "custom-group")

	config := KafkaConfigFromEnv()
	if len(config.Brokers) != 2 || config.Brokers[0] != "broker-a:9092" || config.Brokers[1] != "broker-b:9092" {
		t.Fatalf("Brokers = %+v, want two trimmed brokers", config.Brokers)
	}
	if config.Topic != "custom-topic" || config.GroupID != "custom-group" {
		t.Fatalf("config = %+v, want custom topic and group", config)
	}
}
