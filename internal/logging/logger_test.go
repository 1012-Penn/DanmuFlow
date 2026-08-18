package logging

import "testing"

func TestNewRejectsInvalidLogLevel(t *testing.T) {
	t.Setenv("DANMUFLOW_LOG_LEVEL", "not-a-level")

	logger, err := New()
	if err == nil {
		logger.Sync()
		t.Fatal("New() error = nil, want invalid log level error")
	}
}

func TestNewAcceptsConfiguredLogLevel(t *testing.T) {
	t.Setenv("DANMUFLOW_LOG_LEVEL", "debug")

	logger, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer logger.Sync()
}
