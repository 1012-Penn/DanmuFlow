package main

import (
	"testing"
	"time"
)

func TestRecordSelfDeliveryCountsDuplicateSeparately(t *testing.T) {
	messageID := "loadtest:alice:123"
	results := &stats{
		deliveredMessageIDs: make(map[string]struct{}),
		latencies:           make([]time.Duration, 0, 1),
	}

	results.recordSelfDelivery(messageID, time.Now().Add(-time.Millisecond))
	results.recordSelfDelivery(messageID, time.Now().Add(-time.Millisecond))

	if results.selfDelivered != 1 {
		t.Fatalf("unique self delivery = %d, want 1", results.selfDelivered)
	}
	if results.duplicateSelfDelivered != 1 {
		t.Fatalf("duplicate self delivery = %d, want 1", results.duplicateSelfDelivered)
	}
	if len(results.latencies) != 1 {
		t.Fatalf("latency samples = %d, want 1", len(results.latencies))
	}
}
