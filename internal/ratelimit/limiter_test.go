package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestLimiterAllowsBurstThenRejects(t *testing.T) {
	limiter, err := New(1, 2)
	if err != nil {
		t.Fatal(err)
	}

	if !limiter.Allow("alice") {
		t.Fatal("limiter rejected the first request in the configured burst")
	}
	if !limiter.Allow("alice") {
		t.Fatal("limiter rejected the second request in the configured burst")
	}
	if limiter.Allow("alice") {
		t.Fatal("limiter allowed a third immediate request")
	}
}

func TestLimiterRefillsTokensOverTime(t *testing.T) {
	limiter, err := New(2, 1)
	if err != nil {
		t.Fatal(err)
	}

	currentTime := time.Unix(0, 0)
	limiter.now = func() time.Time { return currentTime }

	if !limiter.Allow("alice") {
		t.Fatal("first request was rejected")
	}
	if limiter.Allow("alice") {
		t.Fatal("request was allowed before refill")
	}

	currentTime = currentTime.Add(500 * time.Millisecond)
	if !limiter.Allow("alice") {
		t.Fatal("request was rejected after one token was refilled")
	}
}

func TestLimiterKeepsUsersIndependent(t *testing.T) {
	limiter, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !limiter.Allow("alice") || !limiter.Allow("bob") {
		t.Fatal("one user's request consumed another user's token")
	}
}

func TestLimiterIsSafeForConcurrentAccess(t *testing.T) {
	limiter, err := New(1, 10)
	if err != nil {
		t.Fatal(err)
	}

	var group sync.WaitGroup
	allowed := make(chan struct{}, 32)
	for i := 0; i < cap(allowed); i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if limiter.Allow("alice") {
				allowed <- struct{}{}
			}
		}()
	}

	group.Wait()
	if got := len(allowed); got != 10 {
		t.Fatalf("allowed requests = %d, want 10", got)
	}
}

func TestLimiterRejectsInvalidConfigurationAndKey(t *testing.T) {
	if _, err := New(0, 1); err != ErrInvalidRate {
		t.Fatalf("New error = %v, want %v", err, ErrInvalidRate)
	}
	if _, err := New(1, 0); err != ErrInvalidBurst {
		t.Fatalf("New error = %v, want %v", err, ErrInvalidBurst)
	}

	limiter, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if limiter.Allow("") {
		t.Fatal("empty key was allowed")
	}
}
