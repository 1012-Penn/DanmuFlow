// Package ratelimit 提供进程内的并发安全限流器。
package ratelimit

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrInvalidRate 表示每秒补充令牌数不是正数。
	ErrInvalidRate = errors.New("rate must be greater than zero")
	// ErrInvalidBurst 表示令牌桶容量不是正数。
	ErrInvalidBurst = errors.New("burst must be greater than zero")
)

// Limiter 为每个 key 维护一个独立的令牌桶。
// mu 保护 buckets；每个 bucket 的令牌数量和最近更新时间都只能在锁内修改。
type Limiter struct {
	mu              sync.Mutex
	buckets         map[string]*bucket
	tokensPerSecond float64
	burst           float64
	now             func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New 创建一个按 key 独立限流的令牌桶限流器。
// tokensPerSecond 是每秒补充的令牌数，burst 是单个 key 可以积累的最大令牌数。
func New(tokensPerSecond float64, burst int) (*Limiter, error) {
	if tokensPerSecond <= 0 {
		return nil, ErrInvalidRate
	}
	if burst <= 0 {
		return nil, ErrInvalidBurst
	}

	return &Limiter{
		buckets:         make(map[string]*bucket),
		tokensPerSecond: tokensPerSecond,
		burst:           float64(burst),
		now:             time.Now,
	}, nil
}

// Allow 判断 key 当前是否可以消耗一个令牌。
// 第一次出现的 key 会获得完整的 burst 额度；之后令牌按时间补充，但不会超过 burst。
func (l *Limiter) Allow(key string) bool {
	if key == "" {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	current, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &bucket{
			tokens: l.burst - 1,
			last:   now,
		}
		return true
	}

	// 先根据距离上次访问的时间补充令牌，再决定本次是否允许通过。
	current.tokens = min(l.burst, current.tokens+now.Sub(current.last).Seconds()*l.tokensPerSecond)
	current.last = now
	if current.tokens < 1 {
		return false
	}

	current.tokens--
	return true
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
