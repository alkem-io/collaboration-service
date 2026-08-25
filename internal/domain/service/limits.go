package service

import "time"

// tokenBucket is an optional per-connection update-rate limiter (FR-024). It
// admits up to `burst` messages immediately and refills at `rate`
// tokens per second; a drained bucket rejects until a token refills. A zero rate
// disables limiting (the default), keeping rate limiting opt-in and config-tunable.
// It is not safe for concurrent use — each bucket is owned by one
// room run loop, the single writer (so no lock is needed, matching the room's
// single-writer invariant).
type tokenBucket struct {
	rate    float64 // tokens per second; 0 disables limiting
	burst   float64 // bucket depth (max accumulated tokens)
	tokens  float64
	last    time.Time
	nowFunc func() time.Time
}

// newTokenBucket builds a bucket admitting `burst` immediately and refilling at
// `ratePerSec`. A nil clock falls back to time.Now. A rate of 0 disables limiting.
func newTokenBucket(ratePerSec, burst int, nowFunc func() time.Time) *tokenBucket {
	if nowFunc == nil {
		nowFunc = time.Now
	}
	b := float64(burst)
	if b <= 0 {
		b = float64(ratePerSec)
	}
	return &tokenBucket{
		rate:    float64(ratePerSec),
		burst:   b,
		tokens:  b,
		last:    nowFunc(),
		nowFunc: nowFunc,
	}
}

// allow admits one message if a token is available, consuming it; it refills the
// bucket by the elapsed time first. A zero-rate bucket always admits.
func (b *tokenBucket) allow() bool {
	if b.rate <= 0 {
		return true
	}
	now := b.nowFunc()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
