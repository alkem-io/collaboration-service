package service

import (
	"testing"
	"time"
)

func TestProtectiveDefaultsDoNotChangeOrdinaryTraffic(t *testing.T) {
	limits := DefaultLimits()
	if limits.UpdateRatePerSec != 0 || limits.UpdateBurst != 0 {
		t.Fatalf("update-rate defaults = rate %d, burst %d; want disabled", limits.UpdateRatePerSec, limits.UpdateBurst)
	}

	room := DefaultRoomConfig()
	if room.ContributionWindow != 10*time.Minute {
		t.Fatalf("contribution window default = %v, want 10m", room.ContributionWindow)
	}
}

// TestTokenBucketAllowsBurstThenRefills asserts the token bucket admits up to its
// burst depth immediately, rejects once drained, then refills at the configured
// rate (the per-connection update-rate limiter, FR-024).
func TestTokenBucketAllowsBurstThenRefills(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	// 10 msg/s, burst 3.
	b := newTokenBucket(10, 3, clock)

	// Burst of 3 is admitted from a full bucket.
	for i := 0; i < 3; i++ {
		if !b.allow() {
			t.Fatalf("token %d should be admitted from the initial burst", i)
		}
	}
	// The 4th is rejected — the bucket is drained.
	if b.allow() {
		t.Fatal("4th token should be rejected (bucket drained)")
	}

	// After 0.1s one token refills (10/s).
	now = now.Add(100 * time.Millisecond)
	if !b.allow() {
		t.Fatal("a token should have refilled after 100ms at 10/s")
	}
	if b.allow() {
		t.Fatal("only one token should have refilled")
	}
}

// TestTokenBucketDisabledWhenRateZero asserts a zero rate disables limiting (the
// bucket always admits) so limits stay opt-in / config-tunable.
func TestTokenBucketDisabledWhenRateZero(t *testing.T) {
	b := newTokenBucket(0, 0, time.Now)
	for i := 0; i < 1000; i++ {
		if !b.allow() {
			t.Fatal("a zero-rate bucket must never reject")
		}
	}
}

// TestTokenBucketCapsAtBurst asserts accumulated tokens never exceed the burst
// depth (a long idle period does not grant an unbounded burst).
func TestTokenBucketCapsAtBurst(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	b := newTokenBucket(10, 2, clock)
	b.allow()
	b.allow() // drain

	// Idle for 10s — would refill 100 tokens, but the cap is the burst (2).
	now = now.Add(10 * time.Second)
	first := b.allow()
	second := b.allow()
	if !first || !second {
		t.Fatal("burst tokens should be available after a long idle")
	}
	if b.allow() {
		t.Fatal("tokens accumulated beyond the burst cap")
	}
}
