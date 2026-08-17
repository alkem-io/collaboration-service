package service

import (
	"context"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvLifecycleBounded — INV-LIFECYCLE-BOUNDED (spec 002 FR-014). A lifecycle
// event must be bounded per delivery on EVERY event type. document.access_changed →
// ReEvaluate enqueues onto a room; if that room's command buffer is full, the enqueue
// must give up at the handler's deadline so one busy room cannot head-of-line-block
// the single-threaded lifecycle consumer. RED before 002: handleAccessChanged took no
// ctx and ReEvaluate's enqueue was unbounded, so a full-buffer room blocked the
// consumer indefinitely.
func TestInvLifecycleBounded(t *testing.T) {
	m, deps := testManager(t, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second})
	id := model.DocumentID("doc-lifecycle-bounded")
	room := wedgeRoom(t, m, deps, id) // registered, no run loop → its command buffer is never drained

	// Fill the command buffer so the next enqueue must take the bounded slow path.
	for i := 0; i < cap(room.commands); i++ {
		room.commands <- command{kind: cmdMessage}
	}

	// A document.access_changed delivery, bounded by a short handler timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { m.ReEvaluate(ctx, id); close(done) }()

	select {
	case <-done:
		// good: ReEvaluate honoured the handler deadline instead of blocking on the
		// full buffer.
	case <-time.After(3 * time.Second):
		t.Fatal("ReEvaluate (access_changed) blocked on a full command buffer — a busy room head-of-line-blocks the lifecycle consumer past its deadline (FR-014)")
	}
}
