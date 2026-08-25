package service

import (
	"context"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvLifecycleBounded — INV-LIFECYCLE-BOUNDED (spec 002 FR-014). A lifecycle
// event must be bounded per delivery. document.deleted → Purge enqueues onto the
// room; if that room's command buffer is full, the enqueue must give up rather
// than block, so one busy room cannot head-of-line-block the single-threaded
// lifecycle consumer.
//
// Purge is now the only lifecycle event — document.access_changed and the
// re-evaluation it drove were removed with the session-lifetime authorization
// contract. It reaches the room through enqueue (a background context), so what
// bounds it is enqueueCtx's own deadline backstop rather than the caller's ctx;
// that is what this pins. On give-up, Purge falls through to the direct durable
// purge, so the cascade still completes.
func TestInvLifecycleBounded(t *testing.T) {
	m, deps := testManager(t, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second})
	id := model.DocumentID("doc-lifecycle-bounded")
	room := wedgeRoom(t, m, deps, id) // registered, no run loop → its command buffer is never drained

	// Fill the command buffer so the next enqueue must take the bounded slow path.
	for i := 0; i < cap(room.commands); i++ {
		room.commands <- command{kind: cmdMessage}
	}

	// A document.deleted delivery against that wedged room.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = m.CloseDeleted(ctx, id); close(done) }()

	select {
	case <-done:
		// good: the enqueue gave up at its backstop instead of blocking on the full
		// buffer, and the cascade completed durably.
	case <-time.After(enqueueDeadline + 5*time.Second):
		t.Fatal("Purge (document.deleted) blocked on a full command buffer — a busy room head-of-line-blocks the single-threaded lifecycle consumer (FR-014)")
	}
}
