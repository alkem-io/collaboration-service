package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend/persistence"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// The teardown-flush matrix (FR-011a, SC-018).
//
// Teardown flushes ONLY when the document is believed good:
//
//	graceful shutdown ................. FLUSH
//	idle release with unsaved changes . FLUSH
//	generation invalidation ........... NO FLUSH
//	escalation after write failure .... NO FLUSH   (US2, not yet implemented)
//	panic on the processing path ...... NO FLUSH   (002 OBS-1, already ratcheted)
//
// The "no flush" half is the half worth defending: a poisoned or half-mutated
// document written over good stored content is silent corruption, and the
// obvious implementation ("always flush on teardown") produces exactly that.

// countingStore counts saves so a test can assert whether a teardown path
// persisted, without depending on what it wrote.
type countingStore struct {
	inner *persistinprocess.Store
	saves atomic.Int64
}

func newCountingStore() *countingStore {
	return &countingStore{inner: persistinprocess.New()}
}

func (s *countingStore) SaveCheckpoint(ctx context.Context, req persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	s.saves.Add(1)
	return s.inner.SaveCheckpoint(ctx, req)
}

func (s *countingStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	return s.inner.LoadCheckpoint(ctx, id)
}

func (s *countingStore) DeleteCheckpoint(ctx context.Context, id backend.DocumentID) error {
	return s.inner.DeleteCheckpoint(ctx, id)
}

func (s *countingStore) FenceMode() persistence.FenceMode { return s.inner.FenceMode() }

// dirtyRoomWithRegistry builds a started room, backed by a shared registry so the
// test can invalidate the document out from under it, and leaves it dirty with
// unsaved content.
func dirtyRoomWithRegistry(t *testing.T, id model.DocumentID) (*Room, *memory.InProcessRegistry, *countingStore) {
	t.Helper()
	deps := newTestDeps()
	store := newCountingStore()
	deps.Checkpoint = store
	reg := memory.NewRegistry()
	deps.Registry = reg

	cfg := DefaultRoomConfig()
	// Long timers: the test drives teardown explicitly, so neither the save
	// debounce nor the idle timer may fire on its own and confuse the count.
	cfg.SaveDebounce = time.Hour
	cfg.IdleTimeout = time.Hour

	room, err := newRoom(context.Background(), id, model.ContentTypeMemo, deps.Deps, cfg, NopMetrics{}, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	insertText(room.doc, "unsaved-content")
	room.dirty = true
	return room, reg, store
}

// TestTeardownDoesNotFlushAnInvalidatedDocument is the FR-011a ratchet for the
// dangerous half of the matrix. When the registry poisons a document's
// generation, the in-memory copy may have diverged from durable state, so the
// room must tear down WITHOUT persisting — writing it would overwrite good
// stored content with content of unknown provenance.
//
// Non-vacuity: change the invalidation case in run() to teardown(r.persistNow)
// and the Put count becomes 1, tripping the assertion.
func TestTeardownDoesNotFlushAnInvalidatedDocument(t *testing.T) {
	room, reg, store := dirtyRoomWithRegistry(t, "doc-invalidated")
	startRoom(room)

	if err := reg.Invalidate(context.Background(), backend.DocumentID("doc-invalidated")); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	select {
	case <-room.done:
	case <-time.After(2 * time.Second):
		t.Fatal("invalidation did not tear the room down")
	}

	if n := store.saves.Load(); n != 0 {
		t.Fatalf("an invalidated document was persisted %d time(s); a poisoned generation must NOT be written over stored content", n)
	}
}

// TestIdleReleaseFlushesAGoodDocument is the other half of the matrix: a room
// releasing because it is empty holds a document believed good, so the release
// MUST persist. Without this, idling out silently costs a window of edits.
//
// This drives releaseIfEmpty directly rather than waiting on the idle timer: the
// timer is only ARMED by join/leave, so a room that never had a member never
// idles out at all. The decision point is what matters here, and it is the same
// one the timer reaches.
//
// Non-vacuity: change releaseIfEmpty to teardown(nil) and the Put count becomes
// 0, tripping the assertion.
func TestIdleReleaseFlushesAGoodDocument(t *testing.T) {
	room, _, store := dirtyRoomWithRegistry(t, "doc-idle")

	if !room.releaseIfEmpty() {
		t.Fatal("an empty room must be released")
	}
	select {
	case <-room.done:
	default:
		t.Fatal("release did not tear the room down")
	}

	if n := store.saves.Load(); n != 1 {
		t.Fatalf("idle release persisted %d time(s), want exactly 1; a good document must not be dropped on release", n)
	}
}

// TestReleaseIfEmptyKeepsARoomWithMembers guards the condition itself: the idle
// timer firing while members are still attached must NOT release the room.
func TestReleaseIfEmptyKeepsARoomWithMembers(t *testing.T) {
	room, _, _ := dirtyRoomWithRegistry(t, "doc-populated")
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}

	if room.releaseIfEmpty() {
		t.Fatal("a room with members must not be released by the idle path")
	}
	select {
	case <-room.done:
		t.Fatal("releaseIfEmpty tore down a room that still had members")
	default:
	}
}

// TestInvalidatedHandleSignalsWithoutRevoking pins the cooperative nature of the
// invalidation signal, and the operational consequence that follows.
//
// The core is explicit: Done is "a cooperative signal, not capability
// revocation" — a holder that ignores it keeps using the *crdt.Doc it already
// obtained. Invalidate therefore POISONS FIRST and then WAITS for outstanding
// handles to release, with the wait bounded by the caller's context.
//
// Two things follow, both asserted here:
//  1. a non-cooperative holder cannot prevent poisoning, only delay destruction;
//  2. any caller of Invalidate MUST bound it with a context, or a wedged holder
//     blocks it indefinitely. This is why the room acts on Done itself rather
//     than relying on the core to stop it.
func TestInvalidatedHandleSignalsWithoutRevoking(t *testing.T) {
	reg := memory.NewRegistry()
	handle, err := reg.Acquire(context.Background(), backend.DocumentID("doc-coop"), func(context.Context) (*ycrdt.Doc, error) {
		return newRoomDoc("doc-coop"), nil
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	doc := handle.Doc()

	// Deliberately do NOT release: model a holder that ignores the signal.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = reg.Invalidate(ctx, backend.DocumentID("doc-coop"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invalidate with an unreleased handle = %v, want the wait to be bounded by the context", err)
	}

	// Poison is installed BEFORE the wait, so the signal fires even though the
	// wait timed out.
	select {
	case <-handle.Done():
	default:
		t.Fatal("Done did not close: poisoning must precede the drain wait")
	}
	// The document remains usable — precisely why the room must stop on its own.
	if doc == nil {
		t.Fatal("Doc became nil after invalidation; the contract says the holder keeps what it obtained")
	}
	// Release is idempotent and completes destruction after the fact.
	handle.Release()
	handle.Release()
}
