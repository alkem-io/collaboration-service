package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// blockingBroadcaster is a ClusterBroadcaster whose Publish blocks until its
// context is cancelled (deadline or parent cancel), recording how many publishes
// unblocked via context. It proves the room bounds an uncancelable backend call
// with BackendTimeout (and cancels it on release) so a hung backend cannot wedge
// the single-writer run loop.
type blockingBroadcaster struct {
	mu         sync.Mutex
	unblocked  int
	ctxErrSeen bool
}

func (b *blockingBroadcaster) Publish(ctx context.Context, _ model.DocumentID, _ []byte, _ bool) error {
	<-ctx.Done() // block until the room bounds/cancels us.
	b.mu.Lock()
	b.unblocked++
	if ctx.Err() != nil {
		b.ctxErrSeen = true
	}
	b.mu.Unlock()
	return ctx.Err()
}

func (b *blockingBroadcaster) Subscribe(_ context.Context, _ model.DocumentID, _ func([]byte, bool)) (func(), error) {
	return func() {}, nil
}

func (b *blockingBroadcaster) stats() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unblocked, b.ctxErrSeen
}

// TestBackendCallIsTimeoutBounded asserts a hung backend publish on the room run
// loop is bounded by BackendTimeout (CR Major: avoid uncancelable I/O on the
// single-writer loop). A client edit triggers publishToPeers; the broadcaster
// blocks on ctx.Done(); the room's bounded opCtx must time it out so the call
// returns rather than wedging the loop forever.
func TestBackendCallIsTimeoutBounded(t *testing.T) {
	bc := &blockingBroadcaster{}
	cfg := fastConfig()
	cfg.BackendTimeout = 50 * time.Millisecond
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	deps.Broadcaster = bc
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "hung-backend", model.ContentTypeMemo)
	a.observeUpdates()

	// One edit → the doc-update observer publishes to peers, which blocks until the
	// bounded context times out. The publish must unblock via context, proving the
	// loop is not permanently wedged behind the hung backend.
	a.insertText("ping")

	waitFor(t, "hung publish unblocks via bounded context", func() bool {
		n, ctxErr := bc.stats()
		return n >= 1 && ctxErr
	})
}

// TestOpCtxDefaultsAndNilParent asserts opCtx is robust: it falls back to the
// default timeout when BackendTimeout is unset, and roots at Background when the
// room context is nil (a bare Room built directly in a test), never panicking.
func TestOpCtxDefaultsAndNilParent(t *testing.T) {
	// Bare room (no newRoom): r.ctx is nil, cfg.BackendTimeout is 0.
	r := &Room{}
	ctx, cancel := r.opCtx()
	defer cancel()
	if ctx == nil {
		t.Fatal("opCtx returned a nil context")
	}
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("opCtx context has no deadline (timeout not applied)")
	}
	// The deadline should be ~defaultBackendTimeout out (allow generous slack).
	if until := time.Until(dl); until <= 0 || until > defaultBackendTimeout+time.Second {
		t.Fatalf("opCtx deadline %v is not ~defaultBackendTimeout (%v)", until, defaultBackendTimeout)
	}

	// With an explicit (short) BackendTimeout, opCtx honors it.
	r2 := &Room{cfg: RoomConfig{BackendTimeout: 5 * time.Millisecond}}
	ctx2, cancel2 := r2.opCtx()
	defer cancel2()
	dl2, ok2 := ctx2.Deadline()
	if !ok2 || time.Until(dl2) > time.Second {
		t.Fatalf("opCtx did not honor the configured BackendTimeout")
	}
}

// TestHungBackendDoesNotWedgeRoomRelease asserts a hung backend call on the run
// loop does not permanently wedge the room: bounded by BackendTimeout, the call
// returns, the loop drains its backlog, and the now-empty room still releases (its
// goroutine is reclaimed). Before the fix a context.Background() publish could
// block the single-writer loop indefinitely, leaking the room.
func TestHungBackendDoesNotWedgeRoomRelease(t *testing.T) {
	bc := &blockingBroadcaster{}
	cfg := fastConfig()
	cfg.BackendTimeout = 50 * time.Millisecond
	cfg.IdleTimeout = 10 * time.Millisecond
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	deps.Broadcaster = bc
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "release-after-hang", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("ping") // a publish that blocks until BackendTimeout bounds it
	a.session.Leave()    // empty the room

	// The bounded publish returns, the loop processes the leave, the idle timer
	// fires, and the room releases — RoomCount returns to zero despite the hang.
	waitFor(t, "room releases despite hung backend", func() bool {
		n, _ := bc.stats()
		return n >= 1 && mgr.RoomCount() == 0
	})
}
