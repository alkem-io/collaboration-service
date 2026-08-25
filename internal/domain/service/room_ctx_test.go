package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/persistence"
	"go.uber.org/zap"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// blockingBroadcaster is a ClusterBroadcaster whose Publish blocks until its
// context is cancelled (deadline or parent cancel), recording how many publishes
// unblocked via context. It proves the room bounds an uncancelable backend call
// with BackendTimeout (and cancels it on release) so a hung backend cannot wedge
// the single-writer run loop.
type blockingHub struct {
	mu         sync.Mutex
	unblocked  int
	ctxErrSeen bool
}

func (b *blockingHub) Publish(ctx context.Context, _ hub.Message) error {
	<-ctx.Done() // block until the room bounds/cancels us.
	b.mu.Lock()
	b.unblocked++
	if ctx.Err() != nil {
		b.ctxErrSeen = true
	}
	b.mu.Unlock()
	return ctx.Err()
}

func (b *blockingHub) Subscribe(context.Context, backend.DocumentID, backend.SourceID, hub.Handler) (hub.Subscription, error) {
	return inertSubscription{}, nil
}

func (b *blockingHub) Close() error { return nil }

// inertSubscription is a hub.Subscription that does nothing on close.
type inertSubscription struct{}

func (inertSubscription) SourceID() backend.SourceID { return "" }
func (inertSubscription) Close() error               { return nil }

func (b *blockingHub) stats() (int, bool) {
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
	bc := &blockingHub{}
	cfg := fastConfig()
	cfg.BackendTimeout = 50 * time.Millisecond
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	deps.Hub = bc
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
	bc := &blockingHub{}
	cfg := fastConfig()
	cfg.BackendTimeout = 50 * time.Millisecond
	cfg.IdleTimeout = 10 * time.Millisecond
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	deps.Hub = bc
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

// hangingLoadStore blocks inside LoadCheckpoint until its context is cancelled,
// modelling a checkpoint backend that has stopped answering.
type hangingLoadStore struct {
	*persistinprocess.Store
	entered chan struct{}
	once    sync.Once
}

func (h *hangingLoadStore) LoadCheckpoint(ctx context.Context, _ backend.DocumentID) (persistence.Checkpoint, error) {
	h.once.Do(func() { close(h.entered) })
	<-ctx.Done()
	return persistence.Checkpoint{}, ctx.Err()
}

// TestMaterializationIsBoundedWhenTheCheckpointStoreHangs is the regression for
// an unbounded first-load found by independent review.
//
// newRoom builds a bounded loadCtx for its materialization I/O, but passed the
// UNBOUNDED context into Registry.Acquire — and the restore happens inside the
// open function, so LoadCheckpoint ran with no deadline at all.
//
// Nothing else could free it. The room is not yet in the Manager's map, so
// shutdown cannot find it to cancel; Registry.Close sees an initializing entry
// and reports ErrInUse. The first-connect cohort coalescing on that open would
// wait indefinitely, holding their WebSocket connections.
//
// Non-vacuity: pass `ctx` to Acquire again and this times out instead of
// returning.
func TestMaterializationIsBoundedWhenTheCheckpointStoreHangs(t *testing.T) {
	deps := newTestDeps()
	hanging := &hangingLoadStore{Store: persistinprocess.New(), entered: make(chan struct{})}
	deps.Checkpoint = hanging
	// A metadata row must exist, or restoreInto is skipped and the store is never
	// reached.
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: "hangs", ContentType: model.ContentTypeMemo, ContentPointer: "ptr",
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := newRoom(context.Background(), "hangs", model.ContentTypeMemo,
			deps.Deps, RoomConfig{SendBuffer: 16, BackendTimeout: 150 * time.Millisecond},
			NopMetrics{}, zap.NewNop())
		done <- err
	}()

	select {
	case <-hanging.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("LoadCheckpoint was never reached")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("materialization against a hung checkpoint store must fail, not succeed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("materialization never returned: the checkpoint restore has no deadline, and nothing else can cancel a room that is not yet registered")
	}
}

// TestRestoreIsBoundedWithoutAnyCallerDeadline pins the wall-clock bound the room
// imposes on its OWN checkpoint restore, independent of the context the CRDT core
// hands the open function.
//
// That context belongs to the REGISTRY, not to any single acquirer: the core
// cancels it when the LAST waiter stops waiting. It therefore bounds an open that
// nobody wants any more, but NOT an open somebody is still waiting for — a
// document that keeps attracting joiners renews the clock on every arrival, so a
// wedged LoadCheckpoint can outlive every deadline the acquirers themselves
// carry. A fixed bound has to originate here.
//
// The parent is context.Background(): no deadline, no cancellation. Nothing but
// the room's own bound can end this call, so the assertion cannot be satisfied by
// a deadline inherited from elsewhere — which is exactly what makes it meaningful
// against a core that still propagates the acquirer's context.
//
// Non-vacuity: drop the WithTimeout inside restoreBounded and this never returns.
func TestRestoreIsBoundedWithoutAnyCallerDeadline(t *testing.T) {
	deps := newTestDeps()
	hanging := &hangingLoadStore{Store: persistinprocess.New(), entered: make(chan struct{})}
	deps.Checkpoint = hanging

	r := &Room{
		id:   "no-caller-deadline",
		cfg:  RoomConfig{SendBuffer: 16, BackendTimeout: 150 * time.Millisecond},
		deps: deps.Deps,
	}

	done := make(chan error, 1)
	go func() {
		done <- r.restoreBounded(context.Background(), newRoomDoc("no-caller-deadline"))
	}()

	select {
	case <-hanging.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("LoadCheckpoint was never reached")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a hung checkpoint store must fail the restore, not succeed")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the restore must end on the room's own deadline; got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restore never returned: with a deadline-free open context, only the room's own bound can stop a wedged store")
	}
}
