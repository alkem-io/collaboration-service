package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"

	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// This file holds the ratchet tests for the OBS-1..OBS-5 observations surfaced by
// the post-redesign full adversarial review. Each asserts a single invariant and
// is proven non-vacuous (it goes RED when its fix is reverted) — see the comment
// on each test for the exact revert and the failure it produces.

// --- OBS-1: a panic on the run loop must tear the room down, not wedge it ---

// panicOnSaveStore panics on SaveCheckpoint (the persist call), modelling a
// handler that panics on the single-writer run loop. Load/Delete delegate to a
// real store so materialization is unaffected.
type panicOnSaveStore struct {
	*persistinprocess.Store
}

func (panicOnSaveStore) SaveCheckpoint(context.Context, persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	panic("induced persist panic on the run loop")
}

// TestRunLoopRecoversFromHandlerPanic is the OBS-1 ratchet. A panic in any run-loop
// handler used to kill the run() goroutine with the room still registered, r.done
// never closed, and Manager.Close blocking to its deadline — one panicking handler
// took down the whole pod's graceful shutdown. The fix is a defer/recover at the top
// of run() that tears the room down (WITHOUT a flush — a mid-panic doc must not be
// persisted over the last good snapshot).
//
// Non-vacuity: revert the `defer func(){ recover()... }()` in room.go run() and the
// unrecovered Put panic crashes the whole `go test` binary instead of being caught —
// the suite goes RED hard.
func TestRunLoopRecoversFromHandlerPanic(t *testing.T) {
	deps := newTestDeps()
	inner := persistinprocess.New()
	panicking := panicOnSaveStore{Store: inner}
	deps.Checkpoint = panicking
	// A short debounce so the edit below triggers a (panicking) persist promptly; a
	// long idle so the room is not released by idleness before that persist runs.
	m := NewManager(deps.Deps, RoomConfig{
		SendBuffer: 16, SaveDebounce: 20 * time.Millisecond, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second,
	}, nil, zap.NewNop())

	id := model.DocumentID("doc-panic-recover")
	c := newFakeClient(t)
	c.join(m, id, model.ContentTypeMemo)
	c.observeUpdates()
	// Dirty the room so the debounce-fired persist actually reaches the blob (a clean
	// room's persist is a no-op and would never call Put).
	c.withDoc(func(doc *ycrdt.Doc) { insertText(doc, "edit that triggers a persist") })

	// The debounce fires → persistNow → Put panics on the run loop → the recover tears
	// the room down → it is removed from the registry.
	waitFor(t, "room released after a handler panic", func() bool { return m.RoomCount() == 0 })

	// And graceful shutdown returns promptly rather than blocking to its deadline on a
	// room whose run loop died mid-handler.
	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Manager.Close hung after a handler panic (run-loop recover did not tear the room down)")
	}
}

// --- OBS-2: dispatching a cmdJoin with a nil done channel must not block the loop ---

// TestDispatchJoinWithNilDoneDoesNotBlockLoop is the OBS-2 ratchet. The cmdJoin
// dispatch sends the join result on cmd.done; the only real producer always supplies
// a buffered channel, but an unguarded send on a nil channel would block the single-
// writer loop forever. The fix guards it: `if cmd.done != nil { cmd.done <- res }`.
//
// Non-vacuity: revert the guard to an unconditional `cmd.done <- res` and the send on
// the nil channel blocks dispatch forever — the goroutine below never reports, and the
// 2s timeout fires.
func TestDispatchJoinWithNilDoneDoesNotBlockLoop(t *testing.T) {
	room := newBareRoom(t)
	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	noop := func() {}
	conn := newFakeClient(t)

	returned := make(chan bool, 1)
	go func() {
		// done is the zero value (nil) — the guard must skip the send.
		returned <- room.dispatch(command{kind: cmdJoin, conn: conn, identity: conn.identity}, noop, noop, idle)
	}()

	select {
	case keep := <-returned:
		if !keep {
			t.Fatal("dispatch(cmdJoin) returned keepRunning=false; a join must not stop the loop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch(cmdJoin) with a nil done channel blocked the run loop (missing nil-channel send guard)")
	}
}

// --- OBS-3: Manager.Close must bound the cmdClose signal by the shutdown deadline ---

// saturateCommandBuffer fills a room's command channel to capacity so the next
// enqueue cannot take the fast path and must bounded-block.
func saturateCommandBuffer(room *Room) {
	for len(room.commands) < cap(room.commands) {
		room.commands <- command{kind: cmdLeave}
	}
}

// TestManagerCloseSignalIsBoundedBySingleDeadline is the OBS-3 ratchet. Manager.Close
// used to enqueue cmdClose SERIALLY with an unbounded (context.Background) enqueue, so
// a single room whose command buffer was saturated blocked the whole signal phase up
// to enqueueDeadline (30s) per room — N rooms ⇒ up to N×30s before the drain even
// started, far past the shutdown deadline. The fix signals every room CONCURRENTLY,
// each bounded by one shared shutdownCtx (budget + grace), and drains against the same
// ctx.
//
// Here three rooms have saturated, never-draining command buffers (no run loop). Close
// must still return within one shutdown deadline (~BackendTimeout + grace), not N×30s.
//
// Non-vacuity: revert Close to the serial `room.enqueue(cmdClose)` loop and each
// saturated room blocks the signal phase for enqueueDeadline (30s); Close cannot return
// within the 15s bound and the test goes RED.
func TestManagerCloseSignalIsBoundedBySingleDeadline(t *testing.T) {
	m, deps := testManager(t, RoomConfig{
		SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 200 * time.Millisecond,
	})

	const rooms = 3
	for i := 0; i < rooms; i++ {
		room := wedgeRoom(t, m, deps, model.DocumentID(fmt.Sprintf("doc-shutdown-bound-%d", i)))
		saturateCommandBuffer(room)
	}

	closed := make(chan struct{})
	go func() { m.Close(); close(closed) }()

	// One shutdown deadline is BackendTimeout(200ms) + shutdownDrainGrace(5s) ≈ 5.2s;
	// 15s leaves ample margin yet is well under a single enqueueDeadline (30s), so the
	// serial-signal regression cannot pass.
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Manager.Close did not return within one shutdown deadline; the cmdClose signal is not bounded (serial unbounded enqueue blocks N×enqueueDeadline)")
	}
}

// --- OBS-5a: the cheap byte-budget skip must never admit an update past the cap ---

// TestBudgetSkipNeverAdmitsPastCap is the OBS-5a ratchet. applyWouldExceedMaxDocBytes
// short-circuits the O(docsize) exact check with a cheap bound based on r.docBytes, the
// cached encoded size. For that skip to be SOUND, docBytes must never under-estimate the
// true encoded size between exact checks — guaranteed by onDocUpdate over-counting every
// applied update by len(update). If docBytes goes stale-low, the skip keeps firing while
// the doc has actually grown past the cap, silently admitting over-budget updates.
//
// Here we replay growing edits through applyUpdate under a small cap. With the
// accumulation in place the budget engages and rejects before the live doc ever exceeds
// the cap. Non-vacuity: remove `r.docBytes += len(update)` from onDocUpdate and docBytes
// freezes at the first exact-check value; the skip then admits updates that push the live
// doc past the cap, tripping the over-cap assertion (and the budget never rejects).
func TestBudgetSkipNeverAdmitsPastCap(t *testing.T) {
	room := newBareRoom(t)
	const capBytes = 4096 // comfortably above budgetSkipSlack so the cheap skip can fire
	room.cfg.Limits.MaxDocBytes = capBytes
	// newBareRoom omits the run-loop update observer; wire it so applyUpdate drives the
	// docBytes accumulation exactly as the live room does.
	room.doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) { room.onDocUpdate(v...) }))

	// A generator doc whose incremental v1 deltas we replay into the room, so each apply
	// grows the room doc the way a real client edit does.
	gen := newRoomDoc("gen-budget")
	var deltas [][]byte
	gen.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		if len(v) == 0 {
			return
		}
		if u, ok := v[0].([]byte); ok {
			deltas = append(deltas, append([]byte(nil), u...))
		}
	}))
	// Plenty of growing edits to push the doc well past a 4 KiB cap (it is reached in
	// ~120 applies; the rest are headroom so the unsound path keeps growing unboundedly).
	for i := 0; i < 1000; i++ {
		insertText(gen, "budget-soundness-filler-text ")
	}

	trueSize := func() int {
		b, err := ycrdt.EncodeStateAsUpdateV2(room.doc, nil)
		if err != nil {
			t.Fatalf("encode live doc: %v", err)
		}
		return len(b)
	}

	rejectedAtLeastOnce := false
	for i, d := range deltas {
		if !room.applyUpdate(d, updateOrigin{}) {
			// The budget engaged and held the line pre-commit (the doc was not mutated).
			// That is the sound steady state — proven, so stop (continuing would re-run
			// the O(docsize) exact check on every remaining delta for no extra coverage).
			rejectedAtLeastOnce = true
			break
		}
		if size := trueSize(); size > capBytes {
			t.Fatalf("budget skip admitted update %d that grew the live doc to %d bytes, past the %d cap — docBytes accumulation is unsound", i, size, capBytes)
		}
	}
	if !rejectedAtLeastOnce {
		t.Fatal("the budget never rejected — the doc never approached the cap, so the soundness regime was not exercised")
	}
}

// --- OBS-5b: RESTRUCTURED — the stranding window no longer exists -------------
//
// The original asserted delete-after-commit ordering: persist uploaded a NEW blob
// per save, and deleting the predecessor before the metadata commit could strand
// the row on a missing blob if that commit failed.
//
// That hazard is gone by construction. A document now owns ONE file for its
// lifetime, rewritten in place, so there is no predecessor to delete and the
// pointer never changes — there is no window in which the row names something
// that is not there. Per FR-018a the test is restructured to assert the property
// it was defending rather than the mechanism that used to provide it, and the
// mechanism-specific doubles (versioningBlob, failNthSaveMeta) are removed with
// it because nothing they modelled can still happen.

// TestFailedSaveLeavesStoredStateIntact is the surviving guarantee: a save whose
// metadata commit fails must leave the previously stored state readable, so the
// document still opens. It is what "never strands the row" means once the
// pointer is stable.
func TestFailedSaveLeavesStoredStateIntact(t *testing.T) {
	deps := newTestDeps()
	failing := &failNthMetaSave{MetadataStore: deps.meta, failOn: 2}
	deps.Metadata = failing

	room, err := newRoom(context.Background(), "doc-strand", model.ContentTypeMemo,
		deps.Deps, DefaultRoomConfig(), NopMetrics{}, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	t.Cleanup(room.finish)

	insertText(room.doc, "first")
	room.dirty = true
	room.persist(context.Background())

	first, err := deps.store.LoadCheckpoint(context.Background(), backend.DocumentID("doc-strand"))
	if err != nil {
		t.Fatalf("stored state after the first save: %v", err)
	}

	// Second save: the state is written, then the metadata commit fails.
	insertText(room.doc, "second")
	room.dirty = true
	room.persist(context.Background())

	after, err := deps.store.LoadCheckpoint(context.Background(), backend.DocumentID("doc-strand"))
	if err != nil {
		t.Fatalf("stored state must remain readable after a failed commit, got: %v", err)
	}
	if len(after.Update) < len(first.Update) {
		t.Fatal("a failed commit left LESS stored state than before; the document must never regress")
	}
}

// failNthMetaSave fails the Nth Save so a test can drive a save whose state write
// succeeds but whose index commit fails.
type failNthMetaSave struct {
	port.MetadataStore
	mu     sync.Mutex
	saves  int
	failOn int
}

func (m *failNthMetaSave) Save(ctx context.Context, meta model.Metadata) error {
	m.mu.Lock()
	m.saves++
	n := m.saves
	m.mu.Unlock()
	if n == m.failOn {
		return errors.New("induced metadata save failure")
	}
	return m.MetadataStore.Save(ctx, meta)
}
