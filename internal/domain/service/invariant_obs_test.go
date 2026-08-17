package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"
	"go.uber.org/zap"

	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// This file holds the ratchet tests for the OBS-1..OBS-5 observations surfaced by
// the post-redesign full adversarial review. Each asserts a single invariant and
// is proven non-vacuous (it goes RED when its fix is reverted) — see the comment
// on each test for the exact revert and the failure it produces.

// --- OBS-1: a panic on the run loop must tear the room down, not wedge it ---

// panicOnPutBlob panics on Put (the snapshot persist call), modelling a handler
// that panics on the single-writer run loop. Get/Delete delegate to a real inline
// store so materialization (which may Get) is unaffected.
type panicOnPutBlob struct{ port.BlobStore }

func (panicOnPutBlob) Put(context.Context, string, string, []byte) (string, error) {
	panic("induced blob put panic on the run loop")
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
	deps.Blob = panicOnPutBlob{BlobStore: blobinline.New()}
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

// --- OBS-5b: persist must delete the predecessor blob only AFTER the commit ---

// versioningBlob returns a fresh pointer per Put (so each save supersedes a distinct
// predecessor) and records every Delete, so a test can assert ordering.
type versioningBlob struct {
	mu      sync.Mutex
	seq     int
	live    map[string][]byte
	deleted []string
}

func newVersioningBlob() *versioningBlob { return &versioningBlob{live: map[string][]byte{}} }

func (b *versioningBlob) Put(_ context.Context, _, _ string, data []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	p := fmt.Sprintf("blob-v%d", b.seq)
	b.live[p] = append([]byte(nil), data...)
	return p, nil
}

func (b *versioningBlob) Get(_ context.Context, pointer string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.live[pointer]
	if !ok {
		return nil, model.ErrNotFound
	}
	return append([]byte(nil), d...), nil
}

func (b *versioningBlob) Delete(_ context.Context, pointer string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.live, pointer)
	b.deleted = append(b.deleted, pointer)
	return nil
}

func (b *versioningBlob) deletedPointers() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.deleted...)
}

var _ port.BlobStore = (*versioningBlob)(nil)

// failNthSaveMeta wraps an inmemory metadata store and fails the Nth Save call, so a
// test can drive a save whose blob upload succeeds but whose metadata commit fails.
type failNthSaveMeta struct {
	*metainmem.Store
	mu     sync.Mutex
	saves  int
	failOn int
}

func (m *failNthSaveMeta) Save(ctx context.Context, meta model.Metadata) error {
	m.mu.Lock()
	m.saves++
	n := m.saves
	m.mu.Unlock()
	if n == m.failOn {
		return errors.New("induced metadata save failure")
	}
	return m.Store.Save(ctx, meta)
}

var _ port.MetadataStore = (*failNthSaveMeta)(nil)

// TestPersistDeletesPredecessorOnlyAfterCommit is the OBS-5b ratchet. persist uploads
// the new snapshot, commits the new pointer to metadata, and only THEN deletes the
// superseded blob (delete-after-commit, 002 FR-002). If the metadata commit fails, the
// predecessor blob and the durable pointer to it must remain intact, so the document
// stays openable — a failed commit leaves a benign forward orphan (the uncommitted new
// blob), never a stranded pointer.
//
// Non-vacuity: move the `Blob.Delete(oldPointer)` in room.persist BEFORE the
// Metadata.Save commit and the failed second commit strands blob-v1 — Get(blob-v1) then
// fails (and blob.deleted is non-empty), tripping the assertions.
func TestPersistDeletesPredecessorOnlyAfterCommit(t *testing.T) {
	room := newBareRoom(t)
	blob := newVersioningBlob()
	meta := &failNthSaveMeta{Store: metainmem.New(), failOn: 2}
	room.deps.Blob = blob
	room.deps.Metadata = meta

	// First save: uploads blob-v1 and commits. oldPointer was empty, so nothing is
	// deleted — this just establishes the predecessor the second save will supersede.
	room.dirty = true
	room.persist(context.Background())
	if room.pointer != "blob-v1" {
		t.Fatalf("first save: room.pointer = %q, want blob-v1", room.pointer)
	}
	if d := blob.deletedPointers(); len(d) != 0 {
		t.Fatalf("first save deleted %v; nothing should be deleted on the first save", d)
	}

	// Second save: uploads blob-v2, THEN the metadata commit FAILS. Delete-after-commit
	// means blob-v1 (still the committed pointer) must NOT be deleted.
	room.dirty = true
	room.persist(context.Background())

	if d := blob.deletedPointers(); len(d) != 0 {
		t.Fatalf("a failed metadata commit deleted %v; the committed predecessor must survive", d)
	}
	if _, err := blob.Get(context.Background(), "blob-v1"); err != nil {
		t.Fatalf("predecessor blob-v1 is gone after a failed commit (stranded pointer): %v", err)
	}
	// The durable row still points at the committed predecessor, not the uncommitted v2.
	got, err := meta.Load(context.Background(), room.id)
	if err != nil {
		t.Fatalf("metadata load: %v", err)
	}
	if got.ContentPointer != "blob-v1" {
		t.Fatalf("durable row points at %q; want the committed blob-v1 (a failed save must not advance it)", got.ContentPointer)
	}
}
