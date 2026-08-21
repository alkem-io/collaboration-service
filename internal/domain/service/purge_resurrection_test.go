package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"

	"go.uber.org/zap"
)

// gatedMeta performs the FIRST existence read, then parks holding its RESULT.
//
// The ORDER inside Load is the whole point, and getting it backwards makes the
// test prove nothing. It reads the inner store FIRST and only then blocks,
// returning the value it captured. So while it is parked, the test can delete the
// row — and the Join still receives the positive answer it got a moment earlier.
// That is a genuine STALE READ: old epoch, plus an existence result that was true
// when taken and is false by the time it is used.
//
// Blocking before the inner read instead would just return not-found once the row
// was gone, the existence gate would refuse the Join, and the epoch would never be
// exercised at all.
//
// Parking here rather than inside a delete is not a convenience: there is no
// delete left to park inside. `server` performs the durable deletion before it
// publishes, so CloseDeleted returns immediately and its window has no duration
// to exploit.
type gatedMeta struct {
	port.MetadataStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newGatedMeta(inner port.MetadataStore) *gatedMeta {
	return &gatedMeta{
		MetadataStore: inner,
		entered:       make(chan struct{}),
		release:       make(chan struct{}),
	}
}

func (g *gatedMeta) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	meta, err := g.MetadataStore.Load(ctx, id)
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return meta, err
}

// newGatedManager wires a Manager whose first existence read can be parked.
func newGatedManager(t *testing.T) (*Manager, *persistinprocess.Store, *gatedMeta, *serverOwnedMeta) {
	t.Helper()
	store := persistinprocess.New()
	owned := &serverOwnedMeta{MetadataStore: metainmem.New(), gone: map[model.DocumentID]bool{}}
	meta := newGatedMeta(owned)
	open := authopen.New()
	mgr := NewManager(Deps{
		Metadata:   meta,
		Checkpoint: store,
		Auth:       open,
		AuthZ:      open,
	}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second, // long: only an explicit close releases a room
		SendBuffer:   256,
	}, nil, zap.NewNop())
	return mgr, store, meta, owned
}

// TestAJoinThatPassedExistenceBeforeTheDeleteIsNotAdmitted is the decisive race
// for the owner-delete slice, and the reason the per-id tombstone was replaced.
//
// The interleaving is the whole test. A client's Join captures the delete epoch,
// reads that the document exists — and THEN the owner deletes it. Everything that
// read is based on is now stale, but the Join has already passed the durable
// gate, so nothing downstream would refuse it on existence. If it is admitted, it
// materializes a room on a document that no longer exists, seeds it empty because
// `server` removed the checkpoint, and its first edit flushes content back.
//
// The old per-id tombstone caught this only by accident: it was raised for the
// duration of two backend deletes. Those deletes are gone, so CloseDeleted now
// returns in microseconds and a duration-based guard catches nothing. The epoch
// does not depend on duration.
//
// Non-vacuity: it drives an admitted Join all the way to the harm — a REVISION
// advance on the seeded checkpoint — so a failure names what came back rather
// than merely reporting a missing error. It goes RED if the epoch is never
// bumped.
//
// It does NOT discriminate WHERE inside acquire the refusal happens. The Join
// arrives already carrying a stale epoch, so the fast-path check alone catches it
// and removing a later check leaves this passing. The placements that are
// independently load-bearing have their own tests:
// TestAStaleEpochIsRefusedEvenWhenAWarmRoomIsRegistered pins the per-caller
// post-singleflight check (the only shape that never materializes), and
// TestACascadeStartingDuringMaterializationLeavesNoLiveRoom pins the final
// pre-insert check (the only shape where the delete lands mid-materialization).
func TestAJoinThatPassedExistenceBeforeTheDeleteIsNotAdmitted(t *testing.T) {
	mgr, store, meta, owned := newGatedManager(t)
	t.Cleanup(mgr.Close)

	const doc model.DocumentID = "raced-by-a-delete"

	// Durable state and an index row, but no live room: the state a document is in
	// between sessions.
	seed := newBareRoom(t)
	insertText(seed.doc, "content the owner deleted")
	update, err := ycrdt.EncodeStateAsUpdateV2(seed.doc, nil)
	if err != nil {
		t.Fatalf("encode seed: %v", err)
	}
	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(doc), Encoding: persistence.EncodingV2,
		Update: update, StateVector: []byte("derived-on-read"),
	}); err != nil {
		t.Fatalf("seed durable state: %v", err)
	}
	if err := meta.Save(context.Background(), model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("seed metadata row: %v", err)
	}
	before, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc))
	if err != nil {
		t.Fatalf("read the seeded revision: %v", err)
	}

	// The Join starts FIRST and parks inside its existence read, holding the
	// pre-delete epoch.
	joiner := newFakeClient(t)
	type joinOutcome struct {
		session *Session
		err     error
	}
	outcome := make(chan joinOutcome, 1)
	go func() {
		s, _, err := mgr.Join(context.Background(), JoinRequest{
			ID: doc, Content: model.ContentTypeMemo, Identity: joiner.identity, Conn: joiner,
		})
		outcome <- joinOutcome{s, err}
	}()
	select {
	case <-meta.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the join never reached its existence read")
	}

	// Production order, both halves. `server` removes the row — the parked Join is
	// already holding the positive answer it read a moment ago, so its result is now
	// stale — and then the event reaches collab.
	owned.serverRemoved(doc)
	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("CloseDeleted: %v", err)
	}

	close(meta.release)

	var got joinOutcome
	select {
	case got = <-outcome:
	case <-time.After(5 * time.Second):
		t.Fatal("the join never completed")
	}

	if got.err == nil {
		reportResurrection(t, joiner, got.session, store, doc, before.Revision)
	}
	if !errors.Is(got.err, errRoomUnavailable) {
		t.Fatalf("join = %v, want errRoomUnavailable: a stale admission must be refused transiently so the client reconnects into a fresh existence read", got.err)
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("%d room(s) registered for a document deleted mid-join", mgr.RoomCount())
	}
	after, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc))
	if err != nil {
		t.Fatalf("read the revision after the refusal: %v", err)
	}
	if after.Revision != before.Revision {
		t.Fatalf("revision advanced %d -> %d without an admitted session; something flushed for a deleted document", before.Revision, after.Revision)
	}
}

// reportResurrection is reached only when the refusal failed: the racing Join was
// admitted. It drives the window to the actual harm — an edit on the resurrected
// room flushing content back for a deleted document — so the failure names what
// came back rather than merely reporting a missing error.
//
// It waits for the REVISION to advance, not for a checkpoint to exist: the
// document was seeded, so "a checkpoint exists" was already true before the race
// and would report a false success.
func reportResurrection(
	t *testing.T,
	joiner *fakeClient,
	session *Session,
	store *persistinprocess.Store,
	doc model.DocumentID,
	seededRevision persistence.Revision,
) {
	t.Helper()
	joiner.mu.Lock()
	joiner.session = session
	joiner.mu.Unlock()
	joiner.observeUpdates()
	joiner.insertText("resurrected ")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cp, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc)); err == nil && cp.Revision > seededRevision {
			t.Fatal("a Join admitted after the document was deleted wrote durable content back for it")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a Join that passed existence before the delete was admitted; it holds a live room on a deleted document")
}
