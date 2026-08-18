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
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"

	"go.uber.org/zap"
)

// gatedDeleteStore is an in-process CheckpointStore that parks inside
// DeleteCheckpoint, AFTER the content is gone but BEFORE the caller moves on to
// the index row. That is the resurrection window, and parking there is what makes
// the race deterministic instead of a timing lottery.
type gatedDeleteStore struct {
	*persistinprocess.Store
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newGatedDeleteStore() *gatedDeleteStore {
	return &gatedDeleteStore{
		Store:   persistinprocess.New(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gatedDeleteStore) DeleteCheckpoint(ctx context.Context, id backend.DocumentID) error {
	if err := g.Store.DeleteCheckpoint(ctx, id); err != nil {
		return err
	}
	// sync.Once: the cascade may reach this twice (the room-loop purge, then the
	// Manager's idempotent durable fallback), and only the first passage is the
	// window under test.
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return nil
}

// newGatedManager wires a Manager over a store that can be parked mid-cascade.
func newGatedManager(t *testing.T) (*Manager, *gatedDeleteStore, *metainmem.Store) {
	t.Helper()
	store := newGatedDeleteStore()
	meta := metainmem.New()
	open := authopen.New()
	mgr := NewManager(Deps{
		Broadcaster: noopBroadcaster{},
		Metadata:    meta,
		Checkpoint:  store,
		Auth:        open,
		AuthZ:       open,
	}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second, // long: only the cascade releases a room
		SendBuffer:   256,
	}, nil, zap.NewNop())
	return mgr, store, meta
}

// TestJoinRacingTheCascadeCannotResurrectDurableContent is the decisive case for
// the owner-delete ordering: refuse new acquisitions FIRST, then delete.
//
// The window is on the no-live-room path. Between deleting the content and
// deleting the index row, a Join finds no room, materializes a fresh one, loads
// no checkpoint, seeds an empty document — and that room is now live on a
// document the owner deleted. Its first flush writes content and an index row
// back. Deleting in the other order does not help; it only changes which artifact
// is orphaned. The fix is a tombstone consulted by acquire, so the racing Join is
// refused instead of served.
//
// Non-vacuity: drop the tombstone check from acquire and the Join below is
// admitted, and the test then drives the resurrection to completion so the
// failure names what actually came back rather than merely reporting a missing
// error.
func TestJoinRacingTheCascadeCannotResurrectDurableContent(t *testing.T) {
	mgr, store, meta := newGatedManager(t)

	const doc model.DocumentID = "resurrect-durable"

	// Durable state and an index row, but NO live room: exactly the state a
	// document is in once its last collaborator has left.
	seed := newBareRoom(t)
	insertText(seed.doc, "owner deleted this ")
	update, err := ycrdt.EncodeStateAsUpdateV2(seed.doc, nil)
	if err != nil {
		t.Fatalf("encode seed state: %v", err)
	}
	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID:  backend.DocumentID(doc),
		Update:      update,
		StateVector: []byte("derived-on-read"),
	}); err != nil {
		t.Fatalf("seed durable state: %v", err)
	}
	if err := meta.Save(context.Background(), model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("seed metadata row: %v", err)
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("precondition: expected no live room, got %d", mgr.RoomCount())
	}

	purged := make(chan error, 1)
	go func() { purged <- mgr.Purge(context.Background(), doc) }()

	// Park the cascade in the window: content gone, index row still present.
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cascade never reached the checkpoint delete")
	}

	// THE RACE: a client connects while the cascade is mid-flight.
	joiner := newFakeClient(t)
	session, _, joinErr := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: joiner.identity, Conn: joiner,
	})
	if joinErr == nil {
		reportResurrection(t, joiner, session, store, purged, doc)
	}
	if !errors.Is(joinErr, ErrDocumentPurging) {
		t.Fatalf("Join during the cascade must be refused with ErrDocumentPurging, got %v", joinErr)
	}

	close(store.release)
	select {
	case err := <-purged:
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("purge did not complete after the gate released")
	}

	// The consequence the refusal exists to prevent: nothing durable survives, and
	// no room was left behind holding the document.
	if _, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc)); err == nil {
		t.Fatal("stored state survived the cascade: content was resurrected")
	}
	if _, err := meta.Load(context.Background(), doc); err == nil {
		t.Fatal("metadata row survived the cascade: the document was resurrected in the index")
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("a room was materialized during the cascade: %d live", mgr.RoomCount())
	}
}

// reportResurrection is reached only when the tombstone is absent: the racing
// Join was admitted. It drives the window to the actual harm — an edit on the
// resurrected room flushing durable content back for a deleted document — so the
// failure names what came back rather than merely reporting a missing error.
func reportResurrection(
	t *testing.T,
	joiner *fakeClient,
	session *Session,
	store *gatedDeleteStore,
	purged <-chan error,
	doc model.DocumentID,
) {
	t.Helper()
	joiner.mu.Lock()
	joiner.session = session
	joiner.mu.Unlock()
	joiner.observeUpdates()
	joiner.insertText("resurrected ")
	close(store.release)
	<-purged

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc)); err == nil {
			t.Fatal("a Join admitted inside the cascade wrote durable content back for a deleted document")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a Join landing inside the owner-delete cascade was admitted; it holds a live room on a deleted document")
}

// TestJoinRacingALiveRoomCascadeIsRefusedCleanly covers the other cascade path.
//
// Here the retry budget in Join already prevented resurrection by accident: the
// draining room stays registered for the length of the cascade, so both acquire
// attempts hand it back, both enqueues bounce off a room in teardown, and the
// caller gets errRoomUnavailable. That is the right outcome for the wrong reason
// — it reports a transient "try again" for a document that is being deleted, and
// it depends on the cascade outlasting two attempts. With the tombstone the
// refusal is explicit and does not rest on that timing.
func TestJoinRacingALiveRoomCascadeIsRefusedCleanly(t *testing.T) {
	mgr, store, _ := newGatedManager(t)

	const doc model.DocumentID = "resurrect-live"

	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("owner deleted this ")
	waitFor(t, "snapshot persisted", func() bool {
		_, err := store.LoadCheckpoint(context.Background(), backend.DocumentID(doc))
		return err == nil
	})

	purged := make(chan error, 1)
	go func() { purged <- mgr.Purge(context.Background(), doc) }()

	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cascade never reached the checkpoint delete")
	}

	b := newFakeClient(t)
	_, _, joinErr := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: b.identity, Conn: b,
	})
	if !errors.Is(joinErr, ErrDocumentPurging) {
		t.Fatalf("Join during a live-room cascade must be refused with ErrDocumentPurging, got %v", joinErr)
	}

	close(store.release)
	select {
	case err := <-purged:
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("purge did not complete after the gate released")
	}
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })
}

// TestTombstoneIsLiftedOnceTheCascadeCompletes asserts the tombstone is scoped to
// the cascade rather than permanent. A document id is not poisoned for the life
// of the process — once the delete is done, a later connect is an ordinary open,
// which authorization decides. A tombstone that leaked would silently make every
// re-created document unjoinable until restart.
func TestTombstoneIsLiftedOnceTheCascadeCompletes(t *testing.T) {
	mgr, _ := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
	})

	const doc model.DocumentID = "recreate-me"
	if err := mgr.Purge(context.Background(), doc); err != nil {
		t.Fatalf("purge of an absent document should be a no-op: %v", err)
	}

	a := newFakeClient(t)
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: a.identity, Conn: a,
	}); err != nil {
		t.Fatalf("join after the cascade completed: %v", err)
	}
}
