package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// gatedStore parks a materializing room inside newRoom, and a cascade inside its
// delete, so the window between "the room has loaded" and "the room is registered"
// can be held open deliberately.
//
// No production seam is needed: the Manager already takes its stores as ports, so
// the window is widened from the outside.
type gatedStore struct {
	persistence.CheckpointStore
	inner *persistinprocess.Store

	loadEntered chan struct{}
	loadRelease chan struct{}
	loadOnce    sync.Once

	deleteEntered chan struct{}
	deleteRelease chan struct{}
	deleteOnce    sync.Once
}

func newGatedStore() *gatedStore {
	inner := persistinprocess.New()
	return &gatedStore{
		CheckpointStore: inner,
		inner:           inner,
		loadEntered:     make(chan struct{}),
		loadRelease:     make(chan struct{}),
		deleteEntered:   make(chan struct{}),
		deleteRelease:   make(chan struct{}),
	}
}

func (g *gatedStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	g.loadOnce.Do(func() {
		close(g.loadEntered)
		<-g.loadRelease
	})
	return g.CheckpointStore.LoadCheckpoint(ctx, id)
}

func (g *gatedStore) Delete(ctx context.Context, req persistence.DeleteRequest) error {
	g.deleteOnce.Do(func() {
		close(g.deleteEntered)
		<-g.deleteRelease
	})
	return g.inner.Delete(ctx, req)
}

// gatedManager builds a Manager over the gated store, with the document already
// persisted — a room only restores when the index says there is something to
// restore, so without durable state the load gate is never reached and the window
// this test is about never opens.
// seedUpdate is a real, decodable v2 update for an empty document — the restore
// path must be able to apply it, or materialization fails for the wrong reason.
func seedUpdate(t *testing.T, guid string) []byte {
	t.Helper()
	u, err := ycrdt.EncodeStateAsUpdateV2(ycrdt.NewDoc(guid), nil)
	if err != nil {
		t.Fatalf("encode seed update: %v", err)
	}
	return u
}

func gatedManager(t *testing.T, store *gatedStore, doc model.DocumentID) *Manager {
	t.Helper()
	meta := metainmem.New()
	ctx := context.Background()
	if _, err := store.inner.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID:  backend.DocumentID(doc),
		Encoding:    persistence.EncodingV2,
		Update:      seedUpdate(t, string(doc)),
		StateVector: []byte("derived-on-read"),
	}); err != nil {
		t.Fatalf("seed durable state: %v", err)
	}
	if err := meta.Save(ctx, model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	open := authopen.New()
	return NewManager(Deps{
		Metadata:   meta,
		Checkpoint: store,
		Auth:       open,
		AuthZ:      open,
	}, fastConfig(), nil, zap.NewNop())
}

// TestACascadeStartingDuringMaterializationLeavesNoLiveRoom is the resurrection
// window, held open on purpose.
//
// acquire materializes OFF the registry lock, because newRoom does backend I/O and
// holding the lock across it would wedge every other document. That means a Purge
// can begin — and raise its tombstone — while a room for the same document is
// already loading. Registering that room afterwards would put a LIVE room on a
// document the owner has deleted, and its first flush would write the content and
// the index row straight back. The owner deletes a document and it reappears.
//
// The fast-path tombstone check cannot catch this: it ran before the cascade
// started. Only the re-check after materialization can, and it has to tear the
// fresh room down rather than merely refusing to register it, or the room's own
// timers keep it alive and flushing.
func TestACascadeStartingDuringMaterializationLeavesNoLiveRoom(t *testing.T) {
	store := newGatedStore()
	const doc model.DocumentID = "cascade-during-materialization"
	mgr := gatedManager(t, store, doc)
	t.Cleanup(mgr.Close)

	joinErr := make(chan error, 1)
	go func() {
		client := newFakeClient(t)
		_, _, err := mgr.Join(context.Background(), JoinRequest{
			ID: doc, Content: model.ContentTypeMemo, Conn: client,
		})
		joinErr <- err
	}()

	// The join is now parked inside newRoom, holding no registry lock.
	<-store.loadEntered

	purgeErr := make(chan error, 1)
	go func() { purgeErr <- mgr.Purge(context.Background(), doc) }()

	// The cascade has raised its tombstone and is parked in the content delete, so
	// it is still in flight when the room finishes loading.
	<-store.deleteEntered

	close(store.loadRelease)
	select {
	case err := <-joinErr:
		if !errors.Is(err, ErrDocumentPurging) {
			t.Fatalf("join during a cascade returned %v, want ErrDocumentPurging", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the join never completed")
	}

	close(store.deleteRelease)
	select {
	case err := <-purgeErr:
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the purge never completed")
	}

	mgr.mu.Lock()
	_, registered := mgr.rooms[doc]
	mgr.mu.Unlock()
	if registered {
		t.Fatal("a room materialized during the cascade was registered anyway; it is live on a deleted document and its first flush writes the content back")
	}
	// Refusing to register is only half of it: the room DID acquire a registry
	// handle during materialization, and nothing else will ever release it. An
	// existing test proves teardown releases the handle when called; this proves
	// acquire actually calls it on the refusal path.
	if residentInRegistry(t, mgr.registry, doc) {
		t.Fatal("the refused room was never torn down; its document stays resident for the life of the process, and no room will ever own it again")
	}
}

// TestAShutdownStartingDuringMaterializationLeavesNoLiveRoom is the same window,
// closed by shutdown rather than by a cascade.
//
// Close snapshots the registry and drains what it finds. A room that registers
// itself after that snapshot is never drained: its buffered edits are never
// flushed, and the process exits with them still in memory. So a materialization
// that finishes after Close began must tear itself down instead of registering.
func TestAShutdownStartingDuringMaterializationLeavesNoLiveRoom(t *testing.T) {
	store := newGatedStore()
	const doc model.DocumentID = "shutdown-during-materialization"
	mgr := gatedManager(t, store, doc)

	joinErr := make(chan error, 1)
	go func() {
		client := newFakeClient(t)
		_, _, err := mgr.Join(context.Background(), JoinRequest{
			ID: doc, Content: model.ContentTypeMemo, Conn: client,
		})
		joinErr <- err
	}()

	<-store.loadEntered

	closed := make(chan struct{})
	go func() { mgr.Close(); close(closed) }()

	// Close has taken the registry lock and found nothing to drain; the room still
	// loading is invisible to it.
	waitFor(t, "shutdown to be under way", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return mgr.closed
	})

	close(store.loadRelease)
	select {
	case err := <-joinErr:
		if !errors.Is(err, errShuttingDown) {
			t.Fatalf("join during shutdown returned %v, want errShuttingDown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the join never completed")
	}

	close(store.deleteRelease)
	<-closed

	mgr.mu.Lock()
	_, registered := mgr.rooms[doc]
	mgr.mu.Unlock()
	if registered {
		t.Fatal("a room materialized during shutdown was registered after the drain snapshot; nothing will ever flush it")
	}
	// Probe only while the registry is still open. Close() closes it last, and a
	// CLOSED registry cannot be holding anything — so "closed" is not evidence of
	// the leak this guards against, it is the absence of the possibility. Racing
	// the probe against Close and calling the resulting error a failure is what
	// made this flake.
	if registryOpen(mgr.registry) && residentInRegistry(t, mgr.registry, doc) {
		t.Fatal("the refused room was never torn down; its document stays resident and its registry handle outlives the Manager that made it")
	}
}

// registryOpen reports whether a registry still accepts acquisitions.
func registryOpen(reg memory.Registry) bool {
	handle, err := reg.Acquire(context.Background(), backend.DocumentID("registry-open-probe"), func(context.Context) (*ycrdt.Doc, error) {
		return newRoomDoc("registry-open-probe"), nil
	})
	if err != nil {
		return false
	}
	handle.Release()
	_ = reg.Evict(backend.DocumentID("registry-open-probe"))
	return true
}
