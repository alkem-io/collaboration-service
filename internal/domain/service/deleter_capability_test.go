package service

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
)

// mustEncode builds a real v2 update carrying s.
func mustEncode(t *testing.T, s string) []byte {
	t.Helper()
	room := newBareRoom(t)
	insertText(room.doc, s)
	update, err := ycrdt.EncodeStateAsUpdateV2(room.doc, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return update
}

// nonDeletingStore satisfies CheckpointStore but NOT persistence.Deleter, which
// the contract explicitly permits: deletion is optional because some media are
// forbidden to delete (WORM storage, object locks, regulated archival tiers).
type nonDeletingStore struct{}

func (nonDeletingStore) SaveCheckpoint(context.Context, persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	return 0, nil
}

func (nonDeletingStore) LoadCheckpoint(context.Context, backend.DocumentID) (persistence.Checkpoint, error) {
	return persistence.Checkpoint{}, persistence.ErrNotFound
}

func (nonDeletingStore) FenceMode() persistence.FenceMode { return persistence.Unfenced }

// TestDeleterFailsLoudlyWhenTheStoreCannotDelete covers the type-assertion miss.
//
// This is the whole reason deletion is an optional capability rather than a
// method on CheckpointStore: a caller that needs erasure must fail loudly when it
// is absent, which is strictly better than a store whose Delete silently does
// nothing. A silent no-op here would report a successful owner-delete cascade
// while the document's content stayed on disk — the one failure mode an erasure
// path must never have.
//
// app.New asserts persistence.DeletingCheckpointStore at construction so this is
// unreachable in a wired service; it is the backstop for a directly-constructed
// Deps, and the error must name the offending type so the misconfiguration is
// identifiable.
func TestDeleterFailsLoudlyWhenTheStoreCannotDelete(t *testing.T) {
	deps := Deps{Checkpoint: nonDeletingStore{}}

	del, err := deps.deleter()
	if err == nil {
		t.Fatal("a store that cannot delete must be reported; a silent no-op would report a successful erasure while the content remained")
	}
	if del != nil {
		t.Fatal("a failed capability check must not return a deleter")
	}
	if !contains(err.Error(), "nonDeletingStore") {
		t.Fatalf("error = %v, want it to name the offending store type", err)
	}
}

// TestDeleterIsDerivedFromTheCheckpointStore covers the hit branch and pins WHY
// it is a type assertion rather than a second Deps field: the reader and the
// deleter must be the same instance. A struct with both invites wiring one store
// as the reader and a different one as the deleter — a bug that compiles, passes
// most tests, and silently deletes nothing.
func TestDeleterIsDerivedFromTheCheckpointStore(t *testing.T) {
	deps := newTestDeps()

	del, err := deps.deleter()
	if err != nil {
		t.Fatalf("deleter(): %v", err)
	}

	// Prove it is the SAME store: seed through Checkpoint, delete through the
	// derived deleter, and the seeded state must be gone.
	ctx := context.Background()
	if err := deps.putState(ctx, "same-instance", mustEncode(t, "content")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := del.Delete(ctx, persistence.DeleteRequest{DocumentID: "same-instance"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := deps.storedState(ctx, "same-instance"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("load after delete = %v, want ErrNotFound; the deleter is not the same store as the reader", err)
	}
}
