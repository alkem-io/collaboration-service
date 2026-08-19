package inprocess

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/antst/go-yjs/backend/conformance"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
)

// realUpdate builds a genuine Yjs v2 update. Opaque bytes will not do: this store
// derives the state vector from the stored update on read, so a fixture that does
// not parse is reported as ErrCorrupt rather than round-tripping.
func realUpdate(t *testing.T, text string) []byte {
	t.Helper()
	doc := ycrdt.NewDoc("fixture")
	doc.GetText("content").Insert(0, text, ycrdt.Object{})
	update, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	return update
}

// TestCheckpointConformance runs the core's adversarial checkpoint contract
// against this store (FR-008, SC-006).
//
// The suite is mutation-tested upstream — deliberately broken stores (aliasing
// on save, aliasing on load, frozen revisions, silent missing, ignored
// cancellation, unchecked fences) are each required to fail it — so passing
// means something rather than merely not crashing.
func TestCheckpointConformance(t *testing.T) {
	conformance.CheckpointPersistence(t, func() persistence.CheckpointStore { return New() })
}

// TestCheckpointFencingConformance exercises stale-owner rejection against a
// FENCED instance (FR-008a, SC-017).
//
// Note the scope: this is the ONLY store that can be fenced. The file-service
// store reports Unfenced by design, because a file row has nowhere durable to
// hold the epoch and keeping it in a separate service is not a substitute for a
// persistence-level backstop (research.md D6a). Exercising the fenced path here
// keeps the capability honest without pretending production has it.
func TestCheckpointFencingConformance(t *testing.T) {
	conformance.CheckpointPersistenceFencing(t, func() persistence.CheckpointStore { return NewFenced() })
}

// TestCheckpointDeletionConformance runs the core's deletion contract (go-yjs
// v0.0.3) against this store (FR-023).
//
// Deletion is OPTIONAL in the contract — some media are forbidden to delete — so
// a store that implements it has to prove all four properties rather than merely
// having a method named Delete: idempotent, load-after-delete reports ErrNotFound
// (not "eventually gone"), cancellation honoured, and a REJECTED delete leaves
// the state intact, which is what stops a superseded owner erasing what its
// replacement is serving.
//
// Four deliberately broken stores are run against this suite upstream and each
// must fail it (non-idempotent, leaves state, purges everything, ignores
// cancellation), so passing means something.
func TestCheckpointDeletionConformance(t *testing.T) {
	conformance.CheckpointPersistenceDeletion(t, func() persistence.DeletingCheckpointStore { return New() })
}

// TestFencedDeleteRejectsAStaleOwnerAndLeavesStateIntact covers the one deletion
// property no shipped suite reaches for this store.
//
// The core ships PersistenceDeletionFencing for the LOG profile only; there is no
// fenced CHECKPOINT deletion suite, and this store implements the checkpoint
// profile. The property is worth asserting anyway, and locally is the only place
// it can be: a superseded owner must not be able to erase state its replacement
// is serving, and "rejected" has to mean the state is still there afterwards —
// an error return that had already removed the blob would be the same data loss
// with a better error message.
func TestFencedDeleteRejectsAStaleOwnerAndLeavesStateIntact(t *testing.T) {
	store := NewFenced()
	ctx := context.Background()

	update := realUpdate(t, "fenced-delete")
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("derived-on-read"), Fence: 7,
	}); err != nil {
		t.Fatalf("save under the current fence: %v", err)
	}

	// A superseded owner (lower epoch) tries to delete.
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc", Fence: 3}); !errors.Is(err, persistence.ErrStaleFence) {
		t.Fatalf("Delete from a stale owner = %v, want ErrStaleFence", err)
	}
	cp, err := store.LoadCheckpoint(ctx, "doc")
	if err != nil {
		t.Fatalf("a REJECTED delete must leave the state intact, but the load failed: %v", err)
	}
	if !bytes.Equal(cp.Update, update) {
		t.Fatal("a rejected delete altered the stored state")
	}

	// A fenced store must also reject a delete carrying no fence at all, rather
	// than treating zero as "unfenced, go ahead".
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc"}); !errors.Is(err, persistence.ErrFenceRequired) {
		t.Fatalf("Delete with fence zero on a FENCED store = %v, want ErrFenceRequired", err)
	}

	// The current owner deletes successfully.
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc", Fence: 7}); err != nil {
		t.Fatalf("Delete from the current owner: %v", err)
	}
	if _, err := store.LoadCheckpoint(ctx, "doc"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("LoadCheckpoint after a successful Delete = %v, want ErrNotFound", err)
	}

	// THE HIGH-WATER MARK SURVIVES THE DELETE. The natural implementation forgets
	// the epoch along with the state — they are both "this document's data" — and
	// that is precisely wrong: a superseded owner could then write again the
	// moment a document is deleted, which is exactly when a cascade is running and
	// a stale node is most likely to still be trying. The document would come back
	// under an epoch that was already retired.
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("derived-on-read"), Fence: 3,
	}); !errors.Is(err, persistence.ErrStaleFence) {
		t.Fatalf("save from a stale owner AFTER a delete = %v, want ErrStaleFence; deleting the state must not reset the fence high-water mark", err)
	}
}
