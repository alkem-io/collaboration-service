package inprocess

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/go-yjs/backend/conformance"
	"github.com/antst/go-yjs/backend/persistence"
)

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

// --- behaviours the shipped suites do not reach for this store ---

// TestSaveRefusesAnUnknownEncoding covers the codec branch no conformance suite
// exercises: a value that is neither V1, V2, nor the unspecified zero.
//
// It matters because the failure it prevents is silent. A store that accepted an
// encoding it does not understand would write the bytes and later hand them back
// labelled with a codec nobody can decode — and the wrong decoder does not error,
// it returns an EMPTY state vector with a nil error, which reads as "this document
// has nothing from any client". Refusing at the boundary is what keeps that from
// becoming durable.
func TestSaveRefusesAnUnknownEncoding(t *testing.T) {
	store := New()
	_, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc",
		Encoding:   persistence.CheckpointEncoding(200),
		Update:     []byte("x"),
	})
	if !errors.Is(err, persistence.ErrUnsupportedEncoding) {
		t.Fatalf("saving an unknown encoding = %v, want ErrUnsupportedEncoding", err)
	}
	if _, err := store.LoadCheckpoint(context.Background(), "doc"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("a refused save left state behind: load = %v, want ErrNotFound", err)
	}
}

// TestDeleteRefusesAFenceThisStoreCannotHonour pins the erasure half of the
// Unfenced contract.
//
// This store reports Unfenced and has no epoch state, so a caller passing a fence
// is asking for stale-owner protection that does not exist here. Accepting the
// delete anyway would erase the document while the caller believes its epoch was
// checked — the caller is wrong about the guarantee, and the cost of being wrong
// is deleted content.
//
// The assertion that carries the weight is the SECOND one: rejection must happen
// before anything is removed. A store returning the right error after erasing
// would satisfy an error-only check while the document was already gone.
func TestDeleteRefusesAFenceThisStoreCannotHonour(t *testing.T) {
	store := New()
	ctx := context.Background()
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2,
		Update: []byte("state"), StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc", Fence: 7})
	if !errors.Is(err, persistence.ErrUnexpectedFence) {
		t.Fatalf("Delete with a fence on an Unfenced store = %v, want ErrUnexpectedFence", err)
	}
	if _, err := store.LoadCheckpoint(ctx, "doc"); err != nil {
		t.Fatalf("a REJECTED delete must leave the state intact, but the load failed: %v", err)
	}
}
