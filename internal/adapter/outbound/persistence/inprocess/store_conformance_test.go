package inprocess

import (
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
