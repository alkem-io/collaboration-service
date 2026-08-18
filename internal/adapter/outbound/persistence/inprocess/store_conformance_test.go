package inprocess

import (
	"testing"

	"github.com/antst/go-yjs/backend/conformance"
	"github.com/antst/go-yjs/backend/persistence"
)

// TestPersistenceConformance runs the core's own adversarial persistence
// contract against this store (FR-008, SC-006). It is the suite that proves the
// implementation honours the contract rather than merely this repo's
// expectations of it.
func TestPersistenceConformance(t *testing.T) {
	conformance.Persistence(t, func() persistence.Store { return New() })
}

// TestPersistenceCompactionConformance runs the checkpoint/compaction contract.
//
// research.md D1a: the feature was specified as checkpoint-only with compaction
// explicitly NOT implemented, on the assumption that this suite would not apply.
// That assumption was wrong — conformance.Persistence requires per-record
// fidelity and pagination, which a latest-value store cannot provide — so the
// store is a genuine log WITH compaction, and this suite applies after all.
func TestPersistenceCompactionConformance(t *testing.T) {
	conformance.PersistenceCompaction(t, func() persistence.CompactingStore { return New() })
}

// TestPersistenceFencingConformance exercises stale-owner rejection against a
// FENCED instance, even though every deployment runs unfenced (FR-008a, SC-017).
// Capability that is never exercised does not deliver the migration-avoidance
// that justified building it; testing it while it is inert is cheap, testing it
// under a coordinator rollout is not.
func TestPersistenceFencingConformance(t *testing.T) {
	conformance.PersistenceFencing(t, func() persistence.Store { return NewFenced() })
}
