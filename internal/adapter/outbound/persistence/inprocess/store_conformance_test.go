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
