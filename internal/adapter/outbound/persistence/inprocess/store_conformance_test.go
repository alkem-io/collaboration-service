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
