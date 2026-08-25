package service

import (
	"context"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestFailedMaterializationReleasesTheRegistryHandle is the regression for a
// permanent, silent leak found in adversarial review (T067).
//
// newRoom acquires the document from the registry and only afterwards subscribes
// to fan-out. If that subscribe fails — a Redis outage at exactly the wrong
// moment — the room returns an error, but the HANDLE it already took is never
// released.
//
// The consequence is worse than a leaked object. A held handle makes Evict
// return ErrInUse forever, so that document can never be evicted OR invalidated
// for the life of the process, and every later Acquire is handed the same stale
// in-memory document. A restart is the only recovery, and nothing in the logs
// points at it: the visible symptom is one document that stops picking up
// changes from storage while every other document behaves.
//
// Non-vacuity: remove the release on the error path and this fails — the probe
// Acquire finds the document still resident.
func TestFailedMaterializationReleasesTheRegistryHandle(t *testing.T) {
	deps := newTestDeps()
	reg := memory.NewRegistry()
	deps.Registry = reg
	deps.Hub = erroringSubHub{} // subscribe fails after the handle is taken

	const doc model.DocumentID = "leaked-handle"
	if _, err := newRoom(context.Background(), doc, model.ContentTypeMemo,
		deps.Deps, fastConfig(), NopMetrics{}, zap.NewNop()); err == nil {
		t.Fatal("newRoom must fail when it cannot subscribe to fan-out")
	}

	// The registry must be able to reclaim the document. Evict returns ErrInUse
	// while any handle is outstanding, so a successful evict proves the failed
	// materialization let go of it.
	if err := reg.Evict(backend.DocumentID(doc)); err != nil {
		t.Fatalf("the failed materialization kept its registry handle (%v); that document can never be evicted or invalidated again, and every later open is handed the same stale in-memory copy", err)
	}

	// And a fresh open genuinely re-opens rather than returning the stale doc.
	opened := false
	h, err := reg.Acquire(context.Background(), backend.DocumentID(doc), func(context.Context) (*ycrdt.Doc, error) {
		opened = true
		return newRoomDoc(string(doc)), nil
	})
	if err != nil {
		t.Fatalf("acquire after the failed materialization: %v", err)
	}
	t.Cleanup(h.Release)
	if !opened {
		t.Fatal("the document was still resident after a FAILED materialization")
	}
}
