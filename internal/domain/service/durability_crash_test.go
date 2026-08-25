package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestRecoversToTheLastCompletedFlushAcrossRestarts is T020 / SC-004.
//
// A restart must return the document to its last COMPLETED flush — not to an
// older one, and not to an empty document. The distinction matters most in the
// repeat: a recovery path that works once but degrades over cycles (each restart
// losing a little, or re-seeding over real content) looks correct in a
// single-cycle test and is catastrophic in service.
//
// Restart is modelled by discarding the Manager and building a new one over the
// SAME store, which is exactly what a pod restart is from the document's point of
// view: process state gone, durable state intact. The registry goes with the old
// Manager, so nothing is carried over in memory.
//
// Non-vacuity: make persist write a delta instead of the complete state, or make
// the restore seed instead of loading, and a later cycle reads back short.
func TestRecoversToTheLastCompletedFlushAcrossRestarts(t *testing.T) {
	deps := newTestDeps() // the store survives every restart below
	const doc model.DocumentID = "restart-me"

	cfg := RoomConfig{
		SendBuffer:   64,
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Millisecond,
	}

	want := ""
	for cycle := range 4 {
		mgr := NewManager(deps.Deps, cfg, nil, zap.NewNop())

		c := newFakeClient(t)
		c.join(mgr, doc, model.ContentTypeMemo)
		c.observeUpdates()

		// Everything written in earlier cycles must already be here BEFORE this
		// cycle writes anything — the recovery assertion.
		if cycle > 0 {
			waitFor(t, fmt.Sprintf("cycle %d sees the previous cycles' content", cycle), func() bool {
				return contains(c.text(), want)
			})
			if got := c.text(); !contains(got, want) {
				t.Fatalf("cycle %d recovered %q, want it to contain %q; a restart must return the document to its last completed flush", cycle, got, want)
			}
		}

		written := fmt.Sprintf("cycle-%d ", cycle)
		c.insertText(written)
		want += written

		// Wait for the flush to COMPLETE before simulating the crash. This test is
		// about recovering to the last completed flush; racing the debounce would
		// make it about whether an in-flight flush landed, which is a different
		// (and legitimately lossy) question.
		waitFor(t, fmt.Sprintf("cycle %d flushed", cycle), func() bool {
			blob, err := deps.storedState(context.Background(), string(doc))
			return err == nil && len(blob) > 0
		})
		waitFor(t, fmt.Sprintf("cycle %d content durable", cycle), func() bool {
			return storedTextContains(t, deps, doc, written)
		})

		// The "crash": drop the Manager (and with it the registry and every live
		// room) without a graceful shutdown flush.
		mgr.Close()
	}

	// Final verification from a cold Manager: every cycle's content survived.
	mgr := NewManager(deps.Deps, cfg, nil, zap.NewNop())
	t.Cleanup(mgr.Close)
	c := newFakeClient(t)
	c.join(mgr, doc, model.ContentTypeMemo)
	c.observeUpdates()
	waitFor(t, "final cold load sees every cycle", func() bool { return contains(c.text(), want) })
	if got := c.text(); !contains(got, want) {
		t.Fatalf("after 4 restart cycles the document reads %q, want it to contain %q; recovery degraded across cycles", got, want)
	}
}

// storedTextContains reports whether the DURABLE state (not the live room) holds
// the given text, by materializing a throwaway doc from the stored bytes.
//
// Asserting against the live room would prove only that the edit was applied in
// memory, which is the thing a restart discards.
func storedTextContains(t *testing.T, deps testDeps, id model.DocumentID, want string) bool {
	t.Helper()
	blob, err := deps.storedState(context.Background(), string(id))
	if err != nil {
		return false
	}
	scratch := newRoomDoc(string(id))
	if err := ycrdt.ApplyUpdateV2(scratch, blob, nil); err != nil {
		return false
	}
	return contains(xmlText(scratch), want)
}
