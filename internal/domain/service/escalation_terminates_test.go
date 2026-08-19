package service

import (
	"testing"
	"time"

	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestEscalationTerminatesTheRoomRatherThanSpinning is the regression for a
// defect found in adversarial review (T067).
//
// Durability escalation tears the room down from INSIDE the save-timer branch of
// the run loop — persistNow → persist → onFlushFailed → escalateUndurable →
// teardown. That branch then fell through to re-arm the retry timer, and the
// loop had no exit on it, so the "torn down" room kept running: timer fires,
// save fails again, threshold is still crossed, escalate again. Forever.
//
// The observability damage is the worst part. DurabilityEscalationsTotal means
// "we discarded someone's unsaved edits" — the signal an operator would page on.
// One document on a broken store would drive it to thousands, so the metric that
// exists to make data loss visible would instead make one incident look like a
// cluster-wide catastrophe, while the room's goroutine and its failing I/O
// continued indefinitely.
//
// Non-vacuity: remove the post-persist exit from the save-timer branch and this
// fails with the escalation count climbing.
func TestEscalationTerminatesTheRoomRatherThanSpinning(t *testing.T) {
	store := newOutageStore() // every save fails
	metrics := &durabilityMetrics{}
	open := authopen.New()

	mgr := NewManager(Deps{
		Metadata:   metainmem.New(),
		Checkpoint: store,
		Auth:       open,
		AuthZ:      open,
	}, RoomConfig{
		SendBuffer:   64,
		SaveDebounce: 5 * time.Millisecond,
		IdleTimeout:  time.Hour, // long: only escalation may end this room
		Limits:       Limits{FlushFailureThreshold: 2},
	}, metrics, zap.NewNop())
	t.Cleanup(mgr.Close)

	a := newFakeClient(t)
	a.join(mgr, model.DocumentID("escalate-once"), model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("doomed ")

	waitFor(t, "the room to escalate", func() bool { return len(metrics.escalations()) > 0 })
	waitFor(t, "the room to be released", func() bool { return mgr.RoomCount() == 0 })

	// Give a spinning loop ample opportunity to escalate again. The retry backoff
	// starts at SaveDebounce, so several cycles would have elapsed.
	first := len(metrics.escalations())
	time.Sleep(300 * time.Millisecond)

	if got := len(metrics.escalations()); got != first {
		t.Fatalf("escalations grew from %d to %d after the room was torn down; the run loop is still spinning on a failing store, inflating the data-loss metric an operator pages on and never releasing its goroutine", first, got)
	}
	if first != 1 {
		t.Fatalf("escalated %d times for one document; the signal means 'unsaved edits were discarded' and must fire once per incident", first)
	}
}
