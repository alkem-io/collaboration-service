package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// degradedManager wires a manager over a store that is failing every save, with
// a threshold high enough that the test stays inside the degraded window and
// never escalates.
func degradedManager(t *testing.T, threshold int) (*Manager, *outageStore, *durabilityMetrics) {
	t.Helper()
	store := newOutageStore()
	metrics := &durabilityMetrics{}
	open := authopen.New()
	cfg := RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
		Limits:       Limits{FlushFailureThreshold: threshold},
	}
	mgr := NewManager(Deps{
		Broadcaster: noopBroadcaster{},
		Metadata:    metainmem.New(),
		Checkpoint:  store,
		Auth:        open,
		AuthZ:       open,
	}, cfg, metrics, zap.NewNop())
	return mgr, store, metrics
}

// TestDegradedDurabilityKeepsServingAndIsVisibleBeforeAnyDisconnect is SC-013.
//
// A failed flush means NOT YET DURABLE, which is not the same as DIVERGED: the
// in-memory document is still authoritative and still correct, so the session
// must keep serving and keep retrying. Tearing it down on the first backend blip
// would trade a real outage for a theoretical one.
//
// The load-bearing part is the ordering. The degraded state has to be visible on
// the METRIC surface while everyone is still connected — otherwise the first
// signal an operator gets is users being kicked off, and by then the decision has
// already been made for them. Asserting only that the metric was eventually
// emitted would pass even if it fired at escalation, so each sample records how
// many disconnects had happened when it was taken.
func TestDegradedDurabilityKeepsServingAndIsVisibleBeforeAnyDisconnect(t *testing.T) {
	// Threshold far above what this test drives: escalation is a different test.
	mgr, store, metrics := degradedManager(t, 50)

	a := newFakeClient(t)
	a.join(mgr, "degraded", model.ContentTypeMemo)
	a.observeUpdates()
	b := newFakeClient(t)
	b.join(mgr, "degraded", model.ContentTypeMemo)
	b.observeUpdates()

	a.insertText("first ")
	waitFor(t, "flush attempted and reported undurable", func() bool {
		return len(metrics.undurableSamples()) > 0
	})

	// 1. It retries rather than giving up after one failure.
	before := store.saveAttempts()
	a.insertText("second ")
	waitFor(t, "flush retried after a failure", func() bool {
		return store.saveAttempts() > before
	})

	// 2. It keeps serving: b still receives a's edits while the backend is down.
	a.insertText("third ")
	waitFor(t, "edits still propagate while undurable", func() bool {
		return contains(b.text(), "third ")
	})

	// 3. Every degraded sample was emitted while everyone was still connected.
	samples := metrics.undurableSamples()
	if len(samples) == 0 {
		t.Fatal("no DocumentUndurable metric emitted; the degraded window was invisible")
	}
	for i, s := range samples {
		if s.connClosedSoFar != 0 {
			t.Fatalf("undurable sample %d was emitted after %d disconnect(s); the degraded state must be visible BEFORE anyone is disconnected (SC-013)", i, s.connClosedSoFar)
		}
		if s.consecutive <= 0 {
			t.Fatalf("undurable sample %d reported consecutive=%d; the failure count must be carried so an operator can see the trend", i, s.consecutive)
		}
	}

	// 4. Collaborators were told their recent edits are not yet safe, with a
	//    reason that says so rather than a generic error.
	if !hasControlReason(a, model.ControlSaveError, model.ReasonNotYetDurable) {
		t.Fatalf("no save-error control carrying %q; clients cannot distinguish 'not yet saved' from 'lost'", model.ReasonNotYetDurable)
	}

	// 5. Recovery is announced too. A one-way notification leaves clients
	//    believing their work is still at risk after the backend comes back
	//    (FR-027, both halves).
	store.recover()
	a.insertText("fourth ")
	waitFor(t, "durability restored metric", func() bool { return metrics.restoredCount() > 0 })
	waitFor(t, "clients told their work is safe again", func() bool {
		return hasControlKind(a, model.ControlSaved)
	})

	// Nobody was disconnected across the whole degraded window.
	if got := metrics.escalations(); len(got) != 0 {
		t.Fatalf("escalated %d time(s) below the threshold; a transient outage must not tear the room down", len(got))
	}
	if _, err := store.LoadCheckpoint(context.Background(), "degraded"); err != nil {
		t.Fatalf("after recovery the document should be durable again: %v", err)
	}
}

// hasControlReason reports whether the client received a control of the given
// kind carrying the given reason code.
func hasControlReason(c *fakeClient, kind model.ControlKind, reason model.ReadOnlyReason) bool {
	for _, m := range c.controlMessages() {
		if m.Kind == kind && m.Reason == reason {
			return true
		}
	}
	return false
}
