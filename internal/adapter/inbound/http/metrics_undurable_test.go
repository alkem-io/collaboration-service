package http

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gaugeValue reads a gauge without pulling in the testutil subpackage (which
// would add module requirements for a two-line helper).
func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	return m.GetGauge().GetValue()
}

// TestOneDocumentRecoveringDoesNotClearAnotherDocumentsAlarm is the regression
// for a silent monitoring failure.
//
// The durability gauges were bare, unlabeled Set() calls, so they were
// last-writer-wins across every room on the pod. Document A failing every flush
// set undurable_flush_failures=3; document B — perfectly healthy, and unrelated —
// recovering or escalating then Set(0) on the SAME series. On a pod serving many
// rooms the alarm oscillated to zero on every unrelated success, so
// `collaboration_undurable_seconds > 0` never fired and the first thing an
// operator learned was that users had been disconnected. That is exactly the
// blindness FR-026/SC-013 exist to remove, and metrics.go already diagnoses this
// last-writer-wins shape for ContributingActors and solves it there.
//
// A per-document LABEL is not the fix (unbounded cardinality, one series per
// document). The fix is a bounded registry of currently-undurable documents, with
// the gauges exporting the worst of them.
//
// Non-vacuity: make DocumentDurabilityRestored call UndurableFlushFailures.Set(0)
// directly instead of deleting from the registry, and this fails — B's recovery
// zeroes A's live alarm.
func TestOneDocumentRecoveringDoesNotClearAnotherDocumentsAlarm(t *testing.T) {
	m := PrometheusMetrics{}
	t.Cleanup(func() {
		m.DocumentDurabilityRestored("doc-A")
		m.DocumentDurabilityRestored("doc-B")
	})

	// A is failing badly; B is failing mildly.
	m.DocumentUndurable("doc-A", 3, 40*time.Second)
	m.DocumentUndurable("doc-B", 1, 5*time.Second)

	if got := gaugeValue(t, UndurableDocuments); got != 2 {
		t.Fatalf("undurable documents = %v, want 2", got)
	}
	if got := gaugeValue(t, UndurableFlushFailures); got != 3 {
		t.Fatalf("flush failures = %v, want 3 (the worst document)", got)
	}

	// B recovers. A is STILL undurable and its alarm must survive.
	m.DocumentDurabilityRestored("doc-B")

	if got := gaugeValue(t, UndurableFlushFailures); got != 3 {
		t.Fatalf("flush failures after an UNRELATED document recovered = %v, want 3; a healthy room must not clear a failing room's alarm", got)
	}
	if got := gaugeValue(t, UndurableSeconds); got != 40 {
		t.Fatalf("undurable seconds after an unrelated recovery = %v, want 40", got)
	}
	if got := gaugeValue(t, UndurableDocuments); got != 1 {
		t.Fatalf("undurable documents = %v, want 1", got)
	}

	// Only when the last failing document clears do the gauges go quiet.
	m.DocumentDurabilityRestored("doc-A")
	if got := gaugeValue(t, UndurableFlushFailures); got != 0 {
		t.Fatalf("flush failures after ALL documents recovered = %v, want 0", got)
	}
	if got := gaugeValue(t, UndurableSeconds); got != 0 {
		t.Fatalf("undurable seconds after all recovered = %v, want 0", got)
	}
}

// TestEscalationClearsOnlyTheEscalatedDocument pins the same property for the
// data-loss path: escalating A (its edits discarded, room gone) must not silence
// B, which is still failing and still has members.
func TestEscalationClearsOnlyTheEscalatedDocument(t *testing.T) {
	m := PrometheusMetrics{}
	t.Cleanup(func() { m.DocumentDurabilityRestored("doc-B") })

	m.DocumentUndurable("doc-A", 5, 60*time.Second)
	m.DocumentUndurable("doc-B", 2, 9*time.Second)

	m.DocumentEscalated("doc-A", 60*time.Second)

	if got := gaugeValue(t, UndurableDocuments); got != 1 {
		t.Fatalf("undurable documents after one escalation = %v, want 1 (B still failing)", got)
	}
	if got := gaugeValue(t, UndurableFlushFailures); got != 2 {
		t.Fatalf("flush failures after A escalated = %v, want 2 (B's, not zero)", got)
	}
}

// TestAClosedRoomStopsBeingCountedUndurable is the regression for a registry
// leak that outlived the room.
//
// DocumentDurabilityRestored and DocumentEscalated each remove their document,
// but a room can end without reaching EITHER. Concrete supported producers:
//
//  1. a document fails a flush, then an owner delete arrives — cmdCloseDeleted
//     tears down with NO flush and no escalation;
//  2. an empty room with SaveDebounce<=0 whose final flush fails — releaseIfEmpty
//     accepts the loss and releases;
//  3. shutdown paths that end a failing room without flushing.
//
// The room is gone, but its id stayed in the registry forever: the worst-case
// gauges and collaboration_undurable_documents kept reporting a document no live
// room owned, and the map never shrank. An operator would see a permanent alarm
// with nothing behind it — which is the same class of blindness as the alarm that
// silently cleared, just inverted.
//
// The drop belongs at the LIFECYCLE owner, not on the durability paths, because
// only the lifecycle sees every ending. RoomClosed already fires exactly once per
// released registered room, so it carries the id rather than a new method being
// added. It does NOT mean "restored" — nothing was persisted, and
// DurabilityEscalationsTotal remains the only data-loss signal.
//
// Non-vacuity: drop the delete from RoomClosed and this fails — A's alarm
// survives its own teardown and undurable_documents stays at 2.
func TestAClosedRoomStopsBeingCountedUndurable(t *testing.T) {
	m := PrometheusMetrics{}
	// RoomClosed decrements the process-global RoomsActive gauge, and its contract
	// is exactly once per room that was counted open. Every RoomClosed below is
	// therefore paired with a RoomOpened, and cleanup uses
	// DocumentDurabilityRestored — which touches only the registry — so this test
	// cannot leave the shared gauge negative for whatever runs next.
	m.RoomOpened()
	m.RoomOpened()
	t.Cleanup(func() {
		m.DocumentDurabilityRestored("doc-A")
		m.DocumentDurabilityRestored("doc-B")
	})

	// Two failing documents. A is worse.
	m.DocumentUndurable("doc-A", 4, 50*time.Second)
	m.DocumentUndurable("doc-B", 2, 7*time.Second)
	if got := gaugeValue(t, UndurableDocuments); got != 2 {
		t.Fatalf("undurable documents = %v, want 2", got)
	}

	// A is deleted by its owner: torn down with no successful flush and no
	// escalation. Neither durability path runs.
	m.RoomClosed("doc-A")

	if got := gaugeValue(t, UndurableDocuments); got != 1 {
		t.Fatalf("undurable documents after a closed room = %v, want 1; the closed document is still being counted", got)
	}
	// B is still failing and its alarm must be intact — and now B is the worst.
	if got := gaugeValue(t, UndurableFlushFailures); got != 2 {
		t.Fatalf("flush failures = %v, want 2 (B's, which is still failing)", got)
	}
	if got := gaugeValue(t, UndurableSeconds); got != 7 {
		t.Fatalf("undurable seconds = %v, want 7 (B's)", got)
	}

	// And closing the last one goes quiet.
	m.RoomClosed("doc-B")
	if got := gaugeValue(t, UndurableDocuments); got != 0 {
		t.Fatalf("undurable documents after every room closed = %v, want 0", got)
	}
}

// TestEscalationThenTeardownRemovesOnce pins the idempotence that matters in
// production: an escalating room removes its own entry, and the teardown that
// follows removes again. The second removal must be a no-op for the registry.
//
// It is expressed as escalate-then-ONE-paired-RoomClosed rather than as two
// RoomClosed calls, because RoomClosed is not idempotent — it decrements
// RoomsActive — and a test that called it twice would be asserting idempotence of
// something that does not have it, while corrupting a process-global gauge.
func TestEscalationThenTeardownRemovesOnce(t *testing.T) {
	m := PrometheusMetrics{}
	m.RoomOpened()
	m.RoomOpened()
	t.Cleanup(func() {
		m.DocumentDurabilityRestored("doc-escalated")
		m.DocumentDurabilityRestored("doc-still-failing")
	})

	m.DocumentUndurable("doc-escalated", 5, 60*time.Second)
	m.DocumentUndurable("doc-still-failing", 1, 3*time.Second)

	// Escalation removes it once...
	m.DocumentEscalated("doc-escalated", 60*time.Second)
	// ...and the teardown that follows removes it again.
	m.RoomClosed("doc-escalated")

	if got := gaugeValue(t, UndurableDocuments); got != 1 {
		t.Fatalf("undurable documents = %v, want 1; the second removal must be a no-op, not a corruption", got)
	}
	if got := gaugeValue(t, UndurableFlushFailures); got != 1 {
		t.Fatalf("flush failures = %v, want 1 (the still-failing document's)", got)
	}

	m.RoomClosed("doc-still-failing")
}

// TestClosingANeverUndurableRoomIsHarmless pins that the registry drop is a no-op
// for the overwhelmingly common case, so every room teardown can call it
// unconditionally without disturbing a live alarm elsewhere on the pod.
func TestClosingANeverUndurableRoomIsHarmless(t *testing.T) {
	m := PrometheusMetrics{}
	m.RoomOpened()
	t.Cleanup(func() { m.DocumentDurabilityRestored("doc-failing") })

	m.DocumentUndurable("doc-failing", 3, 30*time.Second)
	m.RoomClosed("doc-healthy-never-failed")

	if got := gaugeValue(t, UndurableDocuments); got != 1 {
		t.Fatalf("undurable documents = %v, want 1; closing an unrelated healthy room must not disturb a live alarm", got)
	}
	if got := gaugeValue(t, UndurableFlushFailures); got != 3 {
		t.Fatalf("flush failures = %v, want 3", got)
	}
}
