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
