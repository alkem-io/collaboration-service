package http

import (
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// preRebuildSignals is the exact set of metric names this service exported
// BEFORE the persistence rebuild, transcribed from metrics.go at commit
// 45d8267^ (the parent of the CheckpointStore migration).
//
// It is a FROZEN list on purpose. Deriving it from the current code would make
// the test tautological: it would agree with whatever the code exports today,
// including after a rename that silently breaks every alert pointed at the old
// name.
var preRebuildSignals = []string{
	"collaboration_rooms_active",
	"collaboration_connections_active",
	"collaboration_snapshots_total",
	"collaboration_fanout_total",
	"collaboration_fanout_lag_seconds",
	"collaboration_contributing_actors_per_window",
}

// durabilitySignals are the signals the rebuild ADDED (FR-026). Their obligation
// is the mirror of the frozen set's: that one must not shrink, these must exist
// at all. FR-026 requires the degraded and escalated states to be visible as
// METRICS rather than only as log lines, because an operator alerts on metrics
// and reads logs afterwards.
var durabilitySignals = []string{
	"collaboration_undurable_flush_failures",
	"collaboration_undurable_seconds",
	"collaboration_durability_escalations_total",
	"collaboration_escalation_undurable_seconds",
}

// TestNoPersistenceSignalWasLostInTheRebuild is T069 / FR-025 / SC-014.
//
// The persistence layer was rebuilt onto a different contract and four adapters
// were deleted. A metric lost in that churn breaks no test, no build and no
// deploy — it goes unnoticed until an alert fails to fire during an incident,
// which is the worst possible moment to find out. Metric names are an external
// contract with whatever scrapes them, so a RENAME is a break even though the
// underlying signal still exists.
//
// Every hook is driven through the bridge before gathering, which is deliberate:
// a labelled collector (CounterVec) exports no family at all until a label value
// is observed, so a registration-only check would report snapshots_total and
// fanout_total as missing while they are perfectly healthy.
//
// Presence alone is checked here; that each hook actually MOVES its series is
// TestEveryMetricsHookMovesItsSeries, because presence cannot detect it — an
// unlabelled counter is exported at zero whether or not anything increments it.
//
// Non-vacuity: drop any collector from InitMetrics or rename one, and this fails
// naming the missing series.
func TestNoPersistenceSignalWasLostInTheRebuild(t *testing.T) {
	InitMetrics()
	exerciseEveryMetricsHook()

	exported := exportedNames(t)
	for _, name := range preRebuildSignals {
		if !exported[name] {
			t.Errorf("metric %q existed before the persistence rebuild and is not exported now; anything alerting on it fails silently, and the first symptom is a missed page during an incident (FR-025)", name)
		}
	}
	for _, name := range durabilitySignals {
		if !exported[name] {
			t.Errorf("durability metric %q is not exported; the degraded/escalated state must be visible as a METRIC, not only in logs (FR-026)", name)
		}
	}
}

// TestEveryMetricsHookMovesItsSeries asserts the BRIDGE is wired, not merely that
// collectors were declared.
//
// This is the half presence cannot cover. An unlabelled counter or gauge is
// exported at zero from the moment it is registered, so a hook with an empty body
// leaves a series that exists, scrapes cleanly, and never moves — which on a
// dashboard reads as "no failures ever happened" rather than as a broken metric.
// That is strictly worse than a missing series, because a missing series is
// visibly missing.
//
// Non-vacuity: empty any PrometheusMetrics method body and its row fails.
func TestEveryMetricsHookMovesItsSeries(t *testing.T) {
	InitMetrics()
	var m service.Metrics = PrometheusMetrics{}

	for _, c := range []struct {
		hook   string
		series string
		call   func()
	}{
		{"RoomOpened", "collaboration_rooms_active", m.RoomOpened},
		{"ConnOpened", "collaboration_connections_active", m.ConnOpened},
		{"SnapshotSaved", "collaboration_snapshots_total", m.SnapshotSaved},
		{"SnapshotFailed", "collaboration_snapshots_total", m.SnapshotFailed},
		{"DocumentUndurable", "collaboration_undurable_flush_failures", func() { m.DocumentUndurable("doc-inv", 3, time.Second) }},
		{"DocumentUndurable", "collaboration_undurable_seconds", func() { m.DocumentUndurable("doc-inv", 4, 5*time.Second) }},
		{"DocumentEscalated", "collaboration_durability_escalations_total", func() { m.DocumentEscalated("doc-inv", time.Second) }},
		{"DocumentEscalated", "collaboration_escalation_undurable_seconds", func() { m.DocumentEscalated("doc-inv", 2*time.Second) }},
		{"FanoutFailed", "collaboration_fanout_total", m.FanoutFailed},
		{"FanoutPublished", "collaboration_fanout_lag_seconds", func() { m.FanoutPublished(3 * time.Millisecond) }},
		{"ContributingActors", "collaboration_contributing_actors_per_window", func() { m.ContributingActors(4) }},
	} {
		before := seriesValue(t, c.series)
		c.call()
		if after := seriesValue(t, c.series); after == before {
			t.Errorf("%s() left %s unchanged at %v; the hook is declared but not wired, so the series scrapes cleanly and never moves — which reads as 'nothing ever happened' rather than as a broken metric", c.hook, c.series, before)
		}
	}

	// The CLEARING halves, asserted separately so the pairs above are not left
	// netting to zero — which would make the increments unobservable.
	//
	// Each carries a setup call that puts the series somewhere other than its
	// resting value first. Without that, "clears the gauge" and "does nothing" are
	// the same observation, and the row would pass against an empty method body.
	for _, c := range []struct {
		hook   string
		series string
		setup  func()
		call   func()
	}{
		{"RoomClosed", "collaboration_rooms_active", m.RoomOpened, func() { m.RoomClosed("doc-inv") }},
		{"ConnClosed", "collaboration_connections_active", m.ConnOpened, m.ConnClosed},
		{
			"DocumentDurabilityRestored", "collaboration_undurable_flush_failures",
			func() { m.DocumentUndurable("doc-inv", 7, 9*time.Second) }, func() { m.DocumentDurabilityRestored("doc-inv") },
		},
		{
			"DocumentDurabilityRestored", "collaboration_undurable_seconds",
			func() { m.DocumentUndurable("doc-inv", 7, 9*time.Second) }, func() { m.DocumentDurabilityRestored("doc-inv") },
		},
	} {
		c.setup()
		before := seriesValue(t, c.series)
		c.call()
		if after := seriesValue(t, c.series); after == before {
			t.Errorf("%s() left %s unchanged at %v; the clearing half of a durability signal must move too, or a document appears permanently degraded after it recovered", c.hook, c.series, before)
		}
	}
}

// seriesValue sums every sample in a metric family, so one accessor works for
// gauges, counters, labelled counters and histograms alike. Histograms are summed
// by observation COUNT rather than value: an observation of zero is still an
// observation, and a hook that fires with a zero duration must still be visible.
func seriesValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	total := 0.0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, mt := range f.GetMetric() {
			switch {
			case mt.GetHistogram() != nil:
				total += float64(mt.GetHistogram().GetSampleCount())
			case mt.GetCounter() != nil:
				total += mt.GetCounter().GetValue()
			case mt.GetGauge() != nil:
				total += mt.GetGauge().GetValue()
			}
		}
	}
	return total
}

// exerciseEveryMetricsHook calls every method on the domain-facing bridge, so a
// hook that was left unwired shows up as an absent series.
//
// The service.Metrics assertion is the load-bearing part: adding a hook to the
// interface without extending this function fails to compile, so the inventory
// cannot silently fall behind the surface it is meant to inventory.
func exerciseEveryMetricsHook() {
	var m service.Metrics = PrometheusMetrics{}
	m.RoomOpened()
	m.RoomClosed("doc-inv")
	m.ConnOpened()
	m.ConnClosed()
	m.SnapshotSaved()
	m.SnapshotFailed()
	m.DocumentUndurable("doc-inv", 1, time.Second)
	m.DocumentDurabilityRestored("doc-inv")
	m.DocumentEscalated("doc-inv", 2*time.Second)
	m.FanoutPublished(3 * time.Millisecond)
	m.FanoutFailed()
	m.ContributingActors(4)
}

// exportedNames collects every metric family name the SERVICE registry exposes.
//
// It gathers from the registry the /metrics handler serves, not the global
// default: a collector that exists in Go but was never registered here is not
// scrapeable, and therefore not a signal, however healthy it looks in source.
func exportedNames(t *testing.T) map[string]bool {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names
}
