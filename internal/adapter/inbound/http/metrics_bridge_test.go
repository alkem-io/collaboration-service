package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPrometheusMetricsBridge asserts the room-lifecycle hooks move the
// Prometheus collectors the /metrics endpoint exposes. It drives the bridge
// directly (the domain calls these), then scrapes the registry.
func TestPrometheusMetricsBridge(t *testing.T) {
	InitMetrics()
	m := PrometheusMetrics{}

	m.RoomOpened()
	m.RoomOpened()
	m.RoomClosed()
	m.ConnOpened()
	m.ConnClosed()
	m.ConnOpened()
	m.SnapshotSaved()
	m.SnapshotFailed()
	m.FanoutPublished(2 * time.Millisecond)
	m.FanoutFailed()
	m.ContributingActors(3) // north-star contribution histogram (FR-014)

	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()

	for _, want := range []string{
		"collaboration_rooms_active",
		"collaboration_connections_active",
		`collaboration_snapshots_total{outcome="saved"}`,
		`collaboration_snapshots_total{outcome="error"}`,
		`collaboration_fanout_total{outcome="published"}`,
		`collaboration_fanout_total{outcome="error"}`,
		"collaboration_fanout_lag_seconds",
		// The contribution metric is now a histogram (not an unlabeled gauge), so
		// it exposes _bucket/_sum/_count series rather than a single value.
		"collaboration_contributing_actors_per_window_bucket",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

// TestContributingActorsHistogramAggregatesAcrossRooms asserts the contribution
// metric is a histogram that aggregates per-room/per-window observations rather
// than an unlabeled gauge that is last-window-wins (one room clobbering another).
// Three rooms each flush a window; the histogram _count must reflect all three
// observations, not just the most recent one.
func TestContributingActorsHistogramAggregatesAcrossRooms(t *testing.T) {
	InitMetrics()
	m := PrometheusMetrics{}

	// Baseline _count before this test's observations (the registry is shared).
	before := histogramCount(t, "collaboration_contributing_actors_per_window")

	// Three independent rooms each flush their window.
	m.ContributingActors(2) // room A: 2 actors
	m.ContributingActors(5) // room B: 5 actors
	m.ContributingActors(1) // room C: 1 actor

	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "collaboration_contributing_actors_per_window_bucket") {
		t.Fatal("/metrics missing the contribution histogram buckets (is it still a gauge?)")
	}

	after := histogramCount(t, "collaboration_contributing_actors_per_window")
	if got := after - before; got != 3 {
		t.Errorf("histogram _count delta = %d, want 3 (one observation per room — a gauge would be 1, last-window-wins)", got)
	}
}

// histogramCount scrapes the shared registry and returns the named histogram's
// _count value, so a test can assert each flush is a distinct observation.
func histogramCount(t *testing.T, name string) int {
	t.Helper()
	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := name + "_count "
	for _, line := range strings.Split(rr.Body.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			var v int
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, prefix), "%d", &v); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return 0 // not yet observed.
}

// TestLifecycleObserverBridgeMovesItsSeries asserts the two lifecycle signals
// reach the collectors and land on distinct label sets.
//
// The label split is the point. A single undifferentiated "a transfer happened"
// counter would answer neither question the ladder poses: a transfer to the 30s
// tier is a backend that just failed and has ~35 minutes of runway, while a
// transfer to the DLQ is a deletion or revocation that will now never be applied
// without a human. Collapsing them means either alerting on everything or noticing
// nothing.
func TestLifecycleObserverBridgeMovesItsSeries(t *testing.T) {
	InitMetrics()
	o := PrometheusLifecycleObserver{}

	o.EventTransferred("lifecycle-q.retry.30s", true)
	o.EventTransferred("lifecycle-q.dlq", true)
	o.EventTransferred("lifecycle-q.retry.5m", false)
	o.QueueDepth("lifecycle-q.dlq", 7)
	o.QueueDepth("lifecycle-q.retry.30m", 2)

	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()

	for _, want := range []string{
		`collaboration_lifecycle_transfers_total{outcome="confirmed",queue="lifecycle-q.retry.30s"} 1`,
		`collaboration_lifecycle_transfers_total{outcome="confirmed",queue="lifecycle-q.dlq"} 1`,
		`collaboration_lifecycle_transfers_total{outcome="unconfirmed",queue="lifecycle-q.retry.5m"} 1`,
		`collaboration_lifecycle_queue_depth{queue="lifecycle-q.dlq"} 7`,
		`collaboration_lifecycle_queue_depth{queue="lifecycle-q.retry.30m"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}

	// Depth is a LEVEL, not a running total: a later poll replaces the reading.
	// If it accumulated, a queue that drained would still read as backed up and
	// the DLQ alert would never clear.
	o.QueueDepth("lifecycle-q.dlq", 3)
	rr = httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := rr.Body.String(); !strings.Contains(body, `collaboration_lifecycle_queue_depth{queue="lifecycle-q.dlq"} 3`) {
		t.Error("a second depth reading did not replace the first; depth must be a level, or a drained queue still reads as backed up")
	}
}
