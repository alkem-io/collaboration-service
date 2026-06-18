package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

	rr := httptest.NewRecorder()
	MetricsHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()

	for _, want := range []string{
		"collaboration_rooms_active",
		"collaboration_connections_active",
		`collaboration_snapshots_total{outcome="saved"}`,
		`collaboration_snapshots_total{outcome="error"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}
