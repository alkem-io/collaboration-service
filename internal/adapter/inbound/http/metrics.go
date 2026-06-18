// Package http is the inbound HTTP adapter: the chi v5 router that exposes the
// operational surface (liveness, readiness, Prometheus metrics) and mounts the
// inbound WebSocket collaboration endpoint. It owns no business logic — handlers
// translate HTTP to domain calls and back.
package http

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce sync.Once

	// registry is the service's own Prometheus registry. Using a dedicated
	// registry (rather than the global default) keeps the exposed metric set
	// explicit and test-isolated.
	registry = prometheus.NewRegistry()

	// RoomsActive is the number of live, materialized rooms (documents with at
	// least one connected client). Wired by the room lifecycle (task T007).
	RoomsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "collaboration",
		Name:      "rooms_active",
		Help:      "Number of live in-memory document rooms.",
	})

	// ConnectionsActive is the number of open WebSocket connections across all
	// rooms. Wired by the WS inbound adapter (task T008).
	ConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "collaboration",
		Name:      "connections_active",
		Help:      "Number of open collaboration WebSocket connections.",
	})

	// SnapshotsTotal counts persisted snapshots by outcome (saved | error),
	// the alertable signal behind the saved/save-error control messages (R7).
	// Wired by the snapshot persistence task (T011).
	SnapshotsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "collaboration",
		Name:      "snapshots_total",
		Help:      "Persisted Y.Doc snapshots by outcome.",
	}, []string{"outcome"})

	// FanoutTotal counts cross-pod fan-out publishes by outcome
	// (published | error). Zero on single-pod (the in-memory broadcaster never
	// publishes). Wired by the redis fan-out (task T004, R10).
	FanoutTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "collaboration",
		Name:      "fanout_total",
		Help:      "Cross-pod fan-out publishes by outcome.",
	}, []string{"outcome"})

	// FanoutLagSeconds observes the latency of a cross-pod fan-out publish
	// (R10). The local publish duration; cross-pod end-to-end delivery lag is an
	// e2e concern (T017.2).
	FanoutLagSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "collaboration",
		Name:      "fanout_lag_seconds",
		Help:      "Latency of a cross-pod fan-out publish.",
		Buckets:   []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25},
	})
)

// InitMetrics registers the service collectors on the dedicated registry,
// including the standard Go runtime and process collectors. Idempotent.
func InitMetrics() {
	metricsOnce.Do(func() {
		registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
			RoomsActive,
			ConnectionsActive,
			SnapshotsTotal,
			FanoutTotal,
			FanoutLagSeconds,
		)
	})
}

// MetricsHandler returns the /metrics HTTP handler over the service registry.
// InitMetrics must have been called first.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry})
}

// PrometheusMetrics bridges the room-lifecycle observability hooks
// (service.Metrics) to the service's Prometheus collectors. It is the single
// place the domain's lifecycle events become metrics, keeping the core free of a
// Prometheus import (hexagon §I).
type PrometheusMetrics struct{}

// RoomOpened increments the active-rooms gauge.
func (PrometheusMetrics) RoomOpened() { RoomsActive.Inc() }

// RoomClosed decrements the active-rooms gauge.
func (PrometheusMetrics) RoomClosed() { RoomsActive.Dec() }

// ConnOpened increments the active-connections gauge.
func (PrometheusMetrics) ConnOpened() { ConnectionsActive.Inc() }

// ConnClosed decrements the active-connections gauge.
func (PrometheusMetrics) ConnClosed() { ConnectionsActive.Dec() }

// SnapshotSaved counts a persisted snapshot.
func (PrometheusMetrics) SnapshotSaved() { SnapshotsTotal.WithLabelValues("saved").Inc() }

// SnapshotFailed counts a failed snapshot persist.
func (PrometheusMetrics) SnapshotFailed() { SnapshotsTotal.WithLabelValues("error").Inc() }

// FanoutPublished counts a cross-pod publish and records its lag (R10).
func (PrometheusMetrics) FanoutPublished(lag time.Duration) {
	FanoutTotal.WithLabelValues("published").Inc()
	FanoutLagSeconds.Observe(lag.Seconds())
}

// FanoutFailed counts a failed cross-pod publish.
func (PrometheusMetrics) FanoutFailed() { FanoutTotal.WithLabelValues("error").Inc() }
