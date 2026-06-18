// Package http is the inbound HTTP adapter: the chi v5 router that exposes the
// operational surface (liveness, readiness, Prometheus metrics) and mounts the
// inbound WebSocket collaboration endpoint. It owns no business logic — handlers
// translate HTTP to domain calls and back.
package http

import (
	"net/http"
	"sync"

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
		)
	})
}

// MetricsHandler returns the /metrics HTTP handler over the service registry.
// InitMetrics must have been called first.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry})
}
