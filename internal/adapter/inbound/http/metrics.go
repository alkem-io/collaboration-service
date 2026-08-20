// Package http is the inbound HTTP adapter: the chi v5 router that exposes the
// operational surface (liveness, readiness, Prometheus metrics) and mounts the
// inbound WebSocket collaboration endpoint. It owns no business logic — handlers
// translate HTTP to domain calls and back.
package http

import (
	"net/http"
	"strconv"
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

	// ContributingActors is the north-star contribution metric (FR-014): the number
	// of distinct actors that contributed to a document in one flushed window. It is
	// a HISTOGRAM, observed once per room per window — NOT an unlabeled gauge.
	//
	// A single unlabeled gauge .Set() per room is last-window-wins: every room
	// clobbers the same series, so the value reflects whichever room flushed most
	// recently, not the platform. A per-document label would be correct but
	// unbounded-cardinality (one series per document — a Prometheus anti-pattern).
	// A histogram is bounded (fixed buckets) and aggregates correctly across all
	// rooms: _count is total windows flushed, _sum is total actor-windows, and the
	// buckets show the distribution of per-window contributor counts. Always emitted
	// (independent of the RabbitMQ contribution event, which only fires in Alkemio
	// mode). Wired by the room presence machinery (task T013).
	ContributingActors = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "collaboration",
		Name:      "contributing_actors_per_window",
		Help:      "Distinct actors that contributed to a document in one flushed window (histogram, aggregated across rooms).",
		Buckets:   []float64{0, 1, 2, 3, 5, 8, 13, 21, 34, 55},
	})

	// --- durability (FR-026) ---------------------------------------------------
	//
	// These make the DEGRADED window visible before anyone is disconnected. Without
	// them a document failing every flush looks identical to a healthy one until
	// escalation kicks its members off, and the first thing an operator learns is
	// that users were dropped (SC-013).

	// UndurableFlushFailures is the current consecutive-failure count; 0 when
	// persisting normally. A gauge, not a counter: the question is "is anything
	// undurable right now", which a monotonic counter cannot answer.
	UndurableFlushFailures = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collaboration_undurable_flush_failures",
		Help: "Consecutive failed flushes for the most recently failing document (0 when durable).",
	})
	// UndurableSeconds is how long the current undurable window has lasted.
	UndurableSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collaboration_undurable_seconds",
		Help: "Seconds the most recently failing document has been accepting edits it cannot persist (0 when durable).",
	})
	// DurabilityEscalationsTotal counts rooms torn down after repeated persist
	// failures, DISCARDING their unsaved edits. Any non-zero value is data loss,
	// which is why it is its own counter rather than another "error" label on
	// SnapshotsTotal (FR-028, SC-016).
	DurabilityEscalationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "collaboration_durability_escalations_total",
		Help: "Rooms closed after repeated persist failures, discarding unsaved edits.",
	})
	// EscalationUndurableSeconds records how long each escalated document had been
	// failing, so the size of the loss window is visible after the fact.
	EscalationUndurableSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "collaboration_escalation_undurable_seconds",
		Help:    "How long an escalated document had been undurable before its edits were discarded.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	// GenerationInvalidationsTotal counts poisoned in-memory generations.
	GenerationInvalidationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "collaboration_generation_invalidations_total",
		Help: "Document generations poisoned and reloaded from storage.",
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
			UndurableFlushFailures,
			UndurableSeconds,
			DurabilityEscalationsTotal,
			EscalationUndurableSeconds,
			GenerationInvalidationsTotal,
			FanoutTotal,
			FanoutLagSeconds,
			ContributingActors,
			LifecycleTransfersTotal,
			LifecycleQueueReadyDepth,
			LifecycleDeadLetteredTotal,
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

// DocumentUndurable publishes the degraded-durability window: how many
// consecutive flushes have failed and how long the document has been accepting
// edits it cannot persist.
//
// These are the signals that make the state visible BEFORE anyone is
// disconnected (FR-026, SC-013). Without them a document failing every flush
// looks identical to a healthy one until escalation kicks its members off, and
// the first thing an operator learns is that users were dropped. Gauges rather
// than counters: the question is "is anything undurable right now, and for how
// long", which a monotonically increasing counter cannot answer.
func (PrometheusMetrics) DocumentUndurable(consecutive int, since time.Duration) {
	UndurableFlushFailures.Set(float64(consecutive))
	UndurableSeconds.Set(since.Seconds())
}

// DocumentDurabilityRestored clears the degraded-durability gauges.
func (PrometheusMetrics) DocumentDurabilityRestored() {
	UndurableFlushFailures.Set(0)
	UndurableSeconds.Set(0)
}

// DocumentEscalated counts a room torn down for repeated persist failures,
// recording how long it had been undurable. This is the DATA LOSS signal — the
// unsaved edits were discarded — so it is deliberately its own counter rather
// than another "error" label on SnapshotsTotal (FR-028, SC-016).
func (PrometheusMetrics) DocumentEscalated(undurableFor time.Duration) {
	DurabilityEscalationsTotal.Inc()
	UndurableSeconds.Set(0)
	UndurableFlushFailures.Set(0)
	EscalationUndurableSeconds.Observe(undurableFor.Seconds())
}

// GenerationInvalidated counts a poisoned in-memory generation.
func (PrometheusMetrics) GenerationInvalidated() { GenerationInvalidationsTotal.Inc() }

// FanoutPublished counts a cross-pod publish and records its lag (R10).
func (PrometheusMetrics) FanoutPublished(lag time.Duration) {
	FanoutTotal.WithLabelValues("published").Inc()
	FanoutLagSeconds.Observe(lag.Seconds())
}

// FanoutFailed counts a failed cross-pod publish.
func (PrometheusMetrics) FanoutFailed() { FanoutTotal.WithLabelValues("error").Inc() }

// ContributingActors observes the number of distinct actors in the window just
// flushed for one room onto the north-star contribution histogram (FR-014). Each
// flush is one observation, so the metric aggregates correctly across rooms (an
// unlabeled .Set per room would be last-window-wins).
func (PrometheusMetrics) ContributingActors(n int) { ContributingActors.Observe(float64(n)) }

// LifecycleTransfersTotal counts lifecycle events republished onto the retry
// ladder or into the dead-letter queue, by target queue and whether the broker
// confirmed the publish.
//
// The first transfer to a retry tier is the alertable moment. A lifecycle event
// that failed once means the backend behind the cascade is down, and the ladder
// buys ~35 minutes before the event reaches the DLQ — which is the window in which
// a human can act. Waiting for the DLQ to fill up is waiting until it is too late.
//
//	increase(collaboration_lifecycle_transfers_total{queue=~".+\\.retry\\..+"}[5m]) > 0
//
// outcome="unconfirmed" is separate and more serious: the event was NOT handed to
// the broker, so it is still an unacknowledged delivery waiting on a channel
// recycle. A sustained rate there means transfers are not landing at all.
var LifecycleTransfersTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "collaboration",
	Name:      "lifecycle_transfers_total",
	Help:      "Lifecycle events republished to a retry tier or the DLQ, by target queue and publish outcome.",
}, []string{"queue", "outcome"})

// LifecycleQueueReadyDepth is the READY message count of each queue in the
// lifecycle topology.
//
// This is the signal LifecycleTransfersTotal cannot give. A counter only goes up,
// so the increment that put ten events in the DLQ scrolls out of the alert window
// while the events stay there. The DLQ's ready depth is the number of deletions and
// revocations currently NOT applied, and it stays visible until someone drains it:
//
//	collaboration_lifecycle_queue_ready_depth{queue=~".+\\.dlq"} > 0
//
// READY is the honest word. It comes from AMQP's queue.declare-ok, which does not
// report a total, and a message the broker has parked for a pending dead-letter hop
// is neither ready nor unacknowledged — so it reads as zero here while being
// present. That state is reachable only while a dead-letter target is missing, and
// the consumer re-declares the whole topology on every re-attach and every poll, so
// it is bounded rather than open-ended. For the total during such a window, read the
// broker: `rabbitmqctl list_queues name messages messages_ready`.
//
// Per-tier ready depth stands in for message age, quantized by the ladder: an event
// in the 30m tier has already survived 30s + 5m.
var LifecycleQueueReadyDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "collaboration",
	Name:      "lifecycle_queue_ready_depth",
	Help:      "Ready message count of each queue in the lifecycle topology (excludes messages parked for a pending dead-letter hop).",
}, []string{"queue"})

// PrometheusLifecycleObserver bridges the lifecycle consumer's operational
// signals (lifecycle.Observer) to these collectors, keeping the AMQP adapter free
// of a Prometheus import (hexagon §I).
type PrometheusLifecycleObserver struct{}

// EventTransferred counts one republished event by target queue and outcome.
func (PrometheusLifecycleObserver) EventTransferred(queue string, confirmed bool) {
	outcome := "unconfirmed"
	if confirmed {
		outcome = "confirmed"
	}
	LifecycleTransfersTotal.WithLabelValues(queue, outcome).Inc()
}

// QueueReadyDepth publishes one queue's ready message count.
func (PrometheusLifecycleObserver) QueueReadyDepth(queue string, ready int) {
	LifecycleQueueReadyDepth.WithLabelValues(queue).Set(float64(ready))
}

// LifecycleDeadLetteredTotal counts events reaching the dead-letter queue, by
// pattern and by how many operator replays they have already survived.
//
// The replays label is what a plain DLQ counter cannot give. A replay clears the
// attempt count by design — otherwise the event returns to the DLQ on its first
// failure and looks like a replay that worked — so after a replay every event
// reports "attempt 3" again. Only the replay count separates an event that just
// failed for the first time from one a person has already sent round the ladder
// three times:
//
//	increase(collaboration_lifecycle_dead_lettered_total{replays!="0"}[1h]) > 0
//
// That is the escalation signal: the fix that was applied before the last replay
// did not work, and repeating it will not help.
var LifecycleDeadLetteredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "collaboration",
	Name:      "lifecycle_dead_lettered_total",
	Help:      "Lifecycle events moved to the dead-letter queue, by pattern and prior operator replays.",
}, []string{"pattern", "replays"})

// EventDeadLettered counts one event reaching the DLQ.
func (PrometheusLifecycleObserver) EventDeadLettered(pattern string, replays int32) {
	LifecycleDeadLetteredTotal.WithLabelValues(pattern, strconv.FormatInt(int64(replays), 10)).Inc()
}
