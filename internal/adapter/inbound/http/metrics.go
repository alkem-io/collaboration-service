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
		Help: "Consecutive failed flushes for the WORST currently-failing document (0 when all durable).",
	})
	// UndurableDocuments is how many documents are undurable right now. It is what
	// distinguishes "one room is struggling" from "the store is down", which the
	// two worst-case gauges below cannot express on their own.
	UndurableDocuments = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collaboration_undurable_documents",
		Help: "Documents currently accepting edits they cannot persist (0 when all durable).",
	})
	// UndurableSeconds is how long the current undurable window has lasted.
	UndurableSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collaboration_undurable_seconds",
		Help: "Seconds the WORST currently-failing document has been accepting edits it cannot persist (0 when all durable).",
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
			UndurableDocuments,
			UndurableSeconds,
			DurabilityEscalationsTotal,
			EscalationUndurableSeconds,
			FanoutTotal,
			FanoutLagSeconds,
			ContributingActors,
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

// undurableDocs tracks which documents are CURRENTLY failing to persist, so the
// exported gauges can describe the platform rather than whichever room reported
// last.
//
// Why a registry and not a label: one series per document is unbounded
// cardinality, the anti-pattern ContributingActors already documents. Why a
// registry and not a bare gauge: a bare Set() is last-writer-wins, so a HEALTHY
// document's recovery zeroed a DIFFERENT document's active failure — on a pod
// serving many rooms the alarm oscillated to 0 on every unrelated success and
// `collaboration_undurable_seconds > 0` never fired, which is precisely the
// blindness FR-026/SC-013 exist to remove.
//
// The map is bounded by the number of documents currently undurable, not by the
// number that ever existed: entries are removed on recovery and on escalation.
var (
	undurableMu   sync.Mutex
	undurableDocs = map[string]undurableEntry{}
)

type undurableEntry struct {
	consecutive int
	since       time.Duration
}

// publishUndurableLocked recomputes the exported aggregate from the registry.
// The gauges report the WORST currently-undurable document, which is the
// question an operator is actually asking ("is anything undurable right now, and
// how bad is it"). Max rather than sum: summing seconds across documents would
// grow with room count and mean nothing.
func publishUndurableLocked() {
	worstFailures, worstSeconds := 0, 0.0
	for _, e := range undurableDocs {
		if e.consecutive > worstFailures {
			worstFailures = e.consecutive
		}
		if secs := e.since.Seconds(); secs > worstSeconds {
			worstSeconds = secs
		}
	}
	UndurableFlushFailures.Set(float64(worstFailures))
	UndurableSeconds.Set(worstSeconds)
	UndurableDocuments.Set(float64(len(undurableDocs)))
}

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
func (PrometheusMetrics) DocumentUndurable(doc string, consecutive int, since time.Duration) {
	undurableMu.Lock()
	defer undurableMu.Unlock()
	undurableDocs[doc] = undurableEntry{consecutive: consecutive, since: since}
	publishUndurableLocked()
}

// DocumentDurabilityRestored clears THIS document from the degraded set. It must
// not clear the gauges outright: another document may still be failing, and
// zeroing a live alarm because an unrelated room recovered is the bug this
// registry exists to prevent.
func (PrometheusMetrics) DocumentDurabilityRestored(doc string) {
	undurableMu.Lock()
	defer undurableMu.Unlock()
	delete(undurableDocs, doc)
	publishUndurableLocked()
}

// DocumentEscalated counts a room torn down for repeated persist failures,
// recording how long it had been undurable. This is the DATA LOSS signal — the
// unsaved edits were discarded — so it is deliberately its own counter rather
// than another "error" label on SnapshotsTotal (FR-028, SC-016).
func (PrometheusMetrics) DocumentEscalated(doc string, undurableFor time.Duration) {
	DurabilityEscalationsTotal.Inc()
	EscalationUndurableSeconds.Observe(undurableFor.Seconds())
	// The room is gone, so this document is no longer undurable — it is lost. Drop
	// it from the set and republish, WITHOUT zeroing gauges another still-failing
	// document may own.
	undurableMu.Lock()
	defer undurableMu.Unlock()
	delete(undurableDocs, doc)
	publishUndurableLocked()
}

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

// LifecycleQueueReadyDepth is the READY message count of each queue in the
// lifecycle topology.
//
// This is the signal LifecycleDeadLetteredTotal cannot give. A counter only goes
// up, so the increment that put ten events in the DLQ scrolls out of the alert
// window while the events stay there. Ready depth stays visible until someone
// drains the queue — and for the DLQ it is exact, because nothing consumes it and
// nothing dead-letters out of it, so no message there is ever in the parked state
// below:
//
//	collaboration_lifecycle_queue_ready_depth{queue=~".+\\.dlq"} > 0
//
// READY is the honest word, and it makes this a LOWER BOUND on unattended work
// rather than a measure of it. The number comes from AMQP's queue.declare-ok,
// which does not report a total, and a message the broker has parked for a hop
// into a missing DLQ is neither ready nor unacknowledged — so it reads as zero
// here while being present.
//
// The signal for that state is RabbitMQ's own, because only the broker can see it:
// the `messages` column of `rabbitmqctl list_queues name messages messages_ready`,
// or `rabbitmq_queue_messages` where the plugin is scraped, plus the broker log line
// "Cannot forward any dead-letter messages from source quorum queue …". The state is
// reachable only while a dead-letter target is missing, and the consumer re-declares
// the whole topology on every re-attach and every poll, so it is bounded rather than
// open-ended.
var LifecycleQueueReadyDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "collaboration",
	Name:      "lifecycle_queue_ready_depth",
	Help:      "Ready message count of each queue in the lifecycle topology.",
}, []string{"queue"})

// PrometheusLifecycleObserver bridges the lifecycle consumer's operational
// signals (lifecycle.Observer) to these collectors, keeping the AMQP adapter free
// of a Prometheus import (hexagon §I).
type PrometheusLifecycleObserver struct{}

// QueueReadyDepth publishes one queue's ready message count.
func (PrometheusLifecycleObserver) QueueReadyDepth(queue string, ready int) {
	LifecycleQueueReadyDepth.WithLabelValues(queue).Set(float64(ready))
}

// LifecycleDeadLetteredTotal counts events rejected to the dead-letter queue, by
// pattern.
//
// Any non-zero value is actionable: the DLQ now holds ONLY envelopes this service
// can never act on — unparseable, or a pattern outside the contract — so a
// message here is a producer/consumer contract mismatch, not a transient failure
// that might clear itself. Transient failures are requeued and never reach it.
//
//	increase(collaboration_lifecycle_dead_lettered_total[1h]) > 0
var LifecycleDeadLetteredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "collaboration",
	Name:      "lifecycle_dead_lettered_total",
	Help:      "Lifecycle events rejected to the dead-letter queue as unactionable, by pattern.",
}, []string{"pattern"})

// EventDeadLettered counts one event rejected to the DLQ.
func (PrometheusLifecycleObserver) EventDeadLettered(pattern string) {
	LifecycleDeadLetteredTotal.WithLabelValues(pattern).Inc()
}
