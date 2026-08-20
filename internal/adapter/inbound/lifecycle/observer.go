package lifecycle

// Observer receives the lifecycle consumer's operational signals. The adapter
// defines it so the consumer stays free of a Prometheus import; the wiring in
// app supplies the bridge.
//
// Two signals, because they answer two different questions:
//
//   - EventTransferred is a rate. It answers "is anything failing right now, and
//     how far down the ladder has it got". The first transfer to a retry tier is
//     the alertable moment: a lifecycle event that failed once is a backend that
//     is down, and the ~35 minutes of ladder is the window in which a human can
//     act before the event lands in the DLQ.
//   - QueueReadyDepth is a level. It answers "is there unattended work". A counter
//     cannot: it only ever goes up, so the increment that put ten events in the
//     DLQ scrolls out of the alert window while the events stay there. The DLQ's
//     ready depth is the number of deletions and revocations currently NOT applied.
//
// READY, not total, and the difference is not academic. AMQP's queue.declare-ok
// reports only the ready count, and a message the broker is holding for a pending
// dead-letter hop — the state at-least-once puts an expired retry into when its
// target is missing — is neither ready nor unacknowledged. It reads as ZERO here
// while being very much present. That state is measured and reproducible (see
// TestAnExpiredRetryIsRetainedWhenItsTargetIsMissing), so the metric is named for
// what it measures rather than for what would be convenient.
//
// The blind spot is bounded rather than open-ended: it needs the dead-letter
// target to be missing, and the consumer re-declares every queue in the topology
// on each re-attach AND on each depth poll, so a deleted queue comes back within
// one poll interval and the broker releases the retained message a few minutes
// later on its own. The runbook names the broker-side reading (`rabbitmqctl
// list_queues name messages`) for the total when that window is being examined.
//
// Ready depth still stands in for message age across the ladder: an event sitting
// in the 30m tier has already survived 30s + 5m. AMQP offers no age reading at
// all, so the ladder's own quantization is the only in-protocol source.
type Observer interface {
	// EventTransferred records one event republished to another queue: the target
	// queue, and whether the broker confirmed the publish.
	EventTransferred(queue string, confirmed bool)
	// QueueReadyDepth publishes one queue's READY message count — messages the
	// broker would hand to a consumer now. See the note above on what it excludes.
	QueueReadyDepth(queue string, ready int)
	// EventDeadLettered records an event reaching the DLQ, with the number of
	// operator replays it has already survived. A replay clears the attempt count
	// by design, so replays is the only thing that separates "this just failed for
	// the first time" from "someone has now sent this round the ladder three times
	// and it failed again" — which is the difference between waiting and escalating.
	EventDeadLettered(pattern string, replays int32)
}
