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
//   - QueueReadyDepth is a level. It answers "is READY work waiting" — how many
//     messages the broker would hand to a consumer right now. A counter cannot
//     answer even that: it only ever goes up, so the increment that put ten events
//     in the DLQ scrolls out of the alert window while the events stay there.
//
// It does NOT answer "is there unattended work", and the gap is not academic.
// AMQP's queue.declare-ok reports only the ready count, and a message the broker
// is holding for a pending dead-letter hop — the state at-least-once puts an
// expired retry into when its target is missing — is neither ready nor
// unacknowledged. It reads as ZERO here while being very much present, measured
// and reproducible (TestAnExpiredRetryIsRetainedWhenItsTargetIsMissing). So this
// is a lower bound on unattended work, never the whole of it.
//
// The signal for the parked state is RabbitMQ's own, because only the broker can
// see it: the `messages` column of `rabbitmqctl list_queues name messages
// messages_ready` (or `rabbitmq_queue_messages` where the management/prometheus
// plugin is scraped), and the broker log line `Cannot forward any dead-letter
// messages from source quorum queue …`, which it emits while a hop is stuck.
//
// The gap is also bounded rather than open-ended: it needs the dead-letter target
// to be missing, and the consumer re-declares every queue in the topology on each
// re-attach AND on each depth poll, so a deleted queue comes back within one poll
// interval and the broker releases the parked message a few minutes later.
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
