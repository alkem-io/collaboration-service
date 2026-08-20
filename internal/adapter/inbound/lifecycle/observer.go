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
//   - QueueDepth is a level. It answers "is there unattended work". A counter
//     cannot: it only ever goes up, so the increment that put ten events in the
//     DLQ scrolls out of the alert window while the events stay there. The DLQ
//     depth is the number of deletions and revocations currently NOT applied.
//
// Depth also stands in for message age. The ladder quantizes it: an event sitting
// in the 30m tier has already survived 30s + 5m, so per-tier depth says how long
// things have been failing without a separate age metric — which AMQP cannot
// supply anyway without the management API or consuming the queue to peek at it.
type Observer interface {
	// EventTransferred records one event republished to another queue: the target
	// queue, and whether the broker confirmed the publish.
	EventTransferred(queue string, confirmed bool)
	// QueueDepth publishes one queue's current message count.
	QueueDepth(queue string, messages int)
}

// NopObserver discards every signal. It is the default, so a Consumer built
// without observability still runs — and so tests that care about behaviour do
// not have to wire metrics to get it.
type NopObserver struct{}

// EventTransferred discards the signal.
func (NopObserver) EventTransferred(string, bool) {}

// QueueDepth discards the signal.
func (NopObserver) QueueDepth(string, int) {}
