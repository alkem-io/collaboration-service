package lifecycle

// Observer receives the lifecycle consumer's operational signals. The adapter
// defines it so the consumer stays free of a Prometheus import; the wiring in
// app supplies the bridge.
//
// Two signals, because they answer different questions. The dead-letter counter
// is a rate: any increment is a producer/consumer contract mismatch, since the DLQ
// now holds only envelopes this service can never act on. Ready depth is a level,
// and a counter cannot replace it — the increment that put messages there scrolls
// out of an alert window while the messages stay.
type Observer interface {
	// QueueReadyDepth publishes one queue's READY message count — messages the
	// broker would hand to a consumer now. See the note above on what it excludes.
	QueueReadyDepth(queue string, ready int)
	// EventDeadLettered records an event rejected to the DLQ as unactionable. The
	// pattern says WHICH contract drifted: a known pattern means its payload shape
	// changed, an unknown one means the producer is emitting something never agreed.
	EventDeadLettered(pattern string)
}
