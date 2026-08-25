package lifecycle

// NopObserver discards every signal. It is the default, so a Consumer built
// without observability still runs — and so tests that care about behaviour do
// not have to wire metrics to get it.
type NopObserver struct{}

// QueueReadyDepth discards the signal.
func (NopObserver) QueueReadyDepth(string, int) {}

// EventDeadLettered discards the signal.
func (NopObserver) EventDeadLettered(string) {}
