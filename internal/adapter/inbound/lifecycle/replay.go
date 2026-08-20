package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// headerReplays counts how many times an operator has replayed an event out of
// the dead-letter queue.
//
// It exists because the attempt count must be CLEARED on replay — otherwise the
// event returns to the DLQ on its first failure, which is indistinguishable from
// a replay that worked — and clearing it also erases the only evidence that the
// event has been round the ladder before. Without a separate marker, an event
// replayed five times into a backend that is still broken looks exactly like an
// event arriving for the first time, and the consumer's DLQ log says "attempt 3"
// every time.
//
// Written by Replay (below). Read by the consumer when it moves an event to the
// DLQ, so a repeatedly-replayed event is visible as such at the moment it fails
// again.
const headerReplays = "x-collab-replays"

// DeadLetterDepth reports how many events are waiting in the dead-letter queue.
//
// It re-declares rather than declaring passively: an equivalent redeclaration is a
// no-op that returns the count, while a passive declare on a missing queue closes
// the channel — and a missing DLQ is a thing an operator wants reported, not a
// reason for the tool to die.
func DeadLetterDepth(ch brokerChannel, queue string) (int, error) {
	names := namesFor(queue)
	q, err := ch.QueueDeclare(names.dlq, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", names.dlq, err)
	}
	return q.Messages, nil
}

// ReplayResult reports what a replay moved.
type ReplayResult struct {
	// Moved is the number of events republished onto the main queue and removed
	// from the dead-letter queue.
	Moved int
	// Remaining is true when the DLQ still held messages when the limit was hit.
	Remaining bool
}

// Replay moves events from the dead-letter queue back onto the main queue.
//
// Each event is republished with the attempt count CLEARED, so it gets the full
// ladder again, and its replay count incremented, so a repeatedly-replayed event
// stays distinguishable from a first arrival.
//
// The move is durable in the only sense that matters: the DLQ copy is acked only
// after the broker has confirmed the republish, and the republish is mandatory so
// an unroutable one is a failure rather than a silent discard. If anything goes
// wrong the DLQ copy is left in place — a duplicate is survivable (both handlers
// are idempotent), a loss is not. That is why this is not "get with ack, then
// publish": that order drops the event whenever the publish fails.
//
// limit bounds one run; 0 means "everything currently in the queue".
//
// afterConsume is a test seam: it runs once the consumer is attached, which is the
// only point at which a fake broker can start delivering. Production passes none.
func Replay(ctx context.Context, ch brokerChannel, queue string, limit int, afterConsume ...func()) (res ReplayResult, err error) {
	names := namesFor(queue)

	// Exactly ONE place decides whether work is left behind. Every exit except
	// "the queue went quiet" leaves some: the delivery in hand is unacknowledged
	// and anything behind it was never read, and a run that could not even start
	// has not touched the queue at all. Setting the flag per-exit is how three of
	// the five paths came to miss it — and an operator who reads "0 remaining"
	// after a cancelled run believes the queue is drained and stops looking.
	drained := false
	defer func() { res.Remaining = !drained }()

	if err := ch.Confirm(false); err != nil {
		return res, fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

	// Prefetch 1: one event is moved at a time, so a failure leaves exactly one
	// unacknowledged message rather than a window of them.
	if err := ch.Qos(1, 0, false); err != nil {
		return res, fmt.Errorf("set replay prefetch: %w", err)
	}
	// A named tag so the consumer can be cancelled again. Replay is a bounded
	// operation: leaving it attached would keep draining the dead-letter queue into
	// a buffer nobody reads, turning every later arrival into an unacknowledged
	// message that no operator can see and no second replay can reach.
	const tag = "collab-lifecycle-replay"
	deliveries, err := ch.Consume(names.dlq, tag, false, false, false, false, nil)
	if err != nil {
		return res, fmt.Errorf("consume %s: %w", names.dlq, err)
	}
	defer func() { _ = ch.Cancel(tag, false) }()
	// Asynchronously, as a broker delivers: a hook that fills the delivery buffer
	// would otherwise block before this loop ever starts reading it.
	for _, hook := range afterConsume {
		go hook()
	}

	for limit == 0 || res.Moved < limit {
		var d amqp.Delivery
		select {
		case got, ok := <-deliveries:
			if !ok {
				return res, fmt.Errorf("replay: the delivery stream closed after %d event(s)", res.Moved)
			}
			d = got
		case <-time.After(2 * time.Second):
			// Nothing more waiting: the queue is drained. The ONLY exit that says so.
			drained = true
			return res, nil
		case <-ctx.Done():
			return res, ctx.Err()
		}

		if err := republish(ctx, ch, confirms, returns, names.main, d); err != nil {
			// The DLQ copy was never acked, so it is still there. Reject without
			// requeue would dead-letter it out of a terminal queue; leaving it
			// unacknowledged returns it when this channel closes.
			return res, err
		}
		if err := d.Ack(false); err != nil {
			// The event is already on the main queue. A failed ack means the DLQ
			// copy stays and would be replayed again — a duplicate, which both
			// handlers absorb.
			return res, fmt.Errorf("replay: republished but could not ack the dead-letter copy (a duplicate may be replayed): %w", err)
		}
		res.Moved++
	}
	return res, nil
}

// republish sends one event to the main queue with the attempt count cleared and
// the replay count incremented, and returns only once the broker has confirmed it
// and has not returned it.
func republish(ctx context.Context, ch brokerChannel, confirms chan amqp.Confirmation, returns chan amqp.Return, target string, d amqp.Delivery) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	// The whole point: the event gets the full ladder again.
	delete(headers, headerAttempt)
	headers[headerReplays] = replaysOf(d.Headers) + 1

	if err := ch.PublishWithContext(ctx, "", target, true /*mandatory*/, false, amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Headers:      headers,
		Body:         d.Body, // byte-identical; the envelope is never re-encoded
	}); err != nil {
		return fmt.Errorf("replay: publish to %s: %w", target, err)
	}

	deadline := time.NewTimer(DefaultConfirmTimeout)
	defer deadline.Stop()
	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				return errors.New("replay: return channel closed")
			}
			return fmt.Errorf("replay: %s was unroutable, broker returned it", ret.RoutingKey)
		case conf, ok := <-confirms:
			if !ok {
				return errors.New("replay: confirm channel closed")
			}
			if !conf.Ack {
				return fmt.Errorf("replay: broker nacked the publish to %s", target)
			}
			// Same ordering argument as transfer(): a return for this publish is
			// already buffered by the time its confirmation is readable, and a
			// select between two ready channels picks at random.
			select {
			case ret, ok := <-returns:
				if !ok {
					return errors.New("replay: return channel closed")
				}
				return fmt.Errorf("replay: %s was unroutable, broker returned it and the confirm arrived first", ret.RoutingKey)
			default:
			}
			return nil
		case <-deadline.C:
			return fmt.Errorf("replay: no confirmation for %s within %s", target, DefaultConfirmTimeout)
		case <-ctx.Done():
			return fmt.Errorf("replay: %w", ctx.Err())
		}
	}
}

// replaysOf reads the replay counter, treating anything missing or malformed as
// zero. Same width tolerance as attemptOf: AMQP numeric headers arrive in
// whatever type the publisher used.
func replaysOf(h amqp.Table) int32 {
	var raw int64
	switch v := h[headerReplays].(type) {
	case int32:
		raw = int64(v)
	case int64:
		raw = v
	case int:
		raw = int64(v)
	case int16:
		raw = int64(v)
	case uint8:
		raw = int64(v)
	default:
		return 0
	}
	if raw < 0 {
		return 0
	}
	// Saturate rather than wrap. The exact count past a few thousand replays says
	// nothing a human needs; a wrapped negative would.
	const cap32 = int64(1) << 20
	if raw > cap32 {
		return int32(cap32)
	}
	return int32(raw)
}
