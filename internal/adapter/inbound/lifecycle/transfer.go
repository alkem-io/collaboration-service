package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// headerAttempt counts how many times an event has been retried. It is explicit
// rather than derived from x-death, which the broker maintains per queue-and-reason
// and which is awkward to reason about across tiers.
const headerAttempt = "x-collab-attempt"

// errTransferFailed means the event was NOT handed to the broker. The caller must
// leave the original delivery unacknowledged: the message is still broker-owned
// and will be redelivered.
var errTransferFailed = errors.New("lifecycle: transfer not confirmed")

// transfer republishes an event to another queue and returns only once the broker
// has confirmed it.
//
// Two acknowledgements are needed, not one. A publisher CONFIRM says the exchange
// accepted the message; it says nothing about whether anything was routed. With
// the default exchange, publishing to a queue that does not exist is a silent
// discard that still confirms. mandatory=true makes the broker RETURN an
// unroutable message, so success is: a matching confirm ack AND no return.
//
// A nack, a return, a timeout, or a closed channel are all failures. On failure
// the caller must not ack and must not reject — rejecting converts a transient
// publishing problem into terminal handling, and the DLX republish behind a reject
// is not itself publisher-confirmed, so "it will land in the DLQ" would be an
// assumption rather than a guarantee.
func (c *Consumer) transfer(ctx context.Context, target string, d amqp.Delivery, attempt int32) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[headerAttempt] = attempt

	pub := amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		MessageId:    d.MessageId,
		Timestamp:    d.Timestamp,
		Headers:      headers,
		Body:         d.Body, // byte-identical; the envelope is never re-encoded
	}

	// The channel and its two answer streams come from one read so they are always
	// the same attachment's. They cannot be swapped mid-transfer in any case — the
	// supervisor re-attaches from the very goroutine this runs on — but reading them
	// apart would leave that as an accident of scheduling rather than a fact.
	ch, confirms, returns := c.live()
	if err := ch.PublishWithContext(ctx, "", target, true /*mandatory*/, false, pub); err != nil {
		return fmt.Errorf("%w: publish to %s: %w", errTransferFailed, target, err)
	}

	// A return arrives BEFORE the confirm for the same publish, so waiting for the
	// confirm first and then checking returns would race. Select on both — but a
	// select between two READY channels picks at random, and by the time this runs
	// the broker may have delivered both. Winning the confirm case therefore proves
	// nothing on its own; see the drain below.
	deadline := time.NewTimer(c.confirmTimeout)
	defer deadline.Stop()
	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				return fmt.Errorf("%w: return channel closed", errTransferFailed)
			}
			return fmt.Errorf("%w: %s was unroutable (queue missing?), broker returned it", errTransferFailed, ret.RoutingKey)

		case conf, ok := <-confirms:
			if !ok {
				return fmt.Errorf("%w: confirm channel closed", errTransferFailed)
			}
			if !conf.Ack {
				return fmt.Errorf("%w: broker nacked the publish to %s", errTransferFailed, target)
			}
			// An ack is success only once the return channel is known to be empty.
			//
			// This drain is sufficient, not merely likely to help. amqp091-go
			// dispatches every frame from one connection-reader goroutine, and both
			// a basic.return and a basic.ack are delivered by a synchronous send from
			// inside that goroutine (Channel.dispatch → ch.returns / confirms.confirm),
			// in frame order. AMQP puts basic.return before basic.ack for the same
			// mandatory publish. So if a return exists for THIS publish it is already
			// buffered by the time its confirmation is readable — the non-blocking
			// read cannot miss one that is still in flight.
			//
			// It relies on there being exactly one publish outstanding, which the
			// serial consume loop guarantees, and on both channels being buffered so
			// the dispatcher never blocks part-way through.
			select {
			case ret, ok := <-returns:
				if !ok {
					return fmt.Errorf("%w: return channel closed", errTransferFailed)
				}
				return fmt.Errorf("%w: %s was unroutable (queue missing?), broker returned it and the confirm arrived first", errTransferFailed, ret.RoutingKey)
			default:
			}
			// Ack with no return: the exchange accepted it AND it routed.
			return nil

		case <-deadline.C:
			return fmt.Errorf("%w: no confirmation for %s within %s", errTransferFailed, target, c.confirmTimeout)

		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errTransferFailed, ctx.Err())
		}
	}
}

// nextTarget maps an attempt count to the queue that should receive the event: the
// tier for this attempt, or the DLQ once the schedule is exhausted.
func (c *Consumer) nextTarget(attempt int32) string {
	if int(attempt) <= 0 {
		return c.names.tiers[0]
	}
	if int(attempt) >= len(c.names.tiers) {
		return c.names.dlq
	}
	return c.names.tiers[attempt]
}

// attemptOf reads the explicit attempt header, treating anything missing or
// malformed as a first attempt. AMQP numeric headers arrive in whatever width the
// publisher used, so accept the integer types a broker may deliver.
func attemptOf(h amqp.Table) int32 {
	var raw int64
	switch v := h[headerAttempt].(type) {
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
	// Clamp rather than convert. Any attempt at or past the last tier routes to the
	// DLQ, so 0..tierCount are the only values whose difference matters; a forged or
	// corrupted header holding something huge or negative must not wrap into a
	// valid-looking tier index.
	if raw < 0 {
		return 0
	}
	if raw > int64(tierCount) {
		return tierCount
	}
	return int32(raw)
}
