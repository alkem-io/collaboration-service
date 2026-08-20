// Package lifecycle is the inbound RabbitMQ lifecycle consumer: it reacts to the
// Alkemio server's owner-driven document lifecycle events (FR-023) on the same
// bus as the metadata persistence. `document.deleted` cascades a purge (the room
// is closed and the metadata + snapshot are deleted, no orphan);
// per-document authorization for connected clients. The wire shape is the NestJS
// event envelope { pattern, data, id }, so a NestJS @EventPattern publisher on
// the server reaches it natively.
//
// See contracts/lifecycle-events.md. The standalone (no-bus) equivalent is the
// create/delete HTTP API (T016).
package lifecycle

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// Pattern names the lifecycle events the server emits (NestJS @EventPattern).
const (
	// PatternDocumentDeleted is the owner-delete cascade trigger.
	PatternDocumentDeleted = "document.deleted"
)

// DeletedEvent is the document.deleted payload: the document to purge.
type DeletedEvent struct {
	ID string `json:"id"`
}

// envelope is the NestJS event envelope { pattern, data, id } the server
// publishes (identical framing to the metadata-store RPC requests).
type envelope struct {
	Pattern string          `json:"pattern"`
	Data    json.RawMessage `json:"data"`
	ID      string          `json:"id"`
}

// Consumer routes lifecycle events from the bus to the domain Manager.
type Consumer struct {
	mgr    Manager
	logger *zap.Logger
	obs    Observer

	// cfg is retained so the supervisor can re-open a session after the broker
	// drops the current one.
	cfg Config

	// mu guards the live broker attachment. The supervisor goroutine swaps it on
	// reconnect; Close reads it from whatever goroutine calls Close.
	mu       sync.Mutex
	conn     brokerConn
	ch       brokerChannel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return

	// handlerTimeout bounds the processing context of a single delivery (resolved
	// from Config.HandlerTimeout, defaulting to DefaultHandlerTimeout) so one stuck
	// event cannot head-of-line-block the single-threaded consume loop.
	handlerTimeout time.Duration

	// names holds every queue in the topology, derived from the configured main
	// queue so the parts cannot drift apart.
	names queueNames

	// confirmTimeout bounds the wait for the broker's confirm/return answers to a
	// transfer publish. Both channels are read in transfer(); with a bounded QoS and
	// a serial consume loop there is exactly one publish outstanding, so correlation
	// is positional. A broker that neither
	// confirms nor returns must not hold the consume loop open indefinitely; the
	// delivery stays unacked and is redelivered after the recycle.
	confirmTimeout time.Duration

	// recycleBackoff delays the channel close after an unconfirmable transfer, and
	// paces the supervisor's re-attach attempts, so redelivery is retried at a
	// bounded rate rather than spinning.
	recycleBackoff time.Duration

	// closed is shut when Close runs, so a pending recycle does not outlive the
	// consumer or close a channel the shutdown path is already closing.
	closed    chan struct{}
	closeOnce sync.Once
}

// ackAction tells consume how to acknowledge a delivery after handle processed it.
type ackAction int

const (
	// ackSuccess acks the delivery: it was processed, idempotently a no-op, or is
	// unactionable (unparseable / unrelated pattern) so requeuing it is pointless.
	ackSuccess ackAction = iota
	// retryLater is a genuine processing failure that may succeed later — a
	// transient backend error. The event is transferred to the next delay tier and
	// redelivered when its TTL expires.
	retryLater
	// ackTerminal is an envelope this service can never act on: unparseable, or a
	// pattern outside the contract. It is transferred to the DLQ rather than
	// dropped, so a shape mismatch between producer and consumer shows up as queue
	// depth instead of vanishing.
	ackTerminal
)

// handle decodes one event body and routes it to the Manager, returning how the
// delivery should be acknowledged.
//
// The queue is DEDICATED to lifecycle events, so an unparseable body or a pattern
// outside the contract is not incidental traffic — it is a producer/consumer
// mismatch. Those are terminal: recorded in the dead-letter queue rather than
// acked away, so the mismatch is visible as queue depth instead of vanishing.
//
// A genuine cascade failure returns retryLater and is redelivered on the retry
// schedule. The cascade is a correctness requirement and is idempotent, so a
// duplicate delivery is survivable where a lost one is not.
func (c *Consumer) handle(ctx context.Context, body []byte) ackAction {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return ackTerminal // not a lifecycle envelope: record it, do not swallow it.
	}
	switch env.Pattern {
	case PatternDocumentDeleted:
		return c.handleDeleted(ctx, env.Data)
	default:
		return ackTerminal // outside the contract: record it.
	}
}

func (c *Consumer) handleDeleted(ctx context.Context, data json.RawMessage) ackAction {
	var ev DeletedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return ackTerminal // malformed payload: record it rather than swallow it.
	}
	if err := c.mgr.Purge(ctx, model.DocumentID(ev.ID)); err != nil {
		// The purge is idempotent (a not-found delete is success), so a returned
		// error is a transient backend failure worth retrying — down the ladder
		// rather than ack-and-drop, or the document is orphaned.
		c.logger.Warn("document delete cascade failed; moving the event down the retry ladder",
			zap.String("doc", ev.ID), zap.Error(err))
		return retryLater
	}
	return ackSuccess
}
