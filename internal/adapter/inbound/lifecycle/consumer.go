// Package lifecycle is the inbound RabbitMQ lifecycle consumer: it reacts to the
// Alkemio server's owner-driven document lifecycle events (FR-023) on a dedicated
// queue.
//
// `document.deleted` CLOSES AND EVICTS a live room. It deletes nothing durable.
// `server` removes the entity, profile, storage bucket and checkpoint blob BEFORE
// it enqueues the outbox row that becomes this event (server:
// memo.service.ts / whiteboard.service.ts — the cascade runs ahead of the
// transaction that removes the leaf and enqueues), so by the time the event
// arrives there is nothing here left to delete. A document with no live room is a
// no-op.
//
// The wire shape is the NestJS event envelope { pattern, data, id }, so a NestJS
// @EventPattern publisher on the server reaches it natively.
//
// See contracts/lifecycle-events.md.
package lifecycle

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// Pattern names the lifecycle events the server emits (NestJS @EventPattern).
const (
	// PatternDocumentDeleted signals that `server` has ALREADY completed the
	// owner-delete cascade. It is this service's cue to close and evict the live
	// room, not a cascade it performs.
	PatternDocumentDeleted = "document.deleted"
)

// DeletedEvent is the document.deleted payload: the document to close.
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
	mu   sync.Mutex
	conn brokerConn
	ch   brokerChannel

	// handlerTimeout bounds the processing context of a single delivery (resolved
	// from Config.HandlerTimeout, defaulting to DefaultHandlerTimeout) so one stuck
	// event cannot head-of-line-block the single-threaded consume loop.
	handlerTimeout time.Duration

	// names holds every queue in the topology, derived from the configured main
	// queue so the parts cannot drift apart.
	names queueNames

	// reattachBackoff paces the supervisor's re-attach attempts after the broker
	// drops a connection or session, so a broker that is down is retried at a
	// bounded rate rather than spun on.
	reattachBackoff time.Duration

	// closed is shut when Close runs, so a pending recycle does not outlive the
	// consumer or close a channel the shutdown path is already closing.
	closed    chan struct{}
	closeOnce sync.Once
}

// ackAction tells processOne how to acknowledge a delivery after handle ran.
type ackAction int

const (
	// ackSuccess acks: the room was closed, or there was no room to close.
	ackSuccess ackAction = iota
	// requeue is a transient refusal — a still-live room that would not accept the
	// close command within the handler deadline. Nack(requeue=true); the broker
	// redelivers, and the deadline is what keeps that from spinning.
	requeue
	// rejectPoison is an envelope this service can never act on at any future time:
	// unparseable, or a pattern outside the contract. Nack(requeue=false), which
	// the main queue's DLX turns into a DLQ record, so a producer/consumer shape
	// mismatch surfaces as queue depth instead of vanishing.
	rejectPoison
)

// handle decodes one event body and routes it to the Manager, returning how the
// delivery should be acknowledged.
//
// The queue is DEDICATED to lifecycle events, so an unparseable body or a pattern
// outside the contract is not incidental traffic — it is a producer/consumer
// mismatch. Those are terminal: recorded in the dead-letter queue rather than
// acked away, so the mismatch is visible as queue depth instead of vanishing.
//
// A live room that refuses the close returns requeue and is redelivered. Closing
// is idempotent, so a duplicate delivery is survivable where a lost one is not.
func (c *Consumer) handle(ctx context.Context, body []byte) ackAction {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return rejectPoison // not a lifecycle envelope: record it, do not swallow it.
	}
	switch env.Pattern {
	case PatternDocumentDeleted:
		return c.handleDeleted(ctx, env.Data)
	default:
		return rejectPoison // outside the contract: record it.
	}
}

func (c *Consumer) handleDeleted(ctx context.Context, data json.RawMessage) ackAction {
	var ev DeletedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return rejectPoison // malformed payload: record it rather than swallow it.
	}
	if err := c.mgr.CloseDeleted(ctx, model.DocumentID(ev.ID)); err != nil {
		// CloseDeleted is idempotent and deletes nothing durable — `server` has
		// already removed the entity, profile, bucket and checkpoint before the event
		// is enqueued. So an error here means only that a still-live room would not
		// accept the close, which is transient by nature: requeue and try again.
		c.logger.Warn("document close/evict was refused by a live room; requeueing the event",
			zap.String("doc", ev.ID), zap.Error(err))
		return requeue
	}
	return ackSuccess
}
