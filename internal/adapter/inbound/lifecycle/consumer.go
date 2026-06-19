// Package lifecycle is the inbound RabbitMQ lifecycle consumer: it reacts to the
// Alkemio server's owner-driven document lifecycle events (FR-023) on the same
// bus as the metadata persistence. `document.deleted` cascades a purge (the room
// is closed and the metadata + snapshot are deleted, no orphan);
// `document.created` pre-registers metadata; `document.access_changed` re-runs
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

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// Pattern names the lifecycle events the server emits (NestJS @EventPattern).
const (
	// PatternDocumentDeleted is the owner-delete cascade trigger.
	PatternDocumentDeleted = "document.deleted"
	// PatternDocumentCreated optionally pre-registers a document's metadata.
	PatternDocumentCreated = "document.created"
	// PatternDocumentAccessChanged re-evaluates per-document authorization.
	PatternDocumentAccessChanged = "document.access_changed"
)

// DeletedEvent is the document.deleted payload: the document to purge.
type DeletedEvent struct {
	ID string `json:"id"`
}

// CreatedEvent is the optional document.created payload: the document to
// pre-register, with its content type and lifecycle owner.
type CreatedEvent struct {
	ID          string `json:"id"`
	ContentType string `json:"contentType"`
	OwnerRef    string `json:"ownerRef"`
}

// AccessChangedEvent is the optional document.access_changed payload: the
// document whose connected clients must be re-authorized.
type AccessChangedEvent struct {
	ID string `json:"id"`
}

// envelope is the NestJS event envelope { pattern, data, id } the server
// publishes (identical framing to the metastore RPC requests).
type envelope struct {
	Pattern string          `json:"pattern"`
	Data    json.RawMessage `json:"data"`
	ID      string          `json:"id"`
}

// Consumer routes lifecycle events from the bus to the domain Manager.
type Consumer struct {
	mgr    Manager
	logger *zap.Logger

	conn *amqp.Connection
	ch   *amqp.Channel
}

// ackAction tells consume how to acknowledge a delivery after handle processed it.
type ackAction int

const (
	// ackSuccess acks the delivery: it was processed, idempotently a no-op, or is
	// unactionable (unparseable / unrelated pattern) so requeuing it is pointless.
	ackSuccess ackAction = iota
	// nackRequeue nacks the delivery for a bounded requeue: a genuine processing
	// failure that may succeed on retry (a transient backend error). consume
	// requeues once, then drops the message to avoid a poison loop.
	nackRequeue
)

// handle decodes one event body and routes it to the Manager, returning how the
// delivery should be acknowledged. An unparseable body or an unrelated pattern is
// acked (it shares the bus with metastore RPC replies and other traffic — there is
// nothing to retry). A genuine cascade/pre-register failure returns nackRequeue so
// the event is redelivered (bounded by consume) rather than silently lost — the
// cascade is a correctness requirement, idempotent on redelivery.
func (c *Consumer) handle(ctx context.Context, body []byte) ackAction {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return ackSuccess // not a lifecycle envelope we can act on.
	}
	switch env.Pattern {
	case PatternDocumentDeleted:
		return c.handleDeleted(ctx, env.Data)
	case PatternDocumentCreated:
		return c.handleCreated(ctx, env.Data)
	case PatternDocumentAccessChanged:
		c.handleAccessChanged(env.Data)
		return ackSuccess
	default:
		// Not a lifecycle event — ack (nothing to retry).
		return ackSuccess
	}
}

func (c *Consumer) handleDeleted(ctx context.Context, data json.RawMessage) ackAction {
	var ev DeletedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return ackSuccess // malformed payload: nothing to retry.
	}
	if err := c.mgr.Purge(ctx, model.DocumentID(ev.ID)); err != nil {
		// The purge is idempotent (a not-found delete is success), so a returned
		// error is a transient backend failure worth retrying — nack/requeue rather
		// than ack-and-drop, or the document is orphaned.
		c.logger.Warn("document delete cascade failed; requeueing", zap.String("doc", ev.ID), zap.Error(err))
		return nackRequeue
	}
	return ackSuccess
}

func (c *Consumer) handleCreated(ctx context.Context, data json.RawMessage) ackAction {
	var ev CreatedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return ackSuccess // malformed payload: nothing to retry.
	}
	meta := model.Metadata{
		ID:          model.DocumentID(ev.ID),
		ContentType: normalizeContentType(ev.ContentType, c.logger, ev.ID),
		OwnerRef:    ev.OwnerRef,
	}
	if err := c.mgr.PreRegister(ctx, meta); err != nil {
		c.logger.Warn("document create pre-register failed; requeueing", zap.String("doc", ev.ID), zap.Error(err))
		return nackRequeue
	}
	return ackSuccess
}

// normalizeContentType maps a bus-supplied content-type string to a known domain
// ContentType, defaulting an empty or unrecognized value to memo (rather than
// persisting an invalid type that would later break convention application). An
// unexpected value is logged so producer drift is observable.
func normalizeContentType(raw string, logger *zap.Logger, docID string) model.ContentType {
	switch model.ContentType(raw) {
	case model.ContentTypeMemo, model.ContentTypeWhiteboard:
		return model.ContentType(raw)
	case "":
		return model.ContentTypeMemo
	default:
		logger.Warn("document.created carried an unknown contentType; defaulting to memo",
			zap.String("doc", docID), zap.String("contentType", raw))
		return model.ContentTypeMemo
	}
}

func (c *Consumer) handleAccessChanged(data json.RawMessage) {
	var ev AccessChangedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return
	}
	c.mgr.ReEvaluate(model.DocumentID(ev.ID))
}
