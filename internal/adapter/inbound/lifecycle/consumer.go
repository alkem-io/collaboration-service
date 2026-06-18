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

// handle decodes one event body and routes it to the Manager. An unparseable
// body or an unrelated pattern is ignored (the lifecycle consumer shares the bus
// with metastore RPC replies and other traffic). A cascade error is logged and
// dropped — idempotency means a redelivery or an absent document is not fatal.
func (c *Consumer) handle(ctx context.Context, body []byte) {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return
	}
	switch env.Pattern {
	case PatternDocumentDeleted:
		c.handleDeleted(ctx, env.Data)
	case PatternDocumentCreated:
		c.handleCreated(ctx, env.Data)
	case PatternDocumentAccessChanged:
		c.handleAccessChanged(env.Data)
	default:
		// Not a lifecycle event — ignore.
	}
}

func (c *Consumer) handleDeleted(ctx context.Context, data json.RawMessage) {
	var ev DeletedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return
	}
	if err := c.mgr.Purge(ctx, model.DocumentID(ev.ID)); err != nil {
		c.logger.Warn("document delete cascade failed", zap.String("doc", ev.ID), zap.Error(err))
	}
}

func (c *Consumer) handleCreated(ctx context.Context, data json.RawMessage) {
	var ev CreatedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return
	}
	meta := model.Metadata{
		ID:          model.DocumentID(ev.ID),
		ContentType: model.ContentType(ev.ContentType),
		OwnerRef:    ev.OwnerRef,
	}
	if err := c.mgr.PreRegister(ctx, meta); err != nil {
		c.logger.Warn("document create pre-register failed", zap.String("doc", ev.ID), zap.Error(err))
	}
}

func (c *Consumer) handleAccessChanged(data json.RawMessage) {
	var ev AccessChangedEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.ID == "" {
		return
	}
	c.mgr.ReEvaluate(model.DocumentID(ev.ID))
}
