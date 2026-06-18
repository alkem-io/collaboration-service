package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// rpc is the request/reply + fire-and-forget transport the store depends on,
// narrowed so the store's contract serialization is unit-tested without a live
// RabbitMQ. The amqp091-backed implementation lives in conn.go; an in-memory
// fake drives the unit tests.
type rpc interface {
	// Call publishes a NestJS-style RPC request (envelope { pattern, data, id }
	// with correlationId + replyTo) and unmarshals the correlated reply into
	// reply.
	Call(ctx context.Context, pattern string, data, reply any) error
	// Emit publishes a fire-and-forget event (no reply expected).
	Emit(ctx context.Context, pattern string, data any) error
}

// Store is the Alkemio MetadataStore over the server RabbitMQ bus.
type Store struct {
	rpc rpc
}

// newWithRPC wires a store over a custom transport (tests / the amqp client).
func newWithRPC(r rpc) *Store { return &Store{rpc: r} }

// Load fetches the document index over collaboration-fetch, mapping a not-found
// reply to model.ErrNotFound.
func (s *Store) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	var reply FetchReply
	if err := s.rpc.Call(ctx, PatternFetch, FetchData{ID: string(id)}, &reply); err != nil {
		return model.Metadata{}, fmt.Errorf("collaboration-fetch: %w", err)
	}
	if reply.Error != "" {
		return model.Metadata{}, fmt.Errorf("collaboration-fetch: %s", reply.Error)
	}
	if !reply.Found {
		return model.Metadata{}, model.ErrNotFound
	}
	return model.Metadata{
		ID:                    id,
		ContentType:           model.ContentType(reply.ContentType),
		Version:               reply.Version,
		ContentPointer:        reply.ContentPointer,
		BlobStore:             model.BlobStoreKind(reply.BlobStore),
		AuthorizationPolicyID: reply.AuthorizationPolicyID,
		OwnerRef:              reply.OwnerRef,
	}, nil
}

// Save upserts the document index over collaboration-save (index only — the blob
// goes to the BlobStore, never this bus).
func (s *Store) Save(ctx context.Context, meta model.Metadata) error {
	blobStore := meta.BlobStore
	if blobStore == "" {
		blobStore = model.BlobStoreInline
	}
	data := SaveData{
		ID:                    string(meta.ID),
		ContentType:           string(meta.ContentType),
		Version:               meta.Version,
		ContentPointer:        meta.ContentPointer,
		BlobStore:             string(blobStore),
		AuthorizationPolicyID: meta.AuthorizationPolicyID,
		OwnerRef:              meta.OwnerRef,
	}
	var reply SaveReply
	if err := s.rpc.Call(ctx, PatternSave, data, &reply); err != nil {
		return fmt.Errorf("collaboration-save: %w", err)
	}
	if reply.Error != "" {
		return fmt.Errorf("collaboration-save: %s", reply.Error)
	}
	if !reply.Success {
		return fmt.Errorf("collaboration-save: server reported failure")
	}
	return nil
}

// Delete purges the document index over collaboration-delete (the owner-delete
// cascade). Idempotent: the server treats an absent row as success.
func (s *Store) Delete(ctx context.Context, id model.DocumentID) error {
	var reply DeleteReply
	if err := s.rpc.Call(ctx, PatternDelete, DeleteData{ID: string(id)}, &reply); err != nil {
		return fmt.Errorf("collaboration-delete: %w", err)
	}
	if reply.Error != "" {
		return fmt.Errorf("collaboration-delete: %s", reply.Error)
	}
	return nil
}

// envelope is the NestJS RMQ request envelope { pattern, data, id }. Exported as
// a helper so both the amqp client and the contract tests build the identical
// wire shape (DRY — one definition of the envelope).
type envelope struct {
	Pattern string          `json:"pattern"`
	Data    json.RawMessage `json:"data"`
	ID      string          `json:"id"`
}

// marshalEnvelope builds the NestJS request envelope bytes for a pattern+data
// pair with a correlation id.
func marshalEnvelope(pattern, id string, data any) ([]byte, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal data: %w", err)
	}
	return json.Marshal(envelope{Pattern: pattern, Data: raw, ID: id})
}

// nestReply is the NestJS RMQ reply envelope { response, isDisposed, err, id }.
type nestReply struct {
	Response   json.RawMessage `json:"response"`
	IsDisposed bool            `json:"isDisposed"`
	Err        json.RawMessage `json:"err,omitempty"`
	ID         string          `json:"id"`
}

// compile-time assertion that Store satisfies the port.
var _ port.MetadataStore = (*Store)(nil)
