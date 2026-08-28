package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

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

	// Validate the wire enum at the adapter boundary: reply.ContentType is a raw
	// RPC string, so reject an unsupported value here with a clear diagnostic
	// rather than casting it blindly and letting it fail later (much weaker error)
	// deep in the room. Empty is allowed — an unset ContentType is resolved from
	// the ?type= handshake — only a SET-but-unknown value is a corrupt server
	// reply.
	contentType := model.ContentType(reply.ContentType)
	switch contentType {
	case "", model.ContentTypeMemo, model.ContentTypeWhiteboard:
	default:
		return model.Metadata{}, fmt.Errorf("collaboration-fetch: unknown contentType %q", reply.ContentType)
	}

	return model.Metadata{
		ID:                    id,
		ContentType:           contentType,
		Version:               reply.Version,
		ContentPointer:        reply.ContentPointer,
		Migrated:              reply.Migrated,
		IsMultiUser:           reply.IsMultiUser,
		AuthorizationPolicyID: reply.AuthorizationPolicyID,
		StorageBucketID:       reply.StorageBucketID,
		OwnerRef:              reply.OwnerRef,
	}, nil
}

// Save upserts the document index over collaboration-save (index only — the blob
// goes to the checkpoint store, never this bus).
func (s *Store) Save(ctx context.Context, meta model.Metadata) error {
	data := SaveData{
		ID:                    string(meta.ID),
		ContentType:           string(meta.ContentType),
		Version:               meta.Version,
		ContentPointer:        meta.ContentPointer,
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

// Contribution emits the fire-and-forget collaboration-contribution event: the
// per-window set of contributing actor ids (FR-014). It satisfies
// port.Contributor so the room can carry the north-star metric forward to the
// server's analytics over the same bus (contracts/unified-metadata-rmq.md).
func (s *Store) Contribution(ctx context.Context, id model.DocumentID, actorIDs []uuid.UUID) error {
	users := make([]User, 0, len(actorIDs))
	for _, a := range actorIDs {
		users = append(users, User{ID: a.String()})
	}
	if err := s.rpc.Emit(ctx, PatternContribution, ContributionData{ID: string(id), Users: users}); err != nil {
		return fmt.Errorf("collaboration-contribution: %w", err)
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

// compile-time assertions that Store satisfies the metadata + contribution ports.
var (
	_ port.MetadataStore = (*Store)(nil)
	_ port.Contributor   = (*Store)(nil)
)
