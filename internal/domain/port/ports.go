// Package port defines the outbound interfaces the collaboration-service domain
// core depends on — cluster fan-out, metadata persistence, blob persistence,
// handshake authentication, and per-document authorization. Adapters under
// internal/adapter/outbound implement them; the domain only ever sees these
// contracts, which is what keeps the hexagon's dependencies pointing inward
// (constitution §I) and makes scaling/persistence/auth swappable
// (FR-019/020/021/022).
//
// Each interface maps to a frozen cross-repo contract in
// specs/003-unify-collab-yjs/ — the mapping is noted on each type.
package port

import (
	"context"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// ClusterBroadcaster fans document updates and ephemeral/awareness messages out
// to other pods so a multi-pod deployment is transparent to clients (a client
// may connect to any pod). The default adapter is in-memory (single-pod, R4);
// the Redis adapter publishes on doc:{id} and awareness:{id} channels.
//
// Maps to: contracts/ws-protocol.md ("Multi-pod transparent … cross-pod via
// Redis fan-out (R4)").
type ClusterBroadcaster interface {
	// Publish broadcasts a binary payload for a document to every other pod
	// subscribed to its channel. The local pod delivers to its own clients
	// directly and MUST NOT receive its own Publish back. ephemeral selects
	// the awareness:{id} channel (volatile, lossy) versus doc:{id}.
	Publish(ctx context.Context, id model.DocumentID, payload []byte, ephemeral bool) error
	// Subscribe registers a handler invoked for every payload other pods
	// publish for the given document. The returned cancel function tears the
	// subscription down; it is safe to call once and idempotent thereafter.
	Subscribe(ctx context.Context, id model.DocumentID, handler func(payload []byte, ephemeral bool)) (cancel func(), err error)
}

// MetadataStore persists the small, queryable document index (NOT the blob).
// The default Alkemio adapter rides the existing server RabbitMQ save/fetch
// pattern, extended with content_pointer + blob_store; the standalone adapter
// uses Postgres (sqlc/pgx) or a local file.
//
// Maps to: contracts/persistence-ports.md (MetadataStore port) and
// data-model.md (document metadata/index).
type MetadataStore interface {
	// Load returns the index row for a document, or model.ErrNotFound if no
	// row exists.
	Load(ctx context.Context, id model.DocumentID) (model.Metadata, error)
	// Save upserts the index row and bumps its version. It is called on first
	// save and on every persisted snapshot.
	Save(ctx context.Context, meta model.Metadata) error
	// Delete removes the index row on the owner-delete cascade. Idempotent:
	// deleting an absent row is a no-op (lifecycle-events.md idempotency).
	Delete(ctx context.Context, id model.DocumentID) error
}

// BlobStore persists the encoded full Y.Doc v2 snapshot (debounced). The
// default adapter is inline (blob in the main DB); optional adapters offload to
// file-service, S3, or local disk. The pointer is opaque to the domain — its
// shape is the adapter's concern (inline row key / file-service object id / S3
// key).
//
// Maps to: contracts/persistence-ports.md (BlobStore port) and data-model.md
// (Snapshot, content-blob store).
type BlobStore interface {
	// Put stores the encoded snapshot bytes under pointer, overwriting any
	// previous snapshot (latest-only today).
	Put(ctx context.Context, pointer string, data []byte) error
	// Get returns the snapshot bytes for pointer, or model.ErrNotFound when
	// none has been stored.
	Get(ctx context.Context, pointer string) ([]byte, error)
	// Delete removes the snapshot on the owner-delete cascade. Idempotent:
	// deleting an absent pointer is a no-op.
	Delete(ctx context.Context, pointer string) error
}

// Auth resolves the connecting principal at the WebSocket handshake from the
// Alkemio token/cookie (Oathkeeper/Kratos). It is authentication only — the
// per-document grant is AuthZ's job. The 'open' adapter authenticates everyone
// as an anonymous identity for standalone use.
//
// Maps to: contracts/ws-protocol.md ("AuthN at the handshake").
type Auth interface {
	// Authenticate resolves the identity carried by the handshake token. A
	// non-nil error means the handshake MUST be rejected with 401; it MUST
	// NOT be downgraded to anonymous.
	Authenticate(ctx context.Context, token string) (model.Identity, error)
}

// AuthZ evaluates per-document authorization for an authenticated identity via
// the authorization-evaluation-service (h2c HTTP/2 POST /internal/auth/evaluate,
// or NATS). It is re-evaluated on document.access_changed. The 'open' adapter
// grants everything for standalone use.
//
// Maps to: contracts/ws-protocol.md ("AuthZ per document … viewer-vs-
// collaborator") and lifecycle-events.md (document.access_changed).
type AuthZ interface {
	// Evaluate decides whether the identity holds the privilege on the
	// document. A non-nil error means the question could not be answered;
	// callers MUST fail closed (constitution §V, file-service AuthPort
	// convention) — never treat an error as a healthy denial. A clean denial
	// is model.AuthDecision{Allowed: false}.
	Evaluate(ctx context.Context, identity model.Identity, id model.DocumentID, privilege model.Privilege) (model.AuthDecision, error)
}
