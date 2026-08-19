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

// Auth resolves the connecting principal at the WebSocket handshake from the
// domain-typed credential set (model.HandshakeCredentials: gateway actor-id
// header / BFF cookie session / Hydra bearer / guest name). It is authentication
// only — the per-document grant is AuthZ's job. The selected adapter decides
// which credentials it inspects and in what priority (the WS adapter only reads
// them off the transport), keeping the port infra-free (§I). The 'open' adapter
// authenticates everyone as an anonymous identity for standalone use.
//
// Maps to: contracts/ws-protocol.md ("AuthN at the handshake").
type Auth interface {
	// Authenticate resolves the identity carried by the handshake credentials. A
	// non-nil error means the handshake MUST be rejected with 401 — a credential
	// was PRESENTED but is invalid (malformed/expired/signature-rejected/
	// tombstoned), or a required dependency was unreachable; it MUST NOT be
	// downgraded to anonymous (constitution §V: missing ≠ failed). A MISSING
	// credential is not a failure: the oidc adapter resolves it to the anonymous
	// sentinel and lets AuthZ decide.
	Authenticate(ctx context.Context, creds model.HandshakeCredentials) (model.Identity, error)
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

// Contributor emits the north-star contribution event (FR-014): the per-window
// set of actor ids that mutated a document. In the Alkemio deployment the
// rabbitmq adapter publishes the `collaboration-contribution` event so server
// analytics stay unbroken; standalone uses a no-op so a contribution flush costs
// nothing without a bus. The Prometheus gauge is always emitted by the domain
// independently of this port.
//
// Maps to: contracts/unified-metadata-rmq.md (`collaboration-contribution`).
type Contributor interface {
	// Contribution publishes the contributing actor ids for a document window
	// (fire-and-forget). An error is logged and dropped — a missed analytics
	// event MUST NOT break live collaboration.
	Contribution(ctx context.Context, id model.DocumentID, actorIDs []string) error
}
