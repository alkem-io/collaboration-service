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

	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// MetadataStore persists the small, queryable document index (NOT the blob).
// The Alkemio adapter rides the existing server RabbitMQ save/fetch pattern,
// extended with content_pointer. The in-memory adapter serves the in-process
// development and test path and is not durable.
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

// Auth resolves the connecting principal at the WebSocket handshake from the ONE
// credential the transport carries: the value of the configured gateway actor-id
// header. It is authentication only — the per-document grant is AuthZ's job.
//
// A raw string, not a credential struct. There is exactly one credential and two
// adapters, one of which ignores it; a set-of-credentials type described a
// direct-validation adapter that has been removed.
//
// Maps to: contracts/ws-protocol.md ("AuthN at the handshake").
type Auth interface {
	// Authenticate resolves the identity carried by the handshake credential. A
	// non-nil error means the handshake MUST be rejected with 401.
	//
	// WHETHER ABSENCE IS A FAILURE IS MODE-DEPENDENT, and the two modes differ on
	// purpose:
	//   - `header`: an EMPTY credential means the gateway did not run, which is a
	//     failed handshake, never an anonymous downgrade. The gateway always
	//     stamps something — the actor id, or the nil-UUID sentinel for an
	//     un-credentialed caller — so absence is infrastructure failure.
	//   - `open`: absence is expected; everyone is anonymous and AuthZ is bypassed.
	// A credential that is PRESENT but invalid is always a failure (constitution
	// §V: missing != failed cuts both ways — never treat an invalid credential as
	// anonymous).
	Authenticate(ctx context.Context, actorIDCredential string) (model.Identity, error)
}

// AuthZ evaluates per-document authorization for an authenticated identity via
// the authorization-evaluation-service (h2c HTTP/2 POST /internal/auth/evaluate,
// or NATS). It is evaluated ONCE per WebSocket
// session, before the room is materialized, and holds until that socket closes. The 'open' adapter
// grants everything for standalone use.
//
// Maps to: contracts/ws-protocol.md ("AuthZ per document … viewer-vs-
// collaborator").
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
	// (fire-and-forget). An error is returned to the room so the detached actor
	// set can be retained for the next periodic attempt; it never breaks live
	// collaboration.
	Contribution(ctx context.Context, id model.DocumentID, actorIDs []uuid.UUID) error
}
