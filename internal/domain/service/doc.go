// Package service is the application core of the collaboration-service hexagon:
// room lifecycle (lazy materialize on connect, release/purge on idle/delete),
// y-protocols sync + awareness orchestration, debounced snapshot persistence,
// presence/limits, and the document-delete cascade. It depends only on the
// domain ports (internal/domain/port) and types (internal/domain/model) — never
// on a concrete adapter.
//
// This is the Phase-1 (provisioning) skeleton: the package exists, compiles,
// and declares the dependency surface. The room-lifecycle, sync, persistence,
// presence, and lifecycle behavior land with tasks T007–T016 of
// specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package service

import (
	"context"

	"github.com/antst/go-yjs/backend/memory"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Deps is the set of outbound ports the collaboration core is wired against.
// main (cmd/server) selects a concrete adapter per port from configuration and
// constructs this; the core consumes only the interfaces, keeping persistence,
// fan-out, and auth swappable (FR-019/020/021/022).
type Deps struct {
	// Broadcaster fans updates/awareness across pods (in-memory default, R4).
	Broadcaster port.ClusterBroadcaster
	// Metadata persists the queryable document index (RabbitMQ/Postgres).
	Metadata port.MetadataStore
	// Blob persists the encoded Y.Doc v2 snapshot (inline/file-service/S3).
	Blob port.BlobStore
	// Auth resolves the handshake identity (Alkemio token / open).
	Auth port.Auth
	// AuthZ evaluates per-document grants (auth-evaluation-service / open).
	AuthZ port.AuthZ
	// Contributor emits the north-star contribution event in Alkemio mode
	// (rabbitmq); nil defaults to a no-op so standalone pays nothing (T013).
	Contributor port.Contributor
	// Registry owns in-process document identity, coalesced acquisition, eviction
	// and invalidation. It is the CRDT core's own contract, which §II makes the
	// port for this concern — the domain depends on the contract, never on a
	// concrete implementation. Manager supplies one shared registry so concurrent
	// opens of the same document coalesce; nil defaults to a private in-process
	// registry, which is correct for a lone room (one room owns one document) and
	// keeps a directly-constructed Room usable.
	Registry memory.Registry
}

// noopBroadcaster is the single-pod default used when Deps.Broadcaster is nil:
// it publishes nowhere and never invokes a subscriber, so a room wired without
// an explicit cross-pod broadcaster behaves exactly as single-pod. Keeping the
// default inside the domain avoids importing the inmemory adapter here (which
// would break the inward-only dependency rule, §I); the adapter remains the
// configuration-selected production default in cmd/server.
type noopBroadcaster struct{}

// Publish discards the payload — there is no peer pod to fan out to.
func (noopBroadcaster) Publish(context.Context, model.DocumentID, []byte, bool) error {
	return nil
}

// Subscribe registers nothing and returns a no-op cancel.
func (noopBroadcaster) Subscribe(context.Context, model.DocumentID, func([]byte, bool)) (func(), error) {
	return func() {}, nil
}

// noopContributor is the standalone default used when Deps.Contributor is nil:
// it drops the contribution event, so a room without an Alkemio bus pays nothing
// for a window flush (the Prometheus gauge is still emitted by the domain).
type noopContributor struct{}

// Contribution discards the contributing actor ids — no bus to publish to.
func (noopContributor) Contribution(context.Context, model.DocumentID, []string) error {
	return nil
}
