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
}
