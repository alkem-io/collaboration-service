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
	"fmt"

	"github.com/google/uuid"

	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/memory"
	"github.com/antst/go-yjs/backend/persistence"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Deps is the set of outbound ports the collaboration core is wired against.
// main (cmd/server) selects a concrete adapter per port from configuration and
// constructs this; the core consumes only the interfaces, keeping persistence,
// fan-out, and auth swappable (FR-019/020/021/022).
type Deps struct {
	// Hub fans updates and awareness across pods (§II — the core's contract IS
	// the port). A nil Hub defaults to the core's shipped in-process hub, which
	// is the correct single-pod behaviour: no peer exists, so nothing crosses.
	Hub hub.Hub
	// Metadata persists the queryable document index (RabbitMQ/Postgres).
	Metadata port.MetadataStore
	// Checkpoint persists the document's current state — one whole-document
	// snapshot per document, replaced on every save. It is the CRDT core's own
	// contract, which §II makes the port for this concern.
	Checkpoint persistence.CheckpointStore

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

// deleter returns the checkpoint store's deletion capability
// (persistence.Deleter, adopted from the core in go-yjs v0.0.3).
//
// Deletion is OPTIONAL in the contract, and deliberately so: some media are
// forbidden to delete (WORM storage, object locks, regulated archival tiers), and
// a mandatory Delete cannot express that. A caller that needs erasure therefore
// type-asserts and fails loudly when it is absent, which beats a store whose
// Delete silently does nothing.
//
// It is derived from Checkpoint rather than wired as a separate Deps field on
// purpose: the two must be the SAME instance, and a struct with both invites
// wiring one store as the reader and a different one as the deleter — a bug that
// compiles, passes most tests, and silently fails to delete anything.
//
// app.New asserts persistence.DeletingCheckpointStore at construction, so a store
// that cannot delete fails startup rather than surfacing here when an owner
// deletes a document.
func (d Deps) deleter() (persistence.Deleter, error) {
	del, ok := d.Checkpoint.(persistence.Deleter)
	if !ok {
		return nil, fmt.Errorf("checkpoint store %T cannot delete documents; the owner-delete cascade requires it", d.Checkpoint)
	}
	return del, nil
}

// noopContributor is the standalone default used when Deps.Contributor is nil:
// it drops the contribution event, so a room without an Alkemio bus pays nothing
// for a window flush (the Prometheus gauge is still emitted by the domain).
type noopContributor struct{}

// Contribution discards the contributing actor ids — no bus to publish to.
func (noopContributor) Contribution(context.Context, model.DocumentID, []uuid.UUID) error {
	return nil
}
