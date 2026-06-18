// Package inmemory is the single-pod ClusterBroadcaster: it does no cross-pod
// fan-out because there is only one pod. Local delivery to a pod's own clients
// is the room's job; this adapter is the zero-dependency default that satisfies
// the port without a Redis dependency (R4). The Redis adapter (sibling package,
// task T004) provides the multi-pod implementation.
package inmemory

import (
	"context"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Broadcaster is the no-op, single-pod ClusterBroadcaster.
type Broadcaster struct{}

// New constructs the single-pod broadcaster.
func New() *Broadcaster { return &Broadcaster{} }

// Publish is a no-op: with one pod there is no peer to fan out to.
func (b *Broadcaster) Publish(_ context.Context, _ model.DocumentID, _ []byte, _ bool) error {
	return nil
}

// Subscribe registers nothing — no peer pod will ever publish — and returns a
// no-op cancel so callers can treat single-pod and multi-pod uniformly.
func (b *Broadcaster) Subscribe(_ context.Context, _ model.DocumentID, _ func(payload []byte, ephemeral bool)) (func(), error) {
	return func() {}, nil
}

// compile-time assertion that Broadcaster satisfies the port.
var _ port.ClusterBroadcaster = (*Broadcaster)(nil)
