package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// gateMeta wraps a MetadataStore and holds Load open until release is closed, so a
// test can wedge newRoom (which calls Metadata.Load during materialization) mid-
// acquire and observe whether the Manager's global registry lock is held across
// that backend I/O.
type gateMeta struct {
	inner   port.MetadataStore
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (g *gateMeta) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return model.Metadata{}, ctx.Err()
	}
	return g.inner.Load(ctx, id)
}
func (g *gateMeta) Save(ctx context.Context, m model.Metadata) error { return g.inner.Save(ctx, m) }
func (g *gateMeta) Delete(ctx context.Context, id model.DocumentID) error {
	return g.inner.Delete(ctx, id)
}

// TestInvMgrLivenessAcquireDoesNotHoldLockAcrossIO — INV-MGR-LIVENESS (spec 002
// FR-010). A single unresponsive backend reached inside newRoom MUST NOT block other
// Manager operations: acquire must not hold the global registry mutex across backend
// I/O. RED on current code (acquire does `m.mu.Lock(); defer Unlock(); newRoom(...)`,
// and newRoom→Metadata.Load is the gated I/O); GREEN once acquire drops the lock
// before materialization I/O (T019 per-id singleflight).
func TestInvMgrLivenessAcquireDoesNotHoldLockAcrossIO(t *testing.T) {
	deps := newTestDeps()
	gate := &gateMeta{inner: deps.meta, started: make(chan struct{}), release: make(chan struct{})}
	deps.Metadata = gate
	m := NewManager(deps.Deps, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 30 * time.Second}, nil, zap.NewNop())

	// A Join whose materialization wedges in the gated Metadata.Load.
	go func() {
		_, _, _ = m.Join(context.Background(), JoinRequest{ID: "doc-wedge", Content: model.ContentTypeMemo, Conn: newFakeClient(t)})
	}()
	<-gate.started // acquire is now inside newRoom→Load — on current code, holding m.mu

	// An unrelated Manager op that needs m.mu must not block on the wedged backend.
	got := make(chan int, 1)
	go func() { got <- m.RoomCount() }()
	select {
	case <-got:
		// good: the registry lock was not held across the materialization I/O
	case <-time.After(2 * time.Second):
		close(gate.release)
		t.Fatal("Manager.RoomCount blocked while a materialization was in flight — acquire holds the global registry mutex across backend I/O (FR-010)")
	}
	close(gate.release)
}
