package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// gaugeMetrics tracks the room open/close gauge deltas.
type gaugeMetrics struct {
	NopMetrics
	opened atomic.Int64
	closed atomic.Int64
}

func (m *gaugeMetrics) RoomOpened() { m.opened.Add(1) }
func (m *gaugeMetrics) RoomClosed() { m.closed.Add(1) }

// TestInvShutdownAbortNoGaugeUnderflow covers the singleflight shutdown-abort path
// (spec 002 §6, FR-001) and its observability: a first-connect whose materialization
// completes AFTER Manager.Close set m.closed is aborted — the never-registered room
// is torn down rather than leaked, and because it was never counted OPEN it must not
// be counted CLOSED (else the rooms_active gauge underflows).
func TestInvShutdownAbortNoGaugeUnderflow(t *testing.T) {
	deps := newTestDeps()
	gate := &gateMeta{inner: deps.meta, started: make(chan struct{}), release: make(chan struct{})}
	deps.Metadata = gate
	metrics := &gaugeMetrics{}
	m := NewManager(deps.Deps, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 30 * time.Second}, metrics, zap.NewNop())

	// A Join whose materialization wedges in the gated Metadata.Load (m.closed not yet set).
	joinErr := make(chan error, 1)
	go func() {
		_, _, err := m.Join(context.Background(), JoinRequest{ID: "doc-abort", Content: model.ContentTypeMemo, Conn: newFakeClient(t)})
		joinErr <- err
	}()
	<-gate.started

	// Shut down WHILE that materialization is in flight → the room will be aborted.
	m.Close()

	// Let materialization finish: the inner m.closed re-check aborts (tears down the
	// never-registered room) instead of registering it.
	close(gate.release)
	select {
	case err := <-joinErr:
		if !errors.Is(err, errShuttingDown) {
			t.Fatalf("Join during shutdown should return errShuttingDown, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Join did not return after the shutdown abort")
	}

	// The aborted room was never counted open, so it must not be counted closed.
	if opened, closed := metrics.opened.Load(), metrics.closed.Load(); closed != opened {
		t.Fatalf("rooms_active gauge underflow: RoomClosed=%d but RoomOpened=%d (the abort path emitted a Close for a never-opened room)", closed, opened)
	}
}
