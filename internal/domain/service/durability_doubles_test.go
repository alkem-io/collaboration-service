package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// errBackendDown is the sentinel a failing store returns, standing in for an
// unreachable file-service.
var errBackendDown = errors.New("checkpoint backend unreachable")

// outageStore fails every SaveCheckpoint while `down` is set, so a test can hold
// a document in the undurable state and then let it recover. Loads keep working:
// the failure mode under test is a write-path outage, not a corrupt document.
type outageStore struct {
	inner *persistinprocess.Store

	mu    sync.Mutex
	down  bool
	saves int
}

func newOutageStore() *outageStore {
	return &outageStore{inner: persistinprocess.New(), down: true}
}

func (s *outageStore) recover() {
	s.mu.Lock()
	s.down = false
	s.mu.Unlock()
}

// saveAttempts reports how many times a flush was attempted, which is how a test
// distinguishes "retried" from "gave up after the first failure".
func (s *outageStore) saveAttempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *outageStore) SaveCheckpoint(ctx context.Context, req persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	s.mu.Lock()
	s.saves++
	down := s.down
	s.mu.Unlock()
	if down {
		return 0, errBackendDown
	}
	return s.inner.SaveCheckpoint(ctx, req)
}

func (s *outageStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	return s.inner.LoadCheckpoint(ctx, id)
}

func (s *outageStore) DeleteCheckpoint(ctx context.Context, id backend.DocumentID) error {
	return s.inner.DeleteCheckpoint(ctx, id)
}

func (s *outageStore) FenceMode() persistence.FenceMode { return s.inner.FenceMode() }

// durabilityMetrics records the durability signals so a test can assert on the
// METRIC surface rather than only on logs — the distinction FR-026 draws, because
// an operator alerts on metrics and reads logs afterwards.
type durabilityMetrics struct {
	NopMetrics

	mu sync.Mutex
	// undurable is one entry per DocumentUndurable call, in order.
	undurable []undurableSample
	restored  int
	escalated []time.Duration
	// connClosed counts ConnClosed, so a test can prove the degraded state was
	// visible BEFORE anyone was disconnected rather than at the same moment.
	connClosed int
}

type undurableSample struct {
	consecutive int
	since       time.Duration
	// connClosedSoFar snapshots the disconnect count at the instant this sample
	// was emitted. Comparing timestamps would be a race; this is an ordering fact.
	connClosedSoFar int
}

func (m *durabilityMetrics) DocumentUndurable(consecutive int, since time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.undurable = append(m.undurable, undurableSample{
		consecutive: consecutive, since: since, connClosedSoFar: m.connClosed,
	})
}

func (m *durabilityMetrics) DocumentDurabilityRestored() {
	m.mu.Lock()
	m.restored++
	m.mu.Unlock()
}

func (m *durabilityMetrics) DocumentEscalated(undurableFor time.Duration) {
	m.mu.Lock()
	m.escalated = append(m.escalated, undurableFor)
	m.mu.Unlock()
}

func (m *durabilityMetrics) ConnClosed() {
	m.mu.Lock()
	m.connClosed++
	m.mu.Unlock()
}

func (m *durabilityMetrics) undurableSamples() []undurableSample {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]undurableSample(nil), m.undurable...)
}

func (m *durabilityMetrics) restoredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.restored
}

func (m *durabilityMetrics) escalations() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.escalated...)
}

// controlMessages returns every control message the client received, decoded.
func (c *fakeClient) controlMessages() []model.ControlMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.ControlMessage(nil), c.control...)
}
