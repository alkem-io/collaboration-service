package service

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// errRoomUnavailable is returned when a room could not be joined because it kept
// tearing down under a join race (should be vanishingly rare).
var errRoomUnavailable = errors.New("collaboration room unavailable")

// Metrics is the observability surface the room lifecycle drives: the active
// room/connection gauges and the snapshot counter (metrics.go). The inbound
// HTTP adapter owns the concrete Prometheus collectors; the core depends only on
// this narrow interface so the domain has no Prometheus import (hexagon §I). A
// nil Metrics is tolerated (NopMetrics) so tests need not wire one.
type Metrics interface {
	// RoomOpened is called when a room is materialized.
	RoomOpened()
	// RoomClosed is called when a room is released.
	RoomClosed()
	// ConnOpened is called when a connection joins a room.
	ConnOpened()
	// ConnClosed is called when a connection leaves or is evicted.
	ConnClosed()
	// SnapshotSaved is called on each successfully persisted snapshot.
	SnapshotSaved()
	// SnapshotFailed is called on each failed snapshot persist.
	SnapshotFailed()
}

// NopMetrics is the no-op Metrics used when none is supplied.
type NopMetrics struct{}

// RoomOpened does nothing.
func (NopMetrics) RoomOpened() {}

// RoomClosed does nothing.
func (NopMetrics) RoomClosed() {}

// ConnOpened does nothing.
func (NopMetrics) ConnOpened() {}

// ConnClosed does nothing.
func (NopMetrics) ConnClosed() {}

// SnapshotSaved does nothing.
func (NopMetrics) SnapshotSaved() {}

// SnapshotFailed does nothing.
func (NopMetrics) SnapshotFailed() {}

// Manager is the room registry and lifecycle owner (T007). It lazily
// materializes a Room on the first connect for a document id, shares it across
// concurrent connections to the same document, and drops it from the registry
// when the room releases (idle/empty or owner delete). It is the only component
// that creates or destroys rooms, so room identity is process-unique per id.
type Manager struct {
	deps    Deps
	cfg     RoomConfig
	metrics Metrics
	logger  *zap.Logger

	mu    sync.Mutex
	rooms map[model.DocumentID]*Room
}

// NewManager constructs a room manager over the wired dependencies. A zero
// RoomConfig falls back to DefaultRoomConfig; a nil Metrics to NopMetrics.
func NewManager(deps Deps, cfg RoomConfig, metrics Metrics, logger *zap.Logger) *Manager {
	if cfg.SendBuffer == 0 && cfg.SaveDebounce == 0 && cfg.IdleTimeout == 0 {
		cfg = DefaultRoomConfig()
	}
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{
		deps:    deps,
		cfg:     cfg,
		metrics: metrics,
		logger:  logger,
		rooms:   make(map[model.DocumentID]*Room),
	}
}

// Session is the handle a connection holds onto its room: it forwards inbound
// frames and leaves on disconnect. It hides the connID and the room from the
// transport adapter, which only frames bytes.
type Session struct {
	room *Room
	id   connID
}

// SendBuffer is the per-connection outbound queue depth the adapter should use.
func (m *Manager) SendBuffer() int {
	if m.cfg.SendBuffer <= 0 {
		return DefaultRoomConfig().SendBuffer
	}
	return m.cfg.SendBuffer
}

// Join attaches conn to the room for id (materializing it on first connect) and
// returns the session plus the initial frames the connection must send to start
// the y-protocols handshake (SyncStep1 + the current awareness snapshot). The
// content type seeds a freshly created room's convention (T010); for an existing
// room or a persisted document the stored content type wins.
func (m *Manager) Join(ctx context.Context, id model.DocumentID, content model.ContentType, conn Conn) (*Session, [][]byte, error) {
	// Retry once if the acquired room tears down between acquire and join (a
	// narrow race where the last member left and the idle timer fired). A second
	// acquire materializes a fresh room.
	for attempt := 0; attempt < 2; attempt++ {
		room, err := m.acquire(ctx, id, content)
		if err != nil {
			return nil, nil, err
		}

		res := make(chan joinResult, 1)
		if !room.enqueue(command{kind: cmdJoin, conn: conn, done: res}) {
			continue
		}
		jr := <-res
		return &Session{room: room, id: jr.id}, jr.frames, nil
	}
	return nil, nil, errRoomUnavailable
}

// acquire returns the live room for id, creating and starting it under the lock
// if absent so two concurrent first-connects share one room. The room's release
// callback removes it from the registry, closing the lazy-create/idle-release
// loop.
func (m *Manager) acquire(ctx context.Context, id model.DocumentID, content model.ContentType) (*Room, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if room, ok := m.rooms[id]; ok {
		return room, nil
	}

	// Materialization (snapshot load) and the room's run loop outlive the
	// connecting request, so they must not inherit its cancellation: a client
	// disconnecting mid-load must not abort the room that other clients share.
	//
	// Wave-1 note: newRoom is called while holding m.mu, which serializes
	// concurrent first-connects across all documents through one mutex. With
	// the in-memory blob adapter the snapshot load is a map read (nanoseconds)
	// so the lock is never held for meaningful time. When durable blob
	// adapters (T005) land, m.mu should be dropped before I/O and re-acquired
	// only for the map write, using a per-id singleflight to collapse races.
	roomCtx := context.WithoutCancel(ctx)
	room, err := newRoom(roomCtx, id, content, m.deps, m.cfg, m.metrics, m.logger.With(zap.String("doc", string(id))))
	if err != nil {
		return nil, err
	}
	room.onReleased = func() { m.remove(id, room) }
	m.rooms[id] = room

	m.metrics.RoomOpened()
	startRoom(room)
	return room, nil
}

// startRoom launches a room's run loop. It deliberately takes no context: the
// run loop is decoupled from any request lifetime (it ends only on idle/empty or
// explicit close), so it must not inherit a request-scoped context.
func startRoom(room *Room) {
	go room.run()
}

// remove drops room from the registry if it is still the registered instance for
// id (guards against a race where a new room was created for the same id after
// this one released). Invoked from the room's run loop via onReleased.
func (m *Manager) remove(id model.DocumentID, room *Room) {
	m.mu.Lock()
	if cur, ok := m.rooms[id]; ok && cur == room {
		delete(m.rooms, id)
	}
	m.mu.Unlock()
	m.metrics.RoomClosed()
}

// Forward hands one inbound framed wire message to the session's room for
// serialized processing. Non-blocking from the caller's view beyond the room's
// command-channel buffer.
func (s *Session) Forward(frame []byte) {
	s.room.enqueue(command{kind: cmdMessage, src: s.id, data: frame})
}

// Leave detaches the connection from its room. The room releases itself (after a
// final snapshot) once the last member leaves and the idle timer elapses.
func (s *Session) Leave() {
	s.room.enqueue(command{kind: cmdLeave, src: s.id})
}

// Close releases every live room (final snapshot + teardown) — used on graceful
// shutdown so in-flight edits are not lost.
func (m *Manager) Close() {
	m.mu.Lock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, room := range m.rooms {
		rooms = append(rooms, room)
	}
	m.mu.Unlock()

	for _, room := range rooms {
		room.enqueue(command{kind: cmdClose})
	}
}

// RoomCount reports the number of live rooms (test/observability helper).
func (m *Manager) RoomCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}
