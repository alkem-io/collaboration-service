package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/memory"
	"github.com/antst/go-yjs/backend/persistence"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// errRoomUnavailable is returned when a room could not be joined because it kept
// tearing down under a join race (should be vanishingly rare).
var errRoomUnavailable = errors.New("collaboration room unavailable")

// ErrShuttingDown is returned from acquire/Join once Manager.Close has begun, so a
// late connection is refused rather than materializing a room that the shutdown
// drain snapshot already missed and would never drain (002 FR-001).
//
// Exported so the transport can close with a RETRYABLE status: a pod going away
// mid-join is the ordinary rolling-deploy case, and closing it as an internal
// error made clients treat a routine restart as a server fault and retry into it.
var ErrShuttingDown = errors.New("collaboration manager is shutting down")

// ErrRoomFull is returned from Join when the room is at its connection cap
// (FR-024 max connections per room). The handshake is refused; existing
// collaborators are unaffected.
var ErrRoomFull = errors.New("collaboration room is full")

// ErrDocumentPurging is returned from Join while an owner-delete cascade is in
// flight for the document. It is a refusal, not a failure: the document is being
// deleted, and admitting a connection mid-cascade is how deleted content comes
// back (see Manager.Purge).
var ErrDocumentPurging = errors.New("collaboration document is being deleted")

// ErrDocumentUnknown is returned from Join when no document with that id exists.
//
// It is deliberately NOT distinguishable from a denial on the wire — both close
// with the same status and reason — so the service cannot be used to enumerate
// which document ids exist. It is separate from ErrForbidden only so the server
// side can tell them apart in logs and metrics.
var ErrDocumentUnknown = errors.New("collaboration document does not exist")

// ErrForbidden is returned from Join when per-document authorization denies the
// connecting actor read access (a clean deny, not an error — the connection is
// refused, distinct from a fail-closed authZ transport error).
var ErrForbidden = errors.New("collaboration access denied")

// shutdownDrainGrace is the headroom Manager.Close waits on top of one
// BackendTimeout for live rooms to flush their final snapshot on shutdown before
// giving up — a backstop so a room whose run loop is wedged off the backend-call
// path cannot hang process exit.
const shutdownDrainGrace = 5 * time.Second

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
	// DocumentUndurable reports that a document is accepting edits it has not
	// managed to persist: consecutive is the number of failed flushes in a row,
	// and since is how long it has been in that state. It is emitted on EVERY
	// failed flush, not only at escalation, so the degraded window is visible
	// before anyone is disconnected (FR-026).
	DocumentUndurable(consecutive int, since time.Duration)
	// DocumentDurabilityRestored reports that a document that had been failing to
	// persist has succeeded again.
	DocumentDurabilityRestored()
	// DocumentEscalated reports that repeated persist failures crossed the
	// threshold and the document was torn down, DISCARDING undurable edits.
	// undurableFor is how long it had been failing (FR-028).
	DocumentEscalated(undurableFor time.Duration)
	// GenerationInvalidated reports that a document's in-memory generation was
	// poisoned and must be reloaded from storage.
	GenerationInvalidated()
	// FanoutPublished is called on each successful cross-pod publish, carrying
	// the publish latency (R10 fan-out lag).
	FanoutPublished(lag time.Duration)
	// FanoutFailed is called on each failed cross-pod publish.
	FanoutFailed()
	// ContributingActors sets the north-star contribution gauge for a room: the
	// number of distinct actors that contributed in the window just flushed
	// (FR-014). Always emitted, regardless of bus availability.
	ContributingActors(n int)
}

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
	// closed is set by Close under mu; acquire refuses new rooms once it is set, so
	// no room is materialized after the shutdown drain snapshot (002 FR-001).
	closed bool

	// purging is the owner-delete tombstone set: while a document's id is present,
	// acquire refuses to materialize a room for it. Refcounted rather than a bare
	// set so two concurrent Purges of one document cannot have the first to finish
	// lift the tombstone out from under the second.
	purging map[model.DocumentID]int

	// sf collapses concurrent first-connects for the same document onto ONE
	// materialization, so newRoom runs OFF mu (002 FR-010 — no lock across I/O).
	sf singleflight.Group

	// registry owns document identity, coalesced acquisition, eviction and
	// invalidation (§II — the core's contract IS the port). It is shared across
	// every room so two rooms can never hold two live copies of one document, and
	// so Invalidate reaches whichever room currently serves it.
	//
	// Held as the CONTRACT, not the shipped implementation. §II says the contract
	// is the port, and the concrete type bought nothing: nothing here calls a
	// method outside the interface, while pinning it made the failure paths that
	// run during shutdown untestable.
	registry memory.Registry
}

// NewManager constructs a room manager over the wired dependencies. A zero
// RoomConfig falls back to DefaultRoomConfig; a nil Metrics to NopMetrics.
func NewManager(deps Deps, cfg RoomConfig, metrics Metrics, logger *zap.Logger) *Manager {
	// One registry for the whole manager: document identity is process-wide, so a
	// per-room registry would let two rooms hold two live copies of one document.
	registry := memory.NewRegistry()
	deps.Registry = registry
	if cfg.SendBuffer == 0 && cfg.SaveDebounce == 0 && cfg.IdleTimeout == 0 {
		cfg = DefaultRoomConfig()
	}
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if deps.Hub == nil {
		// The core's shipped single-process hub, not a no-op: a room still publishes
		// and subscribes on the single-pod path, and using the real implementation
		// means that path is exercised by the same code multi-pod uses rather than
		// by a stub that silently does nothing.
		deps.Hub = hub.NewInProcess()
	}
	if deps.Contributor == nil {
		deps.Contributor = noopContributor{}
	}
	return &Manager{
		registry: registry,
		deps:     deps,
		cfg:      cfg,
		metrics:  metrics,
		logger:   logger,
		rooms:    make(map[model.DocumentID]*Room),
		purging:  make(map[model.DocumentID]int),
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

// JoinRequest is the set of inputs a connection brings when it joins a room: the
// document id and seed content type, the authenticated identity (empty actor id
// in open/standalone mode), and the outbound connection port. Bundling them in a
// struct keeps the call site readable as the Wave-3 presence/authZ inputs grew.
type JoinRequest struct {
	// ID is the document to join.
	ID model.DocumentID
	// Content seeds a freshly created room's convention (T010); the stored
	// content type wins for a persisted document.
	Content model.ContentType
	// Identity is the authenticated principal (open mode → empty ActorID).
	Identity model.Identity
	// Conn is the room's outbound port to this client.
	Conn Conn
}

// Join attaches a connection to the room for the request's document
// (materializing it on first connect) and returns the session plus the initial
// frames the connection must send to start the y-protocols handshake (SyncStep1 +
// the current awareness snapshot). The Manager evaluates per-document authZ ONCE,
// before the room is materialized, to set the session's collaborator mode; the room
// enforces the connection cap (FR-024). A join can therefore be refused
// (ErrForbidden / errRoomFull) or fail closed on an authZ error.
func (m *Manager) Join(ctx context.Context, req JoinRequest) (*Session, [][]byte, error) {
	// AUTHORIZE FIRST — before acquire, which materializes the room: it loads the
	// document from durable storage, opens a fan-out subscription, and takes a
	// registry slot. Evaluating after that let an actor with no read access make
	// the service fetch and decode any document it could name, and leave a live
	// room behind for the idle timeout. Nothing was ever sent to them, so it was
	// never a disclosure — but the work, the memory, the Redis subscription and
	// the timing signal were all reachable without authorization.
	//
	// The grant is evaluated ONCE per connection and travels with the join. It is
	// the session's capability for as long as the socket is open; see
	// authorizeSession.
	mode, err := m.authorizeSession(ctx, req.ID, req.Identity)
	if err != nil {
		return nil, nil, err
	}

	// THEN check the document exists — after authorization, never before. Ordering
	// here is a disclosure property, not a preference: a check that ran first
	// would answer "does document X exist?" for any id an unauthorized caller
	// cared to name, turning the service into an enumeration oracle for private
	// content. Authorization refuses those callers before existence is consulted,
	// and an unknown document is refused with the SAME external result as a
	// forbidden one (see joinCloseStatus), so the two are indistinguishable from
	// outside.
	if err := m.requireDocument(ctx, req.ID); err != nil {
		return nil, nil, err
	}

	// Retry once if the acquired room tears down between acquire and join (a
	// narrow race where the last member left and the idle timer fired). A second
	// acquire materializes a fresh room.
	for attempt := 0; attempt < 2; attempt++ {
		room, err := m.acquire(ctx, req.ID, req.Content)
		if err != nil {
			return nil, nil, err
		}

		res := make(chan joinResult, 1)
		if !room.enqueue(command{kind: cmdJoin, conn: req.Conn, identity: req.Identity, mode: mode, done: res}) {
			continue
		}
		select {
		case jr := <-res:
			if jr.err != nil {
				return nil, nil, jr.err
			}
			return &Session{room: room, id: jr.id}, jr.frames, nil
		case <-room.done:
			// The room tore down after our enqueue won the buffered-send race but
			// before processing the join: its run loop has exited and nothing will
			// ever write res, so a bare `<-res` would block this goroutine — and leak
			// the hijacked WebSocket behind it — forever. Retry: a fresh acquire
			// materializes a new room, and the 2-attempt budget then surfaces a
			// genuinely unavailable room as errRoomUnavailable rather than hanging.
			continue
		}
	}
	return nil, nil, errRoomUnavailable
}

// requireDocument refuses a join for a document that does not exist, before the
// room is materialized.
//
// The metadata store IS the existence record, and it is durable in every
// deployment that has one: in the Alkemio topology `collaboration-fetch`
// resolves against the memo and whiteboard rows in `server`'s own database, so
// "not found" means the owning entity is gone — deleted, or never created. That
// makes this gate survive a process restart and hold regardless of what the
// client does, which an in-memory tombstone could not.
//
// It is what stops a deleted document from coming back. The owner-delete
// tombstone only spans the cascade itself (beginPurge/endPurge); once it lifts,
// a reconnect used to materialize a fresh empty room, seed it, and write content
// and an index row back for a document the owner had deleted. With no
// authorization configured there was nothing else in the way.
//
// A store error that is NOT not-found fails closed: an unreachable backend must
// not be read as "the document is gone", which would tear down a live document
// during an outage.
func (m *Manager) requireDocument(ctx context.Context, id model.DocumentID) error {
	if _, err := m.deps.Metadata.Load(ctx, id); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("%w: %s", ErrDocumentUnknown, id)
		}
		return fmt.Errorf("resolve document %s: %w", id, err)
	}
	return nil
}

// authorizeSession evaluates the connecting identity's capability for one
// document, once, at connection establishment.
//
// Both privileges are decided here rather than one now and one later: they are a
// single question about one session ("may this connection read, and may it
// write"), and answering half of it before materialization and half after would
// put the expensive work between the two halves for no benefit.
//
// The result is the SESSION's capability and stays valid until the socket closes.
// That is the product contract, not an optimization: authorization is established
// per connection, and a permission revoked elsewhere takes effect when the client
// next connects. A reconnect is a new session and is evaluated again.
//
// Fail-closed (constitution §V): a port error is returned as-is, NOT as
// ErrForbidden, so the handshake closes with a transient/internal status and the
// client keeps retrying rather than treating a backend outage as a denial.
func (m *Manager) authorizeSession(ctx context.Context, id model.DocumentID, identity model.Identity) (model.CollaboratorMode, error) {
	read, err := m.deps.AuthZ.Evaluate(ctx, identity, id, model.PrivilegeRead)
	if err != nil {
		return "", fmt.Errorf("authorize read: %w", err)
	}
	if !read.Allowed {
		return "", ErrForbidden
	}
	write, err := m.deps.AuthZ.Evaluate(ctx, identity, id, model.PrivilegeUpdateContent)
	if err != nil {
		return "", fmt.Errorf("authorize update-content: %w", err)
	}
	if write.Allowed {
		return model.ModeCollaborator, nil
	}
	return model.ModeViewer, nil
}

// Purge runs the owner-delete cascade for a document (FR-023, T015): it
// disconnects any connected clients (session-end/document-deleted), releases the in-memory room,
// and purges the metadata index + the snapshot blob. It is idempotent — deleting
// a document with no live room still purges the durable rows, and deleting an
// absent document is a no-op. The room (when live) performs the durable purge on
// its own run loop so the Y.Doc's single-writer invariant holds; when no room is
// live the Manager purges directly.
func (m *Manager) Purge(ctx context.Context, id model.DocumentID) error {
	// ORDER IS LOAD-BEARING: refuse new acquisitions, THEN tear the room down,
	// THEN delete. Without the first step the cascade has a resurrection window —
	// a Join landing between the content delete and the index delete materializes
	// a fresh room, finds no checkpoint, seeds an empty document, and its first
	// flush writes content and an index row back for a document the owner deleted.
	// The tombstone closes that window by making the racing Join fail
	// (ErrDocumentPurging) instead of materializing.
	m.beginPurge(id)
	defer m.endPurge(id)

	m.mu.Lock()
	room, live := m.rooms[id]
	m.mu.Unlock()

	if live {
		done := make(chan error, 1)
		if room.enqueue(command{kind: cmdPurge, done2: done}) {
			select {
			case err := <-done:
				return err
			case <-room.done:
				// The room tore down without running our purge — its run loop exited
				// after our enqueue won the buffered-send race, so `done` is never
				// written and a bare `<-done` would block forever. Fall through to the
				// direct durable purge below; it is idempotent, so even a purge that
				// DID run is harmless to repeat.
			}
		}
		// The room tore down between lookup and enqueue; fall through to a direct
		// durable purge so no orphan is left.
	}
	return m.purgeDurable(ctx, id)
}

// beginPurge raises the owner-delete tombstone for id, so acquire refuses to
// materialize a room for it until the cascade finishes.
func (m *Manager) beginPurge(id model.DocumentID) {
	m.mu.Lock()
	m.purging[id]++
	m.mu.Unlock()
}

// endPurge lowers the tombstone raised by beginPurge. The tombstone is scoped to
// the cascade, not permanent: once the document is gone, a later connect is an
// ordinary open of a non-existent document, which authorization decides — not
// something this map should be adjudicating forever.
func (m *Manager) endPurge(id model.DocumentID) {
	m.mu.Lock()
	if m.purging[id] <= 1 {
		delete(m.purging, id)
	} else {
		m.purging[id]--
	}
	m.mu.Unlock()
}

// purgeDurable deletes the metadata row and the snapshot blob for a document with
// no live room. It resolves the blob pointer from the metadata row (a missing row
// means nothing to purge). Idempotent: not-found is success.
func (m *Manager) purgeDurable(ctx context.Context, id model.DocumentID) error {
	// Delete the durable state FIRST, then the index row. The deleter is
	// idempotent, so a document that never had stored state is not an error, and
	// the index row is what makes the state findable — dropping it first would
	// orphan the content instead of removing it.
	del, err := m.deps.deleter()
	if err != nil {
		return err
	}
	if err := del.Delete(ctx, persistence.DeleteRequest{DocumentID: backend.DocumentID(id)}); err != nil {
		return err
	}
	if err := m.deps.Metadata.Delete(ctx, id); err != nil && !errors.Is(err, model.ErrNotFound) {
		return err
	}
	return nil
}

// PreRegister writes an initial metadata row for a document ahead of its first
// connect (lifecycle document.created, T015). It is a thin pass-through to the
// MetadataStore so the standalone HTTP create and the bus event share one path.
func (m *Manager) PreRegister(ctx context.Context, meta model.Metadata) error {
	return m.deps.Metadata.Save(ctx, meta)
}

// acquire returns the live room for id, creating and starting it under the lock
// if absent so two concurrent first-connects share one room. The room's release
// callback removes it from the registry, closing the lazy-create/idle-release
// loop.
func (m *Manager) acquire(ctx context.Context, id model.DocumentID, content model.ContentType) (*Room, error) {
	m.mu.Lock()
	// The tombstone is checked BEFORE the registry hit, not after: a room that is
	// already draining under a cascade must not be handed out either, or the
	// caller's retry loop just races the cascade again on a fresh room.
	if _, purging := m.purging[id]; purging {
		m.mu.Unlock()
		return nil, ErrDocumentPurging
	}
	if room, ok := m.rooms[id]; ok {
		m.mu.Unlock()
		return room, nil
	}
	if m.closed {
		m.mu.Unlock()
		return nil, ErrShuttingDown
	}
	m.mu.Unlock()

	// Materialize OFF the registry lock (002 FR-010): newRoom does backend I/O
	// (snapshot load, fan-out subscribe) that must NOT run under m.mu, or one
	// unresponsive backend would wedge every Manager op — including shutdown. A
	// per-id singleflight collapses concurrent first-connects for the same document
	// onto ONE materialization; m.mu is taken only for the brief map check/insert.
	// Materialization and the run loop outlive the connecting request, so they must
	// not inherit its cancellation (context.WithoutCancel).
	v, err, _ := m.sf.Do(string(id), func() (interface{}, error) {
		// Re-check under the lock — another singleflight winner may have inserted,
		// or a Purge may have raised the tombstone since the fast path above.
		m.mu.Lock()
		if _, purging := m.purging[id]; purging {
			m.mu.Unlock()
			return nil, ErrDocumentPurging
		}
		if room, ok := m.rooms[id]; ok {
			m.mu.Unlock()
			return room, nil
		}
		if m.closed {
			m.mu.Unlock()
			return nil, ErrShuttingDown
		}
		m.mu.Unlock()

		roomCtx := context.WithoutCancel(ctx)
		room, err := newRoom(roomCtx, id, content, m.deps, m.cfg, m.metrics, m.logger.With(zap.String("doc", string(id))))
		if err != nil {
			return nil, err
		}
		room.onReleased = func() { m.remove(id, room) }

		m.mu.Lock()
		if _, purging := m.purging[id]; purging {
			// A cascade began while this room was materializing OFF the lock. It has
			// already loaded (or seeded) the document; registering it now would put a
			// live room on a document being deleted, and its first flush would write
			// the content back. Tear the fresh, never-served room down instead.
			m.mu.Unlock()
			room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
			return nil, ErrDocumentPurging
		}
		if m.closed {
			// A shutdown began during materialization: don't register a room the
			// drain snapshot already missed (it would never be drained). Tear the
			// fresh, never-served room down directly.
			m.mu.Unlock()
			room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
			return nil, ErrShuttingDown
		}
		m.rooms[id] = room
		m.mu.Unlock()

		m.metrics.RoomOpened()
		startRoom(room)
		return room, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Room), nil
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
	removed := false
	if cur, ok := m.rooms[id]; ok && cur == room {
		delete(m.rooms, id)
		removed = true
	}
	m.mu.Unlock()
	// Only count a close for a room that was actually registered (and thus counted
	// open via RoomOpened). The shutdown-abort path tears down a never-registered
	// room, so emitting RoomClosed there would underflow the rooms_active gauge.
	if removed {
		m.metrics.RoomClosed()
	}
	// The registry slot is NOT evicted here. It is evicted by the room's own
	// teardown, which covers this path and one this path cannot reach: a room torn
	// down during materialization — a shutdown race, or a purge cascade raising the
	// tombstone while newRoom ran off the lock — never entered m.rooms, so `removed`
	// is false and an evict guarded by it would never run. That room did acquire a
	// registry handle, so the document would outlive every room that ever owned it
	// with nothing left to clean it up.
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
	m.closed = true // refuse new-room materialization from here on (acquire checks it)
	rooms := make([]*Room, 0, len(m.rooms))
	for _, room := range m.rooms {
		rooms = append(rooms, room)
	}
	m.mu.Unlock()

	// ONE deadline bounds the WHOLE shutdown — both the cmdClose signal AND the
	// drain. App.Close drains closers last-in-first-out — Manager.Close first, THEN
	// the durable backends (postgres/rabbitmq/redis) — so returning early would let
	// those backends close out from under a room's in-flight save-on-shutdown persist,
	// losing the last debounce window of edits (the very edits the final snapshot
	// exists to save). cmdClose persists, then finish() closes r.done, so r.done is
	// each room's completion signal; rooms drain concurrently on their own goroutines
	// and each persist is bounded by cfg.BackendTimeout, so the whole drain is ~one
	// BackendTimeout. The shared deadline is a backstop that still guarantees shutdown
	// terminates even if a room's loop is wedged off the backend-call path.
	budget := m.cfg.BackendTimeout
	if budget <= 0 {
		budget = defaultBackendTimeout
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), budget+shutdownDrainGrace)
	defer cancel()

	// Signal every room CONCURRENTLY, bounded by shutdownCtx. A serial enqueue with
	// an unbounded context would let ONE room whose command buffer is saturated block
	// up to enqueueDeadline (30s) before the next room is even signalled — worst case
	// N×30s before the drain phase begins, far past the shutdown deadline (002 FR-001).
	// Each signal goroutine parks at most until shutdownCtx fires, so a full buffer
	// delays only its own room, never the others or the drain; defer cancel() releases
	// any still-parked goroutine when Close returns.
	for _, room := range rooms {
		room := room
		go room.enqueueCtx(shutdownCtx, command{kind: cmdClose})
	}

	// Drain bounded by the SAME deadline. shutdownCtx.Done() is a channel that, once
	// the deadline fires, STAYS closed for EVERY remaining room — unlike a one-shot
	// timer channel, which delivers its value once and then blocks later iterations on
	// room.done indefinitely.
	for _, room := range rooms {
		select {
		case <-room.done:
		case <-shutdownCtx.Done():
			m.logger.Warn("shutdown room drain exceeded deadline; some final snapshots may be incomplete",
				zap.Int("rooms_total", len(rooms)))
			// Still close the registry: the drain gave up, but leaving document
			// identity live after shutdown would pin every doc that never drained.
			m.closeRegistry()
			return
		}
	}
	m.closeRegistry()
}

// closeRegistry releases the document registry once the drain has finished (or
// given up). It runs AFTER the drain so a room's save-on-shutdown persist still
// has a valid document to encode.
func (m *Manager) closeRegistry() {
	if err := m.registry.Close(); err != nil {
		m.logger.Warn("closing document registry failed", zap.Error(err))
	}
}

// RoomCount reports the number of live rooms (test/observability helper).
func (m *Manager) RoomCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}
