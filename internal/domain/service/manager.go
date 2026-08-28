package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/memory"

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

// deleteTombstoneTTL bridges the deliberately short interval between the
// server's confirmed pre-delete publish and its owner-row deletion. Five minutes
// is long enough for the synchronous cascade while remaining self-healing if the
// server crashes after publishing and leaves the document intact.
const deleteTombstoneTTL = 5 * time.Minute

// Metrics is the observability surface the room lifecycle drives: the active
// room/connection gauges and the snapshot counter (metrics.go). The inbound
// HTTP adapter owns the concrete Prometheus collectors; the core depends only on
// this narrow interface so the domain has no Prometheus import (hexagon §I). A
// nil Metrics is tolerated (NopMetrics) so tests need not wire one.
type Metrics interface {
	// RoomOpened is called when a room is materialized.
	RoomOpened()
	// RoomClosed is called when a room is released. It carries the document id
	// because a released room can no longer be accepting edits it cannot persist,
	// so this is also where degraded-durability tracking for that document ends —
	// on EVERY teardown, including the ones that neither restore durability nor
	// escalate.
	RoomClosed(doc string)
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
	// doc identifies WHICH document, so the reporter can keep per-document state
	// and publish a correct aggregate. It is deliberately NOT a Prometheus label:
	// one series per document is unbounded cardinality.
	DocumentUndurable(doc string, consecutive int, since time.Duration)
	// DocumentDurabilityRestored reports that a document that had been failing to
	// persist has succeeded again.
	DocumentDurabilityRestored(doc string)
	// DocumentEscalated reports that repeated persist failures crossed the
	// threshold and the document was torn down, DISCARDING undurable edits.
	// undurableFor is how long it had been failing (FR-028).
	DocumentEscalated(doc string, undurableFor time.Duration)
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
	// deleteTombstones temporarily refuse new admissions for a document whose
	// owner deletion has started. They are process-local by design: a restart also
	// drops every live room, after which the authoritative metadata lookup decides.
	deleteTombstones map[model.DocumentID]time.Time
	// closed is set by Close under mu; acquire refuses new rooms once it is set, so
	// no room is materialized after the shutdown drain snapshot (002 FR-001).
	closed bool

	// deleteEpoch counts owner-deletes, and invalidates admissions in flight across
	// one. Join captures it before its existence read; acquire re-checks it under
	// this mutex. Any delete in between refuses the acquisition, whichever way the
	// two interleave — the guarantee does not depend on a delete taking any
	// particular amount of time, which is what a per-id marker would have needed.
	//
	// It is Manager-wide, so an UNRELATED document's deletion also refuses: a rare
	// transient retry for one connecting client, never a false admission.
	deleteEpoch uint64

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
		registry:         registry,
		deps:             deps,
		cfg:              cfg,
		metrics:          metrics,
		logger:           logger,
		rooms:            make(map[model.DocumentID]*Room),
		deleteTombstones: make(map[model.DocumentID]time.Time),
	}
}

// Session is the handle a connection holds onto its room: it forwards inbound
// frames and leaves on disconnect. It hides the connID and the room from the
// transport adapter, which only frames bytes.
type Session struct {
	room *Room
	id   connID
	// conn is this session's own outbound port. The session end for a refused
	// enqueue MUST NOT route through the room: the room's command queue is the
	// very thing that was unavailable, so telling the client through it could
	// fail for the same reason. Holding the port here keeps the reason delivery
	// independent of the failure it reports.
	conn Conn
	// dropped is set when an inbound frame from THIS session could not be handed
	// to the room — the room left Active, or the command buffer stayed full past
	// the enqueue deadline. It is read on the room loop via the command that
	// carries it, so a later durability request from a session that has already
	// lost a mutation is REFUSED rather than answered.
	//
	// Atomic because Forward runs on the socket's reader goroutine while the flag
	// is consumed on the room's run loop.
	dropped atomic.Bool
}

// SendBuffer is the per-connection outbound queue depth the adapter should use.
func (m *Manager) SendBuffer() int {
	if m.cfg.SendBuffer <= 0 {
		return DefaultRoomConfig().SendBuffer
	}
	return m.cfg.SendBuffer
}

// JoinRequest is the set of inputs a connection brings when it joins a room: the
// document id and seed content type, the authenticated identity (nil actor id
// in open mode), and the outbound connection port. Bundling them in a
// struct keeps the call site readable as the Wave-3 presence/authZ inputs grew.
type JoinRequest struct {
	// ID is the document to join.
	ID model.DocumentID
	// Content seeds a freshly created room's convention (T010); the stored
	// content type wins for a persisted document.
	Content model.ContentType
	// Identity is the authenticated principal (open mode → nil ActorID).
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
	// Capture the delete epoch IMMEDIATELY BEFORE the existence read, so the pair
	// "the row existed" and "no delete has happened since" is checked against one
	// point in time. Reading it after would leave the gap this closes.
	m.mu.Lock()
	now := time.Now()
	if expires, tombstoned := m.deleteTombstones[req.ID]; tombstoned {
		if now.Before(expires) {
			m.mu.Unlock()
			return nil, nil, ErrForbidden
		}
		delete(m.deleteTombstones, req.ID)
	}
	epoch := m.deleteEpoch
	m.mu.Unlock()

	meta, err := m.requireDocument(ctx, req.ID)
	if err != nil {
		return nil, nil, err
	}

	// Retry once if the acquired room tears down between acquire and join (a
	// narrow race where the last member left and the idle timer fired). A second
	// acquire materializes a fresh room.
	for attempt := 0; attempt < 2; attempt++ {
		room, err := m.acquire(ctx, req.ID, req.Content, epoch)
		if err != nil {
			return nil, nil, err
		}

		res := make(chan joinResult, 1)
		if !room.enqueue(command{
			kind: cmdJoin, conn: req.Conn, identity: req.Identity, mode: mode,
			isMultiUser: meta.IsMultiUser, done: res,
		}) {
			continue
		}
		select {
		case jr := <-res:
			if jr.err != nil {
				return nil, nil, jr.err
			}
			return &Session{room: room, id: jr.id, conn: req.Conn}, jr.frames, nil
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

// requireDocument loads the current authoritative row and refuses a join for a
// document that does not exist, before the room is materialized. The returned
// row also carries optional admission inputs that the room applies on its
// serialized join command.
//
// The metadata store IS the existence record, and it is durable in every
// deployment that has one: in the Alkemio topology `collaboration-fetch`
// resolves against the memo and whiteboard rows in `server`'s own database, so
// "not found" means the owning entity is gone — deleted, or never created. That
// makes this gate survive a process restart and hold regardless of what the
// client does, which no in-memory marker could.
//
// It is the durable authority that stops a deleted document from coming back on a
// RECONNECT after the short in-memory tombstone expires or the process restarts.
// The delete epoch invalidates admissions already in flight; the tombstone refuses
// fresh admissions while the owner cascade is expected to finish; only this read
// ultimately knows whether the document still exists.
//
// The three checks are complementary. This gate cannot catch a Join that read the
// row before deletion started; the epoch catches that stale admission; the
// tombstone catches a fresh Join while the row legitimately still exists.
//
// A store error that is NOT not-found fails closed: an unreachable backend must
// not be read as "the document is gone", which would tear down a live document
// during an outage.
func (m *Manager) requireDocument(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	meta, err := m.deps.Metadata.Load(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return model.Metadata{}, fmt.Errorf("%w: %s", ErrDocumentUnknown, id)
		}
		return model.Metadata{}, fmt.Errorf("resolve document %s: %w", id, err)
	}
	// Temporary progressive-rollout gate. Authorization has already succeeded,
	// but retained legacy content is not safe to materialize as an empty Y.Doc.
	// Use the ordinary refusal on the wire; once the migration atomically stores
	// the snapshot pointer and flips this marker, the next join succeeds.
	if !meta.Migrated {
		return model.Metadata{}, ErrForbidden
	}
	return meta, nil
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

// CloseDeleted reacts to the owner deleting a document (FR-023, T015): it
// disconnects connected clients with session-end/document-deleted and releases
// the in-memory room. It DELETES NOTHING.
//
// `server` confirms this persistent event before starting its synchronous owner
// cascade. A short per-document tombstone prevents immediate rematerialization
// while that cascade completes. Idempotent in both directions: a document with no
// live room is a no-op, and a repeated event closes an already-closed room and
// renews its tombstone without error.
//
// The room (when live) runs its own teardown on its run loop so the Y.Doc's
// single-writer invariant holds, and that teardown deliberately does NOT flush —
// flushing would write content back for a document that no longer exists.
func (m *Manager) CloseDeleted(ctx context.Context, id model.DocumentID) error {
	// ORDER IS LOAD-BEARING: invalidate in-flight admissions, THEN tear the room
	// down — and both under the same mutex the admission path uses, or the two can
	// interleave with nothing to see each other.
	//
	// Without the first step there is a resurrection window. A Join that already
	// passed the existence gate (server's row was still there) materializes a fresh
	// room, finds no checkpoint because server deleted it, seeds an empty document,
	// and once a session is admitted its first edit flushes content back for a
	// document the owner deleted. Bumping the epoch here makes that Join refuse
	// admission instead.
	//
	// The Join-time tombstone/existence gate is the SECOND line of defence, not a
	// replacement: it refuses new admissions, but a Join that passed the tombstone
	// and read the row BEFORE this event has already crossed both checks. Only the
	// epoch catches that in-flight admission.
	expires := time.Now().Add(deleteTombstoneTTL)
	m.mu.Lock()
	m.deleteEpoch++
	m.deleteTombstones[id] = expires
	room, live := m.rooms[id]
	m.mu.Unlock()
	time.AfterFunc(deleteTombstoneTTL, func() {
		m.mu.Lock()
		if current, ok := m.deleteTombstones[id]; ok && current.Equal(expires) {
			delete(m.deleteTombstones, id)
		}
		m.mu.Unlock()
	})

	if live {
		done := make(chan error, 1)
		if !room.enqueueCtx(ctx, command{kind: cmdCloseDeleted, done2: done}) {
			// enqueueCtx refuses for two opposite reasons, and they must not be
			// conflated. A room that is no longer active is the outcome we wanted —
			// it already tore down. A room that is STILL active means the caller's
			// deadline elapsed against a wedged run loop, which is transient and must
			// be retried, or the document keeps a live room nobody will ever close.
			if room.lc.is(stateActive) {
				// TWO different reasons land here, and only one of them has a ctx
				// error. enqueueCtx also gives up on its OWN enqueueDeadline while the
				// caller's context is still perfectly healthy — wrapping a nil
				// ctx.Err() there would render as %!w(<nil>) and wrap nothing.
				if cerr := ctx.Err(); cerr != nil {
					return fmt.Errorf("close for %s: a live room did not accept it: %w", id, cerr)
				}
				return fmt.Errorf("close for %s: %w", id, errRoomUnavailable)
			}
			return nil
		}
		select {
		case err := <-done:
			return err
		case <-room.done:
			// The room tore down without running our command — its run loop exited
			// after our enqueue won the buffered-send race, so `done` is never
			// written and a bare `<-done` would block forever. The room is gone,
			// which is the outcome we wanted.
		case <-ctx.Done():
			// Accepted but not completed inside the deadline. Transient: the broker
			// redelivers, and this same deadline is what paces that retry — no sleep
			// and no retry state of our own.
			return fmt.Errorf("close for %s: accepted but not completed: %w", id, ctx.Err())
		}
	}
	// No live room: nothing to close, and nothing durable to delete.
	return nil
}

// PreRegister writes an initial metadata row for a document ahead of its first
// connect (T016). Its ONLY caller is the no-bus document-create HTTP handler,
// which is mounted only when METADATA_STORE is not rabbitmq — so this is a
// tests/local path and is unreachable in the Alkemio deployment, where `server`
// creates the row and this service reads it over collaboration-fetch.
//
// There is no inbound `document.created` event; the lifecycle consumer handles
// `document.deleted` only. It is a thin pass-through to the MetadataStore.
func (m *Manager) PreRegister(ctx context.Context, meta model.Metadata) error {
	return m.deps.Metadata.Save(ctx, meta)
}

// acquire returns the live room for id, materializing one if absent. A per-id
// singleflight coalesces concurrent first-connects onto a single materialization,
// which runs OFF the registry lock; the lock is taken only briefly to register the
// result, and the room is started after that. Its release callback removes it from
// the registry, closing the lazy-create/idle-release loop.
// expectedEpoch is the caller's delete epoch, read before it checked that the
// document exists. Any owner-delete since then refuses this acquisition: the
// caller's "it exists" is stale, and admitting on a stale read is how deleted
// content comes back.
//
// A delete of an UNRELATED document also refuses, because the epoch is
// Manager-wide. That is the deliberate trade: a rare, transient retry for one
// connecting client in exchange for race-free invalidation of admissions that
// crossed the per-id tombstone check before the event arrived.
func (m *Manager) acquire(ctx context.Context, id model.DocumentID, content model.ContentType, expectedEpoch uint64) (*Room, error) {
	// Materialize OFF the registry lock (002 FR-010): newRoom does backend I/O
	// (snapshot load, fan-out subscribe) that must NOT run under m.mu, or one
	// unresponsive backend would wedge every Manager op — including shutdown. A
	// per-id singleflight collapses concurrent first-connects for the same document
	// onto ONE materialization; m.mu is taken only for the brief map check/insert.
	// Materialization and the run loop outlive the connecting request, so they must
	// not inherit its cancellation (context.WithoutCancel).
	v, err, _ := m.sf.Do(string(id), func() (interface{}, error) {
		// No epoch check here. This function is SHARED: singleflight hands one
		// execution's result to every caller that deduplicated behind it, and those
		// callers can hold different epochs. A check here would speak only for
		// whichever caller happened to lead, so the per-caller check lives after
		// sf.Do returns instead.
		m.mu.Lock()
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
		if m.deleteEpoch != expectedEpoch {
			// A delete landed while this room was materializing OFF the lock. This
			// check is about REGISTRATION, not admission — the per-caller check after
			// sf.Do already decides who may be admitted. What it prevents is a room
			// for a just-deleted document being inserted into the registry, where the
			// delete that raced it has already looked and found nothing to close.
			//
			// Tearing down rather than merely declining to register is what stops it
			// leaking: it was never inserted, so nothing will ever release it, and its
			// run loop starts with the idle timer stopped.
			m.mu.Unlock()
			room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
			return nil, errRoomUnavailable
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

	// THE PER-CALLER EPOCH CHECK, and the reason it is HERE rather than inside the
	// function above: singleflight collapses concurrent acquisitions of one document
	// onto a single execution, so a caller holding a stale epoch can be handed a
	// room produced by a caller holding a fresh one. Only a check outside the shared
	// function sees this caller's own epoch.
	//
	// It also covers the warm-room shape, which never materializes and so never
	// reaches the pre-insert check: an existing room is returned straight from the
	// registry, and this is the only thing standing between it and a stale
	// admission.
	//
	// The reverse pairing — a fresh caller behind a stale leader — is refused too,
	// because the leader's error is shared. That is a transient false retry, the same
	// trade an unrelated document's deletion already makes, and the client reconnects
	// into a fresh existence read.
	m.mu.Lock()
	stale := m.deleteEpoch != expectedEpoch
	m.mu.Unlock()
	if stale {
		// The room may have been MATERIALIZED for this caller — registered, counted
		// and started — and this refusal means no cmdJoin will ever follow. The idle
		// timer is armed only by cmdJoin/cmdLeave/cmdMessage, so without this the
		// room's goroutine, its Y.Doc, its registry handle and its hub subscription
		// are held for the life of the process, rooms_active is inflated, and it
		// keeps applying peer updates and flushing for a document nobody is editing.
		//
		// Releasing it directly from here would race a concurrent caller that DID
		// pass its own epoch check and is about to join. Enqueueing instead makes the
		// decision on the room's own single-writer loop, where an ordinary cmdLeave
		// already re-evaluates emptiness and arms the idle timer: a real join queued
		// ahead of this wins and the room stays, otherwise the room is released by
		// the path that already owns empty rooms. No new teardown route, and no
		// race to reason about.
		if room, ok := v.(*Room); ok && room != nil {
			room.enqueue(command{kind: cmdLeave})
		}
		return nil, errRoomUnavailable
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
		m.metrics.RoomClosed(string(id))
	}
	// The registry slot is NOT evicted here. It is evicted by the room's own
	// teardown, which covers this path and one this path cannot reach: a room torn
	// down during materialization — a shutdown race, or a delete bumping the epoch
	// while newRoom ran off the lock — never entered m.rooms, so `removed`
	// is false and an evict guarded by it would never run. That room did acquire a
	// registry handle, so the document would outlive every room that ever owned it
	// with nothing left to clean it up.
}

// Forward hands one inbound framed wire message to the session's room for
// serialized processing. Non-blocking from the caller's view beyond the room's
// command-channel buffer.
func (s *Session) Forward(frame []byte) {
	// OBSERVE THE REFUSAL, AND ACT ON ITS REASON. This used to discard enqueue's
	// bool entirely: the frame vanished, the client was told nothing, and the
	// session carried on editing a generation the server never received.
	//
	// The two refusal reasons need OPPOSITE handling, and collapsing them is what
	// caused the terminal-precedence bug this split exists to fix:
	//
	//	BACKPRESSURE   the room is ALIVE and could not take the frame in time.
	//	               Nobody else will mention it, so this poisons the session AND
	//	               tells the client with a member-scoped transient end, then
	//	               closes after drain so the reason is written before the close.
	//
	//	LIFECYCLE      the room is tearing down. Teardown will send this member its
	//	               own AUTHORITATIVE document-scoped end — document-deleted,
	//	               edits-not-saved, server-shutdown. This poisons the session and
	//	               SAYS NOTHING: a competing member-scoped transient end would
	//	               reach the connection's terminal boundary first and make the
	//	               real one be refused, so a deletion or a data-loss escalation
	//	               would reach the user as "try again later".
	//
	// The poison is set for BOTH, because the frame is lost either way and no
	// durability barrier may be answered over it. Only the announcement differs.
	outcome := s.room.enqueueWithReason(context.Background(), command{
		kind:           cmdMessage,
		src:            s.id,
		data:           frame,
		sessionDropped: s.dropped.Load(),
	})
	if outcome == enqueueAdmitted {
		return
	}

	// THE FRAME IS LOST — on BOTH reasons — so poison first and unconditionally.
	// Any durability request already in flight, or racing in behind this, must find
	// the session marked, so no barrier can be answered `persisted` for a document
	// state that is missing this update.
	//
	// Swap rather than Store: it also reports whether this session has ALREADY been
	// poisoned, which is what stops a second refused frame queueing a second
	// session end behind the first.
	alreadyPoisoned := s.dropped.Swap(true)

	// A LIFECYCLE REFUSAL ANNOUNCES NOTHING. The room is tearing down and will send
	// this member its own authoritative, document-scoped end — server-shutdown,
	// document-deleted, or edits-not-saved. A member-scoped TRANSIENT end from here
	// would reach the connection's terminal boundary FIRST and make the real one be
	// refused, so an owner deletion or a data-loss escalation would be reported to
	// the user as "try again later". Teardown is the sole owner of that ending.
	//
	// The poison above is still set, because the frame is lost either way and no
	// barrier may succeed over it.
	if outcome == enqueueRefusedInactive {
		return
	}
	if alreadyPoisoned {
		return
	}

	// BACKPRESSURE ONLY, from here down. Tell the client, then close after drain so
	// the reason is written before the socket goes. Both go DIRECTLY to this
	// session's connection rather than through the room: the room's command queue
	// is precisely what was unavailable, so routing the explanation through the
	// failure could lose the explanation too.
	if s.conn == nil {
		return
	}
	end := model.NewSessionEnd(model.CodeUpdateNotAccepted)
	if frame := encodeControl(end.Control()); frame != nil {
		_ = s.conn.Send(frame)
	}
	s.conn.CloseAfterDrain(end)
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
	// the durable backends (rabbitmq/redis) — so returning early would let
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
