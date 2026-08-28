package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/antst/go-yjs/backend/persistence"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/memory"
	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// connID is a room-local identifier for a connected client, assigned on join.
// It distinguishes the originator of an update so the room never echoes an
// update back to its sender (the sender already has it).
type connID uint64

// Conn is the room's view of a single connected client. The WS inbound adapter
// implements Send (drains the frame onto its socket) and the room calls it to
// fan messages out. Keeping the room dependent on this narrow interface — not on
// the WebSocket type — keeps the hexagon's transport adapter swappable and the
// room unit-testable without a socket.
type Conn interface {
	// Send delivers a single framed wire message to this client. It MUST NOT
	// block the room: implementations buffer and drop/close on overflow. The
	// returned error signals the room to drop the connection.
	Send(frame []byte) error
	// CloseAfterDrain ends the connection once every frame ALREADY queued for it
	// has been written, mapping the session end to whatever the transport uses to
	// say so. The ordering is the point: the room queues the session-end control
	// and then this, so the client cannot see the close before the reason for it.
	//
	// It MUST NOT block the room loop — the room calls it while holding the
	// single-writer goroutine, and one unreachable client must not stall every
	// other member's teardown. An implementation that cannot queue the intent
	// closes immediately instead; a client that is not draining has already lost
	// the frames a graceful close would have preserved.
	//
	// Once it is admitted, Send MUST fail. Not "should generally": the terminal
	// check and the enqueue have to be ONE step in the implementation, or a close
	// admitted between a sender's check and its enqueue puts that frame behind the
	// close — reported to the sender as delivered, and never written.
	CloseAfterDrain(end model.SessionEnd)
}

// updateOrigin tags a Y.Doc transaction with the connection that caused it so
// the room can skip echoing the resulting update back to its originator. A
// non-connection origin (snapshot load, server-side edit) has src == 0, which
// matches no real connection and therefore fans out to everyone. peer marks an
// update applied from another pod (via the ClusterBroadcaster): it is fanned to
// local members but MUST NOT be re-published to the bus, or pods would ping-pong
// the same update forever.
type updateOrigin struct {
	src  connID
	peer bool
}

// command is a unit of work serialized onto the room's single run-loop
// goroutine. Routing every mutation through one goroutine makes the room the
// single writer to its authoritative Y.Doc (data-model.md "Room"), so no lock
// is needed around the CRDT core and -race stays clean.
type command struct {
	kind     cmdKind
	src      connID
	conn     Conn
	identity model.Identity
	// mode is the session capability the Manager evaluated BEFORE this room was
	// materialized. The room does not re-derive it: authorization is established
	// once per connection and holds for the life of the socket.
	mode model.CollaboratorMode
	// isMultiUser is the latest optional license decision from this join's
	// existence read. Nil means an older producer omitted it, so the room keeps
	// its last explicit decision.
	isMultiUser *bool
	data        []byte
	done        chan joinResult
	// done2 acknowledges a cmdCloseDeleted that ran on the room loop (T015).
	done2 chan error
	// contribution completion is produced by the one bounded off-loop periodic
	// emit and reconciled on the room loop.
	contribution *contributionFlight
	// sessionDropped is true when the sending session has ALREADY had an inbound
	// frame refused by enqueue. It rides on the command rather than being read
	// from the Session, so the run loop never touches state owned by a reader
	// goroutine.
	sessionDropped bool
}

type cmdKind uint8

const (
	cmdJoin cmdKind = iota
	cmdLeave
	cmdMessage
	cmdClose
	// cmdCloseDeleted reacts to the owner deleting the document, on the run loop:
	// disconnect clients (session-end/document-deleted) and release the room. It
	// deletes nothing and does not flush — `server` has begun the owner deletion
	// and confirmed the event before mutating durable state (T015).
	cmdCloseDeleted
	cmdContributionDone
)

type contributionFlight struct {
	actors map[uuid.UUID]struct{}
	done   chan error
}

// peerUpdate is a fan-out payload received from another pod. It is delivered to the
// run loop via the bounded peerUpdates queue (NOT enqueue), so the subscribe
// goroutine never calls back into the loop (002 FR-009 — decoupled fan-out).
type peerUpdate struct {
	data      []byte
	ephemeral bool
}

// joinResult is returned to a joining connection: its room-local id plus the
// initial frames (SyncStep1 + the current awareness state) it must send to
// start the handshake, or an error when the join is refused (connection cap) or
// fails closed (authZ error).
type joinResult struct {
	id     connID
	frames [][]byte
	err    error
}

// roomMember is the room-side bookkeeping for one connection: its outbound port,
// the authenticated actor behind it, its current collaborator mode (viewer vs.
// collaborator, subject to inactivity downgrade), the per-connection update-rate
// token bucket, and the y-awareness client id (learned from the member's first
// awareness frame) used for server-forced eviction on leave (T013/T014).
type roomMember struct {
	id           connID
	conn         Conn
	actorID      *uuid.UUID
	mode         model.CollaboratorMode
	lastActivity time.Time
	bucket       *tokenBucket
	// awarenessID is the member's y-protocols awareness client id, learned when
	// it first sends an awareness frame; 0 means not yet seen.
	awarenessID  ycrdt.Number
	hasAwareness bool
	// durabilityPoisoned is set when ANY of this member's mutating updates was
	// refused while the connection stayed alive. It is STICKY for the life of the
	// member: nothing in the room clears it, and a reconnect gets a fresh member
	// with the zero value.
	//
	// It has to be sticky because of the ordinary ordering. A barrier arrives
	// AFTER the update it covers, so at rejection time there is usually no
	// outstanding request to fail — failing one only covers the inverse order.
	// Without a flag that survives, the request that follows a rejected update
	// finds a clean room and is answered `persisted`, which claims durability for
	// a mutation the service explicitly refused. That is the exact false positive
	// the barrier exists to make impossible.
	durabilityPoisoned bool
	// barrier is the ONE outstanding durability request this member may have.
	// Empty means none. One per connection, not per update: the caller is
	// sequential, and an unbounded waiter map is a denial-of-service surface for
	// a benefit nobody asked for.
	barrier string
}

// Room is a live, in-memory document session (data-model.md "Room"): it owns the
// authoritative plaintext Y.Doc (FR-021), the set of connected clients, and the
// awareness state, and it serializes every mutation through a single run-loop
// goroutine so the Y.Doc has exactly one writer. A room is lazily materialized
// on first connect (loading the latest snapshot), fans each client's updates out
// to the others, throttles snapshot persistence, and is released — persisting a
// final snapshot — when the last client leaves or after an idle timeout.
type Room struct {
	id      model.DocumentID
	content model.ContentType
	doc     *ycrdt.Doc
	// shadow is a candidate document kept in lockstep with doc, used to validate an
	// update BEFORE the live document is mutated. Nil for content types with no
	// assets-root contract (memos), where there is nothing to validate and the
	// second apply would be pure cost.
	//
	// It is fed by exactly the origins that mutate the live doc through
	// applyUpdate — inbound client writes and cross-pod peer updates. The cold
	// restore path validates its own candidate inside the registry's OpenFunc,
	// before any room can serve it, so it needs no shadow.
	shadow    *ycrdt.Doc
	awareness *ycrdt.Awareness
	// handle is this room's acquisition of the document from the registry. The
	// registry owns document identity and lifetime; the room holds the document
	// only for as long as this handle is live, and must stop serving when it is
	// invalidated (contracts/registry-session.md).
	handle  memory.Handle
	deps    Deps
	cfg     RoomConfig
	metrics Metrics
	logger  *zap.Logger

	commands chan command
	// peerUpdates is the bounded queue of cross-pod fan-out payloads (002 FR-009):
	// the subscribe goroutine writes here, the run loop drains it. DECOUPLED from
	// commands/enqueue so the subscribe goroutine never calls back into the loop —
	// making the run-loop↔subscribe↔teardown circular wait impossible.
	peerUpdates chan peerUpdate
	// done is closed by the run loop on teardown so producers (Forward/Leave)
	// never block on commands after the room is gone.
	done    chan struct{}
	members map[connID]roomMember
	nextID  connID

	// dirty is set when the doc changed since the last persisted snapshot;
	// it drives the save timer and the final save-on-release.
	dirty bool
	// docBytes is the live doc's last known encoded-v2 size, used by
	// applyWouldExceedMaxDocBytes as a cheap sound bound so the O(docsize) budget
	// re-encode is skipped while the doc has headroom under MaxDocBytes. It is set
	// from the authoritative encode whenever the exact budget check or a persist
	// runs, and conservatively over-counted by len(update) on each accepted apply
	// so it never under-estimates the true size between exact checks. Zero means
	// "not yet established" — the cheap skip then defers to the exact check.
	docBytes int

	// flushFailures counts CONSECUTIVE failed flushes; undurableSince marks when
	// the run of failures began. Both reset on a successful flush. They drive the
	// durability state machine in flush.go.
	flushFailures  int
	undurableSince time.Time
	version        int
	// policyID is the document's Alkemio authorization policy id (OPEN-1),
	// loaded from metadata and re-persisted on save so the authzeval adapter can
	// evaluate against it (T006).
	policyID string
	// ownerRef is the parent Alkemio entity that owns the document's lifecycle
	// (FR-023), loaded from metadata (set at pre-register) and re-persisted on every
	// snapshot save so a per-snapshot persist carries it forward rather than dropping
	// it. Without this round-trip the first snapshot save rebuilds Metadata with a
	// blank OwnerRef, and a wholesale-replace metadata store (in-memory) wipes the
	// pre-registered owner_ref the delete cascade keys off.
	ownerRef string
	// bucketID is the document's own storage bucket. It does NOT travel on
	// SaveCheckpointRequest — that carries no bucket. The room loads it from the
	// document's metadata on materialization and carries it forward on every
	// metadata persist, and the file-service store's pointer resolver reads it
	// back off that row when a first save needs somewhere to create the blob.
	//
	// The point of the round-trip is that each snapshot lands in the document's
	// own bucket rather than a single flat platform bucket. There is NO configured
	// fallback: when it is empty the file-service store refuses the first save
	// rather than writing the blob somewhere the delete cascade will never reach.
	bucketID string
	// isMultiUser is a read-only, transient admission cache refreshed from the
	// server-owned decision on every join. Nil preserves the last known decision
	// during a rolling deploy. The room never persists this field.
	isMultiUser *bool
	// maxConns is the room's effective connection cap. Today it is the configured
	// fallback (RoomConfig.Limits.MaxConnsPerRoom); per-document refinement from the
	// document's maxCollaborators (carried on the bus metadata contract) is not yet
	// wired into the join path (T014 follow-up). Zero disables the cap.
	maxConns int

	// contributors is the set of actor ids that mutated the document in the
	// current contribution window; flushed and reset on the window tick (T013).
	contributors map[uuid.UUID]struct{}
	// contributionFlight is the one periodic batch currently being emitted off
	// the room loop. While it is non-nil, new actors stay in contributors for the
	// next window.
	contributionFlight *contributionFlight

	// onReleased is invoked once, on the run loop, after the room has drained
	// and persisted, so the Manager can drop it from its registry.
	onReleased func()

	// source is this room's fan-out identity. The hub suppresses echoes by it, so
	// a room never re-applies an update it published itself. Per-room rather than
	// per-pod: two rooms in one process are distinct publishers, and a shared id
	// would make each blind to the other's edits on the single-pod path.
	source backend.SourceID
	// cancelSub tears down the hub subscription on release. It is
	// a no-op for the in-memory (single-pod) broadcaster.
	cancelSub func()

	// ctx is the room-lifetime context every backend call on the run loop derives
	// from (authZ eval, persist, teardown, peer publish). It is cancelled exactly once,
	// on release (finish), so a hung backend call unblocks when the room tears down
	// and a shutdown does not leave the single-writer loop wedged behind I/O. Each
	// individual call is additionally bounded by cfg.BackendTimeout via opCtx, so a
	// slow/hung backend cannot stall the loop (and thus every other member's
	// joins/messages/disconnects) indefinitely.
	ctx    context.Context
	cancel context.CancelFunc

	// lc is the explicit lifecycle state (002 redesign): Materializing during
	// newRoom, Active while serving, Draining through teardown, Closed once released.
	// It replaces the old `released` bool — beginTeardown is the idempotent teardown
	// guard, and enqueue gates on Active so a tearing-down room refuses new work
	// before done is even closed.
	lc lifecycle
}

// backendTimeout is the wall-clock bound for one backend call, falling back to
// defaultBackendTimeout when unset.
func (r *Room) backendTimeout() time.Duration {
	if r.cfg.BackendTimeout > 0 {
		return r.cfg.BackendTimeout
	}
	return defaultBackendTimeout
}

// opCtx returns a timeout-bounded context for a single backend call made on the
// run loop (authZ eval, persist, teardown, publish), derived from the room-lifetime
// context. The returned cancel MUST be called (defer) to release the timer. The
// timeout bounds a slow/hung backend so it cannot stall the single-writer loop;
// the parent ctx cancellation unblocks the call immediately on room release.
func (r *Room) opCtx() (context.Context, context.CancelFunc) {
	timeout := r.backendTimeout()
	// A room built by newRoom always has r.ctx; tolerate a nil parent (a bare Room
	// constructed directly in a unit test) by rooting at Background so opCtx never
	// panics.
	parent := r.ctx
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

// persistNow persists a snapshot under a bounded, room-scoped context. The run
// loop calls this (never persist directly) so a slow/hung blob/metadata backend
// cannot wedge the single-writer loop.
func (r *Room) persistNow() {
	ctx, cancel := r.opCtx()
	defer cancel()
	r.persist(ctx)
}

// flushContributionNow flushes the contribution window under a bounded,
// room-scoped context (the bus Contributor call must not stall the loop).
func (r *Room) flushContributionNow() {
	ctx, cancel := r.opCtx()
	defer cancel()
	r.flushContribution(ctx)
}

// enqueueDeadline backstops a producer blocked on a full command channel (002
// FR-008): the loop stays drained because every handler is bounded, so this rarely
// fires, but a producer must never wait forever on a wedged loop.
// A var, not a const, ONLY so a test can shorten it: the backpressure path is
// otherwise unreachable inside a test's patience, and leaving it untested is what
// let a lifecycle refusal and a genuine backpressure refusal share one code path.
// Production never assigns it.
var enqueueDeadline = 30 * time.Second

// enqueue submits a command to the run loop, returning false if the room has torn
// down or the deadline backstop elapses (so producers never block forever on a full
// channel).
func (r *Room) enqueue(cmd command) bool {
	return r.enqueueCtx(context.Background(), cmd)
}

// enqueueOutcome distinguishes WHY a command was refused, because the two reasons
// mean opposite things to the sender.
//
// A LIFECYCLE refusal means the room is tearing down, and teardown will send this
// member its own authoritative, document-scoped session end (server-shutdown,
// document-deleted, edits-not-saved). A BACKPRESSURE refusal means the room is
// alive and simply could not take the frame in time — nobody else is going to
// mention it.
//
// Collapsing them into one bool is what let a member-scoped TRANSIENT
// "update-not-accepted" be emitted during a real teardown and win the race against
// the document-scoped TERMINAL end that followed, so a deletion or a data-loss
// escalation could reach the user as "try again".
type enqueueOutcome int

const (
	enqueueAdmitted enqueueOutcome = iota
	// enqueueRefusedInactive: the room left Active. Teardown owns the ending; the
	// caller must NOT announce one of its own.
	enqueueRefusedInactive
	// enqueueRefusedBackpressure: the room is running but the command buffer stayed
	// full past the caller's context or the deadline backstop. Nothing else will
	// tell the client, so the caller must.
	enqueueRefusedBackpressure
)

// enqueueWithReason submits a command and reports the outcome, for the one caller
// that has to act differently on each.
func (r *Room) enqueueWithReason(ctx context.Context, cmd command) enqueueOutcome {
	if !r.lc.is(stateActive) {
		return enqueueRefusedInactive
	}
	select {
	case r.commands <- cmd:
		return enqueueAdmitted
	case <-r.done:
		return enqueueRefusedInactive
	default:
	}
	t := time.NewTimer(enqueueDeadline)
	defer t.Stop()
	select {
	case r.commands <- cmd:
		return enqueueAdmitted
	case <-r.done:
		return enqueueRefusedInactive
	case <-ctx.Done():
		return r.classifyRefusal()
	case <-t.C:
		return r.classifyRefusal()
	}
}

// classifyRefusal decides what a timed-out enqueue MEANS, at the moment it is
// reported rather than at the moment it started.
//
// The pre-block check is not sufficient. A producer can enter the block while the
// room is Active, the room can begin tearing down during the block, and the
// deadline can then fire BEFORE done closes — a window bounded by however long
// teardown's final flush takes, which is exactly the seconds-long window the
// terminal-precedence bug lived in. Reporting that as backpressure would let
// Forward emit its member-scoped transient end during a teardown, which is the
// bug this whole distinction exists to remove.
//
// Re-reading the lifecycle here costs one atomic load on a path that has already
// waited out a deadline.
func (r *Room) classifyRefusal() enqueueOutcome {
	if !r.lc.is(stateActive) {
		return enqueueRefusedInactive
	}
	return enqueueRefusedBackpressure
}

// enqueueCtx submits a command to the run loop, returning false if the room has torn
// down (state left Active) OR the producer's context/deadline elapses before a
// buffer slot frees. The state check refuses new work BEFORE done is closed, so a
// tearing-down room rejects producers early and Join/CloseDeleted retry into a fresh room.
func (r *Room) enqueueCtx(ctx context.Context, cmd command) bool {
	return r.enqueueWithReason(ctx, cmd) == enqueueAdmitted
}

// RoomConfig carries the per-room tunables (R7 save cadence, idle release).
// Values come from configuration; the defaults suit tests and local development.
type RoomConfig struct {
	// SaveDebounce is the time from the first dirty mutation until a snapshot
	// is persisted (R7; 2000ms default, configurable). The timer
	// is armed once per clean→dirty cycle (on the first edit after a save) and
	// fires once, bounding the staleness window regardless of edit frequency.
	SaveDebounce time.Duration
	// IdleTimeout releases an empty room after this long with no members. Zero
	// releases immediately when the last member leaves.
	IdleTimeout time.Duration
	// SendBuffer is the per-connection outbound queue depth the adapter uses.
	SendBuffer int

	// Limits carries the configurable enforcement bounds (FR-024, epic R9).
	Limits Limits
	// CollaboratorInactivity downgrades an idle collaborator to viewer after this
	// long with no document mutation. Zero disables the downgrade and is the
	// default because volatile cursor activity is not counted here.
	CollaboratorInactivity time.Duration
	// ContributionWindow is the flush cadence for the north-star contribution
	// metric/event: the set of actors that contributed in the window is emitted
	// then reset (FR-014). Zero disables contribution flushing.
	ContributionWindow time.Duration
	// BackendTimeout bounds each backend call made on the room's single-writer run
	// loop (authZ evaluation, snapshot persist, owner-delete close, cross-pod
	// publish). A slow or hung backend would otherwise stall the loop and block
	// every other member's joins/messages/disconnects. Zero falls back to
	// defaultBackendTimeout; the call is still cancelled when the room releases.
	BackendTimeout time.Duration
}

// Limits are the configurable per-room enforcement bounds (FR-024, epic R9
// defaults). A breach disconnects only the offending connection with a control
// message; other collaborators are unaffected (constitution §V).
type Limits struct {
	// FlushFailureThreshold is how many CONSECUTIVE failed flushes are tolerated
	// before the room is torn down and its unsaved edits discarded. It is not 1 by
	// design: one transient backend blip must not cost a healthy session. Zero uses
	// the documented default.
	FlushFailureThreshold int
	// MaxDocBytes rejects an update that would grow the encoded snapshot past
	// this size (epic R9 default ~32 MiB). Zero disables the size check.
	MaxDocBytes int
	// MaxConnsPerRoom caps concurrent connections to a room (epic R9 default 50).
	// This is the global fallback; per-document refinement from a document's
	// maxCollaborators is a future enhancement (not yet wired into the cap). Zero
	// disables the connection cap.
	MaxConnsPerRoom int
	// UpdateRatePerSec is the per-connection token-bucket refill rate in messages
	// per second (epic R9 default ~50/s). Zero disables rate limiting.
	UpdateRatePerSec int
	// UpdateBurst is the token-bucket depth (max burst above the steady rate).
	// Zero defaults to UpdateRatePerSec.
	UpdateBurst int
}

// Default limit/presence values (epic R9; OPEN-4 resolved).
const (
	// MaxDocBytes is capped BELOW file-service's 32 MiB request-body limit on
	// PUT /internal/file/{id}/content, not at it. A document sitting exactly on a
	// 32 MiB budget would encode to slightly more than 32 MiB once v2 framing is
	// added, so the snapshot would be refused by the transport after passing our
	// own budget check — the document would be accepted and then permanently
	// unsaveable. 30 MiB leaves headroom for framing.
	// defaultFlushFailureThreshold is how many CONSECUTIVE failed flushes are
	// tolerated before a room is torn down and its unsaved edits discarded. It is
	// deliberately not 1: a single transient backend blip must not cost a healthy
	// session, and the retry usually succeeds.
	defaultFlushFailureThreshold = 5

	// flushRetryMaxShift caps the exponent on the retry backoff, and
	// maxFlushRetryBackoff caps the result. Together they keep a long outage from
	// either hammering the backend or stretching the retry interval so far that
	// escalation is unreachable in practice.
	flushRetryMaxShift   = 5
	maxFlushRetryBackoff = 30 * time.Second

	defaultMaxDocBytes             = 30 << 20 // 30 MiB
	defaultMaxConnsPerRoom         = 50
	defaultUpdateRatePerSec        = 0
	defaultCollaboratorInactivity  = 0
	defaultContributionWindowEvery = 10 * time.Minute
	// defaultBackendTimeout bounds each backend call on the run loop (authZ,
	// persist, teardown, publish) so a hung backend cannot wedge the single-writer
	// loop. Generous enough for a slow-but-alive backend; far below any human-
	// noticeable room stall.
	defaultBackendTimeout = 30 * time.Second
	// budgetSkipSlack is the fixed headroom (on top of the 2x update-length margin)
	// the cheap MaxDocBytes short-circuit requires before skipping the exact
	// O(docsize) re-encode. It absorbs the small per-snapshot v2 framing/varint
	// overhead so the skip stays sound even at tiny limits; it is negligible against
	// the ~32 MiB production cap, so the skip still fires on essentially every edit
	// until the doc nears half the cap.
	budgetSkipSlack = 1024
)

// DefaultLimits are the epic R9 defaults (all config-tunable, OPEN-4).
func DefaultLimits() Limits {
	return Limits{
		MaxDocBytes:           defaultMaxDocBytes,
		FlushFailureThreshold: defaultFlushFailureThreshold,
		MaxConnsPerRoom:       defaultMaxConnsPerRoom,
		UpdateRatePerSec:      defaultUpdateRatePerSec,
		UpdateBurst:           defaultUpdateRatePerSec,
	}
}

// DefaultRoomConfig is the Wave-1 default cadence, with the Wave-3
// limit/presence defaults (epic R9, OPEN-4) layered on.
func DefaultRoomConfig() RoomConfig {
	return RoomConfig{
		SaveDebounce:           2 * time.Second,
		IdleTimeout:            30 * time.Second,
		SendBuffer:             64,
		Limits:                 DefaultLimits(),
		CollaboratorInactivity: defaultCollaboratorInactivity,
		ContributionWindow:     defaultContributionWindowEvery,
		BackendTimeout:         defaultBackendTimeout,
	}
}

// withRoomDefaults fills in the dependencies a directly-constructed Room would
// otherwise be missing.
//
// A Room built without the Manager must still work: the Manager supplies these,
// but tests and any future direct caller do not, and both are load-bearing on the
// very first edit — the room publishes to the hub and holds a registry handle. A
// nil either way is a panic, not a degraded mode.
func withRoomDefaults(deps Deps) Deps {
	if deps.Hub == nil {
		// The core's shipped single-process hub, which is exactly single-pod
		// behaviour: no peer exists, so nothing crosses.
		deps.Hub = hub.NewInProcess()
	}
	if deps.Registry == nil {
		// A lone room owns exactly one document, so a private registry is
		// semantically correct; the Manager supplies a shared one so concurrent
		// opens of the same document coalesce onto a single materialization.
		deps.Registry = memory.NewRegistry()
	}
	if deps.Contributor == nil {
		deps.Contributor = noopContributor{}
	}
	return deps
}

// newRoom materializes a room for id: it constructs the authoritative GC'd
// Y.Doc, loads the latest snapshot (if any) into it, and wires the update
// observer that fans applied changes out to members. It does not start the run
// loop — Manager.start does, after registering the room — so a concurrent
// second joiner can never observe a half-built room.
func newRoom(ctx context.Context, id model.DocumentID, content model.ContentType, deps Deps, cfg RoomConfig, metrics Metrics, logger *zap.Logger) (*Room, error) {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if cfg.BackendTimeout <= 0 {
		cfg.BackendTimeout = defaultBackendTimeout
	}
	deps = withRoomDefaults(deps)
	registry := deps.Registry

	// The room struct exists BEFORE the document does, because the document is
	// initialized inside the registry's open function (below) and that function
	// needs somewhere to record what it learned.
	roomCtx, cancel := context.WithCancel(ctx)
	r := &Room{
		id:           id,
		content:      content,
		deps:         deps,
		cfg:          cfg,
		metrics:      metrics,
		logger:       logger,
		commands:     make(chan command, 256),
		peerUpdates:  make(chan peerUpdate, 256),
		done:         make(chan struct{}),
		members:      make(map[connID]roomMember),
		maxConns:     cfg.Limits.MaxConnsPerRoom,
		contributors: make(map[uuid.UUID]struct{}),
		ctx:          roomCtx,
		cancel:       cancel,
	}

	// Bound the materialization I/O (metadata load + state fetch) so a hung backend
	// cannot park the first-connect cohort indefinitely — the run loop's per-call
	// opCtx bound is otherwise applied only after the room starts (002 FR-006/FR-010).
	loadCtx, loadCancel := r.opCtx()
	defer loadCancel()

	// The metadata row is read OUTSIDE the open function, because it is per-ROOM
	// state (version, pointer, policy id, owner, bucket, content type) that every
	// room needs even when the document is already live and the open function will
	// not run. Reading it inside would leave a room that joined a cached document
	// with a zero version and no pointer, and its first flush would then write a
	// metadata row claiming the document had never been saved.
	_, found, err := r.loadMetadata(loadCtx)
	if err != nil {
		cancel()
		return nil, err
	}

	// The DOCUMENT's content is initialized inside the open function, which is what
	// makes first-open restore exactly-once BY CONSTRUCTION rather than by luck.
	// Acquire coalesces concurrent cache misses onto one open call and publishes
	// nothing until it returns, so no session can observe a document that has been
	// created but not yet restored — the half-built state FR-004a forbids. Loading
	// after Acquire would publish an empty document first and fill it in afterwards.
	opened := false
	// loadCtx, NOT ctx. It bounds how long THIS acquirer waits for materialization:
	// a hung checkpoint store must not park the first-connect cohort with nothing
	// able to free it. The room is not in the Manager's map yet, so shutdown cannot
	// find it to cancel, and Registry.Close reports ErrInUse for an initializing
	// entry. loadCtx derives from the room-lifetime context, which the Manager
	// already built with context.WithoutCancel — so this bounds materialization
	// without making it cancellable by the connecting request.
	//
	// It does NOT bound the open itself; see the open function below.
	handle, err := registry.Acquire(loadCtx, backend.DocumentID(id), func(openCtx context.Context) (*ycrdt.Doc, error) {
		opened = true
		doc := newRoomDoc(string(id))
		if found {
			// openCtx is the REGISTRY's context, not this acquirer's: the core
			// cancels it when the LAST waiter stops waiting, so it bounds an open
			// nobody wants any more — not an open somebody is still waiting for.
			// A document that keeps attracting joiners therefore renews that clock
			// indefinitely, and a wedged LoadCheckpoint would hold the generation
			// open with it. The fixed bound has to come from here, so apply the
			// same BackendTimeout every other backend call on this room gets.
			//
			// Derived FROM openCtx rather than a fresh root: the core preserves
			// request-scoped VALUES on it (only cancellation and the deadline are
			// dropped), and a checkpoint store may key off them.
			if err := r.restoreBounded(openCtx, doc); err != nil {
				return nil, err
			}
		}
		// The convention seeds the root shared type with the right shape, and belongs
		// inside the open function for the same reason the restore does: a document
		// published without its root would be observable in a shape no client expects.
		//
		// r.content, NOT the `content` handshake parameter: loadMetadata has already
		// corrected it to the PERSISTED content type (the persisted type wins per the
		// documented contract). Seeding off the stale handshake value would, for a
		// document pre-registered as whiteboard but opened by a client that omits
		// ?type= (which the WS adapter defaults to memo), materialize the MEMO root —
		// a spurious Y.XmlFragment "default" instead of the whiteboard roots. That is
		// a durable wrong-type root; persist() already keys off r.content, so only the
		// convention was ever inconsistent.
		applyConvention(doc, r.content)

		// Validate the restored candidate BEFORE the registry publishes it. This doc
		// is not yet reachable by any room, so failing here fails materialization
		// with no live room — which is the only way to refuse a poisoned checkpoint
		// at all. Nothing downstream re-checks stored state, so a document poisoned
		// before this existed would otherwise reload forever, which is exactly the
		// client discard-and-reseed loop.
		//
		// BOTH content types, each through its own validator.
		//
		// This was whiteboard-only, justified by "inspecting a root MATERIALIZES it,
		// so validating a memo here would add a files map to a document that should
		// not have one". That reasoning was true when it was written and is not true
		// now: validateMemoImages reads the memo's OWN XmlFragment root, which
		// applyConvention has just materialized on the line above, and touches
		// nothing else. It cannot add a files map because it never looks at one.
		//
		// Leaving memos out left them with no cold-load check at all, so a memo
		// carrying a legacy inline data: src — from the client generation that still
		// had the dataURL paste fallback, or migrated from the old service — loaded
		// happily, poisoned the shadow, and then had EVERY subsequent update rejected
		// against pre-existing poison. The client discards its generation, reloads
		// server state, and reloads the same poison: precisely the discard-and-reseed
		// loop this check exists to prevent, running forever.
		if verr := validateStoredContent(doc, r.content); verr != nil {
			return nil, fmt.Errorf("stored document %s violates the assets-root contract: %w", id, verr)
		}
		return doc, nil
	})
	if err != nil {
		cancel()
		if errors.Is(err, memory.ErrClosed) {
			// The registry closes only in Manager.Close, so this IS the shutdown, and
			// the caller must see it as one: a connection arriving during shutdown is
			// refused, not failed. Mapping it here rather than leaving it as an opaque
			// materialization error also makes the answer independent of WHERE in
			// materialization the shutdown lands — before this change, a Join wedged
			// on the metadata read reported "registry closed" while one wedged a step
			// later reported ErrShuttingDown, for the same event.
			return nil, ErrShuttingDown
		}
		return nil, fmt.Errorf("acquiring document: %w", err)
	}
	doc := handle.Doc()
	r.doc = doc
	r.handle = handle

	if serr := r.initShadow(doc); serr != nil {
		cancel()
		// releaseHandle, not handle.Release: Release alone leaves the document
		// resident in the registry, so a failure here would make it permanently
		// un-evictable — the same leak the Hub.Subscribe failure path below avoids.
		r.releaseHandle()
		return nil, serr
	}

	if !opened {
		r.measureLiveDoc(doc)
	}
	r.awareness = newServerAwareness(doc, logger)

	// Observe applied updates and fan them out. The observer runs synchronously
	// inside ApplyUpdate on the run-loop goroutine, so reading members here is
	// race-free (single writer).
	doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		r.onDocUpdate(v...)
	}))

	// Subscribe to peer-pod fan-out (R4) under the room-lifetime context so the
	// subscription tracks the room (not the bootstrap request). DECOUPLED fan-out
	// (002 FR-009): the handler writes to the bounded peerUpdates queue the run loop
	// drains — it NEVER calls enqueue, so the subscribe goroutine cannot park inside
	// the run loop and deadlock teardown. The write is cancellable on roomCtx, which
	// teardown cancels, so a parked write frees without waiting on the run loop. The
	// in-memory broadcaster's Subscribe is a no-op that never fires the handler, so
	// single-pod deployments pay nothing.
	// The source id is per-ROOM, so the hub's echo suppression drops this room's
	// own publishes without the room having to filter them itself.
	r.source = backend.SourceID(uuid.NewString())
	sub, err := deps.Hub.Subscribe(roomCtx, backend.DocumentID(id), r.source, func(_ context.Context, msg hub.Message) error {
		select {
		case r.peerUpdates <- peerUpdate{data: msg.Payload, ephemeral: msg.Kind == hub.AwarenessUpdate}:
			return nil
		case <-roomCtx.Done():
			// Teardown cancelled roomCtx: drop this peer delta rather than block the
			// subscribe goroutine. Acceptable — the ORIGINATING pod keeps its doc dirty
			// and persists it, and the CRDT re-merges on next load (self-healing); a
			// draining pod's final snapshot is not a relay guarantee (FR-015).
			return nil
		}
	})
	if err != nil {
		// Release what we already took. The handle was acquired several steps ago,
		// and a held handle makes Evict return ErrInUse FOREVER — so a room that
		// failed here would leave its document permanently un-evictable and
		// un-invalidatable, with every later open handed the same stale in-memory
		// copy. Nothing else can clean it up: the room never entered the Manager's
		// map, so no teardown will ever run for it.
		//
		// The shadow needs the same treatment for the same reason. It was built above
		// and is this room's private document, so abandoning it here — before the run
		// loop exists to own teardown — leaks one full copy of the board.
		r.destroyShadow()
		r.releaseHandle()
		cancel()
		return nil, err
	}
	r.cancelSub = func() { _ = sub.Close() }

	// Materialized and wired — the room is now ready to serve.
	r.lc.activate()
	return r, nil
}

// measureLiveDoc sets docBytes from a document this room did not restore.
//
// A cache hit means another room already opened the document, so there is no
// stored-update length to take it from. Leaving it at zero would under-report
// every cheap MaxDocBytes skip until the first flush re-synced it — the budget
// check would wave through updates it should have measured.
func (r *Room) measureLiveDoc(doc *ycrdt.Doc) {
	encoded, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		r.logger.Warn("measuring the live document failed; the size budget starts from zero",
			zap.String("doc", string(r.id)), zap.Error(err))
		return
	}
	r.docBytes = len(encoded)
}

// newServerAwareness builds the room's awareness and clears the server's own
// local state.
//
// ycrdt.NewAwareness seeds a doc-local empty client state for the server's client
// id; left in place, awarenessSnapshot would emit a synthetic presence entry for
// the SERVER on the first join. The server holds no presence, so it is cleared
// immediately — the zero Object is the cleared/null state (Object is a struct, so
// SetLocalState(ycrdt.Object{}) removes the entry rather than setting a nil) — and
// a fresh room then reports zero awareness states until a real client announces
// one.
func newServerAwareness(doc *ycrdt.Doc, logger *zap.Logger) *ycrdt.Awareness {
	awareness := ycrdt.NewAwareness(doc)
	if err := awareness.SetLocalState(ycrdt.Object{}); err != nil {
		// The core surfaces this now; a failure here means the server would carry a
		// phantom local awareness entry, so it is worth seeing rather than dropping.
		logger.Warn("clearing server local awareness state failed", zap.Error(err))
	}
	return awareness
}

// loadMetadata reads the document's index row and adopts it as this room's state.
//
// Split out of the restore because the two have different scopes. This is
// per-ROOM state — version, content pointer, policy id, owner, bucket, content
// type, blob kind — and every room needs it, including one that joins a document
// already live in the registry and therefore never runs the open function. The
// document's CONTENT, by contrast, is per-DOCUMENT and belongs inside that open
// function (restoreInto).
//
// A missing row is not an error: a fresh document has nothing to load or seed.
// The bool reports whether a row was found, so the caller can skip the restore
// rather than infer it from a zero-valued struct.
func (r *Room) loadMetadata(ctx context.Context) (model.Metadata, bool, error) {
	meta, err := r.deps.Metadata.Load(ctx, r.id)
	if err != nil {
		if isNotFound(err) {
			return model.Metadata{}, false, nil
		}
		return model.Metadata{}, false, err
	}

	r.version = meta.Version
	r.policyID = meta.AuthorizationPolicyID
	r.ownerRef = meta.OwnerRef
	r.bucketID = meta.StorageBucketID
	r.isMultiUser = meta.IsMultiUser
	if meta.ContentType != "" {
		r.content = meta.ContentType
	}
	return meta, true, nil
}

// restoreInto rehydrates a NEWLY OPENED document from durable state.
//
// It runs inside the registry's open function (FR-004a/b), which is what makes
// first-open restore exactly-once by construction: Acquire coalesces concurrent
// cache misses onto one call and publishes nothing until it returns, so no
// session can observe a document that exists but has not been restored. Doing
// this after Acquire would publish an empty document and fill it in afterwards,
// leaving a window in which a second opener sees an empty editor for a document
// that has content.
//
// It takes the doc explicitly rather than using r.doc, which is not assigned
// until Acquire returns — and that is the point: there is no way to write this
// against the room's own document, because the room does not have one yet.
//
// When no stored state exists, the document opens as a fresh EMPTY editable room
// (FR-010) — not an error. A document's only content lives in the checkpoint
// store, reached through its contentPointer, so a document with no pointer has no
// content by definition.
// restoreBounded runs the checkpoint restore under a wall-clock bound of its own.
// It is a named method, not an inline WithTimeout, so the bound can be exercised
// against a context that carries no deadline — which is precisely what the core
// hands the open function once the last-waiter semantics apply.
func (r *Room) restoreBounded(openCtx context.Context, doc *ycrdt.Doc) error {
	restoreCtx, cancel := context.WithTimeout(openCtx, r.backendTimeout())
	defer cancel()
	return r.restoreInto(restoreCtx, doc)
}

func (r *Room) restoreInto(ctx context.Context, doc *ycrdt.Doc) error {
	// ErrNotFound means the document has no stored state: it opens EMPTY and
	// editable, and the first save creates its blob. ErrCorrupt means the index
	// says state EXISTS but could not be read; that must fail materialization
	// rather than fall back to an empty document, because an empty document would
	// be persisted over the last good state on the next save (FR-014).
	cp, err := r.deps.Checkpoint.LoadCheckpoint(ctx, backend.DocumentID(r.id))
	switch {
	case err == nil:
	case errors.Is(err, persistence.ErrNotFound):
		return nil
	default:
		return fmt.Errorf("loading stored state for %s: %w", r.id, err)
	}

	// The stored state is a full v2 update; applying it with a non-connection,
	// peer-flagged origin means the update observer fans it to all members
	// (there are none yet at load time) without re-publishing it to the bus —
	// rehydration is local state, not a new edit to broadcast.
	if err := ycrdt.ApplyUpdateV2(doc, cp.Update, updateOrigin{src: 0, peer: true}); err != nil {
		return fmt.Errorf("applying stored state: %w", err)
	}
	r.docBytes = len(cp.Update)
	return nil
}

// run is the room's single goroutine. It owns the Y.Doc and the member registry,
// draining commands until closed and managed by the save/idle timers plus the
// Wave-3 presence tickers (inactivity sweep, contribution-window flush). All
// Y.Doc, awareness, and member mutation happens here, making the room the lone
// writer.
func (r *Room) run() {
	// A panic on the single-writer loop must not wedge the room: without recovery the
	// goroutine dies with the room still registered, r.done never closed, and
	// Manager.Close blocking to its deadline — one panicking handler would take the
	// whole pod's graceful shutdown down with it. Recover, log with the stack, and
	// tear the room down WITHOUT a flush: a doc left mid-mutation by a panic must not
	// be persisted over the last good snapshot.
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("room run loop panicked; tearing down without persist",
				zap.Any("panic", rec), zap.Stack("stack"))
			// Unsaved edits are being abandoned here exactly as in escalation, so
			// members are told the same thing: their work since the last flush is
			// gone (FR-011a groups the no-flush teardowns for this reason).
			r.teardown(model.NewSessionEnd(model.CodeEditsNotSaved), nil)
		}
	}()

	saveTimer := time.NewTimer(time.Hour)
	stopTimer(saveTimer)
	idleTimer := time.NewTimer(time.Hour)
	stopTimer(idleTimer)

	// Presence tickers (T013): the inactivity sweep downgrades idle collaborators;
	// the contribution ticker flushes the per-window contributing-actor set. Both
	// are disabled (a stopped far-future timer) when their interval is zero.
	sweepTimer, sweepEvery := newOptionalTicker(r.cfg.CollaboratorInactivity)
	contribTimer, contribEvery := newOptionalTicker(r.cfg.ContributionWindow)
	defer sweepTimer.Stop()
	defer contribTimer.Stop()

	armSave := func() { r.armSaveTimer(saveTimer) }
	armIdle := func() { r.armIdleTimer(idleTimer) }

	for {
		select {
		case cmd := <-r.commands:
			if !r.dispatch(cmd, armSave, armIdle, idleTimer) {
				return
			}

		case pu := <-r.peerUpdates:
			// Cross-pod fan-out, applied on the single-writer loop (002 FR-009 —
			// decoupled fan-out: the subscribe goroutine writes here, never enqueue).
			if r.handlePeer(pu.data, pu.ephemeral) {
				armSave()
			}

		case <-saveTimer.C:
			r.persistNow()
			// persistNow can tear this room down: a flush failing past the threshold
			// escalates, and escalation IS a teardown. Leave the loop when that has
			// happened — every other branch that tears down returns, and this one
			// falling through instead would re-arm the retry timer on a dead room and
			// spin: fail, escalate again, log again, count another data-loss event,
			// forever, with the goroutine never released.
			if !r.lc.is(stateActive) {
				return
			}
			// A failed flush leaves the document dirty, and the save timer is armed
			// only on the CLEAN→DIRTY transition — so without this the room would
			// never try again: further edits find it already dirty, the threshold is
			// never reached, and the durability state machine stalls in `undurable`
			// with escalation unreachable. Re-arm on backoff so the retry the state
			// machine promises actually happens.
			r.armRetryTimer(saveTimer)

		case <-idleTimer.C:
			if r.releaseIfEmpty(saveTimer, idleTimer) {
				return
			}

		case <-sweepTimer.C:
			r.sweepInactive()
			sweepTimer.Reset(sweepEvery)

		case <-contribTimer.C:
			r.startContributionFlush()
			contribTimer.Reset(contribEvery)
		}
	}
}

// newOptionalTicker returns a timer that fires every `every` (and the interval),
// or a stopped far-future timer when `every` is zero (the feature is disabled).
// The caller Resets it after each fire.
func newOptionalTicker(every time.Duration) (*time.Timer, time.Duration) {
	if every <= 0 {
		t := time.NewTimer(time.Hour)
		stopTimer(t)
		return t, time.Hour
	}
	return time.NewTimer(every), every
}

// handleMessageCmd applies an inbound client frame and arms the save timer on a
// clean-to-dirty transition, and the idle timer if a rate/size-limit self-
// disconnect inside handleMessage dropped the last member (002 FR-011 — so an
// emptied room is released, not leaked). Extracted from dispatch to keep its
// branching low.
func (r *Room) handleMessageCmd(cmd command, armSave, armIdle func()) {
	if r.handleMessage(cmd.src, cmd.data, cmd.sessionDropped) {
		armSave()
	}
	if len(r.members) == 0 {
		armIdle()
	}
}

// dispatch handles one command on the run-loop goroutine, arming the save/idle
// timers as needed. It returns false when the room must tear down (cmdClose), so
// run can exit. Splitting this out of run keeps each function's branching low.
func (r *Room) dispatch(cmd command, armSave, armIdle func(), idleTimer *time.Timer) (keepRunning bool) {
	switch cmd.kind {
	case cmdJoin:
		stopTimer(idleTimer)
		res := r.handleJoin(cmd.conn, cmd.identity, cmd.mode, cmd.isMultiUser)
		// Guard the result send like cmd.done2: the only cmdJoin producer always
		// supplies a buffered done, but a nil channel here would panic the loop.
		if cmd.done != nil {
			cmd.done <- res
		}
		// Re-arm the idle timer whenever the room is empty after handling the join,
		// so a freshly materialized room never leaks its goroutine. This covers a
		// refused join (room full / access denied / fail-closed admits no member)
		// AND a join that returned success but whose presence broadcast immediately
		// dropped the just-added member (a Send failure re-enters dropMember, leaving
		// the room empty with res.err == nil — the gap that the old res.err gate missed).
		if len(r.members) == 0 {
			armIdle()
		}

	case cmdLeave:
		r.handleLeave(cmd.src)
		if len(r.members) == 0 {
			armIdle()
		}

	case cmdMessage:
		r.handleMessageCmd(cmd, armSave, armIdle)

	case cmdCloseDeleted:
		// No flush hook. The owner deletion has started; persisting while its profile,
		// bucket and row are being removed races the owner and can only create stale
		// content or a failed save.
		r.teardown(model.NewSessionEnd(model.CodeDocumentDeleted), nil)
		if cmd.done2 != nil {
			cmd.done2 <- nil
		}
		return false

	case cmdClose:
		r.teardown(model.NewSessionEnd(model.CodeServerShutdown), func() {
			r.persistNow()
		})
		return false
	case cmdContributionDone:
		r.finishContributionFlush(cmd.contribution)
	}
	return true
}

// handleJoin registers a connection after enforcing the connection cap (FR-024).
// It returns the joiner's id and the initial frames it must send (SyncStep1 + the
// current awareness snapshot so the newcomer sees existing presence) and a
// `read-only-state` control for a viewer; or ErrRoomFull.
//
// Authorization is NOT decided here. The Manager evaluated it before this room
// was materialized (authorizeSession) and passed the result in: a connection that
// lacks read access never reaches a room at all. Re-deriving it here would mean
// the expensive materialization had already happened for a caller who may be
// refused, which is exactly what the pre-acquire check removed.
func (r *Room) handleJoin(
	c Conn,
	identity model.Identity,
	mode model.CollaboratorMode,
	isMultiUser *bool,
) joinResult {
	if r.maxConns > 0 && len(r.members) >= r.maxConns {
		return joinResult{err: ErrRoomFull}
	}
	if isMultiUser != nil {
		decision := *isMultiUser
		r.isMultiUser = &decision
	}
	readOnlyReason := readOnlyReasonForIdentity(identity)
	// Room membership is authoritative in the supported durable topology because
	// startup rejects Redis fan-out with file-service. Enabling durable multi-pod
	// operation must move this admission decision into its ownership mechanism.
	if mode == model.ModeCollaborator && r.isMultiUser != nil && !*r.isMultiUser {
		for _, member := range r.members {
			if member.mode == model.ModeCollaborator {
				mode = model.ModeViewer
				readOnlyReason = model.ReasonMultiUserNotAllowed
				break
			}
		}
	}

	r.nextID++
	id := r.nextID
	r.members[id] = roomMember{
		id:           id,
		conn:         c,
		actorID:      identity.ActorID,
		mode:         mode,
		lastActivity: time.Now(),
		bucket:       newTokenBucket(r.cfg.Limits.UpdateRatePerSec, r.cfg.Limits.UpdateBurst, time.Now),
	}
	r.metrics.ConnOpened()

	frames := [][]byte{protocol.EncodeSyncStep1(r.doc)}
	if aw := awarenessSnapshot(r.awareness); aw != nil {
		frames = append(frames, aw)
	}
	// Tell a viewer it is read-only up front so the client disables local editing
	// (the collaborator default needs no control — editing is the baseline). The
	// reason mirrors today's read-only UX (OPEN-1): an authenticated actor denied
	// update-content is no-update-access; an unauthenticated one is not-authenticated.
	if mode == model.ModeViewer {
		ctrl := encodeControl(model.ControlMessage{
			Kind:     model.ControlReadOnlyState,
			ReadOnly: model.ReadOnlyState(true),
			Reason:   readOnlyReason,
		})
		if ctrl != nil {
			frames = append(frames, ctrl)
		}
		if readOnlyReason == model.ReasonMultiUserNotAllowed {
			ctrl = encodeControl(model.ControlMessage{
				Kind:   model.ControlCollaboratorMode,
				Mode:   model.ModeViewer,
				Reason: model.ReasonMultiUserNotAllowed,
			})
			if ctrl != nil {
				frames = append(frames, ctrl)
			}
		}
	}

	r.broadcastControl(model.ControlMessage{
		Kind:  model.ControlRoomUserChange,
		Users: len(r.members),
	})

	return joinResult{id: id, frames: frames}
}

// readOnlyReasonForIdentity maps a viewer's identity to its read-only reason
// code (OPEN-1): an anonymous connection is read-only because it is
// not-authenticated; an authenticated actor that was denied update-content is
// read-only because it has no-update-access. This preserves the granularity of
// today's read-only UX (the memo-footer readOnlyCode). An anonymous viewer
// surfaces two ways — a nil ActorID (open mode, AuthZ bypassed) OR a pointer to
// uuid.Nil, which is what the GATEWAY stamps for an un-credentialed caller
// (server: ANONYMOUS_ACTOR_ID) — so both map to not-authenticated, else an
// anonymous viewer wrongly reports no-update-access.
func readOnlyReasonForIdentity(identity model.Identity) model.ReadOnlyReason {
	if identity.ActorID == nil || *identity.ActorID == uuid.Nil {
		return model.ReasonNotAuthenticated
	}
	return model.ReasonNoUpdateAccess
}

// handleLeave drops a connection and tells the remaining members the count
// changed. dropMember forces a server-side awareness eviction for the departed
// client (T013) so peers stop rendering its cursor immediately rather than
// waiting out the y-awareness TTL.
func (r *Room) handleLeave(id connID) {
	if !r.dropMember(id) {
		return
	}
	r.broadcastControl(model.ControlMessage{
		Kind:  model.ControlRoomUserChange,
		Users: len(r.members),
	})
}

// dropMember removes a member from the registry, decrements the connection
// gauge, and forces a server-side awareness eviction for the departed client so
// peers stop rendering its cursor immediately rather than waiting out the 30s
// y-awareness TTL (T013, closing the Wave-1 D6 deferral). It returns false when
// the member was already gone (idempotent).
//
// The eviction maps the room-local connID to the member's y-awareness client id
// (learned from its first awareness frame) and broadcasts a null-state awareness
// update with a bumped clock — the y-protocols "client offline" convention.
func (r *Room) evictAwareness(m roomMember) {
	if !m.hasAwareness {
		return
	}
	frame := r.forcedAwarenessRemoval(m.awarenessID)
	if frame == nil {
		return
	}
	r.broadcast(frame, m.id)
	r.publishToPeers(frame, true)
}

// forcedAwarenessRemoval builds a framed awareness update that marks clientID
// offline: it deletes the client's state from the room awareness and bumps its
// clock, then encodes a null-state update the y-protocols clients apply as a
// removal. Returns nil when the client is unknown to the room awareness.
//
// The client's States entry is deleted but its Meta entry (the monotonic clock) is
// deliberately RETAINED with a bumped clock — that is the y-protocols tombstone
// that makes a late or re-ordered state update for the same client id be rejected
// rather than resurrect it. Meta therefore grows by one small entry per DISTINCT
// y-awareness client id seen over a room's lifetime, bounded by that cardinality
// within a single room session and fully reclaimed when the room is released and
// GC'd. A periodic Meta sweep for clients absent beyond a TTL is the y-protocols
// "outdated timeout" mechanism. go-yjs judges expiry ON ACCESS in GetStates
// (a remote client past OutdatedTimeout is simply not returned as present), so
// this is the core's concern rather than a room-level reimplementation.
func (r *Room) forcedAwarenessRemoval(clientID ycrdt.Number) []byte {
	// RemoveAwarenessStates drops the client from the active set and is the core's
	// own removal path; the encode that follows carries a null state at the client's
	// current clock. A receiver accepts that as a removal — its merge rule admits an
	// equal-clock null state when it still holds one (currClock == clock && state
	// nil && exists) — so no manual clock bump is needed. This previously reached
	// into Awareness.Meta/States directly; those are unexported in go-yjs, and the
	// native call expresses the same intent without a local re-implementation
	// (FR-007).
	ycrdt.RemoveAwarenessStates(r.awareness, []ycrdt.Number{clientID}, updateOrigin{src: 0})
	update := ycrdt.EncodeAwarenessUpdate(r.awareness, []ycrdt.Number{clientID}, nil)
	return encodeAwarenessFrame(update)
}

// dropMember removes a member from the registry, evicts its awareness, and
// decrements the connection gauge. It returns false when the member was already
// gone (idempotent).
func (r *Room) dropMember(id connID) bool {
	m, ok := r.members[id]
	if !ok {
		return false
	}
	// Deregister BEFORE evicting awareness. evictAwareness broadcasts, and a
	// broadcast send can fail for another member (a full send buffer — likely when
	// a large frame is in flight) and re-enter dropMember for it. If this member
	// were still registered during that nested broadcast it would be re-dropped in
	// turn, and two cross-failing members would recurse into each other without
	// bound (sendMember→broadcast→evictAwareness→dropMember→…) until the goroutine
	// stack overflows and the process crashes. Removing it first makes the
	// re-entrant dropMember a no-op (already gone) and bounds the cascade to one
	// drop per member. Safe on the single-writer run loop (no concurrent r.members
	// access); evictAwareness only needs the already-captured member value.
	delete(r.members, id)
	r.metrics.ConnClosed()
	r.evictAwareness(m)
	return true
}

// handleMessage dispatches one framed wire message from a connection. It returns
// true when the message mutated the persistent document (a sync update), so the
// caller can arm the save timer. Sync messages are applied to the
// authoritative doc — whose update observer fans the delta to the other members.
// Awareness and ephemeral messages are fanned out but never touch the snapshot
// (FR-008).
//
// Before any handling it enforces the per-connection update rate (FR-024): a
// connection that breaches its token bucket is disconnected with a control
// message; other collaborators are unaffected.
func (r *Room) handleMessage(src connID, frame []byte, sessionDropped bool) (mutated bool) {
	// Ignore late frames from an already-evicted source. After disconnect/leave
	// the socket can still forward buffered frames before its close propagates;
	// without this gate an evicted connection could keep publishing awareness or
	// ephemeral payloads to peers.
	if _, ok := r.members[src]; !ok {
		return false
	}

	// Enforce the per-connection update rate BEFORE parsing (the doc above promises
	// this): a flood of malformed frames must be charged a token — and ultimately
	// disconnected — exactly like valid ones. Parsing first would let a client spam
	// unparseable frames (each costing a parse attempt and a WARN log) without ever
	// tripping the limit (FR-024).
	if !r.allowRate(src) {
		r.disconnect(src, model.CodeUpdateRateExceeded)
		return false
	}

	in := bytes.NewBuffer(frame)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		r.logger.Warn("dropping malformed frame", zap.Error(err))
		return false
	}

	switch model.WireMessageType(msgType) {
	case model.WireSync:
		return r.handleSync(src, payload)

	case model.WireAwareness:
		// The awareness payload is a canonical y-protocols length-prefixed body
		// (awareness_wire.go). Decode it once to the raw update body, then learn
		// the member's y-awareness client id (for server-forced eviction on
		// leave) and apply it to the room's awareness (so a late joiner gets a
		// snapshot). Fan the raw frame out to local members verbatim and publish
		// it to peer pods on the awareness:{id} channel. Never persisted (FR-008).
		body, ok := awarenessBody(frame)
		if !ok {
			r.logger.Warn("dropping malformed awareness frame")
			return false
		}
		r.trackAwarenessID(src, body)
		if err := ycrdt.ApplyAwarenessUpdate(r.awareness, body, updateOrigin{src: src}); err != nil {
			// DROP it. An earlier version relayed the frame anyway, reasoning that peers
			// would "apply it against their own state" — but an awareness apply fails on
			// the bytes, not on the state, so a frame that fails here fails identically
			// for every recipient. Relaying it makes one client's bad frame cost every
			// other client and every other pod a failed decode, which is precisely the
			// offender-only property (FR-009c) inverted.
			r.logger.Warn("dropping awareness frame the decoder rejected", zap.Error(err))
			return false
		}
		r.broadcast(frame, src)
		r.publishToPeers(frame, true)
		return false

	case model.WireEphemeral:
		// Volatile whiteboard ephemerals (cursor/emoji/countdown): fan out to
		// local members and to peer pods (awareness:{id}), drop on the floor
		// otherwise. Never applied to the doc, never persisted (FR-008, T009).
		r.broadcast(frame, src)
		r.publishToPeers(frame, true)
		return false

	case model.WireDurabilityRequest:
		return r.handleDurabilityRequest(src, payload, sessionDropped)

	case model.WireHeartbeat:
		// Echo only to the sender. This is transport liveness, not awareness:
		// it never fans out, touches the Y.Doc, or arms persistence.
		r.sendTo(src, frame)
		return false

	default:
		// Control is server→client only; ignore client-sent control/unknown
		// types (matches y-protocols leniency).
		return false
	}
}

// handleDurabilityRequest answers one barrier request, or refuses it.
//
// It runs on the run loop, AFTER the sender's preceding mutation, because both
// arrive on the same per-connection FIFO and this room consumes that FIFO in
// order. That ordering is the entire correlation mechanism: no id is attached to
// the update, and none is needed.
func (r *Room) handleDurabilityRequest(src connID, payload []byte, sessionDropped bool) bool {
	id := durabilityRequestID(payload)
	if id == "" {
		// Unreadable body, or an id that violates the contract. There is no id to
		// correlate a failure to, so this is the ONE case answered with silence —
		// which is why the guarantee below is stated for every VALID request.
		r.logger.Warn("dropping a durability request with a missing or invalid request id")
		return false
	}

	// ONE LOOKUP for every per-member decision below. An unknown src — a member
	// already evicted — has nothing to answer to and nothing to park on, so it
	// leaves here.
	m, ok := r.members[src]
	if !ok {
		return false
	}

	// A MEMBER WHOSE WRITE WAS REFUSED CAN NEVER BE ANSWERED `persisted` AGAIN on
	// this connection. Its generation contains a struct the server rejected, so
	// nothing it asks about is fully in the durable document, and no later flush —
	// including one triggered by an entirely different member — may be allowed to
	// answer for it. The client must resync on a new connection first.
	if m.durabilityPoisoned {
		r.failBarrier(src, id, "an earlier update from this session was rejected; resync before requesting durability")
		return false
	}

	// A SESSION THAT LOST A FRAME CAN NEVER BE ANSWERED `persisted`. The lost
	// frame may have been the very mutation this asks about, and nothing on the
	// server can tell. Fail it explicitly rather than let the client infer from
	// silence.
	if sessionDropped {
		r.failBarrier(src, id, "a previous update from this session was not accepted")
		return false
	}

	// Only a member that may WRITE may ask whether its write is durable. A viewer's
	// mutation was refused, so a barrier over it would be asking about work the
	// room deliberately discarded.
	if m.mode != model.ModeCollaborator {
		r.failBarrier(src, id, "this session is read-only")
		return false
	}

	// ONE OUTSTANDING PER CONNECTION. A second request is refused rather than
	// queued: the caller is sequential by contract, so a second concurrent one is
	// a caller bug, and answering it would need a waiter map with no bound.
	if m.barrier != "" {
		r.failBarrier(src, id, "a durability request is already outstanding on this session")
		return false
	}

	// ALREADY DURABLE. A clean room means the live document IS the durable one, by
	// both of the ways clean arises: a cold load restores the checkpoint BEFORE the
	// update observer is registered, so a freshly opened room is clean and equal to
	// storage; and thereafter dirty is cleared at exactly one place, inside persist,
	// after BOTH stores accepted.
	//
	// This is the ambiguous-close case: the client reconnects and resends an update
	// the server already has, the apply changes nothing, and the barrier resolves
	// without forcing a redundant write.
	if !r.dirty {
		r.answerBarrier(src, id)
		return false
	}

	// SAVE-ON-RELEASE MODE HAS NO FLUSH TO WAIT FOR. With SaveDebounce <= 0 the
	// only persist is the one at release, so a parked request would hang for the
	// life of the room. Answer it now, correlated, rather than silently.
	//
	// Triggering a persist from here instead was considered and rejected: persist
	// can escalate, escalation IS a teardown, and dispatch expects a normal return
	// from this path — so it would add a re-entrant teardown route for a
	// configuration that deliberately has no periodic saving.
	if r.cfg.SaveDebounce <= 0 {
		r.failBarrier(src, id, "durability requests are unavailable while periodic saving is disabled")
		return false
	}

	// Otherwise wait for the next successful persist. COALESCING IS INHERITED, not
	// added: the save timer is armed once per clean→dirty cycle, so this joins the
	// flight already scheduled rather than forcing a second one. A burst of
	// requests across one dirty epoch therefore costs at most one write.
	m.barrier = id
	r.members[src] = m
	// PARK WITHOUT TOUCHING THE TIMER. Arming here would be actively harmful:
	// armSaveTimer is stop-then-Reset, NOT idempotent, so every request in a dirty
	// epoch pushes a nearly-due flush back out by a full SaveDebounce. With several
	// members asking, durability could be postponed indefinitely by the very
	// requests waiting for it.
	//
	// Nothing needs arming. A room is dirty only because the update observer fired,
	// and the clean->dirty transition that produced it already armed the timer; a
	// failed flush arms the retry timer instead. So a dirty room always has a flush
	// scheduled, and this joins it.
	return false
}

// answerBarrier resolves one member's outstanding request as durable.
func (r *Room) answerBarrier(src connID, id string) {
	if ctrl := encodeControl(model.ControlMessage{
		Kind:      model.ControlPersisted,
		RequestID: id,
	}); ctrl != nil {
		r.sendTo(src, ctrl)
	}
}

// failBarrier resolves one request as failed. Every VALID request gets exactly
// one outcome — persisted or persist-failed. The single exception is a request
// whose id is missing or violates the id contract: there is nothing to correlate
// an answer to, so it is dropped. A caller that sees silence may therefore treat
// the barrier as failed rather than guess.
func (r *Room) failBarrier(src connID, id, reason string) {
	if ctrl := encodeControl(model.ControlMessage{
		Kind:      model.ControlPersistFailed,
		RequestID: id,
		Error:     reason,
	}); ctrl != nil {
		r.sendTo(src, ctrl)
	}
}

// poisonDurability marks a member as permanently unable to be told its work is
// durable on THIS connection, because one of its updates was refused.
//
// Sticky by design and cleared by nothing: the member is removed on disconnect,
// so a reconnecting client gets a fresh member with a clean flag — which is
// exactly the resync the rejection already demanded.
func (r *Room) poisonDurability(src connID) {
	if m, ok := r.members[src]; ok && !m.durabilityPoisoned {
		m.durabilityPoisoned = true
		r.members[src] = m
	}
}

// failMemberBarrier fails ONE member's outstanding request, if it has one.
// Used where a specific member's write is refused: the request that write was
// covered by can no longer succeed, and a later unrelated flush must not be
// allowed to answer it.
func (r *Room) failMemberBarrier(src connID, reason string) {
	m, ok := r.members[src]
	if !ok || m.barrier == "" {
		return
	}
	// State first, send second — see resolveBarriers.
	pending := m.barrier
	m.barrier = ""
	r.members[src] = m
	r.failBarrier(src, pending, reason)
}

// resolveBarriers answers every outstanding request after a SUCCESSFUL persist.
// It runs at the one place dirty is cleared, so a resolution can never claim more
// than both stores accepted.
func (r *Room) resolveBarriers() {
	// CLEAR THE STATE BEFORE SENDING, always. sendTo -> sendMember drops a member
	// SYNCHRONOUSLY when its Send fails, which deletes it from r.members. Writing a
	// captured roomMember back afterwards would RESURRECT the dead member: it
	// reappears in the map with ConnClosed already counted, inflating occupancy and
	// outliving teardown and awareness eviction. Same re-entrancy rule dropMember
	// documents.
	for id, m := range r.members {
		if m.barrier == "" {
			continue
		}
		pending := m.barrier
		m.barrier = ""
		r.members[id] = m
		r.answerBarrier(id, pending)
	}
}

// failBarriers resolves every outstanding request as failed. Called from the
// teardown and flush-failure paths, so a pending request never outlives the room
// or a failed save without a correlated answer.
func (r *Room) failBarriers(reason string) {
	// State first, send second — see resolveBarriers.
	for id, m := range r.members {
		if m.barrier == "" {
			continue
		}
		pending := m.barrier
		m.barrier = ""
		r.members[id] = m
		r.failBarrier(id, pending, reason)
	}
}

// durabilityRequestID reads the request id out of a durability-request payload.
// The body is a small JSON object rather than a bare string so the frame has room
// to grow without a second wire type; an unreadable one yields "" and the request
// is dropped rather than answered under a guessed id.
func durabilityRequestID(payload []byte) string {
	var body struct {
		RequestID string `json:"requestId"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return ""
	}
	if !validRequestID(body.RequestID) {
		return ""
	}
	return body.RequestID
}

// maxRequestIDLen bounds the caller-chosen request id. It is generous for a UUID
// (36 chars) and small enough that echoing one back is never a payload.
const maxRequestIDLen = 64

// validRequestID enforces the request-id contract BEFORE the id is stored on a
// member or echoed into a control frame.
//
// This matters because the id is UNBOUNDED CLIENT INPUT that the server keeps and
// then sends back. The socket's read limit is the document size limit — tens of
// megabytes — so without this one authenticated request could park a multi-
// megabyte string on the member and make the server re-encode and transmit it.
// "One outstanding request per connection" bounds the COUNT, not the bytes.
//
// The contract is deliberately narrow and explicit rather than inferred from
// whatever the first caller happens to send: at most 64 bytes of
// [A-Za-z0-9._-]. A UUID satisfies it unchanged.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for _, c := range []byte(id) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// allowRate consumes a token from the source connection's bucket, reporting
// whether the message is admitted (FR-024 per-connection update rate). An unknown
// source (already evicted) is admitted — there is nothing left to limit.
func (r *Room) allowRate(src connID) bool {
	m, ok := r.members[src]
	if !ok || m.bucket == nil {
		return true
	}
	return m.bucket.allow()
}

// trackAwarenessID records the y-awareness client id a member announced in its
// first awareness frame, so dropMember can force a removal for exactly that id on
// disconnect (T013). The first client entry of the awareness payload is the
// member's own client id (one connection = one y client id).
func (r *Room) trackAwarenessID(src connID, payload []byte) {
	m, ok := r.members[src]
	if !ok || m.hasAwareness {
		return
	}
	clientID, _, err := protocol.DecodeAwarenessMessage(payload)
	if err != nil {
		return
	}
	m.awarenessID = clientID
	m.hasAwareness = true
	r.members[src] = m
}

// handlePeer applies a payload received from another pod via the
// ClusterBroadcaster (R4). A doc payload (ephemeral == false) is an applied v1
// update: it is applied with a peer origin so onDocUpdate fans it to local
// members but does NOT re-publish it to the bus (no ping-pong). An ephemeral
// payload (awareness or the custom ephemeral channel) is fanned to local members
// verbatim and never persisted. It returns whether the document was mutated so
// the run loop can arm the save timer — only the originating pod persists, but
// every pod that applied the update keeps its in-memory doc dirty for its own
// final snapshot, so a pod can survive the originator vanishing.
func (r *Room) handlePeer(payload []byte, ephemeral bool) (mutated bool) {
	if ephemeral {
		// Could be a y-awareness update (apply to local awareness so late
		// joiners on THIS pod see the cursor) or a custom ephemeral frame.
		// applyPeerEphemeral fans it to local members only — it deliberately does
		// NOT route through handleMessage (which would re-publish the frame to the
		// bus and ping-pong it between pods).
		r.applyPeerEphemeral(payload)
		return false
	}

	wasDirty := r.dirty
	_ = r.applyUpdate(payload, updateOrigin{src: 0, peer: true})
	return r.dirty && !wasDirty
}

// applyResult is applyUpdate's verdict. It exists because "applied" and "the
// budget was fine" used to be the same bool, so the candidate path had no way to
// report that it had refused an update before the live document was touched.
//
// It deliberately does NOT change what a no-candidate (memo) apply reports; see the
// live-apply branch in applyUpdate.
type applyResult uint8

const (
	// applyOK: the update is in the live document.
	applyOK applyResult = iota
	// applyRejectedTooLarge: a local write would exceed MaxDocBytes. The caller
	// disconnects the offender (FR-024).
	applyRejectedTooLarge
	// applyRejectedSchema: the update violates the assets-root contract. Rejected
	// in-band; the connection stays open.
	applyRejectedSchema
	// applyCandidateFailed: the CANDIDATE could not decode the bytes, so the live
	// document was never touched. Nothing landed and the room carries on. This
	// guarantee holds only because the candidate is what failed.
	applyCandidateFailed
)

// applyUpdate is the SINGLE guarded chokepoint every doc-mutating update routes
// through (002 FR-005), so both the MaxDocBytes budget and the assets-root schema
// cover EVERY entry point — local client writes AND cross-pod peer updates.
//
// Order is deliberate: budget, then schema, then live. The budget check is a cheap
// sound skip in the common case, and rejecting on size first avoids paying a
// shadow apply for an update that is going to be refused anyway.
//
// The schema is validated on the SHADOW, never on the live document. Applying
// first and inspecting afterwards would mean the poison is already in the
// authoritative document — and ApplyUpdate has no undo. The shadow is one update
// ahead only between these two applies, and rejoins lockstep before returning.
//
// A PEER write cannot be rejected for SIZE without diverging from the pod that
// already accepted it, so an over-budget peer update is logged and applied
// (correctness then relies on a uniform MaxDocBytes, a documented operational
// constraint). SCHEMA is different: poison is poison whichever pod it came from,
// and accepting it to preserve convergence would converge every pod on an unusable
// document.
func (r *Room) applyUpdate(update []byte, origin updateOrigin) applyResult {
	if r.applyWouldExceedMaxDocBytes(update) {
		if !origin.peer {
			return applyRejectedTooLarge
		}
		r.logger.Warn("peer update would exceed MaxDocBytes; applied to avoid cross-pod divergence (check for MaxDocBytes config skew)",
			zap.String("doc", string(r.id)))
	}

	if r.shadow != nil {
		if err := ycrdt.ApplyUpdate(r.shadow, update, origin); err != nil {
			// The shadow may be PARTIALLY mutated: an update truncated at its final
			// byte applies in full and then errors (measured). Validating the next
			// update against a contaminated mirror would compare against the wrong
			// state, so the shadow is replaced rather than reused.
			r.logger.Warn("candidate apply failed; rebuilding the validation shadow",
				zap.String("doc", string(r.id)), zap.Error(err))
			r.rebuildShadow("after a failed candidate apply")
			return applyCandidateFailed
		}
		if err := r.validateSchema(r.shadow); err != nil {
			r.logger.Warn("update rejected: locator schema",
				zap.String("doc", string(r.id)), zap.Bool("peer", origin.peer), zap.Error(err))
			// The shadow now holds the poison. Rebuild from the still-clean live doc.
			r.rebuildShadow("after a schema rejection")
			return applyRejectedSchema
		}
	}

	if err := ycrdt.ApplyUpdate(r.doc, update, origin); err != nil {
		if r.shadow != nil {
			// DEFENSIVE. The candidate accepted these exact bytes from an identical
			// state a moment ago, and apply is deterministic, so reaching here means
			// the mirror was not a mirror — an internal invariant failure, not anything
			// a client did. The live document may be partially mutated and must never
			// be persisted, so this panics: the run loop's deferred recover tears the
			// room down WITHOUT a flush, which is exactly the required behaviour.
			r.invariantFailed("live apply failed after the candidate accepted the same bytes", err)
		}
		// No candidate was involved, so this room has no recognized convention and
		// therefore no shadow. Both memo and whiteboard now carry one, so in practice
		// this branch is not reached by either; it remains because the fallthrough
		// must stay defined rather than depend on that.
		//
		// Deliberately not "corrected" here. Without a candidate nothing proves the
		// live document was untouched; apply has no undo, the synchronous observer may
		// already have set dirty and broadcast, and reporting not-applied would change
		// whether a save is armed for a document that may have been partially mutated.
		r.logger.Warn("applying update failed", zap.String("doc", string(r.id)), zap.Error(err))
	}
	return applyOK
}

// invariantFailed aborts the run loop when the room can no longer be trusted with
// the document.
//
// It PANICS deliberately, rather than threading a second failure lifecycle through
// every caller. The run loop already has exactly the right boundary: its deferred
// recover logs with a stack and tears down WITHOUT a flush, precisely because "a
// doc left mid-mutation must not be persisted over the last good snapshot". That is
// the required behaviour, so this reuses it instead of building a parallel one for
// a branch with no reachable producer.
//
// Note what it does NOT do: call Registry.Invalidate. Invalidate closes the handle
// and then waits for the generation to drain, and the generation cannot drain until
// this room releases its handle — which happens in teardown, on the very goroutine
// that would be blocked inside Invalidate. That is a deterministic self-deadlock.
// It is also unnecessary here: production has one Acquire site and one room per
// document, so tearing down releases and evicts the sole generation.
func (r *Room) invariantFailed(what string, cause error) {
	panic(fmt.Sprintf("collaboration: %s for document %s: %v", what, r.id, cause))
}

// initShadow builds the validation shadow from the document the registry just
// published, so it is in lockstep from the first update onward.
//
// Both conventions carry a shadow, because both carry blob locators that must be
// references rather than bytes: a whiteboard in `files`, a memo in an image
// node's `src`.
//
// The memo check must read XmlFragment("default") and nothing else. INSPECTING a
// root materializes it, so reaching for `files` here would grow a memo a map its
// convention forbids — which is why memos were originally excluded outright
// rather than validated against the whiteboard rule.
//
// A memo pays what a whiteboard already paid: one clone at open, one candidate
// apply per update.
func (r *Room) initShadow(doc *ycrdt.Doc) error {
	if r.content != model.ContentTypeWhiteboard && r.content != model.ContentTypeMemo {
		return nil
	}
	shadow, err := cloneDoc(doc, string(r.id))
	if err != nil {
		return fmt.Errorf("building the validation shadow for %s: %w", r.id, err)
	}
	r.replaceShadow(shadow)
	return nil
}

// rebuildShadow replaces the validation shadow with a fresh copy of the live
// document. Called only after a rejected or failed candidate apply, never on the
// happy path — it is the expensive operation in this design.
//
// A failure here is fatal, and the decision is made HERE rather than by each
// caller because no caller has a choice: leaving a stale mirror would validate
// later updates against a state the document never had, and dropping it and
// carrying on would apply unvalidated bytes into the authoritative document.
// Fail closed — what an absent validator lets through is permanent.
func (r *Room) rebuildShadow(when string) {
	// Build the replacement BEFORE letting go of the old one: on failure the old
	// shadow is still owned by the room, so teardown destroys it rather than leaking
	// it. A client that can force repeated rejections would otherwise accumulate one
	// full copy of the board per rejected update.
	fresh, err := cloneDoc(r.doc, string(r.id))
	if err != nil {
		r.invariantFailed("cannot rebuild the validation shadow "+when, err)
	}
	r.replaceShadow(fresh)
}

// replaceShadow installs a new shadow and destroys the one it displaces.
func (r *Room) replaceShadow(fresh *ycrdt.Doc) {
	old := r.shadow
	r.shadow = fresh
	if old != nil {
		old.Destroy()
	}
}

// destroyShadow releases the shadow. Idempotent, so every teardown path can call
// it without coordinating with the others.
func (r *Room) destroyShadow() {
	if r.shadow != nil {
		r.shadow.Destroy()
		r.shadow = nil
	}
}

// applyPeerEphemeral applies a peer-pod awareness/ephemeral frame: an awareness
// update is merged into the room's awareness (so a late joiner on this pod sees
// the remote cursor) and fanned to local members; a custom ephemeral frame is
// fanned to local members. Neither is persisted nor re-published.
func (r *Room) applyPeerEphemeral(frame []byte) {
	// Classify without allocating a reader: InspectMessage parses the outer type
	// over the caller-owned frame, and awarenessBody re-derives the body below.
	info, err := protocol.InspectMessage(frame)
	if err != nil {
		r.logger.Warn("dropping malformed peer frame", zap.Error(err))
		return
	}
	switch model.WireMessageType(info.Type) {
	case model.WireAwareness:
		body, ok := awarenessBody(frame)
		if !ok {
			r.logger.Warn("dropping malformed peer awareness frame")
			return
		}
		if err := ycrdt.ApplyAwarenessUpdate(r.awareness, body, updateOrigin{src: 0, peer: true}); err != nil {
			r.logger.Warn("applying peer awareness update failed", zap.Error(err))
		}
		r.broadcast(frame, 0)
	case model.WireEphemeral:
		r.broadcast(frame, 0)
	default:
		// Sync/control never travel the awareness channel; ignore.
	}
}

// handleSync feeds a sync sub-message (SyncStep1 / SyncStep2 / Update) to the
// authoritative doc via dispatchSync. A SyncStep1 yields a framed SyncStep2
// reply sent only to the requesting connection (the offline→reconnect catch-up,
// US5). Applied structs (SyncStep2 / Update) flow through the doc's update
// observer, which both marks the room dirty and fans the delta to the other
// members. It returns whether the document became dirty so the run loop can arm
// the save timer.
func (r *Room) handleSync(src connID, payload []byte) (mutated bool) {
	// Re-frame the sync payload as a MessageSync envelope for dispatchSync.
	var framed bytes.Buffer
	protocol.WriteMessage(&framed, protocol.MessageSync, payload)

	canMutate := r.canMutate(src)

	wasDirty := r.dirty
	var reply bytes.Buffer
	outcome, err := r.dispatchSync(framed.Bytes(), &reply, src, canMutate)
	if err != nil {
		r.logger.Warn("sync dispatch failed", zap.Error(err))
		return false
	}

	if reply.Len() > 0 {
		r.sendTo(src, reply.Bytes())
	}

	// MaxDocBytes is enforced pre-commit (dispatchSync), so an oversized update
	// never mutated or broadcast the live doc. Disconnect only the offender
	// (FR-024 offender-only impact); other collaborators are untouched.
	if outcome.rejectedTooLarge {
		// The offender is being disconnected, but it is still owed an answer for a
		// request it has outstanding — and that answer must be a failure, not the
		// silence its close would otherwise leave behind.
		r.failMemberBarrier(src, "the update this request covered was rejected")
		r.disconnect(src, model.CodeDocumentSizeLimitExceeded)
		return false
	}

	// A schema rejection refuses the WRITE, not the writer: nothing was applied,
	// broadcast or saved, and the connection stays open. Only the sender is told —
	// no other member ever saw the update.
	//
	// The sender cannot just continue after this: its rejected struct leaves a gap
	// in its own clock sequence, so anything it writes next stays pending behind the
	// missing one. It has to drop that generation and resync. That recovery is the
	// client's; the server's job is to refuse the write and say so.
	if outcome.rejectedSchema {
		// POISON ANY OUTSTANDING BARRIER FIRST. A refused write must never be
		// convertible into a durability success: without this, a request parked
		// before the rejection would be resolved `persisted` by the next unrelated
		// flush, telling the caller its refused edit is safely stored.
		//
		// The failure is queued BEFORE the update-rejected frame, on the same
		// per-connection FIFO, so the client learns the barrier died before it
		// learns why.
		r.poisonDurability(src)
		r.failMemberBarrier(src, "the update this request covered was rejected")
		if ctrl := encodeControl(model.ControlMessage{
			Kind:  model.ControlUpdateRejected,
			Error: "update rejected: file locators must be references, not inline data",
		}); ctrl != nil {
			r.sendTo(src, ctrl)
		}
		return false
	}

	// A write from a member that may not write is refused the same way, and for the
	// same reason: silence is what makes it dangerous. The sender applied the
	// struct locally and, told nothing, keeps writing at k+1, k+2 against a server
	// that never received k — every one of them pending behind the missing struct,
	// forever.
	//
	// WHAT THIS GUARANTEES IS THE SIGNAL, NOT THE RECOVERY. The service truthfully
	// reports that the write was refused; what a client does with that is the
	// client's. Today that differs by surface: the whiteboard consumes
	// update-rejected and discards its generation to resync
	// (client-web useCollab.ts), while the memo editor's control handler does not
	// handle the kind at all (client-web useCollaboration.ts handles saved,
	// save-error and read-only-state, then falls through). So a memo client is
	// told and does not act — a known client-side residual with a separate owner,
	// not something this branch can fix or should pretend away.
	//
	// This reports the CURRENT capability; it does not change it. Restoring write
	// access is a separate question and deliberately not answered here.
	if outcome.rejectedNotWritable {
		// Same rule for a refused write from a member that may not write.
		r.poisonDurability(src)
		r.failMemberBarrier(src, "the update this request covered was rejected")
		if ctrl := encodeControl(model.ControlMessage{
			Kind:  model.ControlUpdateRejected,
			Error: "update rejected: this session is read-only",
		}); ctrl != nil {
			r.sendTo(src, ctrl)
		}
		return false
	}

	if outcome.applied {
		// A collaborator just wrote: record activity (resets the inactivity
		// downgrade timer) and record the actor for the contribution window.
		r.recordActivity(src)
	}

	// onDocUpdate flips dirty=true synchronously inside ApplyUpdate when the
	// message carried new structs; a SyncStep1 (reply-only) leaves it untouched.
	return r.dirty && !wasDirty
}

// canMutate reports whether the source connection may write to the document: a
// collaborator can, a viewer cannot (T014, the read-only gate). An unknown source
// (already evicted) cannot mutate.
func (r *Room) canMutate(src connID) bool {
	m, ok := r.members[src]
	return ok && m.mode == model.ModeCollaborator
}

// recordActivity marks a member as active now (resetting the inactivity-downgrade
// window) and records its actor id in the current contribution window (T013).
func (r *Room) recordActivity(src connID) {
	m, ok := r.members[src]
	if !ok {
		return
	}
	m.lastActivity = time.Now()
	r.members[src] = m
	// Collect only when the feature is on — see contributionEnabled. An actor id
	// recorded with the window disabled would never be emitted, only accumulated.
	if m.actorID != nil && r.contributionEnabled() {
		r.contributors[*m.actorID] = struct{}{}
	}
}

// applyWouldExceedMaxDocBytes reports whether applying update to the authoritative
// doc would grow its encoded v2 snapshot past MaxDocBytes — WITHOUT mutating the
// live doc, so an oversized write is rejected pre-commit (no mutation, no
// broadcast of the live doc) rather than evicted after the fact (FR-024
// offender-only impact). Returns false when the limit is disabled (MaxDocBytes <=
// 0). Runs on the single-writer run loop, so reading r.doc is race-free.
//
// The exact answer requires re-encoding the whole doc (EncodeStateAsUpdateV2) and
// a full scratch rebuild — O(docsize) work. Doing that on EVERY mutating update
// lets one client editing a doc near the cap monopolize the single-writer loop
// (every other member's joins/messages/disconnects queue behind the re-encode).
// So we gate it behind a cheap, SOUND short-circuit: r.docBytes tracks the live
// doc's last encoded v2 size (refreshed here whenever the exact path runs, and
// over-counted by len(update) on each accepted apply in onDocUpdate, so it never
// under-estimates the true size between exact checks). A v1 update of length L
// re-encodes to at most a small multiple of L of new v2 content; requiring a 2x
// margin on top — docBytes + 2*L + slack <= limit — means the post-apply snapshot
// cannot reach the cap, so the exact check is skipped. The skip only fires with
// real headroom: a doc anywhere below ~half the cap pays O(L) per edit, and the
// full check engages only as the doc approaches the limit (where exactness matters
// and edits are rarer). docBytes==0 (not yet established) forces the exact path so
// the bound is never trusted before it is known.
func (r *Room) applyWouldExceedMaxDocBytes(update []byte) bool {
	limit := r.cfg.Limits.MaxDocBytes
	if limit <= 0 {
		return false
	}
	// Cheap sound skip: with a 2x margin on the update length over the last known
	// encoded size, applying it cannot reach the cap, so skip the O(docsize) check.
	// budgetSkipSlack absorbs v2 framing/varint overhead at very small limits, and
	// docBytes==0 (not yet established) forces the exact path below.
	if r.docBytes > 0 && r.docBytes+2*len(update)+budgetSkipSlack <= limit {
		return false
	}
	scratch := newRoomDoc(string(r.id))
	// A Doc registers handlers and accelerators, so abandoning one leaks a whole
	// document — the invariant cloneDoc states in this same package and honours.
	// This function returns on five separate paths and every one of them abandoned
	// the scratch, on a hot path: past half of MaxDocBytes the cheap skip stops
	// firing and this runs on EVERY mutating update. Deferring covers all five,
	// including the two that return early on an encode failure.
	defer scratch.Destroy()
	curr, err := ycrdt.EncodeStateAsUpdateV2(r.doc, nil)
	if err != nil {
		r.logger.Error("budget check: encoding live doc failed", zap.String("doc", string(r.id)), zap.Error(err))
		return false
	}
	// Refresh the cached size from the authoritative encode we just paid for, so the
	// next cheap skip is measured against the true current size, not a stale one.
	r.docBytes = len(curr)
	// A scratch-measurement failure is a SERVER fault, not client misbehaviour.
	// Returning true here would reject the update and disconnect the sender as an
	// offender, punishing a client for our own error — so these fail OPEN and log
	// loudly, matching the encode-error path below (§VIII). The next exact check
	// re-measures, and the failure is visible in the persistence signals (FR-026).
	if err := ycrdt.ApplyUpdateV2(scratch, curr, nil); err != nil {
		r.logger.Error("budget check: seeding scratch doc failed", zap.String("doc", string(r.id)), zap.Error(err))
		return false
	}
	if err := ycrdt.ApplyUpdate(scratch, update, nil); err != nil {
		r.logger.Error("budget check: applying candidate update to scratch failed", zap.String("doc", string(r.id)), zap.Error(err))
		return false
	}
	encoded, err := ycrdt.EncodeStateAsUpdateV2(scratch, nil)
	if err != nil {
		r.logger.Error("budget check: encoding scratch doc failed", zap.String("doc", string(r.id)), zap.Error(err))
		return false
	}
	return len(encoded) > limit
}

// onDocUpdate is the doc "update" observer: it frames the v1 update and fans it
// to every member except the one whose edit produced it (origin filtering). It
// runs synchronously on the run-loop goroutine inside ApplyUpdate, so touching
// the member map here is race-free.
func (r *Room) onDocUpdate(v ...interface{}) {
	if len(v) == 0 {
		return
	}
	update, ok := v[0].([]uint8)
	if !ok {
		return
	}
	var origin updateOrigin
	if len(v) > 1 {
		if o, ok := v[1].(updateOrigin); ok {
			origin = o
		}
	}
	r.dirty = true
	// Keep the cached encoded-size estimate sound between exact budget checks: every
	// applied update (client edit, peer update, or snapshot load) only grows the
	// doc, and a v1 update of length L adds at most ~L of re-encoded v2 content, so
	// over-counting by len(update) guarantees docBytes never under-estimates the
	// true encoded size — the conservative direction for the cheap skip in
	// applyWouldExceedMaxDocBytes. The next exact check (or persist) re-syncs it to
	// the authoritative size, bounding the drift.
	r.docBytes += len(update)
	r.broadcast(protocol.EncodeUpdate(update), origin.src)

	// Publish locally-originated updates to peer pods (R4) as the RAW v1 update
	// bytes (not the wire-framed message) so a peer pod can ApplyUpdate them
	// directly. A peer-applied update is NOT re-published (it already crossed the
	// bus once) — the ping-pong guard. The snapshot load (also peer-flagged via
	// restoreInto's origin) likewise stays local.
	if !origin.peer {
		r.publishToPeers(update, false)
	}
}

// publishToPeers fans a frame to other pods via the ClusterBroadcaster and
// records the fan-out result on the metrics surface (R10). It is a no-op cost on
// single-pod (the in-memory broadcaster's Publish returns immediately).
func (r *Room) publishToPeers(frame []byte, ephemeral bool) {
	start := time.Now()
	ctx, cancel := r.opCtx()
	defer cancel()
	kind := hub.DocumentUpdate
	if ephemeral {
		kind = hub.AwarenessUpdate
	}
	if err := r.deps.Hub.Publish(ctx, hub.Message{
		DocumentID: backend.DocumentID(r.id),
		SourceID:   r.source,
		Kind:       kind,
		Payload:    frame,
	}); err != nil {
		r.logger.Warn("cluster fan-out publish failed", zap.Error(err))
		r.metrics.FanoutFailed()
		return
	}
	r.metrics.FanoutPublished(time.Since(start))
}

// broadcast fans a framed message to every member except the one identified by except.
func (r *Room) broadcast(frame []byte, except connID) {
	for id, m := range r.members {
		if id == except {
			continue
		}
		r.sendMember(m, frame)
	}
}

// broadcastControl encodes a control message (type 3) and fans it out to every
// member. Control messages are server-originated, so there is no sender to skip.
func (r *Room) broadcastControl(msg model.ControlMessage) {
	frame := encodeControl(msg)
	if frame == nil {
		return
	}
	r.broadcast(frame, 0)
}

// sendTo delivers a framed message to a single member.
func (r *Room) sendTo(id connID, frame []byte) {
	if m, ok := r.members[id]; ok {
		r.sendMember(m, frame)
	}
}

// sendMember delivers to one member, dropping it on a fatal send error.
func (r *Room) sendMember(m roomMember, frame []byte) {
	if err := m.conn.Send(frame); err != nil {
		r.logger.Debug("dropping unreachable connection", zap.Error(err))
		r.dropMember(m.id)
	}
}

// persist writes a throttled full v2 snapshot to the blob store and upserts the index
// (R7). It is a no-op when nothing changed since the last save. On success it
// emits a `saved` control message to the room; on failure a `save-error`, and
// the room keeps serving from memory (the crash-loss window is one save
// interval, data-model.md).
func (r *Room) persist(ctx context.Context) {
	if !r.dirty {
		return
	}

	snapshot, err := ycrdt.EncodeStateAsUpdateV2(r.doc, nil)
	if err != nil {
		r.logger.Error("snapshot encode failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "snapshot encode failed"})
		return
	}
	// Re-sync the cached encoded size from this authoritative snapshot so the cheap
	// MaxDocBytes skip is measured against the true size (it would otherwise drift
	// upward from the conservative per-update over-counting in onDocUpdate).
	r.docBytes = len(snapshot)

	// The state vector is required by the contract even where the store derives it
	// on read. Deriving it here from the update we just encoded is free — we are
	// holding the bytes — and keeps the store's obligation to bytes alone.
	vector, err := ycrdt.EncodeStateVectorFromUpdateV2(snapshot)
	if err != nil {
		r.logger.Error("state vector derive failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "snapshot encode failed"})
		return
	}

	// SaveCheckpoint carries the document's COMPLETE state. That is the caller
	// obligation the contract cannot check: a store replaces rather than merges, so
	// a save covering less than a previous one discards the difference permanently
	// and silently. EncodeStateAsUpdateV2 over the live doc gives that property by
	// construction — never narrow it to a delta here.
	if _, err := r.deps.Checkpoint.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(r.id),
		// Declared, never inferred. The snapshot above is EncodeStateAsUpdateV2 and
		// the vector is EncodeStateVectorFromUpdateV2, so this is a statement of
		// fact about the bytes rather than a preference. It matters because the
		// wrong decoder does not fail: it returns an empty state vector with a nil
		// error, which reads as "this document has nothing from any client".
		Encoding:    persistence.EncodingV2,
		Update:      snapshot,
		StateVector: vector,
	}); err != nil {
		r.onFlushFailed(err)
		return
	}

	// ContentPointer is omitted: the room does not own it. metapointer.Record is
	// its only non-blank writer, and MetadataStore.Save preserves a blank one
	// rather than clearing it — so the room writes the fields it owns and cannot
	// overwrite a pointer the store recorded.
	newVersion := r.version + 1
	meta := model.Metadata{
		ID:                    r.id,
		ContentType:           r.content,
		Version:               newVersion,
		AuthorizationPolicyID: r.policyID,
		OwnerRef:              r.ownerRef,
		StorageBucketID:       r.bucketID,
	}
	if err := r.deps.Metadata.Save(ctx, meta); err != nil {
		r.logger.Error("snapshot metadata save failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.onFlushFailed(err)
		return
	}

	r.version = newVersion
	// r.version is the room's own save counter, not a read-back of the store's
	// version — a redelivered no-op PreRegister can bump the persisted row ahead of
	// it; harmless while Metadata.Version is reserved/unused (FR-025).
	r.dirty = false
	// RESOLVE OUTSTANDING BARRIERS HERE, and only here. This is the single point
	// where dirty is cleared, and it is reached only after SaveCheckpoint AND
	// Metadata.Save both returned success — so a `persisted` answer can never
	// claim more than both configured stores accepted. It says accepted-by-both-
	// stores; it does NOT say fsync, and this service has no way to say that.
	r.resolveBarriers()
	r.onFlushSucceeded()
	r.metrics.SnapshotSaved()
	r.broadcastControl(model.ControlMessage{Kind: model.ControlSaved, Version: r.version})
}

// teardown runs the single, ordered room-release sequence (002 FR-013) — the ONE
// place teardown ordering lives, so it cannot be mis-sequenced per call site: stop
// accepting (beginTeardown) → flush (the caller's final persist/broadcast, may
// be nil) → cancel the room context (unblocking the decoupled fan-out) → tear down
// the fan-out subscription → close(done) →
// notify the Manager → mark Closed. beginTeardown is the idempotent guard: only the
// first caller runs the sequence; the rest return immediately (no double close, no
// re-notify). Runs on the run-loop goroutine.
// armSaveTimer arms the trailing save throttle, unless periodic saving is
// disabled — in which case the save-on-release path persists instead. Normal
// edits call it only on a clean-to-dirty transition, so later edits in the same
// window do not reset the timer.
func (r *Room) armSaveTimer(saveTimer *time.Timer) {
	if r.cfg.SaveDebounce <= 0 {
		return
	}
	stopTimer(saveTimer)
	saveTimer.Reset(r.cfg.SaveDebounce)
}

// armRetryTimer re-arms the save timer after a flush that left the document
// dirty, so an undurable document keeps retrying instead of waiting for an edit
// that may never come.
//
// The backoff is exponential in the consecutive-failure count and capped, so a
// backend outage does not turn into a hot retry loop against a service that is
// already struggling — but it stays bounded well under the escalation threshold's
// worth of attempts, so escalation remains reachable in finite time.
//
// It is a no-op when the flush succeeded (nothing to retry) and when periodic
// saving is disabled (SaveDebounce <= 0 means persist only on release/close, and a retry
// timer would quietly reintroduce the periodic save the operator turned off).
func (r *Room) armRetryTimer(saveTimer *time.Timer) {
	if !r.dirty || r.flushFailures == 0 || r.cfg.SaveDebounce <= 0 {
		return
	}
	backoff := r.cfg.SaveDebounce << min(r.flushFailures-1, flushRetryMaxShift)
	if backoff > maxFlushRetryBackoff {
		backoff = maxFlushRetryBackoff
	}
	stopTimer(saveTimer)
	saveTimer.Reset(backoff)
}

// armIdleTimer (re)arms the idle-release timer. A non-positive IdleTimeout means
// release as soon as the room is empty, expressed as a zero-length timer so the
// decision still runs on the next loop tick rather than inline.
func (r *Room) armIdleTimer(idleTimer *time.Timer) {
	stopTimer(idleTimer)
	if r.cfg.IdleTimeout <= 0 {
		idleTimer.Reset(time.Nanosecond)
		return
	}
	idleTimer.Reset(r.cfg.IdleTimeout)
}

// releaseIfEmpty releases an idle room that has no members left, reporting
// whether the run loop should stop. An idle release DOES flush: the document is
// believed good, so idling out must not silently cost a window of edits
// (FR-011a).
func (r *Room) releaseIfEmpty(saveTimer, idleTimer *time.Timer) bool {
	if len(r.members) != 0 {
		return false
	}

	// Flush BEFORE committing to teardown, so the outcome can be inspected.
	// teardown(r.persistNow) ran the flush inside a sequence that then released
	// unconditionally — so a transient backend failure on the final save
	// destroyed the room, and with it the edits it had just failed to persist.
	// The last client has already left, so the save-error control reaches nobody:
	// the loss was silent from the user's side.
	r.persistNow()

	if !r.lc.is(stateActive) {
		// persistNow escalated and tore the room down itself.
		return true
	}

	// Still dirty after a failed flush: keep the room alive so the retry and
	// escalation machine owns it, exactly as it would for a room with members.
	// Escalation is what eventually ends it, loudly, if the backend stays down.
	//
	// Only when a retry is actually possible: with the debounce disabled there is
	// no retry timer, so staying alive would leak an empty room forever. Then the
	// loss is accepted, already counted by SnapshotFailed and logged by
	// onFlushFailed.
	if r.dirty && r.flushFailures > 0 && r.cfg.SaveDebounce > 0 {
		r.armRetryTimer(saveTimer)
		r.armIdleTimer(idleTimer)
		return false
	}

	// No members by construction (checked at the top), so the code is unobservable
	// here. It is still the honest one: the room is being released and the
	// document is intact and flushed, which is precisely "come back later".
	r.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
	return true
}

// teardown ends the room, telling every member WHY and then closing its socket.
//
// The session end is a required argument, not an option, because a teardown that
// forgets it is exactly the bug this funnel exists to prevent: before typed ends,
// four of the nine teardown paths said nothing at all and simply abandoned the
// sockets, leaving clients attached to a room that no longer existed with their
// frames silently discarded. Requiring the argument makes a silent path
// impossible to write.
//
// Emission is HERE and nowhere else, for the same reason — a caller that
// broadcast its own control before calling teardown could drift from the code it
// passed. Callers name a code; the funnel announces it and closes behind it.
func (r *Room) teardown(end model.SessionEnd, flush func()) {
	// The shadow is this room's private document, not the registry's. Nothing else
	// can free it, so it is released here — once, on the single teardown funnel.
	r.destroyShadow()
	if !r.lc.beginTeardown() {
		return
	}
	if flush != nil {
		flush()
	}
	// Balance the connection gauge for members still attached at teardown.
	// cmdClose/cmdCloseDeleted tear the room down without each client traversing the
	// per-connection Leave path, so their ConnOpened would otherwise never be
	// matched by a ConnClosed — leaking connections_active upward by the member
	// count. dropMember (the only other ConnClosed caller) deletes from r.members
	// before it counts, so any already-closed member is absent here: no double
	// count. Runs on the run-loop goroutine (single-writer), so the walk is safe.
	//
	// Each member is TOLD before it is closed: the control goes onto the same
	// per-connection queue the close intent then joins, so ordering is a property
	// of the queue rather than of timing. Send's error is ignored deliberately —
	// a client that has already gone away still needs dropping, and there is no
	// one left to report the failure to.
	// PENDING BARRIERS FAIL FIRST, before the session-end frame is queued. Four of
	// the five teardown endings run with NO successful flush (escalation, panic,
	// owner delete, shutdown abort), so a request outstanding at teardown would
	// otherwise be answered by nothing at all. Queuing the failure ahead of the
	// end on the same per-connection FIFO means the client learns the barrier
	// failed BEFORE it learns the session is over, rather than having to infer it.
	r.failBarriers("the room ended before the document could be persisted")

	endFrame := encodeControl(end.Control())
	for id, m := range r.members {
		if endFrame != nil {
			_ = m.conn.Send(endFrame)
		}
		m.conn.CloseAfterDrain(end)
		delete(r.members, id)
		r.metrics.ConnClosed()
	}

	// ANALYTICS LAST, and deliberately so. Everything above is critical: the
	// durable flush, and telling every member why its session ended. Contribution
	// emission is best-effort by contract (FR-014) and talks to the same bus that
	// may be the reason the shutdown is slow, so it must never be able to delay
	// durability or a client's terminal control.
	//
	// It ran BEFORE the flush in an earlier revision of this slice. That put a
	// best-effort analytics call — up to a full backend timeout of it — ahead of
	// the final snapshot, inside a shutdown whose WHOLE drain is budgeted at about
	// one backend timeout (Manager.Close). A wedged bus would then burn the budget
	// and the drain would be abandoned before persisting, losing exactly the edits
	// the shutdown flush exists to save.
	//
	// Still before the context cancel below, so the emit runs with a live roomCtx.
	//
	// NOT on the owner-delete path — and this is a KNOWN CONTRACT GAP, not a
	// settled design, so it is written down as one.
	//
	// What is established: `server` confirms the lifecycle publish before starting
	// deletion, but broker confirmation is NOT consumer acknowledgement. When this
	// teardown runs, the memo/whiteboard row may still exist or may already be gone.
	// Its contribution consumer looks the id up in those rows, so an emit here would
	// be timing-dependent: sometimes accepted, sometimes discarded as unknown.
	//
	// What is NOT established: that losing it is acceptable. The final partial
	// window — contributions since the last periodic tick — IS dropped when a
	// document is deleted, and the requirement is that teardown flushes the
	// current contributor map. That the current arrangement cannot emit it
	// USEFULLY is not the same as saying it should not be emitted at all.
	//
	// No ordering INSIDE this funnel can make it reliable. The only deterministic
	// option is producer-side enrichment — the event carrying what the consumer
	// would otherwise look up.
	// That spans repos and is not decided here. Tracked as BASIC-004 in the
	// canonical remediation ledger — alkem-io/agents-hq ->
	// specs/006-collab-content-unification/kiss-remediation-ledger.md — which
	// carries its current status; do not read this branch as a settled design.
	//
	// Any in-flight periodic emit is left alone either way: its goroutine
	// completes on its own bounded context and exits on r.done.
	if end.Code != model.CodeDocumentDeleted {
		r.settleContributionFlight()
		// The current window may never have reached a periodic tick. Flush it once;
		// an empty set is a no-op.
		r.flushContributionNow()
	}

	// Cancel the room-lifetime context (roomCtx) BEFORE tearing down the subscription:
	// it unblocks any decoupled peer-update write parked on roomCtx.Done(), so cancelSub
	// — which may WAIT for the subscribe goroutine (e.g. redis pubsub.Close) — cannot
	// deadlock against it (002 FR-009). The flush above already ran with the context
	// live, so cancelling here never aborts in-progress save-on-release work.
	if r.cancel != nil {
		r.cancel()
	}
	if r.cancelSub != nil {
		r.cancelSub()
	}
	// Release the registry acquisition LAST, after the flush and the auxiliary
	// teardown above: the document must stay valid for the save-on-release path.
	// Release is idempotent.
	r.releaseHandle()
	close(r.done)
	if r.onReleased != nil {
		r.onReleased()
	}
	r.lc.finishDraining()
}

// releaseHandle relinquishes this room's registry acquisition and evicts the
// document once no other room holds it.
//
// One helper for both callers — normal teardown and a failed materialization —
// because the failure path is the one that gets forgotten, and forgetting it
// costs a permanently un-evictable document rather than a leaked object.
// Idempotent: Release is safe to call twice and Evict treats an absent entry as
// success.
func (r *Room) releaseHandle() {
	if r.handle == nil {
		return
	}
	r.handle.Release()
	r.evictFromRegistry()
}

// evictFromRegistry destroys the document's registry entry once this room has
// released its handle.
//
// Release alone only drops a reference — the entry, and the Y.Doc behind it, stay
// resident. Nothing else evicts, so without this every document ever opened is
// retained for the lifetime of the process: a service that serves N documents
// holds N Y.Docs forever, each up to MAX_DOC_BYTES. That is a leak with no
// symptom until the pod is OOM-killed, and it grows with traffic rather than with
// concurrency, so it does not show up in load tests that reuse a few document ids.
//
// ErrInUse is the expected outcome, not a failure, whenever another handle is
// still out: the registry refuses rather than invalidating a live acquisition,
// and that room will evict when it releases. Anything else is worth seeing.
func (r *Room) evictFromRegistry() {
	if r.deps.Registry == nil {
		return
	}
	err := r.deps.Registry.Evict(backend.DocumentID(r.id))
	if err == nil || errors.Is(err, memory.ErrInUse) {
		return
	}
	r.logger.Warn("evicting the document from the registry failed; it stays resident",
		zap.String("doc", string(r.id)), zap.Error(err))
}

// encodeControl marshals a control message into a framed type-3 wire message.
func encodeControl(msg model.ControlMessage) []byte {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	protocol.WriteMessage(&buf, uint8(model.WireControl), body)
	return buf.Bytes()
}

// awarenessSnapshot frames the room's current awareness states as a type-1
// message so a joining client immediately learns existing presence, or nil when
// no states are present.
func awarenessSnapshot(aw *ycrdt.Awareness) []byte {
	states := aw.GetStates()
	if len(states) == 0 {
		return nil
	}
	clients := make([]ycrdt.Number, 0, len(states))
	for id := range states {
		clients = append(clients, id)
	}
	update := ycrdt.EncodeAwarenessUpdate(aw, clients, nil)
	return encodeAwarenessFrame(update)
}

// stopTimer drains a timer's channel if it already fired, so a subsequent Reset
// is clean.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
