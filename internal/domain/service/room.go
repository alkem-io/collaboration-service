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
	"github.com/antst/go-yjs/backend/memory"
	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
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
	data     []byte
	done     chan joinResult
	// done2 returns the result of a cmdPurge run on the room loop (T015).
	done2 chan error
}

type cmdKind uint8

const (
	cmdJoin cmdKind = iota
	cmdLeave
	cmdMessage
	cmdPersist
	cmdClose
	// cmdPurge runs the owner-delete cascade on the run loop: disconnect clients
	// (room-closed), purge metadata + blob, and release the room (T015).
	cmdPurge
	// cmdReEvaluate re-runs per-document authZ for connected members on the run
	// loop (lifecycle document.access_changed, T014).
	cmdReEvaluate
)

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
	actorID      string
	mode         model.CollaboratorMode
	lastActivity time.Time
	bucket       *tokenBucket
	// awarenessID is the member's y-protocols awareness client id, learned when
	// it first sends an awareness frame; 0 means not yet seen.
	awarenessID  ycrdt.Number
	hasAwareness bool
}

// Room is a live, in-memory document session (data-model.md "Room"): it owns the
// authoritative plaintext Y.Doc (FR-021), the set of connected clients, and the
// awareness state, and it serializes every mutation through a single run-loop
// goroutine so the Y.Doc has exactly one writer. A room is lazily materialized
// on first connect (loading the latest snapshot), fans each client's updates out
// to the others, debounces snapshot persistence, and is released — persisting a
// final snapshot — when the last client leaves or after an idle timeout.
type Room struct {
	id        model.DocumentID
	content   model.ContentType
	doc       *ycrdt.Doc
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
	// it drives the debounce timer and the final save-on-release.
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
	// pointerChecked records that we have already looked for a store-assigned
	// content pointer after the first save.
	pointerChecked bool
	// seededPending is true when the room materialized from the first-open seed
	// (Metadata.SeedContent) and that seed has not yet been promoted to a real
	// per-document snapshot. The run loop arms the save debounce once at start so
	// the seed is persisted promptly (ContentPointer set) without waiting for an
	// edit or the idle release (T004). Cleared by the first persist.
	seededPending bool
	version       int
	pointer       string
	// blobKind is the configured blob backend persisted in the metadata row so a
	// document rehydrates from the right backend regardless of running config
	// (data-model.md BlobStore; T005.6).
	blobKind model.BlobStoreKind
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
	// bucketID is the document's own storage bucket, loaded from metadata and
	// passed to BlobStore.Put so each snapshot is persisted into the document's
	// own bucket (not a single flat platform bucket). Empty in standalone /
	// no-metadata mode, where the BlobStore falls back to its configured bucket.
	bucketID string
	// maxConns is the room's effective connection cap. Today it is the configured
	// fallback (RoomConfig.Limits.MaxConnsPerRoom); per-document refinement from the
	// document's maxCollaborators (carried on the bus metadata contract) is not yet
	// wired into the join path (T014 follow-up). Zero disables the cap.
	maxConns int

	// contributors is the set of actor ids that mutated the document in the
	// current contribution window; flushed and reset on the window tick (T013).
	contributors map[string]struct{}

	// onReleased is invoked once, on the run loop, after the room has drained
	// and persisted, so the Manager can drop it from its registry.
	onReleased func()

	// cancelSub tears down the ClusterBroadcaster subscription on release. It is
	// a no-op for the in-memory (single-pod) broadcaster.
	cancelSub func()

	// ctx is the room-lifetime context every backend call on the run loop derives
	// from (authZ eval, persist, purge, peer publish). It is cancelled exactly once,
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

// opCtx returns a timeout-bounded context for a single backend call made on the
// run loop (authZ eval, persist, purge, publish), derived from the room-lifetime
// context. The returned cancel MUST be called (defer) to release the timer. The
// timeout bounds a slow/hung backend so it cannot stall the single-writer loop;
// the parent ctx cancellation unblocks the call immediately on room release.
func (r *Room) opCtx() (context.Context, context.CancelFunc) {
	timeout := r.cfg.BackendTimeout
	if timeout <= 0 {
		timeout = defaultBackendTimeout
	}
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

// reEvaluateNow re-evaluates members under a bounded, room-scoped context so a
// slow/hung authZ backend cannot wedge the loop.
func (r *Room) reEvaluateNow() {
	ctx, cancel := r.opCtx()
	defer cancel()
	r.reEvaluateMembers(ctx)
}

// handleReEvaluate runs a re-evaluation and re-arms the idle timer if it emptied
// the room. A re-evaluation can disconnect members whose read access was revoked;
// if that left the room empty, the idle timer must be re-armed so the now-empty
// room is released (and its goroutine reclaimed) rather than leaking — mirroring
// cmdLeave.
func (r *Room) handleReEvaluate(armIdle func()) {
	r.reEvaluateNow()
	if len(r.members) == 0 {
		armIdle()
	}
}

// flushContributionNow flushes the contribution window under a bounded,
// room-scoped context (the bus Contributor call must not stall the loop).
func (r *Room) flushContributionNow() {
	ctx, cancel := r.opCtx()
	defer cancel()
	r.flushContribution(ctx)
}

// purgeNow runs the owner-delete cascade under a bounded, room-scoped context so a
// slow/hung blob/metadata backend cannot wedge the loop.
func (r *Room) purgeNow() error {
	ctx, cancel := r.opCtx()
	defer cancel()
	return r.purge(ctx)
}

// enqueueDeadline backstops a producer blocked on a full command channel (002
// FR-008): the loop stays drained because every handler is bounded, so this rarely
// fires, but a producer must never wait forever on a wedged loop.
const enqueueDeadline = 30 * time.Second

// enqueue submits a command to the run loop, returning false if the room has torn
// down or the deadline backstop elapses (so producers never block forever on a full
// channel).
func (r *Room) enqueue(cmd command) bool {
	return r.enqueueCtx(context.Background(), cmd)
}

// enqueueCtx submits a command to the run loop, returning false if the room has torn
// down (state left Active) OR the producer's context/deadline elapses before a
// buffer slot frees. The state check refuses new work BEFORE done is closed, so a
// tearing-down room rejects producers early and Join/Purge retry into a fresh room.
func (r *Room) enqueueCtx(ctx context.Context, cmd command) bool {
	if !r.lc.is(stateActive) {
		return false
	}
	// Fast path: an immediate send when the buffer has space — the common case, no timer.
	select {
	case r.commands <- cmd:
		return true
	case <-r.done:
		return false
	default:
	}
	// Slow path: the command buffer is full. Bounded-block so a producer (Forward /
	// Leave / ReEvaluate) is never wedged behind a stuck loop — it gives up at the
	// caller's ctx (e.g. the lifecycle handler timeout) or the deadline backstop.
	t := time.NewTimer(enqueueDeadline)
	defer t.Stop()
	select {
	case r.commands <- cmd:
		return true
	case <-r.done:
		return false
	case <-ctx.Done():
		return false
	case <-t.C:
		return false
	}
}

// RoomConfig carries the per-room tunables (R7 save cadence, idle release).
// Values come from configuration; the defaults are standalone-friendly.
type RoomConfig struct {
	// SaveDebounce is the time from the first dirty mutation until a snapshot
	// is persisted (R7; memo/whiteboard ~500ms default, configurable). The timer
	// is armed once per clean→dirty cycle (on the first edit after a save) and
	// fires once, bounding the staleness window regardless of edit frequency.
	SaveDebounce time.Duration
	// IdleTimeout releases an empty room after this long with no members. Zero
	// releases immediately when the last member leaves.
	IdleTimeout time.Duration
	// SendBuffer is the per-connection outbound queue depth the adapter uses.
	SendBuffer int
	// BlobKind is the configured blob backend, persisted in each saved metadata
	// row so a document rehydrates from the right backend (T005.6). Empty
	// defaults to inline.
	BlobKind model.BlobStoreKind

	// Limits carries the configurable enforcement bounds (FR-024, epic R9).
	Limits Limits
	// CollaboratorInactivity downgrades an idle collaborator to viewer after this
	// long with no mutation (FR-014, whiteboard parity). Zero disables the
	// downgrade. Reset on any client mutation.
	CollaboratorInactivity time.Duration
	// ContributionWindow is the flush cadence for the north-star contribution
	// metric/event: the set of actors that contributed in the window is emitted
	// then reset (FR-014). Zero disables contribution flushing.
	ContributionWindow time.Duration
	// BackendTimeout bounds each backend call made on the room's single-writer run
	// loop (authZ evaluation, snapshot persist, owner-delete purge, cross-pod
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

	defaultMaxDocBytes             = 30 << 20 // 30 MiB
	defaultMaxConnsPerRoom         = 50
	defaultUpdateRatePerSec        = 50
	defaultCollaboratorInactivity  = 120 * time.Second
	defaultContributionWindowEvery = 60 * time.Second
	// defaultBackendTimeout bounds each backend call on the run loop (authZ,
	// persist, purge, publish) so a hung backend cannot wedge the single-writer
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

// DefaultRoomConfig is the Wave-1 standalone default cadence, with the Wave-3
// limit/presence defaults (epic R9, OPEN-4) layered on.
func DefaultRoomConfig() RoomConfig {
	return RoomConfig{
		SaveDebounce:           500 * time.Millisecond,
		IdleTimeout:            30 * time.Second,
		SendBuffer:             64,
		BlobKind:               model.BlobStoreInline,
		Limits:                 DefaultLimits(),
		CollaboratorInactivity: defaultCollaboratorInactivity,
		ContributionWindow:     defaultContributionWindowEvery,
		BackendTimeout:         defaultBackendTimeout,
	}
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
	blobKind := cfg.BlobKind
	if blobKind == "" {
		blobKind = model.BlobStoreInline
	}
	if deps.Contributor == nil {
		deps.Contributor = noopContributor{}
	}
	if cfg.BackendTimeout <= 0 {
		cfg.BackendTimeout = defaultBackendTimeout
	}
	registry := deps.Registry
	if registry == nil {
		// A lone room owns exactly one document, so a private registry is
		// semantically correct here; Manager supplies a shared one so concurrent
		// opens of the same document coalesce onto a single materialization.
		registry = memory.NewRegistry()
	}
	deps.Registry = registry
	// The registry coalesces concurrent cache misses for the same document onto ONE
	// open, which is what makes first-open restore exactly-once by construction
	// rather than guarded by a racing emptiness check (FR-004a/b).
	handle, err := registry.Acquire(ctx, backend.DocumentID(id), func(context.Context) (*ycrdt.Doc, error) {
		return newRoomDoc(string(id)), nil
	})
	if err != nil {
		return nil, fmt.Errorf("acquiring document: %w", err)
	}
	doc := handle.Doc()
	// ycrdt.NewAwareness seeds the doc-local empty client state for the server's own
	// client id; left in place, awarenessSnapshot would emit a synthetic presence
	// entry for the server on the first join. The server holds no presence, so clear
	// the local state immediately — the zero Object is the cleared/null state
	// (Object is a struct, so SetLocalState(ycrdt.Object{}) removes the entry, not a
	// nil) — and a fresh room then reports zero awareness states until a real client
	// announces one.
	awareness := ycrdt.NewAwareness(doc)
	if err := awareness.SetLocalState(ycrdt.Object{}); err != nil {
		// The core surfaces this now; a failure here means the server would carry a
		// phantom local awareness entry, so it is worth seeing rather than dropping.
		logger.Warn("clearing server local awareness state failed", zap.Error(err))
	}
	// The room-lifetime context: every backend call on the run loop derives from
	// it, and it is cancelled on release (finish) so a hung call unblocks at
	// teardown. It is decoupled from any request lifetime (the caller already
	// passes a context.WithoutCancel(reqCtx)).
	roomCtx, cancel := context.WithCancel(ctx)
	r := &Room{
		id:           id,
		content:      content,
		doc:          doc,
		awareness:    awareness,
		deps:         deps,
		cfg:          cfg,
		metrics:      metrics,
		logger:       logger,
		commands:     make(chan command, 256),
		peerUpdates:  make(chan peerUpdate, 256),
		done:         make(chan struct{}),
		members:      make(map[connID]roomMember),
		blobKind:     blobKind,
		maxConns:     cfg.Limits.MaxConnsPerRoom,
		contributors: make(map[string]struct{}),
		ctx:          roomCtx,
		cancel:       cancel,
		handle:       handle,
	}

	// Bound the materialization I/O (metadata load + blob fetch) so a hung backend
	// cannot park the first-connect cohort indefinitely — the run loop's per-call
	// opCtx bound is otherwise applied only after the room starts (002 FR-006/FR-010).
	loadCtx, loadCancel := r.opCtx()
	err = r.loadSnapshot(loadCtx)
	loadCancel()
	if err != nil {
		cancel()
		return nil, err
	}

	// Apply the document-type convention to a freshly created (empty) doc so the
	// root shared type exists with the right shape (T010). Use r.content, NOT the
	// `content` handshake parameter: loadSnapshot has already corrected r.content to
	// the PERSISTED meta.ContentType (the persisted type wins per the documented
	// contract), so seeding the convention off the stale handshake value would, for a
	// document pre-registered as whiteboard but opened by a client that omits ?type=
	// (which the WS adapter defaults to memo), materialize the MEMO root — a spurious
	// Y.XmlFragment "default" instead of the whiteboard roots (elements/files/
	// appState). That is a durable wrong-type root that defeats applyConvention's
	// anti-race guarantee; persist() already keys off r.content, so only the
	// convention was inconsistent.
	applyConvention(doc, r.content)

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
	cancelSub, err := deps.Broadcaster.Subscribe(roomCtx, id, func(payload []byte, ephemeral bool) {
		select {
		case r.peerUpdates <- peerUpdate{data: payload, ephemeral: ephemeral}:
		case <-roomCtx.Done():
			// Teardown cancelled roomCtx: drop this peer delta rather than block the
			// subscribe goroutine. Acceptable — the ORIGINATING pod keeps its doc dirty
			// and persists it, and the CRDT re-merges on next load (self-healing); a
			// draining pod's final snapshot is not a relay guarantee (FR-015).
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}
	r.cancelSub = cancelSub

	// Materialized and wired — the room is now ready to serve.
	r.lc.activate()
	return r, nil
}

// loadSnapshot lazily rehydrates the authoritative doc from the latest persisted
// snapshot (US2/US5 no-regression): it reads the metadata index for the blob
// pointer, fetches the v2-encoded bytes, and applies them. When no live snapshot
// exists yet — a freshly-created document whose content the server persisted but
// has no collaboration snapshot (no ContentPointer / no blob) — it falls back to
// the first-open SEED, materializing the doc from the content the server
// delivered on collaboration-fetch (Metadata.SeedContent, R4/US1) so the first
// opener sees the creation content rather than an empty editor (FR-003). A
// document with neither a snapshot nor a seed is a fresh empty room (FR-010), not
// an error.
func (r *Room) loadSnapshot(ctx context.Context) error {
	meta, err := r.deps.Metadata.Load(ctx, r.id)
	if err != nil {
		if isNotFound(err) {
			return nil // no metadata row — fresh document, nothing to load or seed.
		}
		return err
	}

	r.version = meta.Version
	r.pointer = meta.ContentPointer
	r.policyID = meta.AuthorizationPolicyID
	r.ownerRef = meta.OwnerRef
	r.bucketID = meta.StorageBucketID
	if meta.ContentType != "" {
		r.content = meta.ContentType
	}
	if meta.BlobStore != "" {
		// Record the backend the document was last saved to so subsequent saves
		// re-persist it in the metadata row (the row stays truthful about where the
		// snapshot lives). Note this does not re-route the read below: r.deps.Blob
		// is the single adapter selected at startup, so a running config whose
		// BLOB_STORE differs from meta.BlobStore must point that adapter at the same
		// backing store to rehydrate (T005.6).
		r.blobKind = meta.BlobStore
	}

	// Load the document's stored state. ErrNotFound means it has never been
	// saved — seed from the row's create-time content if any. ErrCorrupt means the
	// index says state EXISTS but it could not be read; that must fail
	// materialization rather than fall back to seeding, because seeding would
	// resurrect stale content and the next save would overwrite the last good
	// state with it (FR-014).
	cp, err := r.deps.Checkpoint.LoadCheckpoint(ctx, backend.DocumentID(r.id))
	switch {
	case err == nil:
	case errors.Is(err, persistence.ErrNotFound):
		r.seedFromContent(meta.SeedContent)
		return nil
	default:
		return fmt.Errorf("loading stored state for %s: %w", r.id, err)
	}

	// The stored state is a full v2 update; applying it with a non-connection,
	// peer-flagged origin means the update observer fans it to all members
	// (there are none yet at load time) without re-publishing it to the bus —
	// rehydration is local state, not a new edit to broadcast. Stored state is
	// authoritative: any SeedContent on the row is a stale create-time bootstrap
	// and is deliberately ignored (re-applying it would resurrect old content).
	if err := ycrdt.ApplyUpdateV2(r.doc, cp.Update, updateOrigin{src: 0, peer: true}); err != nil {
		return fmt.Errorf("applying stored state: %w", err)
	}
	r.docBytes = len(cp.Update)
	return nil
}

// seedFromContent materializes a never-yet-saved document's Y.Doc from the
// content the server delivered on collaboration-fetch (Metadata.SeedContent, R4).
// The bytes are a full Yjs-V2 state for both document types — memo (the rich-text
// snapshot) and whiteboard (the scene snapshot the binding produced server-side)
// — so both apply via ApplyUpdateV2. It runs during materialization, before the
// update observer is wired (so it does not fan out or re-publish), and marks the
// room dirty so the FIRST save promotes the seed into a real per-document snapshot
// (ContentPointer set) — after which the blob is the source of truth and the seed
// is never consulted again (T004). A nil/empty seed is a no-op: the room opens
// empty and editable (FR-010).
func (r *Room) seedFromContent(content []byte) {
	if len(content) == 0 {
		return
	}
	if err := ycrdt.ApplyUpdateV2(r.doc, content, updateOrigin{src: 0, peer: true}); err != nil {
		// Do NOT mark dirty on a failed seed: promoting it would persist an EMPTY
		// document as this document's first real snapshot, destroying the content the
		// seed was carrying. Leave the room clean so the stored content is retried.
		r.logger.Error("seeding room from stored content failed; leaving room unseeded",
			zap.String("doc", string(r.id)), zap.Error(err))
		return
	}
	// Mark dirty so the seed is persisted as the document's first real snapshot.
	// Unlike a loaded snapshot (which already has a ContentPointer and stays
	// clean), the seed has no blob yet; promoting it on first save means
	// subsequent opens load the blob instead of re-seeding. seededPending tells
	// the run loop to arm the save debounce at start so the promotion happens
	// promptly rather than only on the next edit or idle release.
	r.dirty = true
	r.seededPending = true
}

// run is the room's single goroutine. It owns the Y.Doc and the member registry,
// draining commands until closed and managed by the debounce/idle timers plus the
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
			r.teardown(nil)
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

	// A room materialized from the first-open seed has unpersisted content but no
	// edit to arm the debounce; promote the seed to a real snapshot on the normal
	// cadence so it is durable promptly, without waiting for the first edit or the
	// idle release (T004). With SaveDebounce disabled, the seed is persisted by the
	// save-on-release path as before.
	if r.seededPending {
		armSave()
	}

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

		case <-r.handle.Done():
			r.teardownInvalidated()
			return

		case <-saveTimer.C:
			r.persistNow()

		case <-idleTimer.C:
			if r.releaseIfEmpty() {
				return
			}

		case <-sweepTimer.C:
			r.sweepInactive()
			sweepTimer.Reset(sweepEvery)

		case <-contribTimer.C:
			r.flushContributionNow()
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

// handleMessageCmd applies an inbound client frame and re-arms the timers: the save
// debounce if it mutated the doc, and the idle timer if a rate/size-limit self-
// disconnect inside handleMessage dropped the last member (002 FR-011 — so an
// emptied room is released, not leaked). Extracted from dispatch to keep its
// branching low.
func (r *Room) handleMessageCmd(cmd command, armSave, armIdle func()) {
	if r.handleMessage(cmd.src, cmd.data) {
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
		res := r.handleJoin(cmd.conn, cmd.identity)
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

	case cmdPersist:
		r.persistNow()

	case cmdReEvaluate:
		r.handleReEvaluate(armIdle)

	case cmdPurge:
		r.teardown(func() {
			err := r.purgeNow()
			if cmd.done2 != nil {
				cmd.done2 <- err
			}
		})
		return false

	case cmdClose:
		r.teardown(func() {
			r.persistNow()
			r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomClosed})
		})
		return false
	}
	return true
}

// handleJoin registers a connection after enforcing the connection cap (FR-024)
// and resolving its collaborator mode via per-document authZ (T014). It returns
// the joiner's id and the initial frames it must send (SyncStep1 + the current
// awareness snapshot so the newcomer sees existing presence) and a `read-only-
// state` control for a viewer; or an error when the room is full (ErrRoomFull),
// read is denied (ErrForbidden), or authZ failed closed.
func (r *Room) handleJoin(c Conn, identity model.Identity) joinResult {
	if r.maxConns > 0 && len(r.members) >= r.maxConns {
		return joinResult{err: ErrRoomFull}
	}

	joinCtx, cancel := r.opCtx()
	mode, err := r.resolveMode(joinCtx, identity)
	cancel()
	if err != nil {
		return joinResult{err: err}
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
			Reason:   readOnlyReasonForIdentity(identity),
		})
		if ctrl != nil {
			frames = append(frames, ctrl)
		}
	}

	r.broadcastControl(model.ControlMessage{
		Kind:  model.ControlRoomUserChange,
		Users: len(r.members),
	})

	return joinResult{id: id, frames: frames}
}

// resolveMode decides a joiner's collaborator mode from per-document authZ
// (T014). It first checks read access (a clean deny → ErrForbidden), then
// update-content (granted → collaborator, denied → viewer). Any authZ port error
// is returned so the join fails closed (constitution §V). In open/standalone mode
// the AuthZ adapter grants everything, so every join is a collaborator.
func (r *Room) resolveMode(ctx context.Context, identity model.Identity) (model.CollaboratorMode, error) {
	read, err := r.deps.AuthZ.Evaluate(ctx, identity, r.id, model.PrivilegeRead)
	if err != nil {
		return "", fmt.Errorf("authorize read: %w", err)
	}
	if !read.Allowed {
		return "", ErrForbidden
	}
	write, err := r.deps.AuthZ.Evaluate(ctx, identity, r.id, model.PrivilegeUpdateContent)
	if err != nil {
		return "", fmt.Errorf("authorize update-content: %w", err)
	}
	if write.Allowed {
		return model.ModeCollaborator, nil
	}
	return model.ModeViewer, nil
}

// readOnlyReasonForIdentity maps a viewer's identity to its read-only reason
// code (OPEN-1): an anonymous connection is read-only because it is
// not-authenticated; an authenticated actor that was denied update-content is
// read-only because it has no-update-access. This preserves the granularity of
// today's read-only UX (the memo-footer readOnlyCode). An anonymous viewer
// surfaces two ways — an empty ActorID (open mode, AuthZ bypassed) OR the nil-UUID
// sentinel (oidc mode maps a missing credential to model.AnonymousIdentity(),
// whose ActorID is ANONYMOUS_ACTOR_ID, which is NON-empty) — so both must map to
// not-authenticated, else an anonymous oidc viewer wrongly reports no-update-access.
func readOnlyReasonForIdentity(identity model.Identity) model.ReadOnlyReason {
	if identity.ActorID == "" || identity.ActorID == model.ANONYMOUS_ACTOR_ID {
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
// caller can (re)arm the save debounce. Sync messages are applied to the
// authoritative doc — whose update observer fans the delta to the other members.
// Awareness and ephemeral messages are fanned out but never touch the snapshot
// (FR-008).
//
// Before any handling it enforces the per-connection update rate (FR-024): a
// connection that breaches its token bucket is disconnected with a control
// message; other collaborators are unaffected.
func (r *Room) handleMessage(src connID, frame []byte) (mutated bool) {
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
		r.disconnect(src, "update rate exceeded")
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
			// Best-effort: a malformed awareness update can't update the room's snapshot,
			// but the raw frame is still fanned out so peers apply it against their own state.
			r.logger.Warn("applying awareness update failed", zap.Error(err))
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

	default:
		// Control is server→client only; ignore client-sent control/unknown
		// types (matches y-protocols leniency).
		return false
	}
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
// the run loop can arm the save debounce — only the originating pod persists, but
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
	r.applyUpdate(payload, updateOrigin{src: 0, peer: true})
	return r.dirty && !wasDirty
}

// applyUpdate is the SINGLE guarded chokepoint every doc-mutating update routes
// through (002 FR-005), so the MaxDocBytes budget covers EVERY entry point — local
// client writes AND cross-pod peer updates — not just one. It returns false WITHOUT
// applying iff a LOCAL write would exceed the cap (the caller then rejects the
// offender pre-commit, FR-024). A PEER write cannot be rejected without diverging
// from the pod that already accepted it, so an over-budget peer update is logged but
// applied; correctness then relies on a uniform MaxDocBytes across pods (a documented
// operational constraint).
func (r *Room) applyUpdate(update []byte, origin updateOrigin) bool {
	if r.applyWouldExceedMaxDocBytes(update) {
		if !origin.peer {
			return false
		}
		r.logger.Warn("peer update would exceed MaxDocBytes; applied to avoid cross-pod divergence (check for MaxDocBytes config skew)",
			zap.String("doc", string(r.id)))
	}
	if err := ycrdt.ApplyUpdate(r.doc, update, origin); err != nil {
		// The size verdict is unchanged (this bool reports the budget, not decode
		// validity) but a malformed update reaching the chokepoint is worth seeing:
		// dispatchSync rejects most of these earlier, so one arriving here means a
		// path bypassed inspection.
		r.logger.Warn("applying update failed", zap.String("doc", string(r.id)), zap.Error(err))
	}
	return true
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
// members. It returns whether the document was mutated so the run loop can arm
// the save debounce.
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
		r.disconnect(src, "document size limit exceeded")
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
	if m.actorID != "" {
		r.contributors[m.actorID] = struct{}{}
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
	// loadSnapshot's origin) likewise stays local.
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
	if err := r.deps.Broadcaster.Publish(ctx, r.id, frame, ephemeral); err != nil {
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

// persist debounces a full v2 snapshot to the blob store and upserts the index
// (R7). It is a no-op when nothing changed since the last save. On success it
// emits a `saved` control message to the room; on failure a `save-error`, and
// the room keeps serving from memory (the crash-loss window is one debounce
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
		DocumentID:  backend.DocumentID(r.id),
		Update:      snapshot,
		StateVector: vector,
	}); err != nil {
		r.onFlushFailed(err)
		return
	}

	// First save only: a store that addresses content by pointer (file-service)
	// created the document's file and recorded its pointer, so refresh the cached
	// row to pick it up. The pointer is stable thereafter. The flag makes this cost
	// one extra read per document LIFETIME rather than per flush — without it a
	// store that keeps no pointer at all (the in-process one) would re-read on
	// every single save, forever, and never find one.
	if !r.pointerChecked {
		r.pointerChecked = true
		if meta, lerr := r.deps.Metadata.Load(ctx, r.id); lerr == nil && meta.ContentPointer != "" {
			r.pointer = meta.ContentPointer
		}
	}

	newVersion := r.version + 1
	meta := model.Metadata{
		ID:                    r.id,
		ContentType:           r.content,
		Version:               newVersion,
		ContentPointer:        r.pointer,
		BlobStore:             r.blobKind,
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
	// The seed (if any) is now real stored state; subsequent opens load it and
	// never re-seed (T004).
	r.seededPending = false
	r.onFlushSucceeded()
	r.metrics.SnapshotSaved()
	r.broadcastControl(model.ControlMessage{Kind: model.ControlSaved, Version: r.version})
}

// teardown runs the single, ordered room-release sequence (002 FR-013) — the ONE
// place teardown ordering lives, so it cannot be mis-sequenced per call site: stop
// accepting (beginTeardown) → flush (the caller's final persist/purge/broadcast, may
// be nil) → cancel the room context (unblocking the decoupled fan-out) → tear down
// the fan-out subscription → close(done) →
// notify the Manager → mark Closed. beginTeardown is the idempotent guard: only the
// first caller runs the sequence; the rest return immediately (no double close, no
// re-notify). Runs on the run-loop goroutine.
// armSaveTimer (re)arms the save debounce, unless debouncing is disabled — in
// which case the save-on-release path persists instead.
func (r *Room) armSaveTimer(saveTimer *time.Timer) {
	if r.cfg.SaveDebounce <= 0 {
		return
	}
	stopTimer(saveTimer)
	saveTimer.Reset(r.cfg.SaveDebounce)
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

// teardownInvalidated tears the room down after the registry poisoned this
// document's generation. It does NOT flush: the in-memory copy may have diverged
// from durable state, and writing a document of doubtful integrity over good
// stored content is precisely the failure the teardown-flush matrix exists to
// prevent (FR-011a). New acquisitions reload from persistence.
func (r *Room) teardownInvalidated() {
	r.logger.Warn("document generation invalidated; tearing down without persist",
		zap.String("doc", string(r.id)))
	r.metrics.GenerationInvalidated()
	r.teardown(nil)
}

// releaseIfEmpty releases an idle room that has no members left, reporting
// whether the run loop should stop. An idle release DOES flush: the document is
// believed good, so idling out must not silently cost a window of edits
// (FR-011a).
func (r *Room) releaseIfEmpty() bool {
	if len(r.members) != 0 {
		return false
	}
	r.teardown(r.persistNow)
	return true
}

func (r *Room) teardown(flush func()) {
	if !r.lc.beginTeardown() {
		return
	}
	if flush != nil {
		flush()
	}
	// Balance the connection gauge for members still attached at teardown.
	// cmdClose/cmdPurge tear the room down without each client traversing the
	// per-connection Leave path, so their ConnOpened would otherwise never be
	// matched by a ConnClosed — leaking connections_active upward by the member
	// count. dropMember (the only other ConnClosed caller) deletes from r.members
	// before it counts, so any already-closed member is absent here: no double
	// count. Runs on the run-loop goroutine (single-writer), so the walk is safe.
	for id := range r.members {
		delete(r.members, id)
		r.metrics.ConnClosed()
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
	if r.handle != nil {
		r.handle.Release()
	}
	close(r.done)
	if r.onReleased != nil {
		r.onReleased()
	}
	r.lc.finishDraining()
}

// finish releases the room with no extra flush (the caller already persisted/purged).
// teardown owns the ordering; this is the entry used by the idle path and tests.
func (r *Room) finish() { r.teardown(nil) }

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
