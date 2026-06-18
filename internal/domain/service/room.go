package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
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
	// ephemeral distinguishes a peer-pod doc payload (false → doc:{id}) from a
	// peer-pod awareness/ephemeral payload (true → awareness:{id}) for cmdPeer.
	ephemeral bool
	done      chan joinResult
	// done2 returns the result of a cmdPurge run on the room loop (T015).
	done2 chan error
}

type cmdKind uint8

const (
	cmdJoin cmdKind = iota
	cmdLeave
	cmdMessage
	cmdPeer
	cmdPersist
	cmdClose
	// cmdPurge runs the owner-delete cascade on the run loop: disconnect clients
	// (room-closed), purge metadata + blob, and release the room (T015).
	cmdPurge
	// cmdReEvaluate re-runs per-document authZ for connected members on the run
	// loop (lifecycle document.access_changed, T014).
	cmdReEvaluate
)

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
	deps      Deps
	cfg       RoomConfig
	metrics   Metrics
	logger    *zap.Logger

	commands chan command
	// done is closed by the run loop on teardown so producers (Forward/Leave)
	// never block on commands after the room is gone.
	done    chan struct{}
	members map[connID]roomMember
	nextID  connID

	// dirty is set when the doc changed since the last persisted snapshot;
	// it drives the debounce timer and the final save-on-release.
	dirty   bool
	version int
	pointer string
	// blobKind is the configured blob backend persisted in the metadata row so a
	// document rehydrates from the right backend regardless of running config
	// (data-model.md BlobStore; T005.6).
	blobKind model.BlobStoreKind
	// policyID is the document's Alkemio authorization policy id (OPEN-1),
	// loaded from metadata and re-persisted on save so the authzeval adapter can
	// evaluate against it (T006).
	policyID string
	// maxConns is the room's effective connection cap: the document metadata's
	// maxCollaborators when known, else the configured fallback (T014). Zero
	// disables the cap.
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

	// released guards against double release notification.
	released atomic.Bool
}

// enqueue submits a command to the run loop, returning false if the room has
// already torn down (so producers never block on a dead room's full channel).
func (r *Room) enqueue(cmd command) bool {
	select {
	case <-r.done:
		return false
	default:
	}
	select {
	case r.commands <- cmd:
		return true
	case <-r.done:
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
}

// Limits are the configurable per-room enforcement bounds (FR-024, epic R9
// defaults). A breach disconnects only the offending connection with a control
// message; other collaborators are unaffected (constitution §V).
type Limits struct {
	// MaxDocBytes rejects an update that would grow the encoded snapshot past
	// this size (epic R9 default ~32 MiB). Zero disables the size check.
	MaxDocBytes int
	// MaxConnsPerRoom caps concurrent connections to a room — sourced from the
	// document metadata's maxCollaborators when known, else this fallback (epic
	// R9 default 50). Zero disables the connection cap.
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
	defaultMaxDocBytes             = 32 << 20 // 32 MiB
	defaultMaxConnsPerRoom         = 50
	defaultUpdateRatePerSec        = 50
	defaultCollaboratorInactivity  = 120 * time.Second
	defaultContributionWindowEvery = 60 * time.Second
)

// DefaultLimits are the epic R9 defaults (all config-tunable, OPEN-4).
func DefaultLimits() Limits {
	return Limits{
		MaxDocBytes:      defaultMaxDocBytes,
		MaxConnsPerRoom:  defaultMaxConnsPerRoom,
		UpdateRatePerSec: defaultUpdateRatePerSec,
		UpdateBurst:      defaultUpdateRatePerSec,
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
	doc := newRoomDoc(string(id))
	r := &Room{
		id:           id,
		content:      content,
		doc:          doc,
		awareness:    ycrdt.NewAwareness(doc),
		deps:         deps,
		cfg:          cfg,
		metrics:      metrics,
		logger:       logger,
		commands:     make(chan command, 256),
		done:         make(chan struct{}),
		members:      make(map[connID]roomMember),
		blobKind:     blobKind,
		maxConns:     cfg.Limits.MaxConnsPerRoom,
		contributors: make(map[string]struct{}),
	}

	if err := r.loadSnapshot(ctx); err != nil {
		return nil, err
	}

	// Apply the document-type convention to a freshly created (empty) doc so
	// the root shared type exists with the right shape (T010).
	applyConvention(doc, content)

	// Observe applied updates and fan them out. The observer runs synchronously
	// inside ApplyUpdate on the run-loop goroutine, so reading members here is
	// race-free (single writer).
	doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		r.onDocUpdate(v...)
	}))

	// Subscribe to peer-pod fan-out (R4). The handler runs off the run loop, so
	// it enqueues a cmdPeer onto the single-writer loop rather than touching the
	// doc directly. The in-memory broadcaster's Subscribe is a no-op that never
	// fires the handler, so single-pod deployments pay nothing here.
	cancel, err := deps.Broadcaster.Subscribe(ctx, id, func(payload []byte, ephemeral bool) {
		r.enqueue(command{kind: cmdPeer, data: payload, ephemeral: ephemeral})
	})
	if err != nil {
		return nil, err
	}
	r.cancelSub = cancel

	return r, nil
}

// loadSnapshot lazily rehydrates the authoritative doc from the latest persisted
// snapshot (US2/US5 no-regression): it reads the metadata index for the blob
// pointer, fetches the v2-encoded bytes, and applies them. A missing document is
// a fresh room (the metadata/blob rows are written on first save), not an error.
func (r *Room) loadSnapshot(ctx context.Context) error {
	meta, err := r.deps.Metadata.Load(ctx, r.id)
	if err != nil {
		if isNotFound(err) {
			return nil // fresh document — nothing to load.
		}
		return err
	}

	r.version = meta.Version
	r.pointer = meta.ContentPointer
	r.policyID = meta.AuthorizationPolicyID
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

	data, err := r.deps.Blob.Get(ctx, meta.ContentPointer)
	if err != nil {
		if isNotFound(err) {
			return nil // index row without a blob yet — treat as empty.
		}
		return err
	}

	// The snapshot is a full v2 update; applying it with a non-connection,
	// peer-flagged origin means the update observer fans it to all members
	// (there are none yet at load time) without re-publishing it to the bus —
	// rehydration is local state, not a new edit to broadcast.
	ycrdt.ApplyUpdateV2(r.doc, data, updateOrigin{src: 0, peer: true})
	return nil
}

// run is the room's single goroutine. It owns the Y.Doc and the member registry,
// draining commands until closed and managed by the debounce/idle timers plus the
// Wave-3 presence tickers (inactivity sweep, contribution-window flush). All
// Y.Doc, awareness, and member mutation happens here, making the room the lone
// writer.
func (r *Room) run() {
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

	armSave := func() {
		if r.cfg.SaveDebounce <= 0 {
			return
		}
		stopTimer(saveTimer)
		saveTimer.Reset(r.cfg.SaveDebounce)
	}
	armIdle := func() {
		stopTimer(idleTimer)
		if r.cfg.IdleTimeout <= 0 {
			// Release immediately on the next loop tick via a zero-length timer.
			idleTimer.Reset(time.Nanosecond)
			return
		}
		idleTimer.Reset(r.cfg.IdleTimeout)
	}

	for {
		select {
		case cmd := <-r.commands:
			if !r.dispatch(cmd, armSave, armIdle, idleTimer) {
				return
			}

		case <-saveTimer.C:
			r.persist(context.Background())

		case <-idleTimer.C:
			if len(r.members) == 0 {
				r.persist(context.Background())
				r.finish()
				return
			}

		case <-sweepTimer.C:
			r.sweepInactive()
			sweepTimer.Reset(sweepEvery)

		case <-contribTimer.C:
			r.flushContribution(context.Background())
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

// dispatch handles one command on the run-loop goroutine, arming the save/idle
// timers as needed. It returns false when the room must tear down (cmdClose), so
// run can exit. Splitting this out of run keeps each function's branching low.
func (r *Room) dispatch(cmd command, armSave, armIdle func(), idleTimer *time.Timer) (keepRunning bool) {
	switch cmd.kind {
	case cmdJoin:
		stopTimer(idleTimer)
		cmd.done <- r.handleJoin(cmd.conn, cmd.identity)

	case cmdLeave:
		r.handleLeave(cmd.src)
		if len(r.members) == 0 {
			armIdle()
		}

	case cmdMessage:
		if r.handleMessage(cmd.src, cmd.data) {
			armSave()
		}

	case cmdPeer:
		if r.handlePeer(cmd.data, cmd.ephemeral) {
			armSave()
		}

	case cmdPersist:
		r.persist(context.Background())

	case cmdReEvaluate:
		r.reEvaluateMembers(context.Background())

	case cmdPurge:
		err := r.purge(context.Background())
		if cmd.done2 != nil {
			cmd.done2 <- err
		}
		r.finish()
		return false

	case cmdClose:
		r.persist(context.Background())
		r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomClosed})
		r.finish()
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

	mode, err := r.resolveMode(context.Background(), identity)
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
	// (the collaborator default needs no control — editing is the baseline).
	if mode == model.ModeViewer {
		if ctrl := encodeControl(model.ControlMessage{Kind: model.ControlReadOnlyState, ReadOnly: true}); ctrl != nil {
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

// handleLeave drops a connection and tells the remaining members the count
// changed. Awareness eviction for the departed client is not forced here (see
// dropMember); peers converge its vanished cursor via the client's own
// local-state-clear on a clean close and via awareness TTL otherwise, with
// explicit server-side eviction deferred to presence (T013).
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
func (r *Room) forcedAwarenessRemoval(clientID ycrdt.Number) []byte {
	meta := r.awareness.Meta[clientID]
	clock := 0
	if meta != nil {
		if c, ok := meta["clock"].(ycrdt.Number); ok {
			clock = c
		}
	}
	delete(r.awareness.States, clientID)
	r.awareness.Meta[clientID] = ycrdt.Object{
		"clock":       clock + 1,
		"lastUpdated": ycrdt.GetUnixTime(),
	}
	update := ycrdt.EncodeAwarenessUpdate(r.awareness, []ycrdt.Number{clientID}, nil)
	return protocol.EncodeAwarenessUpdateMessage(update)
}

// dropMember removes a member from the registry, evicts its awareness, and
// decrements the connection gauge. It returns false when the member was already
// gone (idempotent).
func (r *Room) dropMember(id connID) bool {
	m, ok := r.members[id]
	if !ok {
		return false
	}
	r.evictAwareness(m)
	delete(r.members, id)
	r.metrics.ConnClosed()
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
	in := bytes.NewBuffer(frame)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		r.logger.Warn("dropping malformed frame", zap.Error(err))
		return false
	}

	if !r.allowRate(src) {
		r.disconnect(src, "update rate exceeded")
		return false
	}

	switch model.WireMessageType(msgType) {
	case model.WireSync:
		return r.handleSync(src, payload)

	case model.WireAwareness:
		// Learn the member's y-awareness client id (for server-forced eviction on
		// leave), then apply to the room's awareness (so a late joiner gets a
		// snapshot), fan the raw frame out to local members, and publish it to peer
		// pods on the awareness:{id} channel. Never persisted (FR-008).
		r.trackAwarenessID(src, payload)
		ycrdt.ApplyAwarenessUpdate(r.awareness, payload, updateOrigin{src: src})
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
	ycrdt.ApplyUpdate(r.doc, payload, updateOrigin{src: 0, peer: true})
	return r.dirty && !wasDirty
}

// applyPeerEphemeral applies a peer-pod awareness/ephemeral frame: an awareness
// update is merged into the room's awareness (so a late joiner on this pod sees
// the remote cursor) and fanned to local members; a custom ephemeral frame is
// fanned to local members. Neither is persisted nor re-published.
func (r *Room) applyPeerEphemeral(frame []byte) {
	in := bytes.NewBuffer(frame)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		r.logger.Warn("dropping malformed peer frame", zap.Error(err))
		return
	}
	switch model.WireMessageType(msgType) {
	case model.WireAwareness:
		ycrdt.ApplyAwarenessUpdate(r.awareness, payload, updateOrigin{src: 0, peer: true})
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

	if outcome.applied {
		// A collaborator just wrote: record activity (resets the inactivity
		// downgrade timer), record the actor for the contribution window, and
		// enforce the max-doc-size limit (FR-024).
		r.recordActivity(src)
		if r.cfg.Limits.MaxDocBytes > 0 && r.docByteSize() > r.cfg.Limits.MaxDocBytes {
			r.disconnect(src, "document size limit exceeded")
		}
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

// docByteSize is the encoded v2 size of the authoritative doc, the measure the
// max-doc-size limit is enforced against (the persisted snapshot size, FR-024).
func (r *Room) docByteSize() int {
	return len(ycrdt.EncodeStateAsUpdateV2(r.doc, nil))
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
	if err := r.deps.Broadcaster.Publish(context.Background(), r.id, frame, ephemeral); err != nil {
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

	snapshot := ycrdt.EncodeStateAsUpdateV2(r.doc, nil)
	hint := r.pointer
	if hint == "" {
		hint = string(r.id) // first save: hint the document id (inline pointer == id).
	}

	pointer, err := r.deps.Blob.Put(ctx, hint, snapshot)
	if err != nil {
		r.logger.Error("snapshot blob put failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "blob put failed"})
		return
	}

	newVersion := r.version + 1
	meta := model.Metadata{
		ID:                    r.id,
		ContentType:           r.content,
		Version:               newVersion,
		ContentPointer:        pointer,
		BlobStore:             r.blobKind,
		AuthorizationPolicyID: r.policyID,
	}
	if err := r.deps.Metadata.Save(ctx, meta); err != nil {
		r.logger.Error("snapshot metadata save failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "metadata save failed"})
		return
	}

	r.pointer = pointer
	r.version = newVersion
	r.dirty = false
	r.metrics.SnapshotSaved()
	r.broadcastControl(model.ControlMessage{Kind: model.ControlSaved, Version: r.version})
}

// finish releases the room: it notifies the Manager exactly once so the registry
// drops it. The doc's observers are detached implicitly by dropping the room.
func (r *Room) finish() {
	if r.released.Swap(true) {
		return
	}
	if r.cancelSub != nil {
		r.cancelSub()
	}
	close(r.done)
	if r.onReleased != nil {
		r.onReleased()
	}
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
	return protocol.EncodeAwarenessUpdateMessage(update)
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
