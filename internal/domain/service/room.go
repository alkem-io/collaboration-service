package service

import (
	"bytes"
	"context"
	"encoding/json"
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
// matches no real connection and therefore fans out to everyone.
type updateOrigin struct{ src connID }

// command is a unit of work serialized onto the room's single run-loop
// goroutine. Routing every mutation through one goroutine makes the room the
// single writer to its authoritative Y.Doc (data-model.md "Room"), so no lock
// is needed around the CRDT core and -race stays clean.
type command struct {
	kind cmdKind
	src  connID
	conn Conn
	data []byte
	done chan joinResult
}

type cmdKind uint8

const (
	cmdJoin cmdKind = iota
	cmdLeave
	cmdMessage
	cmdPersist
	cmdClose
)

// joinResult is returned to a joining connection: its room-local id plus the
// initial frames (SyncStep1 + the current awareness state) it must send to
// start the handshake.
type joinResult struct {
	id     connID
	frames [][]byte
}

// roomMember is the room-side bookkeeping for one connection.
type roomMember struct {
	id   connID
	conn Conn
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

	// onReleased is invoked once, on the run loop, after the room has drained
	// and persisted, so the Manager can drop it from its registry.
	onReleased func()

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
}

// DefaultRoomConfig is the Wave-1 standalone default cadence.
func DefaultRoomConfig() RoomConfig {
	return RoomConfig{
		SaveDebounce: 500 * time.Millisecond,
		IdleTimeout:  30 * time.Second,
		SendBuffer:   64,
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
	doc := newRoomDoc(string(id))
	r := &Room{
		id:        id,
		content:   content,
		doc:       doc,
		awareness: ycrdt.NewAwareness(doc),
		deps:      deps,
		cfg:       cfg,
		metrics:   metrics,
		logger:    logger,
		commands:  make(chan command, 256),
		done:      make(chan struct{}),
		members:   make(map[connID]roomMember),
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
	if meta.ContentType != "" {
		r.content = meta.ContentType
	}

	data, err := r.deps.Blob.Get(ctx, meta.ContentPointer)
	if err != nil {
		if isNotFound(err) {
			return nil // index row without a blob yet — treat as empty.
		}
		return err
	}

	// The snapshot is a full v2 update; applying it with a nil/non-connection
	// origin means the update observer fans it to all members (there are none
	// yet at load time).
	ycrdt.ApplyUpdateV2(r.doc, data, updateOrigin{src: 0})
	return nil
}

// run is the room's single goroutine. It owns the Y.Doc and the member registry,
// draining commands until closed and managed by the debounce/idle timers. All
// Y.Doc mutation happens here, making the room the lone writer.
func (r *Room) run() {
	saveTimer := time.NewTimer(time.Hour)
	stopTimer(saveTimer)
	idleTimer := time.NewTimer(time.Hour)
	stopTimer(idleTimer)

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
			switch cmd.kind {
			case cmdJoin:
				stopTimer(idleTimer)
				cmd.done <- r.handleJoin(cmd.conn)

			case cmdLeave:
				r.handleLeave(cmd.src)
				if len(r.members) == 0 {
					armIdle()
				}

			case cmdMessage:
				if r.handleMessage(cmd.src, cmd.data) {
					armSave()
				}

			case cmdPersist:
				r.persist(context.Background())

			case cmdClose:
				r.persist(context.Background())
				r.broadcastControl(model.ControlMessage{Kind: model.ControlRoomClosed})
				r.finish()
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
		}
	}
}

// handleJoin registers a connection, returns its id and the initial frames it
// must send (SyncStep1 to drive the handshake + the current awareness snapshot
// so the newcomer sees existing presence), and notifies the room of the new
// participant count.
func (r *Room) handleJoin(c Conn) joinResult {
	r.nextID++
	id := r.nextID
	r.members[id] = roomMember{id: id, conn: c}
	r.metrics.ConnOpened()

	frames := [][]byte{protocol.EncodeSyncStep1(r.doc)}
	if aw := awarenessSnapshot(r.awareness); aw != nil {
		frames = append(frames, aw)
	}

	r.broadcastControl(model.ControlMessage{
		Kind:  model.ControlRoomUserChange,
		Users: len(r.members),
	})

	return joinResult{id: id, frames: frames}
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

// dropMember removes a member from the registry and decrements the connection
// gauge. It returns false when the member was already gone (idempotent).
//
// Awareness cleanup for a departed client is intentionally not forced here: the
// y-protocols awareness client id is the per-connection y client id carried in
// the client's own updates, which the room does not map to its room-local
// connID. Peers converge a vanished cursor via the client's explicit
// local-state-clear on a clean close and via awareness TTL otherwise — the
// y-websocket convention. Explicit server-side eviction lands with presence
// (T013).
func (r *Room) dropMember(id connID) bool {
	if _, ok := r.members[id]; !ok {
		return false
	}
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
func (r *Room) handleMessage(src connID, frame []byte) (mutated bool) {
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
		// Apply to the room's awareness (so a late joiner gets a snapshot) and
		// fan the raw frame out to peers. Never persisted (FR-008).
		ycrdt.ApplyAwarenessUpdate(r.awareness, payload, updateOrigin{src: src})
		r.broadcast(frame, src)
		return false

	case model.WireEphemeral:
		// Volatile whiteboard ephemerals (cursor/emoji/countdown): fan out
		// verbatim, drop on the floor otherwise. Never applied to the doc,
		// never persisted (FR-008, T009).
		r.broadcast(frame, src)
		return false

	default:
		// Control is server→client only; ignore client-sent control/unknown
		// types (matches y-protocols leniency).
		return false
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

	wasDirty := r.dirty
	var reply bytes.Buffer
	if err := r.dispatchSync(framed.Bytes(), &reply, src); err != nil {
		r.logger.Warn("sync dispatch failed", zap.Error(err))
		return false
	}

	if reply.Len() > 0 {
		r.sendTo(src, reply.Bytes())
	}

	// onDocUpdate flips dirty=true synchronously inside ApplyUpdate when the
	// message carried new structs; a SyncStep1 (reply-only) leaves it untouched.
	return r.dirty && !wasDirty
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
	var src connID
	if len(v) > 1 {
		if o, ok := v[1].(updateOrigin); ok {
			src = o.src
		}
	}
	r.dirty = true
	r.broadcast(protocol.EncodeUpdate(update), src)
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
	pointer := r.pointer
	if pointer == "" {
		pointer = string(r.id) // inline pointer == document id (data-model.md).
	}

	if err := r.deps.Blob.Put(ctx, pointer, snapshot); err != nil {
		r.logger.Error("snapshot blob put failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "blob put failed"})
		return
	}

	meta := model.Metadata{
		ID:             r.id,
		ContentType:    r.content,
		ContentPointer: pointer,
		// BlobStore is hardcoded to inline for Wave 1; T005 will thread the
		// configured store through once non-inline adapters land.
		BlobStore: model.BlobStoreInline,
	}
	if err := r.deps.Metadata.Save(ctx, meta); err != nil {
		r.logger.Error("snapshot metadata save failed", zap.String("doc", string(r.id)), zap.Error(err))
		r.metrics.SnapshotFailed()
		r.broadcastControl(model.ControlMessage{Kind: model.ControlSaveError, Error: "metadata save failed"})
		return
	}

	r.pointer = pointer
	r.version++
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
