package service

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/persistence"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// --- local fakes for the error-injection branches ---

// erroringSubHub is a hub.Hub whose Subscribe fails, to drive newRoom's
// subscription-error path (a pod that cannot subscribe to peer fan-out must fail
// materialization rather than silently run unsubscribed, missing every remote
// edit).
type erroringSubHub struct{}

func (erroringSubHub) Publish(context.Context, hub.Message) error { return nil }

func (erroringSubHub) Subscribe(context.Context, backend.DocumentID, backend.SourceID, hub.Handler) (hub.Subscription, error) {
	return nil, errors.New("subscribe failed")
}

func (erroringSubHub) Close() error { return nil }

// erroringPubHub is a hub.Hub whose Publish fails (Subscribe is a no-op), to
// drive publishToPeers' fan-out-failure path and the FanoutFailed metric.
type erroringPubHub struct{}

func (erroringPubHub) Publish(context.Context, hub.Message) error {
	return errors.New("publish failed")
}

func (erroringPubHub) Subscribe(context.Context, backend.DocumentID, backend.SourceID, hub.Handler) (hub.Subscription, error) {
	return noopSubscription{}, nil
}

func (erroringPubHub) Close() error { return nil }

// noopSubscription satisfies hub.Subscription for the fakes above.
type noopSubscription struct{}

func (noopSubscription) SourceID() backend.SourceID { return "" }
func (noopSubscription) Close() error               { return nil }

// --- limits.go ---

// TestNewTokenBucketDefaultsBurstToRate asserts that a non-positive burst
// defaults to the rate (so a misconfigured burst still admits a sensible burst of
// messages rather than zero, which would reject everything at a positive rate).
func TestNewTokenBucketDefaultsBurstToRate(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	// burst <= 0 with a positive rate → burst defaults to the rate (5).
	b := newTokenBucket(5, 0, clock)
	admitted := 0
	for i := 0; i < 10; i++ {
		if b.allow() {
			admitted++
		}
	}
	if admitted != 5 {
		t.Fatalf("burst defaulted to %d, want 5 (the rate)", admitted)
	}
}

// TestNewTokenBucketNilClockUsesWallClock asserts a nil clock falls back to
// time.Now (the bucket still admits its burst), so callers may omit a clock.
func TestNewTokenBucketNilClockUsesWallClock(t *testing.T) {
	b := newTokenBucket(10, 3, nil)
	if b.nowFunc == nil {
		t.Fatal("nil clock was not defaulted to a wall clock")
	}
	if !b.allow() {
		t.Fatal("a fresh bucket with a default clock should admit")
	}
}

// --- awareness_wire.go ---

// TestAwarenessBodyRejectsMalformed asserts a frame whose awareness payload is
// not a well-formed varUint8Array is rejected (returns false), so a corrupt
// awareness frame is dropped rather than misapplied to the room awareness.
//
// Restructured for the go-yjs port (FR-018a): the helper now takes the FULL
// framed message rather than the post-type payload, because the core's
// InspectMessage parses the frame. Every property the payload-level version
// asserted is preserved, and frame-level type validation is additionally
// covered — the assertions are strengthened, never weakened. 0x01 is the
// awareness message type.
func TestAwarenessBodyRejectsMalformed(t *testing.T) {
	// A length varint with the continuation bit set but no following byte is a
	// truncated/unterminated length prefix → the array read errors.
	if _, ok := awarenessBody([]byte{0x01, 0xff}); ok {
		t.Fatal("an unterminated length varint must be rejected")
	}
	// An empty frame is not a readable message at all.
	if _, ok := awarenessBody(nil); ok {
		t.Fatal("an empty frame must be rejected")
	}
	// A frame with only a type byte has no array to read.
	if _, ok := awarenessBody([]byte{0x01}); ok {
		t.Fatal("a frame with no awareness payload must be rejected")
	}
	// A well-formed array decodes to exactly its body.
	if body, ok := awarenessBody([]byte{0x01, 0x02, 0xAA, 0xBB}); !ok || len(body) != 2 {
		t.Fatalf("a clean length-2 array must decode: ok=%v body=%v", ok, body)
	}
	// A well-formed array followed by trailing bytes is non-canonical and must be
	// rejected (a valid awareness frame is exactly one length-prefixed array).
	if _, ok := awarenessBody([]byte{0x01, 0x02, 0xAA, 0xBB, 0xCC}); ok {
		t.Fatal("a frame with trailing bytes after the array must be rejected")
	}
	// A non-awareness frame must not be decoded as awareness.
	if _, ok := awarenessBody([]byte{0x00, 0x02, 0xAA, 0xBB}); ok {
		t.Fatal("a non-awareness frame must be rejected")
	}
}

// --- manager.go ---

// TestSendBufferConfiguredValueWins asserts a positive configured SendBuffer is
// returned verbatim (the non-default branch), distinct from the default fallback.
func TestSendBufferConfiguredValueWins(t *testing.T) {
	mgr := NewManager(Deps{}, RoomConfig{SendBuffer: 17, SaveDebounce: time.Second, IdleTimeout: time.Second}, nil, nil)
	if got := mgr.SendBuffer(); got != 17 {
		t.Fatalf("SendBuffer = %d, want the configured 17", got)
	}
}

// TestNewManagerNilMetricsDefaultsToNop asserts NewManager tolerates a nil
// Metrics by installing NopMetrics, so a lifecycle callback on a metrics-less
// manager does not panic (the nil-metrics default path).
func TestNewManagerNilMetricsDefaultsToNop(t *testing.T) {
	mgr := NewManager(newTestDeps().Deps, fastConfig(), nil, nil)
	// RoomOpened/RoomClosed run against the installed default on join/release.
	if mgr.metrics == nil {
		t.Fatal("nil metrics was not defaulted")
	}
	// Drive a full join → release so RoomOpened/RoomClosed fire against the nop.
	a := newFakeClient(t)
	a.join(mgr, "nil-metrics", model.ContentTypeMemo)
	a.session.Leave()
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })
}

// TestPurgeFallsThroughToDurableWhenRoomGone asserts Purge purges the durable
// rows directly when no live room exists (the room-less branch + purgeDurable):
// deleting a document whose room already released must still remove its metadata
// and blob, leaving no orphan.
func TestPurgeFallsThroughToDurableWhenRoomGone(t *testing.T) {
	meta := metainmem.New()
	blob := persistinprocess.New()
	open := authopen.New()
	ctx := context.Background()

	// Seed a durable document with stored state, but never materialize a room.
	if _, err := blob.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "orphan", Encoding: persistence.EncodingV2, Update: []byte("snapshot"), StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed stored state: %v", err)
	}
	if err := meta.Save(ctx, model.Metadata{
		ID: "orphan", ContentType: model.ContentTypeMemo,
		ContentPointer: "file-orphan", BlobStore: model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Checkpoint: blob, Auth: open, AuthZ: open}, fastConfig(), nil, nil)
	if err := mgr.Purge(ctx, "orphan"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	if _, err := meta.Load(ctx, "orphan"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("metadata row not purged: err=%v", err)
	}
	if _, err := blob.LoadCheckpoint(ctx, "orphan"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("stored state not purged: err=%v", err)
	}
}

// TestPurgeDurableAbsentDocumentIsNoOp asserts purging a document with no
// metadata row (and no live room) is a successful no-op (idempotent delete).
func TestPurgeDurableAbsentDocumentIsNoOp(t *testing.T) {
	mgr := NewManager(newTestDeps().Deps, fastConfig(), nil, nil)
	if err := mgr.Purge(context.Background(), "never-existed"); err != nil {
		t.Fatalf("purging an absent document must be a no-op, got: %v", err)
	}
}

// TestJoinReturnsRoomUnavailableAfterRetries asserts Join fails with
// errRoomUnavailable when every acquired room is already torn down (enqueue
// returns false on both attempts) — the narrow acquire/join teardown race
// exhausting its retry budget.
func TestJoinReturnsRoomUnavailableAfterRetries(t *testing.T) {
	mgr := NewManager(newTestDeps().Deps, fastConfig(), nil, nil)

	// Pre-register a room whose done channel is already closed, so enqueue always
	// reports the room is gone. acquire returns this cached instance, so both
	// attempts hit the dead room and Join exhausts its retries.
	dead := &Room{
		id:       "dead",
		done:     make(chan struct{}),
		commands: make(chan command, 1),
	}
	close(dead.done)
	mgr.mu.Lock()
	mgr.rooms["dead"] = dead
	mgr.mu.Unlock()

	_, _, err := mgr.Join(context.Background(), JoinRequest{ID: "dead", Content: model.ContentTypeMemo, Conn: &captureConn{}})
	if !errors.Is(err, errRoomUnavailable) {
		t.Fatalf("Join err = %v, want errRoomUnavailable", err)
	}
}

// --- room.go: enqueue ---

// TestEnqueueRejectedWhenRoomDone asserts enqueue returns false once the room is
// torn down (its done channel closed), so producers never block on a dead room's
// channel.
func TestEnqueueRejectedWhenRoomDone(t *testing.T) {
	room := newBareRoom(t)
	close(room.done)
	if room.enqueue(command{kind: cmdLeave}) {
		t.Fatal("enqueue must report false after the room is done")
	}
}

// --- room.go: newRoom ---

// TestNewRoomNilMetricsAndContributorDefault asserts newRoom installs the nop
// metrics and nop contributor defaults when both are nil, so a room built without
// them does not panic on a lifecycle/contribution hook.
func TestNewRoomNilMetricsAndContributorDefault(t *testing.T) {
	deps := newTestDeps().Deps
	deps.Contributor = nil
	room, err := newRoom(context.Background(), "defaults", model.ContentTypeMemo, deps, DefaultRoomConfig(), nil, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	if room.metrics == nil {
		t.Fatal("nil metrics not defaulted to NopMetrics")
	}
	if room.deps.Contributor == nil {
		t.Fatal("nil contributor not defaulted to the nop contributor")
	}
}

// TestNewRoomFailsOnSubscribeError asserts newRoom propagates a broadcaster
// Subscribe error (the pod cannot receive peer fan-out, so materialization must
// fail rather than run blind to remote edits).
func TestNewRoomFailsOnSubscribeError(t *testing.T) {
	deps := newTestDeps().Deps
	deps.Hub = erroringSubHub{}
	if _, err := newRoom(context.Background(), "sub-fail", model.ContentTypeMemo, deps, DefaultRoomConfig(), NopMetrics{}, zap.NewNop()); err == nil {
		t.Fatal("newRoom must fail when the broadcaster Subscribe errors")
	}
}

// --- room.go: run arm helpers (SaveDebounce<=0, IdleTimeout<=0) ---

// TestSaveDebounceDisabledPersistsOnlyOnRelease asserts that with SaveDebounce<=0
// the debounce timer is never armed (armSave is a no-op), so the dirty doc is
// persisted only at release — proving the disabled-debounce branch does not
// silently drop the edit.
func TestSaveDebounceDisabledPersistsOnlyOnRelease(t *testing.T) {
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: 0,                     // debounce disabled
		IdleTimeout:  10 * time.Millisecond, // release shortly after the last leave
		SendBuffer:   64,
	})

	a := newFakeClient(t)
	a.join(mgr, "no-debounce", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("kept ")
	a.session.Leave()

	// No debounce fired; the release-time snapshot is the one that persists.
	waitFor(t, "release snapshot", func() bool {
		_, err := deps.storedState(context.Background(), "no-debounce")
		return err == nil
	})
	snap, _ := deps.storedState(context.Background(), "no-debounce")
	reloaded := newRoomDoc("guid")
	_ = ycrdt.ApplyUpdateV2(reloaded, snap, nil)
	if !contains(xmlText(reloaded), "kept") {
		t.Fatalf("release snapshot missing content: %q", xmlText(reloaded))
	}
}

// TestIdleTimeoutZeroReleasesImmediately asserts IdleTimeout<=0 releases the room
// immediately on the last leave (the zero-timeout immediate-release branch).
func TestIdleTimeoutZeroReleasesImmediately(t *testing.T) {
	mgr, _ := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Second, // never fires in this test
		IdleTimeout:  0,                // release immediately on empty
		SendBuffer:   64,
	})
	a := newFakeClient(t)
	a.join(mgr, "instant-idle", model.ContentTypeMemo)
	a.session.Leave()
	waitFor(t, "immediate release", func() bool { return mgr.RoomCount() == 0 })
}

// --- room.go: dispatch cmdPersist ---

// TestDispatchPersistFlushesDirtyDoc asserts the cmdPersist command flushes a
// dirty doc to the blob store on the run loop (the explicit persist command path,
// distinct from the debounce timer).
func TestDispatchPersistFlushesDirtyDoc(t *testing.T) {
	room := newBareRoom(t)
	insertText(room.doc, "persist-me ")
	room.dirty = true

	armNoop := func() {}
	idle := time.NewTimer(time.Hour)
	defer idle.Stop()
	if !room.dispatch(command{kind: cmdPersist}, armNoop, armNoop, idle) {
		t.Fatal("cmdPersist must keep the room running")
	}
	if room.dirty {
		t.Fatal("cmdPersist did not flush the dirty doc")
	}
	store := room.deps.Checkpoint.(*persistinprocess.Store)
	if _, err := store.LoadCheckpoint(context.Background(), "unit"); err != nil {
		t.Fatalf("cmdPersist did not write stored state: %v", err)
	}
}

// --- room.go: evictAwareness nil-frame guard ---

// TestEvictAwarenessFansRemovalForTrackedMember asserts evictAwareness builds and
// fans a forced-removal frame for a member that announced an awareness id, so
// peers stop rendering its cursor immediately on disconnect (the hasAwareness==true
// path). The frame==nil guard (room.go:591) is unreachable: forcedAwarenessRemoval
// always encodes a non-nil null-state frame — intentionally left uncovered
// (constitution §XII: do not test code that cannot fail).
func TestEvictAwarenessFansRemovalForTrackedMember(t *testing.T) {
	room := newBareRoom(t)
	peer := &captureConn{}
	room.members[2] = roomMember{id: 2, conn: peer}

	// A member that announced an awareness id absent from the room awareness:
	// forcedAwarenessRemoval still encodes a (null-state) frame, which is fanned.
	m := roomMember{id: 1, conn: &captureConn{}, hasAwareness: true, awarenessID: 999}
	room.evictAwareness(m)
	// The eviction frame is broadcast to peer 2 (the removal is real).
	if peer.count() == 0 {
		t.Fatal("a tracked member's eviction frame was not fanned to peers")
	}
}

// --- room.go: handleMessage malformed awareness body ---

// TestHandleMessageDropsMalformedAwareness asserts an awareness frame whose body
// is not a well-formed varUint8Array is dropped (not applied, not fanned), so a
// corrupt cursor frame cannot corrupt the room awareness or crash a peer.
func TestHandleMessageDropsMalformedAwareness(t *testing.T) {
	room := newBareRoom(t)
	peer := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}
	room.members[2] = roomMember{id: 2, conn: peer}

	// A well-framed awareness MESSAGE whose inner payload is an unterminated
	// length varint → awarenessBody rejects it.
	var frame bytes.Buffer
	protocol.WriteMessage(&frame, uint8(model.WireAwareness), []byte{0xff})
	if room.handleMessage(1, frame.Bytes()) {
		t.Fatal("a malformed awareness frame must not mutate the document")
	}
	if peer.count() != 0 {
		t.Fatal("a malformed awareness frame must not be fanned to peers")
	}
}

// --- room.go: applyPeerEphemeral malformed peer awareness body ---

// TestApplyPeerEphemeralDropsMalformedAwarenessBody asserts a peer-pod awareness
// frame whose body is malformed is dropped without fan-out, so a corrupt remote
// awareness update cannot corrupt this pod's awareness state.
func TestApplyPeerEphemeralDropsMalformedAwarenessBody(t *testing.T) {
	room := newBareRoom(t)
	local := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: local}

	var frame bytes.Buffer
	protocol.WriteMessage(&frame, uint8(model.WireAwareness), []byte{0xff}) // malformed body
	room.applyPeerEphemeral(frame.Bytes())
	if local.count() != 0 {
		t.Fatal("a malformed peer awareness frame must not be fanned to local members")
	}
}

// --- room.go: handleSync max-doc-size disconnect ---

// TestHandleSyncDisconnectsOnMaxDocSize asserts an applied update that grows the
// encoded snapshot past MaxDocBytes disconnects the offending connection with a
// control message (FR-024), while leaving the room running for the others.
func TestHandleSyncDisconnectsOnMaxDocSize(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 1 // any non-trivial edit blows the 1-byte cap
	c := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: c, mode: model.ModeCollaborator}

	// Build a real sync Update payload from a peer doc and feed it as a collaborator.
	peer := newRoomDoc("unit")
	insertText(peer, "well-past-one-byte ")
	update, err := ycrdt.EncodeStateAsUpdate(peer, nil)
	if err != nil {
		t.Fatalf("encode peer state: %v", err)
	}
	framed := protocol.EncodeUpdate(update)
	// handleSync expects the inner sync payload (it re-frames as MessageSync).
	in := bytes.NewBuffer(framed)
	_, payload, err := protocol.ReadMessage(in)
	if err != nil {
		t.Fatalf("read sync frame: %v", err)
	}

	room.handleSync(1, payload)

	if _, ok := room.members[1]; ok {
		t.Fatal("the connection breaching the max doc size was not disconnected")
	}
}

// --- room.go: onDocUpdate guards ---

// TestOnDocUpdateGuardsEmptyAndNonBytes asserts the update observer ignores an
// empty event and a non-[]uint8 first argument (defensive guards against a
// malformed observer callback), without panicking or fanning anything out.
func TestOnDocUpdateGuardsEmptyAndNonBytes(t *testing.T) {
	room := newBareRoom(t)
	c := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: c}

	room.onDocUpdate()                   // len(v) == 0 → early return
	room.onDocUpdate("not-bytes")        // v[0] not []uint8 → early return
	room.onDocUpdate(42, updateOrigin{}) // non-[]uint8 with an origin → early return

	if c.count() != 0 {
		t.Fatal("a malformed update event must not be fanned out")
	}
	if room.dirty {
		t.Fatal("a malformed update event must not mark the room dirty")
	}
}

// --- room.go: publishToPeers failure ---

// TestPublishToPeersRecordsFanoutFailure asserts a broadcaster Publish error is
// swallowed (the room keeps serving) but recorded on the FanoutFailed metric, so
// a flaky cross-pod bus surfaces in observability without breaking local
// collaboration.
func TestPublishToPeersRecordsFanoutFailure(t *testing.T) {
	room := newBareRoom(t)
	metrics := &countingMetrics{}
	room.metrics = metrics
	room.deps.Hub = erroringPubHub{}

	room.publishToPeers([]byte{1, 2, 3}, false)

	if metrics.fanoutFailed.Load() != 1 {
		t.Fatalf("FanoutFailed = %d, want 1", metrics.fanoutFailed.Load())
	}
	if metrics.fanoutPub.Load() != 0 {
		t.Fatalf("FanoutPublished = %d, want 0 on a failed publish", metrics.fanoutPub.Load())
	}
}

// TestPublishToPeersRecordsFanoutSuccess asserts a successful publish records the
// FanoutPublished metric (the success branch, paired with the failure test).
func TestPublishToPeersRecordsFanoutSuccess(t *testing.T) {
	room := newBareRoom(t)
	metrics := &countingMetrics{}
	room.metrics = metrics
	// newTestDeps wires the noop broadcaster whose Publish succeeds.
	room.publishToPeers([]byte{1, 2, 3}, false)
	if metrics.fanoutPub.Load() != 1 {
		t.Fatalf("FanoutPublished = %d, want 1", metrics.fanoutPub.Load())
	}
}

// --- room.go: broadcastControl nil frame ---

// broadcastControl's nil-frame guard fires only when encodeControl returns nil,
// which requires json.Marshal of a ControlMessage to fail — unreachable with the
// current struct (all fields are JSON-marshalable). See the skip note in
// TestEncodeControlMarshalsKnownMessage below.

// --- room.go: finish idempotent ---

// TestFinishIsIdempotent asserts a second finish is a no-op: the release callback
// fires exactly once and the done channel is not double-closed (which would
// panic).
func TestFinishIsIdempotent(t *testing.T) {
	room := newBareRoom(t)
	released := 0
	room.onReleased = func() { released++ }

	room.finish()
	room.finish() // must not panic (double close) and must not re-notify

	if released != 1 {
		t.Fatalf("onReleased fired %d times, want exactly 1", released)
	}
	select {
	case <-room.done:
		// expected: closed exactly once
	default:
		t.Fatal("finish did not close the done channel")
	}
}

// --- room.go: encodeControl + awarenessSnapshot ---

// TestEncodeControlMarshalsKnownMessage asserts a well-formed control message
// frames into a type-3 wire message (the success path). The nil-on-marshal-error
// branch (encodeControl line ~985) is unreachable: ControlMessage has only
// JSON-marshalable fields, so json.Marshal cannot fail — intentionally left
// uncovered (constitution §XII: do not test code that cannot fail).
func TestEncodeControlMarshalsKnownMessage(t *testing.T) {
	frame := encodeControl(model.ControlMessage{Kind: model.ControlSaved, Version: 3})
	if frame == nil {
		t.Fatal("encodeControl returned nil for a marshalable message")
	}
	in := bytes.NewBuffer(frame)
	msgType, _, err := protocol.ReadMessage(in)
	if err != nil {
		t.Fatalf("control frame is not a valid wire message: %v", err)
	}
	if model.WireMessageType(msgType) != model.WireControl {
		t.Fatalf("control frame type = %d, want WireControl", msgType)
	}
}

// TestAwarenessSnapshotEmptyReturnsNil asserts awarenessSnapshot returns nil when
// no awareness states are present (so a join into an empty room sends no awareness
// frame), and a non-nil frame once a state exists.
func TestAwarenessSnapshotEmptyReturnsNil(t *testing.T) {
	doc := newRoomDoc("snap")
	aw := ycrdt.NewAwareness(doc)
	// A fresh awareness carries the doc's own (empty) local-client state; clearing
	// it drops the state count to zero — the no-presence case a brand-new room is
	// in before any client announces a cursor.
	_ = aw.SetLocalState(ycrdt.Object{})
	if awarenessSnapshot(aw) != nil {
		t.Fatal("an empty awareness must snapshot to nil")
	}
	_ = aw.SetLocalState(ycrdt.MakeObject("user", "x"))
	if awarenessSnapshot(aw) == nil {
		t.Fatal("a populated awareness must snapshot to a non-nil frame")
	}
}

// --- sync.go: dispatchSync malformed sub-message ---

// TestDispatchSyncMalformedSyncStep1Errors asserts a MessageSync whose SyncStep1
// state-vector array is truncated returns a decode error (so a malformed sync
// sub-message is reported, not silently treated as an empty reply).
func TestDispatchSyncMalformedSyncStep1Errors(t *testing.T) {
	room := newBareRoom(t)

	// Inner sync payload: [SyncStep1 sub-tag][0xff = unterminated length varint].
	// ReadVarUint8Array fails to read the state-vector length → decode error.
	inner := []byte{byte(protocol.SyncMessageStep1), 0xff}
	var framed bytes.Buffer
	protocol.WriteMessage(&framed, protocol.MessageSync, inner)

	var reply bytes.Buffer
	if _, err := room.dispatchSync(framed.Bytes(), &reply, 1, true); err == nil {
		t.Fatal("a malformed SyncStep1 state vector must produce a decode error")
	}
}

// TestDispatchSyncMalformedUpdateErrors asserts a MessageSync whose Update payload
// array is malformed returns a decode error (the SyncStep2/Update decode-error
// branch), so a corrupt update is reported rather than applied.
func TestDispatchSyncMalformedUpdateErrors(t *testing.T) {
	room := newBareRoom(t)

	inner := []byte{byte(protocol.SyncMessageUpdate), 0xff} // unterminated length varint
	var framed bytes.Buffer
	protocol.WriteMessage(&framed, protocol.MessageSync, inner)

	var reply bytes.Buffer
	if _, err := room.dispatchSync(framed.Bytes(), &reply, 1, true); err == nil {
		t.Fatal("a malformed Update payload must produce a decode error")
	}
}

// --- manager.go: SendBuffer default branch ---

// TestSendBufferDefaultsWhenZeroWithOtherTimers asserts SendBuffer() falls back to
// the default when the configured buffer is zero but the other cadences are
// non-zero (so NewManager keeps the supplied config rather than substituting the
// whole DefaultRoomConfig). This is the adapter's outbound-queue-depth contract.
func TestSendBufferDefaultsWhenZeroWithOtherTimers(t *testing.T) {
	mgr := NewManager(Deps{}, RoomConfig{SaveDebounce: time.Second, IdleTimeout: time.Second}, nil, nil)
	if got := mgr.SendBuffer(); got != DefaultRoomConfig().SendBuffer {
		t.Fatalf("SendBuffer = %d, want the default %d", got, DefaultRoomConfig().SendBuffer)
	}
}

// --- manager.go: purgeDurable blob-delete error ---

// failingDeleteStore is a CheckpointStore whose delete fails, to drive
// purgeDurable's error branch.
type failingDeleteStore struct {
	persistence.CheckpointStore
}

func (failingDeleteStore) Delete(context.Context, persistence.DeleteRequest) error {
	return errors.New("blob backend down")
}

// TestPurgeDurablePropagatesBlobDeleteError asserts a non-NotFound blob Delete
// error during a room-less purge propagates (so a failed cascade is surfaced, not
// silently swallowed leaving a half-deleted document).
func TestPurgeDurablePropagatesBlobDeleteError(t *testing.T) {
	meta := metainmem.New()
	open := authopen.New()
	ctx := context.Background()
	if err := meta.Save(ctx, model.Metadata{
		ID: "del-fail", ContentType: model.ContentTypeMemo,
		ContentPointer: "del-fail", BlobStore: model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{
		Metadata:   meta,
		Checkpoint: failingDeleteStore{CheckpointStore: persistinprocess.New()},
		Auth:       open, AuthZ: open,
	}, fastConfig(), nil, nil)

	if err := mgr.Purge(ctx, "del-fail"); err == nil {
		t.Fatal("Purge must propagate a non-NotFound blob delete error")
	}
}

// --- room.go: loadSnapshot blob-not-found (index row without a blob yet) ---

// TestLoadSnapshotMissingBlobBehindPointerFailsMaterialization asserts that a
// metadata row with a NON-EMPTY ContentPointer whose blob is absent fails
// materialization rather than silently seeding an empty room. persist() always
// writes the blob BEFORE upserting the pointer, so a populated pointer must have
// a blob — a missing one is corruption, and seeding/blanking it would materialize
// stale/empty content and let the next save overwrite the last good snapshot.
// (The legitimate "no snapshot yet" case is an EMPTY ContentPointer, covered by
// NOTE (FR-018a): "a populated pointer whose blob is missing must fail
// materialization" moved to the file-service store's tests. The property is
// unchanged and still defended — a document whose index says state EXISTS but
// whose content is gone must NOT be treated as "nothing stored", or seeding would
// resurrect stale content and the next save would overwrite the last good state.
//
// It cannot be expressed here any more: the distinction requires a store that
// addresses content by POINTER, so that "pointer set, content missing" is
// representable. The in-process store keeps state by document id and has no
// pointer, so for it there is only "present" or "absent". The file-service store
// returns ErrCorrupt (not ErrNotFound) for exactly this case, and loadSnapshot
// only seeds on ErrNotFound.

// --- room.go: handleLeave / dropMember absent member ---

// TestHandleLeaveAbsentMemberEmitsNoControl asserts leaving a connection that is
// not in the registry (already evicted) is a no-op: dropMember returns false and
// no user-change control is broadcast to the remaining members.
func TestHandleLeaveAbsentMemberEmitsNoControl(t *testing.T) {
	room := newBareRoom(t)
	peer := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: peer}

	room.handleLeave(99) // connection 99 was never a member

	if peer.count() != 0 {
		t.Fatal("leaving an absent member must not broadcast a user-change control")
	}
}

// --- room.go: applyPeerEphemeral default (non-awareness/ephemeral) ignored ---

// TestApplyPeerEphemeralIgnoresSyncTyped asserts a peer frame on the
// awareness/ephemeral channel that carries a sync (or control) type is ignored —
// sync/control never travel that channel, so it must not be fanned to members.
func TestApplyPeerEphemeralIgnoresSyncTyped(t *testing.T) {
	room := newBareRoom(t)
	local := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: local}

	var frame bytes.Buffer
	protocol.WriteMessage(&frame, uint8(model.WireSync), []byte{0x00})
	room.applyPeerEphemeral(frame.Bytes())

	if local.count() != 0 {
		t.Fatal("a sync-typed peer ephemeral frame must be ignored, not fanned")
	}
}

// --- room.go: handleSync dispatch error path ---

// TestHandleSyncDropsMalformedSyncPayload asserts handleSync swallows a
// dispatchSync decode error (logs and returns no mutation), so a corrupt sync
// sub-message from one client cannot crash the room or wedge the others.
func TestHandleSyncDropsMalformedSyncPayload(t *testing.T) {
	room := newBareRoom(t)
	room.members[1] = roomMember{id: 1, conn: &captureConn{}, mode: model.ModeCollaborator}

	// Inner sync payload with an unterminated array length → dispatchSync errors.
	malformed := []byte{byte(protocol.SyncMessageUpdate), 0xff}
	if room.handleSync(1, malformed) {
		t.Fatal("a malformed sync payload must not report a mutation")
	}
}

// --- sync.go: unknown sub-tag falls through to the no-op outcome ---

// TestDispatchSyncUnknownSubTagIsNoOp asserts an unrecognized sync sub-tag is a
// no-op (no reply, no error, no mutation) — y-protocols leniency for a sub-message
// the server does not handle.
func TestDispatchSyncUnknownSubTagIsNoOp(t *testing.T) {
	room := newBareRoom(t)

	// A sub-tag of 99 is neither SyncStep1/2 nor Update.
	inner := []byte{99}
	var framed bytes.Buffer
	protocol.WriteMessage(&framed, protocol.MessageSync, inner)

	var reply bytes.Buffer
	out, err := room.dispatchSync(framed.Bytes(), &reply, 1, true)
	if err != nil {
		t.Fatalf("an unknown sub-tag must not error: %v", err)
	}
	if reply.Len() != 0 {
		t.Fatal("an unknown sub-tag must not produce a reply")
	}
	if out.mutating || out.applied {
		t.Fatal("an unknown sub-tag must not be treated as a mutation")
	}
}

// compile-time assertions that the local fakes satisfy the adopted contracts.
var (
	_ hub.Hub             = erroringSubHub{}
	_ hub.Hub             = erroringPubHub{}
	_ persistence.Deleter = failingDeleteStore{}
)
