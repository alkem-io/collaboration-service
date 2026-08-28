package service

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"

	"go.uber.org/zap"
)

// requestDurability sends one durability-request frame carrying the given id.
func requestDurability(c *fakeClient, id string) {
	c.t.Helper()
	var frame bytes.Buffer
	body := []byte(`{"requestId":"` + id + `"}`)
	protocol.WriteMessage(&frame, uint8(model.WireDurabilityRequest), body)
	c.session.Forward(frame.Bytes())
}

// barrierOutcome returns the control answering id, or nil if none has arrived.
func barrierOutcome(c *fakeClient, id string) *model.ControlMessage {
	for _, m := range controlMessages(c) {
		if m.RequestID != id {
			continue
		}
		if m.Kind == model.ControlPersisted || m.Kind == model.ControlPersistFailed {
			got := m
			return &got
		}
	}
	return nil
}

// TestABarrierResolvesAfterBothStoresAccept is the core case: a mutation followed
// by a request on the same connection resolves as persisted, and only once the
// checkpoint AND the metadata save have both succeeded (the one place dirty is
// cleared).
func TestABarrierResolvesAfterBothStoresAccept(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-happy"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("durable please")
	requestDurability(client, "req-1")

	waitFor(t, "the barrier to resolve", func() bool { return barrierOutcome(client, "req-1") != nil })
	got := barrierOutcome(client, "req-1")
	if got.Kind != model.ControlPersisted {
		t.Fatalf("barrier resolved %s (%q), want persisted", got.Kind, got.Error)
	}
}

// TestADeleteOnlyMutationIsCoveredByTheBarrier is the case that disproved the
// state-vector design and is the reason a barrier exists at all.
//
// A delete introduces no new structs, so the document's state vector is
// BYTE-IDENTICAL before and after it — measured against the shipped go-yjs on
// Text, Array, Map and the whiteboard scene map. Any watermark built on struct
// coverage therefore reports a deletion as already durable while the durable
// checkpoint still contains the deleted content: silent resurrection on the next
// cold load.
//
// The barrier does not have that failure mode because it rests on `dirty`, which
// the update observer sets for a DeleteSet-only update. The second assertion is
// the one a state vector could not make: the durable BYTES no longer carry the
// deleted content.
func TestADeleteOnlyMutationIsCoveredByTheBarrier(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-delete-only"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeWhiteboard)
	client.observeUpdates()

	client.addElement("el-1", map[string]interface{}{"type": "rect"})
	client.addElement("el-2", map[string]interface{}{"type": "ellipse"})
	requestDurability(client, "seed")
	waitFor(t, "the seed to be durable", func() bool { return barrierOutcome(client, "seed") != nil })

	// A DELETE ONLY: removing a scene element introduces no new structs, so the
	// document's state vector does not move at all.
	client.withDoc(func(d *ycrdt.Doc) { d.GetMap("elements").Delete("el-1") })
	requestDurability(client, "req-delete")

	waitFor(t, "the delete barrier to resolve", func() bool { return barrierOutcome(client, "req-delete") != nil })
	got := barrierOutcome(client, "req-delete")
	if got.Kind != model.ControlPersisted {
		t.Fatalf("delete-only barrier resolved %s (%q), want persisted", got.Kind, got.Error)
	}

	stored, err := deps.storedState(t.Context(), string(doc))
	if err != nil {
		t.Fatalf("stored state: %v", err)
	}
	check := newRoomDoc(string(doc))
	if err := ycrdt.ApplyUpdateV2(check, stored, nil); err != nil {
		t.Fatalf("decode stored: %v", err)
	}
	if check.GetMap("elements").Get("el-1") != nil {
		t.Fatal("the durable document still holds the deleted element; the deletion was reported durable before it was")
	}
	if check.GetMap("elements").Get("el-2") == nil {
		t.Fatal("the surviving element is missing from the durable document")
	}
}

// TestASecondOutstandingRequestIsRefused pins the one-per-connection cap. The
// caller is sequential by contract, so a second concurrent request is a caller
// bug — refused explicitly rather than queued into an unbounded waiter map.
func TestASecondOutstandingRequestIsRefused(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = time.Hour // long enough that nothing flushes during the test
	mgr, _ := testManager(t, cfg)
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-one-outstanding"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("pending")
	requestDurability(client, "first")
	requestDurability(client, "second")

	waitFor(t, "the second request to be refused", func() bool { return barrierOutcome(client, "second") != nil })
	if got := barrierOutcome(client, "second"); got.Kind != model.ControlPersistFailed {
		t.Fatalf("second concurrent request resolved %s, want persist-failed", got.Kind)
	}
	if outcome := barrierOutcome(client, "first"); outcome != nil {
		t.Fatalf("the FIRST request was resolved %s by the second one's arrival", outcome.Kind)
	}
}

// TestAnInvalidRequestIDInstallsNoWaiterAndIsNotEchoed bounds unvalidated client
// input.
//
// The id is stored on the member and echoed back in the answer, and the socket's
// read limit is the DOCUMENT size limit — tens of megabytes — so without a
// contract one authenticated request could park a multi-megabyte string and make
// the server re-encode and transmit it. "One outstanding per connection" bounds
// the COUNT, not the bytes.
func TestAnInvalidRequestIDInstallsNoWaiterAndIsNotEchoed(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-bad-id"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	huge := strings.Repeat("A", 5000)
	requestDurability(client, huge)

	// A valid request behind them proves the loop kept working AND drains it.
	client.insertText("something")
	requestDurability(client, "valid-after")
	waitFor(t, "the valid request to resolve", func() bool { return barrierOutcome(client, "valid-after") != nil })

	for _, m := range controlMessages(client) {
		if len(m.RequestID) > maxRequestIDLen {
			t.Fatalf("the server echoed a %d-byte request id", len(m.RequestID))
		}
	}
	if got := barrierOutcome(client, huge); got != nil {
		t.Fatalf("an oversized request id was answered (%s) rather than dropped", got.Kind)
	}
}

// TestARefusedWriteFailsTheOutstandingBarrier is amendment 4: a rejected update
// must never become a durability success via a later unrelated flush.
func TestARefusedWriteFailsTheOutstandingBarrier(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = time.Hour
	mgr, _ := testManager(t, cfg)
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-refused-write"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("legit")
	requestDurability(client, "req-poisoned")

	// A write the schema validator refuses.
	client.withDoc(func(d *ycrdt.Doc) { setMemoImage(t, d, "data:image/png;base64,iVBORw0KGgo=") })

	waitFor(t, "the barrier to be poisoned", func() bool { return barrierOutcome(client, "req-poisoned") != nil })
	if got := barrierOutcome(client, "req-poisoned"); got.Kind != model.ControlPersistFailed {
		t.Fatalf("barrier resolved %s after a refused write, want persist-failed; a rejected update must not become a durability success", got.Kind)
	}
}

// TestAViewerCannotRequestDurability: only a session that may WRITE may ask
// whether its write is durable. A viewer's mutation was refused, so a barrier
// over it would be asking about work the room deliberately discarded.
func TestAViewerCannotRequestDurability(t *testing.T) {
	const doc model.DocumentID = "barrier-viewer"
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	requestDurability(viewer, "viewer-req")

	waitFor(t, "the viewer's request to be refused", func() bool { return barrierOutcome(viewer, "viewer-req") != nil })
	if got := barrierOutcome(viewer, "viewer-req"); got.Kind != model.ControlPersistFailed {
		t.Fatalf("a read-only session's durability request resolved %s, want persist-failed", got.Kind)
	}
}

// TestARejectedUpdateBlocksEveryLaterBarrierOnThatConnection is the false-positive
// regression, and it is the ORDINARY ordering rather than the convenient one.
//
// A barrier arrives AFTER the update it covers. So at the moment a write is
// rejected there is usually NO outstanding request to fail — failing one only
// covers the inverse order. Without a flag that survives the rejection, the
// request that follows finds a clean room and is answered `persisted`, claiming
// durability for a mutation the service explicitly refused.
//
// The poison is therefore per-member and STICKY: nothing in the room clears it,
// and a reconnect gets a fresh member. This asserts the whole shape — including
// that an unrelated member's successful save cannot launder it.
//
// Non-vacuity: delete the durabilityPoisoned check in handleDurabilityRequest and
// this fails with a `persisted` for a rejected update.
func TestARejectedUpdateBlocksEveryLaterBarrierOnThatConnection(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-poison-sticky"

	author := newFakeClient(t)
	author.join(mgr, doc, model.ContentTypeMemo)
	author.observeUpdates()

	other := newFakeClient(t)
	other.join(mgr, doc, model.ContentTypeMemo)
	other.observeUpdates()

	// A schema-invalid mutation. The connection stays open by design.
	author.withDoc(func(d *ycrdt.Doc) { setMemoImage(t, d, "data:image/png;base64,iVBORw0KGgo=") })
	waitFor(t, "the update to be rejected", func() bool {
		return hasControlKind(author, model.ControlUpdateRejected)
	})

	// Now the barrier, on the same connection, in the ordinary order.
	requestDurability(author, "after-rejection")
	waitFor(t, "the barrier to be refused", func() bool { return barrierOutcome(author, "after-rejection") != nil })
	if got := barrierOutcome(author, "after-rejection"); got.Kind != model.ControlPersisted {
		if got.Kind != model.ControlPersistFailed {
			t.Fatalf("barrier resolved %s, want persist-failed", got.Kind)
		}
	} else {
		t.Fatal("a barrier following a REJECTED update was answered `persisted`; the service claimed durability for a mutation it refused")
	}

	// An unrelated member's successful save must not launder the poison.
	other.insertText("a perfectly good edit from someone else")
	requestDurability(other, "others-request")
	waitFor(t, "the other member's barrier", func() bool { return barrierOutcome(other, "others-request") != nil })
	if got := barrierOutcome(other, "others-request"); got.Kind != model.ControlPersisted {
		t.Fatalf("an unpoisoned member's barrier resolved %s, want persisted", got.Kind)
	}

	requestDurability(author, "after-someone-elses-save")
	waitFor(t, "the second poisoned request", func() bool {
		return barrierOutcome(author, "after-someone-elses-save") != nil
	})
	if got := barrierOutcome(author, "after-someone-elses-save"); got.Kind == model.ControlPersisted {
		t.Fatal("another member's successful save laundered the poison and answered the rejected session `persisted`")
	}
}

// TestAFailedBarrierSendDoesNotResurrectTheMember pins the re-entrancy rule.
//
// sendTo -> sendMember drops a member SYNCHRONOUSLY when its Send fails, deleting
// it from r.members. Barrier resolution therefore has to clear the pending state
// BEFORE it sends: writing a captured roomMember back afterwards reinserts a dead
// member whose ConnClosed has already been counted, inflating occupancy and
// outliving teardown and awareness eviction.
//
// Non-vacuity: move the r.members[id] = m write back after answerBarrier and this
// fails on the room-count assertion.
func TestAFailedBarrierSendDoesNotResurrectTheMember(t *testing.T) {
	room, _ := dirtyRoomWithRegistry(t, "barrier-send-failure")
	failing := &failingConn{}
	room.members[7] = roomMember{id: 7, conn: failing, mode: model.ModeCollaborator, barrier: "doomed"}

	room.resolveBarriers()

	if _, still := room.members[7]; still {
		t.Fatal("a member whose barrier delivery failed was reinserted into the room after being dropped")
	}
}

// TestABarrierIsRefusedWhenPeriodicSavingIsDisabled: SaveDebounce <= 0 is the
// supported save-on-release configuration. There is no scheduled flush to wait
// for, so a parked request would hang for the life of the room. It is answered
// immediately and correlated instead — never silently.
func TestABarrierIsRefusedWhenPeriodicSavingIsDisabled(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = 0
	mgr, _ := testManager(t, cfg)
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-release-only"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("edited in release-only mode")
	requestDurability(client, "release-only")

	waitFor(t, "the request to be answered", func() bool { return barrierOutcome(client, "release-only") != nil })
	if got := barrierOutcome(client, "release-only"); got.Kind != model.ControlPersistFailed {
		t.Fatalf("barrier resolved %s in save-on-release mode, want persist-failed; silence would hang the caller", got.Kind)
	}
}

// TestParkingABarrierDoesNotArmTheSaveTimer pins that a parked request leaves the
// flush deadline alone.
//
// armSaveTimer is stop-then-Reset, NOT idempotent, so arming from the request path
// would push a nearly-due flush out by a full SaveDebounce every time — and with
// several members asking, the requests waiting for durability could postpone it
// indefinitely.
//
// Asserted on the DETERMINISTIC SEAM rather than on wall-clock timing:
// handleDurabilityRequest's boolean IS handleMessageCmd's armSave decision, and
// the run loop's existing coverage already establishes that true means
// armSaveTimer. A timing-based version of this would gate CI on scheduler latency
// under -race and a live broker, which is a different and much worse property to
// depend on.
//
// Non-vacuity: return true from the parked path and this fails immediately.
func TestParkingABarrierDoesNotArmTheSaveTimer(t *testing.T) {
	room, _ := dirtyRoomWithRegistry(t, "barrier-parks-quietly")
	room.members[1] = roomMember{id: 1, conn: &captureConn{}, mode: model.ModeCollaborator}
	if !room.dirty {
		t.Fatal("precondition: the room must be dirty for the request to park")
	}

	armSave := room.handleDurabilityRequest(1, []byte(`{"requestId":"parked"}`), false)

	if armSave {
		t.Fatal("parking a durability request asked the caller to arm the save timer; armSaveTimer is stop-then-Reset, so every request would postpone the flush it is waiting for")
	}
	if got := room.members[1].barrier; got != "parked" {
		t.Fatalf("the request was not parked on the member (barrier = %q)", got)
	}
}

// TestOnePersistResolvesEveryParkedBarrier pins the coalescing property that
// matters: several members waiting in ONE dirty epoch are all answered, by the
// single flush that epoch already scheduled.
//
// It asserts the outcome, not the schedule — the "at most one flush" half is
// established deterministically by TestParkingABarrierDoesNotArmTheSaveTimer plus
// armSaveTimer's clean->dirty-only arming, so this does not need to measure timing
// to be meaningful.
func TestOnePersistResolvesEveryParkedBarrier(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	t.Cleanup(mgr.Close)
	const doc model.DocumentID = "barrier-coalesce"

	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	a.observeUpdates()
	b := newFakeClient(t)
	b.join(mgr, doc, model.ContentTypeMemo)
	b.observeUpdates()

	// ONE dirty epoch, two waiters.
	a.insertText("one dirty epoch")
	requestDurability(a, "a-req")
	requestDurability(b, "b-req")

	waitFor(t, "both barriers to resolve", func() bool {
		return barrierOutcome(a, "a-req") != nil && barrierOutcome(b, "b-req") != nil
	})
	for _, tc := range []struct {
		c  *fakeClient
		id string
	}{{a, "a-req"}, {b, "b-req"}} {
		if got := barrierOutcome(tc.c, tc.id); got.Kind != model.ControlPersisted {
			t.Fatalf("%s resolved %s, want persisted; one flush must answer every request parked in its epoch", tc.id, got.Kind)
		}
	}
}

// TestARefusedFrameIsPoisonedAndTypedClosed covers the FIRST half of the A1
// prerequisite: what Forward does at the moment an enqueue is refused.
//
// Forward used to DISCARD enqueue's refusal. A frame could vanish — the room left
// Active, or the command buffer stayed full past its deadline — and the client
// was told nothing, so it kept editing a generation the server never received.
// Layer a durability barrier on that and the service answers "your work is safe"
// about a mutation that never arrived.
//
// Now the refusal is observed: the session is POISONED FIRST, so no later barrier
// can succeed, and only then is the client told with a typed member-scoped
// TRANSIENT end and closed after drain.
//
// Driven at the Session level, against a room that is not Active, because that is
// where the behaviour lives and it needs no wedged buffer or 30-second deadline to
// reach deterministically.
//
// SCOPE, stated so the name does not overclaim: this proves the POISON IS SET and
// the typed end is delivered. That a later request actually carries the poison to
// a correlated failure is the other half, and it is proved end-to-end by
// TestADroppedFrameThenARequestIsRefusedOnTheRunLoop below.
//
// Non-vacuity: restore Forward's discard of the return value and this fails at the
// session-end assertion; keep the close but drop the poison Swap and the second
// frame queues a second end.
func TestARefusedFrameIsPoisonedAndTypedClosed(t *testing.T) {
	room := newBareRoom(t)
	// GENUINE BACKPRESSURE, not a lifecycle refusal: the room is ACTIVE and simply
	// cannot take the frame, because the buffer is full and nothing is draining it.
	// That distinction is the whole point — a lifecycle refusal is teardown's to
	// announce, and this code must not compete with it.
	room.lc.state.Store(int32(stateActive))
	room.commands = make(chan command, 1)
	room.commands <- command{kind: cmdLeave} // full, no consumer
	enqueueDeadlineForTest(t, 20*time.Millisecond)
	conn := &recordingConn{}
	session := &Session{room: room, id: 1, conn: conn}

	session.Forward([]byte("an update that cannot be admitted"))

	if !session.dropped.Load() {
		t.Fatal("the session was not poisoned after its frame was refused; a later durability request could still be answered")
	}
	end := conn.end()
	if end == nil {
		t.Fatal("a dropped frame ended nothing: the client was not told its update was not accepted")
	}
	if end.Code != model.CodeUpdateNotAccepted {
		t.Fatalf("session end code = %q, want %q", end.Code, model.CodeUpdateNotAccepted)
	}
	if end.Scope != model.ScopeMember {
		t.Fatalf("scope = %q, want member: one connection's frame was refused, not the room's", end.Scope)
	}
	if end.Disposition != model.DispositionTransient {
		t.Fatalf("disposition = %q, want transient: the client should reconnect with backoff", end.Disposition)
	}
	if !conn.toldBeforeClose() {
		t.Fatal("the socket was closed before the reason was written")
	}

	// A second refused frame must NOT queue a second session end.
	before := conn.closes()
	session.Forward([]byte("another one"))
	if conn.closes() != before {
		t.Fatalf("a second refused frame queued another session end (%d -> %d)", before, conn.closes())
	}
}

// recordingConn records the control frames and the close intent, so the ordering
// the contract requires — reason written BEFORE the close — is observable.
type recordingConn struct {
	mu       sync.Mutex
	controls []model.ControlMessage
	ended    *model.SessionEnd
	told     bool
	closeN   int
}

func (c *recordingConn) Send(frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	in := bytes.NewBuffer(frame)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil || model.WireMessageType(msgType) != model.WireControl {
		return nil
	}
	var msg model.ControlMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil
	}
	c.controls = append(c.controls, msg)
	if msg.Kind == model.ControlSessionEnd {
		c.told = true
	}
	return nil
}

func (c *recordingConn) CloseAfterDrain(end model.SessionEnd) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeN++
	if c.ended == nil {
		got := end
		c.ended = &got
	}
}

func (c *recordingConn) end() *model.SessionEnd {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ended
}

func (c *recordingConn) toldBeforeClose() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.told
}

func (c *recordingConn) closes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeN
}

// TestAFailedPersistFailsThePendingBarrier: a save that does not happen must
// produce a correlated failure, never silence. Silence is indistinguishable from
// "still working", so a caller waiting on it would hang until its own timeout and
// then have to guess whether the write landed.
//
// Non-vacuity: remove failBarriers from onFlushFailed and this fails on the wait.
func TestAFailedPersistFailsThePendingBarrier(t *testing.T) {
	store := newOutageStore() // every save fails
	mgr := NewManager(Deps{
		Metadata:   metainmem.New(),
		Checkpoint: store,
		Auth:       authopen.New(),
		AuthZ:      authopen.New(),
	}, RoomConfig{
		SaveDebounce: 5 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
		Limits:       Limits{FlushFailureThreshold: 50}, // do not escalate during the test
	}, nil, zap.NewNop())
	t.Cleanup(mgr.Close)

	const doc model.DocumentID = "barrier-persist-fails"
	if err := mgr.PreRegister(t.Context(), model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	client := newFakeClient(t)
	client.joinExisting(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("this will never reach the store")
	requestDurability(client, "doomed")

	waitFor(t, "the barrier to fail", func() bool { return barrierOutcome(client, "doomed") != nil })
	got := barrierOutcome(client, "doomed")
	if got.Kind != model.ControlPersistFailed {
		t.Fatalf("barrier resolved %s while every save was failing, want persist-failed", got.Kind)
	}
	if got.Error == "" {
		t.Error("the failure carried no reason")
	}
}

// TestTeardownFailsEveryPendingBarrier: four of the five teardown endings run with
// NO successful flush, so a request outstanding when the room ends would otherwise
// be answered by nothing at all.
//
// Non-vacuity: remove the failBarriers call from teardown and this fails.
func TestTeardownFailsEveryPendingBarrier(t *testing.T) {
	cfg := fastConfig()
	cfg.SaveDebounce = time.Hour // the request parks and nothing flushes
	mgr, _ := testManager(t, cfg)
	const doc model.DocumentID = "barrier-teardown"

	client := newFakeClient(t)
	client.join(mgr, doc, model.ContentTypeMemo)
	client.observeUpdates()

	client.insertText("pending forever without a teardown")
	requestDurability(client, "outstanding")

	waitFor(t, "the request to be parked", func() bool {
		mgr.mu.Lock()
		room := mgr.rooms[doc]
		mgr.mu.Unlock()
		if room == nil {
			return false
		}
		return barrierOutcome(client, "outstanding") == nil
	})

	// The owner deletes the document: teardown with NO flush.
	if err := mgr.CloseDeleted(t.Context(), doc); err != nil {
		t.Fatalf("CloseDeleted: %v", err)
	}

	waitFor(t, "the pending barrier to be failed by teardown", func() bool {
		return barrierOutcome(client, "outstanding") != nil
	})
	if got := barrierOutcome(client, "outstanding"); got.Kind != model.ControlPersistFailed {
		t.Fatalf("barrier resolved %s when the room ended without a flush, want persist-failed", got.Kind)
	}
}

// TestTheRequestIDContractIsEnforcedAtTheBoundary walks the id contract.
//
// The id is UNBOUNDED CLIENT INPUT that the server stores on a member and echoes
// back, and the socket's read limit is the document size limit. So the contract is
// enforced before either happens: at most 64 bytes of [A-Za-z0-9._-], which a UUID
// satisfies unchanged.
//
// A request with no usable id is the ONE case answered with silence, because there
// is nothing to correlate an answer to — which is exactly why the guarantee
// elsewhere is stated for every VALID request rather than every request.
func TestTheRequestIDContractIsEnforcedAtTheBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"malformed JSON", `{"requestId":`},
		{"absent id", `{}`},
		{"empty id", `{"requestId":""}`},
		{"spaces are not in the alphabet", `{"requestId":"has spaces"}`},
		{"quotes and control characters", `{"requestId":"a\tb"}`},
		{"over the length bound", `{"requestId":"` + strings.Repeat("x", maxRequestIDLen+1) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			room, _ := dirtyRoomWithRegistry(t, "barrier-id-contract")
			spy := &captureConn{}
			room.members[1] = roomMember{id: 1, conn: spy, mode: model.ModeCollaborator}

			if armSave := room.handleDurabilityRequest(1, []byte(tc.body), false); armSave {
				t.Error("an unusable request asked the caller to arm the save timer")
			}
			if got := room.members[1].barrier; got != "" {
				t.Errorf("an unusable request installed a waiter (%q)", got)
			}
			if spy.count() != 0 {
				t.Errorf("an unusable request produced %d frame(s); there is no id to correlate an answer to", spy.count())
			}
		})
	}

	t.Run("a UUID is accepted", func(t *testing.T) {
		room, _ := dirtyRoomWithRegistry(t, "barrier-id-uuid")
		room.members[1] = roomMember{id: 1, conn: &captureConn{}, mode: model.ModeCollaborator}
		body := `{"requestId":"3f2504e0-4f89-11d3-9a0c-0305e82c3301"}`
		room.handleDurabilityRequest(1, []byte(body), false)
		if got := room.members[1].barrier; got != "3f2504e0-4f89-11d3-9a0c-0305e82c3301" {
			t.Fatalf("a UUID request id was not accepted (barrier = %q)", got)
		}
	})
}

// TestADroppedSessionsRequestIsRefusedOnTheRoomLoop covers the poison arriving
// with the command, which is how a barrier behind a dropped frame is refused even
// though the drop was detected on a different goroutine.
func TestADroppedSessionsRequestIsRefusedOnTheRoomLoop(t *testing.T) {
	room, _ := dirtyRoomWithRegistry(t, "barrier-dropped-session")
	spy := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: spy, mode: model.ModeCollaborator}

	room.handleDurabilityRequest(1, []byte(`{"requestId":"after-drop"}`), true)

	if got := room.members[1].barrier; got != "" {
		t.Fatalf("a request from a session that lost a frame installed a waiter (%q)", got)
	}
	if spy.count() == 0 {
		t.Fatal("the request was neither answered nor refused; the caller would wait on a barrier that can never resolve")
	}
}

// TestARequestFromAnUnknownConnectionIsIgnored: an already-evicted member has
// nothing to answer to.
func TestARequestFromAnUnknownConnectionIsIgnored(t *testing.T) {
	room, _ := dirtyRoomWithRegistry(t, "barrier-unknown-conn")
	if armSave := room.handleDurabilityRequest(99, []byte(`{"requestId":"ghost"}`), false); armSave {
		t.Fatal("a request from an unknown connection asked for a save")
	}
}

// TestAmbiguousCloseNoOpResendResolvesWithoutARedundantWrite is the reconnect
// case: the client cannot tell whether its last update landed, so it resends. The
// resend applies nothing (Yjs updates are idempotent), the room is clean, and the
// barrier resolves immediately from the fact that the live document already IS
// the durable one.
//
// This is the case the state-vector design was meant to solve and the reason it
// had to be honest about `dirty`: answering here is only sound because clean is
// produced by exactly two things — a cold load, which restores the checkpoint
// BEFORE the update observer exists, and a successful persist. Neither can leave
// the live document ahead of storage.
func TestAmbiguousCloseNoOpResendResolvesWithoutARedundantWrite(t *testing.T) {
	room, store := dirtyRoomWithRegistry(t, "barrier-ambiguous-close")
	room.members[1] = roomMember{id: 1, conn: &captureConn{}, mode: model.ModeCollaborator}
	// Simulate the state after a successful flush: the live doc IS the durable one.
	room.dirty = false
	before := store.saves.Load()

	if armSave := room.handleDurabilityRequest(1, []byte(`{"requestId":"resend"}`), false); armSave {
		t.Fatal("a request on a clean room asked for a save; the document is already durable")
	}
	if got := room.members[1].barrier; got != "" {
		t.Fatalf("a request on a clean room parked a waiter (%q) instead of answering", got)
	}
	if after := store.saves.Load(); after != before {
		t.Fatalf("a no-op resend forced %d redundant write(s)", after-before)
	}
}

// TestForwardIsSafeWithoutAConnection covers the guard that keeps a Session
// usable when it has no outbound port — the shape a unit test constructs, and one
// that must not panic while reporting a refused frame.
func TestForwardIsSafeWithoutAConnection(t *testing.T) {
	room := newBareRoom(t)
	room.lc.state.Store(int32(stateDraining))
	session := &Session{room: room, id: 1} // no conn

	session.Forward([]byte("refused, with nobody to tell"))

	if !session.dropped.Load() {
		t.Fatal("the session was not poisoned when its frame was refused")
	}
}

// TestADroppedFrameThenARequestIsRefusedOnTheRunLoop drives the COMBINED path the
// A1 prerequisite is actually about, end to end, with no timing:
//
//	U is refused by enqueue  ->  the session is poisoned
//	the room accepts work again
//	R is accepted, carries sessionDropped, and comes back persist-failed
//
// The two halves matter together. Poisoning alone would be inert if the flag never
// reached the run loop, and the seam test that calls handleDurabilityRequest with
// true directly cannot show that Forward is what supplies it.
//
// Determinism comes from toggling the room's lifecycle state — the same seam the
// enqueue-refusal tests already use — rather than wedging a 256-slot buffer for 30
// seconds or building a fake scheduler.
//
// Non-vacuity: stop Forward threading s.dropped onto the command and this fails
// with a `persisted`, because the room is clean and would answer immediately.
func TestADroppedFrameThenARequestIsRefusedOnTheRunLoop(t *testing.T) {
	room := newBareRoom(t)
	conn := &recordingConn{}

	// Join BEFORE the loop starts: nothing else is running, so touching room state
	// here is safe and avoids racing the run loop for a member.
	res := room.handleJoin(conn, model.Identity{}, model.ModeCollaborator, nil)
	if res.err != nil {
		t.Fatalf("handleJoin: %v", res.err)
	}
	startRoom(room)
	// Tear down THROUGH THE LOOP. releaseRoom calls teardown on the caller's
	// goroutine, which races the running run loop over r.members — a harness race,
	// but a real one, and the race detector is right to flag it.
	t.Cleanup(func() {
		room.enqueue(command{kind: cmdClose})
		select {
		case <-room.done:
		case <-time.After(2 * time.Second):
			t.Error("the room did not tear down")
		}
	})

	session := &Session{room: room, id: res.id, conn: conn}

	// 1. The room stops accepting work, so U's enqueue is refused.
	room.lc.state.Store(int32(stateDraining))
	session.Forward([]byte("the update that never lands"))
	if !session.dropped.Load() {
		t.Fatal("the session was not poisoned by the refused update")
	}

	// 2. The room accepts work again — a momentary backlog clearing, which is
	//    exactly the transient condition the typed end tells the client to retry.
	room.lc.state.Store(int32(stateActive))

	// 3. The request is now ACCEPTED by the room, and must still be refused, because
	//    it carries the session's poison with it.
	requestDurability(&fakeClient{t: t, session: session}, "after-the-drop")

	waitFor(t, "the request to be answered", func() bool { return conn.outcome("after-the-drop") != nil })
	got := conn.outcome("after-the-drop")
	if got.Kind != model.ControlPersistFailed {
		t.Fatalf("a request following a DROPPED update resolved %s, want persist-failed; the service would be claiming durability for a mutation it never received", got.Kind)
	}
}

// outcome returns the barrier control answering id, if one has arrived.
func (c *recordingConn) outcome(id string) *model.ControlMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.controls {
		if m.RequestID == id && (m.Kind == model.ControlPersisted || m.Kind == model.ControlPersistFailed) {
			got := m
			return &got
		}
	}
	return nil
}

// enqueueDeadlineForTest shortens the backpressure backstop for one test, so the
// refusal path is reachable without a 30-second wait. Restored on cleanup.
func enqueueDeadlineForTest(t *testing.T, d time.Duration) {
	t.Helper()
	prev := enqueueDeadline
	enqueueDeadline = d
	t.Cleanup(func() { enqueueDeadline = prev })
}

// TestATeardownEndingIsNeverOverriddenByATransientRefusal is the regression for a
// defect that would have reported DATA LOSS AS A RETRY.
//
// Room.enqueue answered one bool for two different situations: the room left
// Active (teardown in progress) and the command buffer stayed full (backpressure).
// Forward treated both as a dropped update and emitted a member-scoped TRANSIENT
// `update-not-accepted`, then closed the connection's terminal boundary.
//
// A teardown's final flush takes real time, so a socket frame arriving during it
// hits the lifecycle refusal — and the transient end raced AHEAD of the
// authoritative one. The connection's terminal boundary then REFUSED the real
// ending. An owner deletion (document-deleted, terminal) or an escalation
// (edits-not-saved, terminal — the user's work is gone) would reach the client as
// "try again later", and the client would cheerfully reconnect.
//
// Teardown is the sole owner of a session's ending. A lifecycle refusal announces
// NOTHING; only genuine backpressure does, because in that case the room is alive
// and nobody else will say anything.
//
// Non-vacuity: make Forward emit on enqueueRefusedInactive as well and this fails
// — the member sees update-not-accepted instead of, or ahead of, the real end.
func TestATeardownEndingIsNeverOverriddenByATransientRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		code model.SessionEndCode
	}{
		{"owner deleted the document", model.CodeDocumentDeleted},
		{"escalation discarded unsaved edits", model.CodeEditsNotSaved},
		{"graceful shutdown", model.CodeServerShutdown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			room := newBareRoom(t)
			conn := &recordingConn{}
			res := room.handleJoin(conn, model.Identity{}, model.ModeCollaborator, nil)
			if res.err != nil {
				t.Fatalf("handleJoin: %v", res.err)
			}
			session := &Session{room: room, id: res.id, conn: conn}

			// ONE REAL TEARDOWN, with its flush GATED so the late frame lands inside
			// the genuine window. beginTeardown owns the move to Draining and the same
			// teardown emits the authoritative end — the two halves are not
			// synthesized separately, because the bug lives in their overlap.
			lateFrameHandled := make(chan struct{})
			go func() {
				room.teardown(model.NewSessionEnd(tc.code), func() {
					// We are INSIDE teardown: beginTeardown has already set Draining and
					// the authoritative end has NOT been sent yet. This is exactly the
					// window a real final flush holds open for seconds.
					session.Forward([]byte("a frame that arrives mid-teardown"))
					close(lateFrameHandled)
				})
			}()

			select {
			case <-lateFrameHandled:
			case <-time.After(2 * time.Second):
				t.Fatal("the gated flush never ran")
			}
			select {
			case <-room.done:
			case <-time.After(2 * time.Second):
				t.Fatal("teardown did not complete")
			}

			if got := conn.endCode(); got != tc.code {
				t.Fatalf("the client was told %q, want the authoritative %q; a late frame's transient end won the terminal boundary and the real reason was refused", got, tc.code)
			}
			if conn.sawEndCode(model.CodeUpdateNotAccepted) {
				t.Fatal("a member-scoped transient end was delivered during teardown; a deletion or data loss would read to the user as a retry")
			}
			// The frame was still LOST, so the session is still poisoned — the fix
			// changes who announces the ending, not whether the update survived.
			if !session.dropped.Load() {
				t.Fatal("the late frame was silently accepted; it was refused and must poison the session")
			}
		})
	}
}

// endCode reports the code of the FIRST session end delivered, or "" if none.
func (c *recordingConn) endCode() model.SessionEndCode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended == nil {
		return ""
	}
	return c.ended.Code
}

// sawEndCode reports whether any session-end CONTROL carried this code.
func (c *recordingConn) sawEndCode(code model.SessionEndCode) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.controls {
		if m.Kind == model.ControlSessionEnd && m.Code == code {
			return true
		}
	}
	return false
}

// TestARefusalDuringTeardownIsClassifiedByCurrentLifecycle covers the residual
// race in the classification itself.
//
// A producer can enter the blocking wait while the room is ACTIVE, the room can
// begin tearing down during that wait, and the deadline can fire BEFORE done
// closes — a window as long as teardown's final flush. Classifying by the
// PRE-BLOCK check would call that backpressure and let Forward emit its
// member-scoped transient end mid-teardown, which is the precedence bug returning
// through the timeout path.
//
// Non-vacuity: make classifyRefusal return enqueueRefusedBackpressure
// unconditionally and this fails.
func TestARefusalDuringTeardownIsClassifiedByCurrentLifecycle(t *testing.T) {
	room := newBareRoom(t)

	// Active when the producer starts waiting.
	room.lc.state.Store(int32(stateActive))
	if got := room.classifyRefusal(); got != enqueueRefusedBackpressure {
		t.Fatalf("an ACTIVE room's timeout classified as %v, want backpressure", got)
	}

	// The room begins tearing down while the producer is still blocked. done has
	// NOT closed yet — that is the window.
	room.lc.state.Store(int32(stateDraining))
	if got := room.classifyRefusal(); got != enqueueRefusedInactive {
		t.Fatalf("a timeout during teardown classified as %v, want inactive; Forward would emit a competing transient end and the authoritative one would be refused", got)
	}
}
