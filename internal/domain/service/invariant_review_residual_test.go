package service

import (
	"context"
	"errors"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// This file ratchets the residual findings surfaced by the full adversarial review
// of the 002 redesign HEAD. Each was LOW severity (no data-loss / liveness /
// security consequence) but each becomes a permanent, NON-VACUOUS test here: the
// per-test comment names exactly which fix it locks and what regression re-opens it.

// TestInvTeardownBalancesConnGauge — finding [1]. teardown (cmdClose/cmdCloseDeleted) must
// account ConnClosed for every member still attached, because those clients never
// traverse the per-connection Leave path. Without the balancing walk in teardown the
// connections_active gauge leaks upward by the live-member count on every forced
// close/purge. NON-VACUOUS: delete the `for id := range r.members` loop in teardown
// and connsClosed stays 0 → this fails.
func TestInvTeardownBalancesConnGauge(t *testing.T) {
	room := newBareRoom(t)
	metrics := &countingMetrics{}
	room.metrics = metrics

	// Three clients attached at teardown; each opened the gauge at join.
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}
	room.members[2] = roomMember{id: 2, conn: &captureConn{}}
	room.members[3] = roomMember{id: 3, conn: &captureConn{}}
	metrics.connsOpen.Store(3)

	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	if c := metrics.connsClosed.Load(); c != 3 {
		t.Fatalf("teardown closed %d connections, want 3 (connections_active gauge leaks by %d)", c, 3-c)
	}
	if o, c := metrics.connsOpen.Load(), metrics.connsClosed.Load(); o != c {
		t.Fatalf("connections_active gauge unbalanced after teardown: opened=%d closed=%d", o, c)
	}
	if n := len(room.members); n != 0 {
		t.Fatalf("teardown left %d members attached", n)
	}
}

// TestInvReadOnlyReasonAnonymousSentinel — finding [2]. An anonymous viewer must
// report read-only reason "not-authenticated", not "no-update-access".
//
// TWO DISTINCT ANONYMOUS SHAPES, and that is the whole point. Open mode yields a
// NIL ActorID. The gateway, for an un-credentialed caller, stamps the NIL-UUID
// sentinel (server: ANONYMOUS_ACTOR_ID), which parses to a NON-nil pointer to
// uuid.Nil — so a bare `ActorID == nil` test misclassifies it as an authenticated
// actor and reports no-update-access.
//
// The sentinel is constructed HERE rather than via a helper: the helper existed
// only for the removed direct-validation adapter, while the VALUE still arrives
// from the gateway on every anonymous production handshake.
//
// NON-VACUOUS: drop the nil-UUID comparison and the first assertion fails.
func TestInvReadOnlyReasonAnonymousSentinel(t *testing.T) {
	// Gateway-stamped anonymous: pointer to uuid.Nil, NON-nil.
	anonymous := uuid.Nil
	if got := readOnlyReasonForIdentity(model.Identity{ActorID: &anonymous}); got != model.ReasonNotAuthenticated {
		t.Fatalf("anonymous sentinel identity → %q, want %q", got, model.ReasonNotAuthenticated)
	}
	// open mode (nil ActorID).
	if got := readOnlyReasonForIdentity(model.Identity{}); got != model.ReasonNotAuthenticated {
		t.Fatalf("nil ActorID → %q, want %q", got, model.ReasonNotAuthenticated)
	}
	// A real, resolved actor that was denied update-content.
	if got := readOnlyReasonForIdentity(testIdentity("authenticated")); got != model.ReasonNoUpdateAccess {
		t.Fatalf("resolved actor identity → %q, want %q", got, model.ReasonNoUpdateAccess)
	}
}

// TestInvRateLimitChargesMalformedFrames — finding [4]. The per-connection update
// rate must be enforced BEFORE parsing, so a flood of unparseable frames is charged
// (and ultimately disconnected) like valid traffic — otherwise a client spams
// parse-failing frames (each a parse attempt + a WARN log) without ever tripping the
// limit. NON-VACUOUS: move allowRate back below protocol.ReadMessage and the
// truncated frames return at the parse error before being charged → no disconnect →
// this times out.
func TestInvRateLimitChargesMalformedFrames(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.UpdateRatePerSec = 5
	cfg.Limits.UpdateBurst = 2
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	c := newFakeClient(t)
	c.join(mgr, "rate-malformed", model.ContentTypeMemo)
	c.observeUpdates()

	// A truncated frame: a lone continuation-bit byte fails the OUTER ReadMessage, so
	// pre-fix it returned before the rate gate and was never charged.
	malformed := []byte{0xFF}
	for i := 0; i < 30; i++ {
		c.session.Forward(malformed)
	}

	waitFor(t, "malformed-frame flood disconnected (rate charged before parse)", func() bool {
		for _, m := range controlMessages(c) {
			if m.Kind == model.ControlSessionEnd && m.Code == model.CodeUpdateRateExceeded {
				return true
			}
		}
		return false
	})
}

// TestInvJoinIsNotBroadcastVisibleBeforeActivation pins the two-phase join: no
// ordinary room Send may reach a connection before the handler has patiently
// queued the complete initial batch.
func TestInvJoinIsNotBroadcastVisibleBeforeActivation(t *testing.T) {
	room := newBareRoom(t)

	var armed bool
	done := make(chan joinResult, 1)
	cmd := command{
		kind:     cmdJoin,
		conn:     &failingConn{},
		identity: testIdentity("a"),
		done:     done,
	}
	room.dispatch(cmd, func() {}, func() { armed = true }, time.NewTimer(time.Hour))

	result := <-done
	member, ok := room.members[result.id]
	if !ok || !member.pending {
		t.Fatalf("joined member = (%v, pending=%v), want pending", ok, member.pending)
	}
	if armed {
		t.Fatal("a pending join was treated as an empty room")
	}
	if len(result.frames) == 0 {
		t.Fatal("pending join returned no patient initial batch")
	}
}

// A client may answer the already-queued SyncStep1 before the handler's
// activation command reaches the room loop. Pending means broadcast-invisible,
// not unable to complete the handshake.
func TestInvPendingJoinHandlesHandshakeReply(t *testing.T) {
	room := newBareRoom(t)
	conn := &captureConn{}
	result := room.handleJoin(conn, testIdentity("pending-handshake"), model.ModeCollaborator, nil)
	if result.err != nil {
		t.Fatalf("join: %v", result.err)
	}

	peer := ycrdt.NewDoc("pending-peer")
	defer peer.Destroy()
	room.handleMessage(result.id, protocol.EncodeSyncStep1(peer), false)
	if conn.count() != 1 {
		t.Fatalf("pending handshake replies = %d, want 1", conn.count())
	}
}

func TestInvPendingLeaveReleasesReservationWithoutPresenceBroadcast(t *testing.T) {
	room := newBareRoom(t)
	active := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: active}
	pending := room.handleJoin(&captureConn{}, testIdentity("pending-leave"), model.ModeCollaborator, nil)
	if pending.err != nil {
		t.Fatalf("join: %v", pending.err)
	}

	room.handleLeave(pending.id)
	if _, exists := room.members[pending.id]; exists {
		t.Fatal("pending reservation leaked after leave")
	}
	if active.count() != 0 {
		t.Fatalf("pending leave emitted %d presence frame(s), want 0", active.count())
	}
}

func TestInvActivationIsIdempotentAndRejectsUnknownMember(t *testing.T) {
	room := newBareRoom(t)
	metrics := &countingMetrics{}
	room.metrics = metrics

	if err := room.handleActivate(99); !errors.Is(err, errRoomUnavailable) {
		t.Fatalf("activate absent member = %v, want errRoomUnavailable", err)
	}

	room.members[1] = roomMember{id: 1, conn: &captureConn{}, pending: true}
	if err := room.handleActivate(1); err != nil {
		t.Fatalf("first activation: %v", err)
	}
	if err := room.handleActivate(1); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
	if got := metrics.connsOpen.Load(); got != 1 {
		t.Fatalf("ConnOpened calls = %d, want 1", got)
	}
}

func TestInvSessionActivationAlwaysSettles(t *testing.T) {
	t.Run("inactive room refuses before enqueue", func(t *testing.T) {
		room := newBareRoom(t)
		room.lc.state.Store(int32(stateDraining))
		session := &Session{room: room, id: 1}
		if err := session.Activate(context.Background()); !errors.Is(err, errRoomUnavailable) {
			t.Fatalf("Activate = %v, want errRoomUnavailable", err)
		}
	})

	t.Run("caller cancellation settles an admitted command", func(t *testing.T) {
		room := newBareRoom(t)
		room.lc.state.Store(int32(stateActive))
		session := &Session{room: room, id: 1}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := session.Activate(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Activate = %v, want context.Canceled", err)
		}
	})

	t.Run("room teardown settles an admitted command", func(t *testing.T) {
		room := newBareRoom(t)
		room.lc.state.Store(int32(stateActive))
		session := &Session{room: room, id: 1}
		result := make(chan error, 1)
		go func() { result <- session.Activate(context.Background()) }()
		waitFor(t, "activation command enqueue", func() bool { return len(room.commands) == 1 })
		close(room.done)
		if err := <-result; !errors.Is(err, errRoomUnavailable) {
			t.Fatalf("Activate = %v, want errRoomUnavailable", err)
		}
	})
}
