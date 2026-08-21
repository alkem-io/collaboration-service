package service

import (
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// This file ratchets the residual findings surfaced by the full adversarial review
// of the 002 redesign HEAD. Each was LOW severity (no data-loss / liveness /
// security consequence) but each becomes a permanent, NON-VACUOUS test here: the
// per-test comment names exactly which fix it locks and what regression re-opens it.

// TestInvTeardownBalancesConnGauge — finding [1]. teardown (cmdClose/cmdPurge) must
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
// report read-only reason "not-authenticated", not "no-update-access". In oidc mode
// an anonymous connection resolves to model.AnonymousIdentity(), whose ActorID is
// the nil-UUID sentinel (NON-nil) — so a bare `ActorID == nil` test misclassifies
// it. NON-VACUOUS: drop the nil-UUID comparison and the first
// assertion fails.
func TestInvReadOnlyReasonAnonymousSentinel(t *testing.T) {
	// oidc anonymous (nil-UUID sentinel, non-nil).
	if got := readOnlyReasonForIdentity(model.AnonymousIdentity()); got != model.ReasonNotAuthenticated {
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

// TestInvJoinDropDuringBroadcastReArmsIdle — finding [5]. A join that returns success
// but whose presence broadcast immediately drops the just-added member (its first
// Send fails) leaves the room empty with res.err == nil. The idle re-arm must fire on
// emptiness, not only on a refused join, or the run-loop goroutine + Y.Doc leak.
// NON-VACUOUS: restore the `res.err != nil &&` guard on the cmdJoin re-arm and armIdle
// is never called here.
func TestInvJoinDropDuringBroadcastReArmsIdle(t *testing.T) {
	room := newBareRoom(t)

	// A failing joiner: handleJoin admits it (ConnOpened, res.err == nil), then
	// broadcastControl(RoomUserChange) sends to it, the Send fails, dropMember empties
	// the room — all before handleJoin returns success.
	var armed bool
	cmd := command{
		kind:     cmdJoin,
		conn:     &failingConn{},
		identity: testIdentity("a"),
		done:     make(chan joinResult, 1),
	}
	room.dispatch(cmd, func() {}, func() { armed = true }, time.NewTimer(time.Hour))

	if n := len(room.members); n != 0 {
		t.Fatalf("expected the failing joiner to be dropped during the presence broadcast, got %d member(s)", n)
	}
	if !armed {
		t.Fatal("a join that succeeded but left the room empty must re-arm the idle timer, else the room leaks its goroutine")
	}
}
