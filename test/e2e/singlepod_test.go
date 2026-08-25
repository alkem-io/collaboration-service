//go:build e2e

package e2e

import (
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
)

// TestSinglePodMemoConvergence drives two real WebSocket clients editing a memo
// concurrently through the full app wiring; both edits survive and both clients
// converge to identical state within 1s (SC-002, NOT last-writer-wins).
func TestSinglePodMemoConvergence(t *testing.T) {
	base := testApp(t, standaloneConfig())

	a := dial(t, base, "e2e-memo", "memo")
	b := dial(t, base, "e2e-memo", "memo")
	time.Sleep(100 * time.Millisecond) // let both handshakes settle

	a.insertMemo("alpha ")
	b.insertMemo("beta ")

	if !eventually(func() bool {
		return a.memoText() == b.memoText() &&
			contains(a.memoText(), "alpha") &&
			contains(a.memoText(), "beta")
	}) {
		t.Fatalf("memo did not converge:\n  a=%q\n  b=%q", a.memoText(), b.memoText())
	}
}

// TestSinglePodWhiteboardConvergence drives two clients adding distinct id-keyed
// elements to a whiteboard concurrently; both elements converge on both clients —
// per-element merge, no loss (SC-001/SC-002).
func TestSinglePodWhiteboardConvergence(t *testing.T) {
	base := testApp(t, standaloneConfig())

	a := dial(t, base, "e2e-wb", "whiteboard")
	b := dial(t, base, "e2e-wb", "whiteboard")
	time.Sleep(100 * time.Millisecond)

	a.addElement("el-a", 10)
	b.addElement("el-b", 20)

	if !eventually(func() bool {
		return a.hasElement("el-a") && a.hasElement("el-b") &&
			b.hasElement("el-a") && b.hasElement("el-b")
	}) {
		t.Fatal("whiteboard elements did not converge across both clients")
	}
}

// TestSinglePodPersistenceRoundTrip edits a memo, lets the debounced snapshot
// persist and the room idle-release (final snapshot), then a fresh client
// reconnects and receives the persisted content — the v2 snapshot round-trip end
// to end through the real wiring (SC-003).
func TestSinglePodPersistenceRoundTrip(t *testing.T) {
	cfg := standaloneConfig()
	base := testApp(t, cfg)

	a := dial(t, base, "e2e-reload", "memo")
	time.Sleep(80 * time.Millisecond)
	a.insertMemo("durable-content ")

	// Let the (short, test-configured) debounce persist the snapshot, then close
	// the last client. With IdleReleaseSeconds=0 the room releases immediately on
	// leave — persisting a final snapshot and dropping out of the registry — so the
	// reconnect below is a genuine cold reload, not a join to a still-live room.
	time.Sleep(100 * time.Millisecond)
	a.close()
	time.Sleep(100 * time.Millisecond)

	b := dial(t, base, "e2e-reload", "memo")
	if !eventually(func() bool { return contains(b.memoText(), "durable-content") }) {
		t.Fatalf("reconnecting client did not receive persisted content: %q", b.memoText())
	}
}

// TestSinglePodPresenceAndAwarenessEviction proves the presence channel end to
// end: a peer's awareness state is fanned to the other client (SC-009), and on
// disconnect the server forces an awareness eviction so the peer stops rendering
// the departed cursor (T013 — the highest-fidelity check of the canonical
// awareness framing inside the Go suite).
func TestSinglePodPresenceAndAwarenessEviction(t *testing.T) {
	base := testApp(t, standaloneConfig())

	a := dial(t, base, "e2e-presence", "whiteboard")
	b := dial(t, base, "e2e-presence", "whiteboard")
	time.Sleep(100 * time.Millisecond)

	// A announces presence; B must observe a second awareness client state.
	a.setAwareness(ycrdt.MakeObject("user", "alice", "cursor", "10:4"))
	if !eventually(func() bool { return b.awarenessClientCount() >= 2 }) {
		t.Fatalf("B never received A's awareness; B holds %d states", b.awarenessClientCount())
	}

	// A disconnects: the server forces an awareness removal, so B drops back to
	// only its own state (the departed cursor is evicted).
	a.close()
	if !eventually(func() bool { return b.awarenessClientCount() <= 1 }) {
		t.Fatalf("B still renders A's cursor after A left; B holds %d states", b.awarenessClientCount())
	}
}

// TestSinglePodLimitBreachDisconnectsOnlyOffender proves a configured limit
// breach (here: the max-connections-per-room cap) refuses the offending
// connection while existing collaborators keep working (SC-009, constitution §V).
func TestSinglePodLimitBreachDisconnectsOnlyOffender(t *testing.T) {
	cfg := standaloneConfig()
	cfg.Limits.MaxConnsPerRoom = 1 // only one connection allowed per room
	base := testApp(t, cfg)

	// First client occupies the single slot and keeps editing successfully.
	a := dial(t, base, "e2e-capped", "memo")
	time.Sleep(80 * time.Millisecond)
	a.insertMemo("first ")
	if !eventually(func() bool { return contains(a.memoText(), "first") }) {
		t.Fatal("first client could not edit its own room")
	}

	// A second connection to the same room is refused (room full): its socket is
	// closed by the server, so a read returns an error promptly.
	if !secondConnectionRefused(t, base, "e2e-capped") {
		t.Fatal("second connection over the cap was not refused")
	}

	// The first client is unaffected by the refused join — it can still edit.
	a.insertMemo("again ")
	if !eventually(func() bool { return contains(a.memoText(), "again") }) {
		t.Fatal("capped-room incumbent was disturbed by a refused join")
	}
}

// TestSinglePodOpenModeNoAuthRequired confirms the standalone open-auth default
// upgrades a handshake with no token (SC-008: 401 is authzeval-mode only;
// open mode authenticates everyone).
func TestSinglePodOpenModeNoAuthRequired(t *testing.T) {
	base := testApp(t, standaloneConfig())
	// dial would t.Fatal on a refused handshake; reaching a successful edit proves
	// the open-mode handshake upgraded without a token.
	a := dial(t, base, "e2e-open", "memo")
	time.Sleep(60 * time.Millisecond)
	a.insertMemo("no-token ")
	if !eventually(func() bool { return contains(a.memoText(), "no-token") }) {
		t.Fatal("open-mode client could not connect/edit without a token")
	}
}

// TestSinglePodConvergenceBound is the T071/SC-002 bound on the single-pod path.
//
// "Eventually converges" is not a promise anyone typing can use. SC-002 sets a
// bound — connected clients reach identical state within one second of edits
// settling — and asserting the bound is what distinguishes a healthy fan-out from
// one that still works but has picked up a multi-second debounce, retry backoff,
// or poll interval. An unbounded eventually() passes against all of those.
//
// Both content types, because they take different paths through the room: a memo
// is a Y.XmlFragment and a whiteboard an id-keyed Y.Map, and only the whiteboard
// exercises per-property merge.
func TestSinglePodConvergenceBound(t *testing.T) {
	base := testApp(t, standaloneConfig())

	t.Run("memo", func(t *testing.T) {
		a := dial(t, base, "e2e-bound-memo", "memo")
		b := dial(t, base, "e2e-bound-memo", "memo")
		time.Sleep(80 * time.Millisecond)

		a.insertMemo("bounded ")
		if !convergedWithin(time.Second, func() bool {
			return a.memoText() == b.memoText() && contains(b.memoText(), "bounded")
		}) {
			t.Fatalf("memo clients did not converge within 1s of the edit settling:\n  a=%q\n  b=%q", a.memoText(), b.memoText())
		}
	})

	t.Run("whiteboard", func(t *testing.T) {
		a := dial(t, base, "e2e-bound-wb", "whiteboard")
		b := dial(t, base, "e2e-bound-wb", "whiteboard")
		time.Sleep(80 * time.Millisecond)

		a.addElement("el-bound", 1)
		if !convergedWithin(time.Second, func() bool { return b.hasElement("el-bound") }) {
			t.Fatal("whiteboard clients did not converge within 1s of the edit settling")
		}
	})
}
