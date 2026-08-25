package service

import (
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"
)

// TestBudgetCheapSkipPreservesVerdict asserts the cheap O(1) short-circuit in
// applyWouldExceedMaxDocBytes (PR #10 finding (b)) is a refinement, not a behavior
// change: with a large cap and ample headroom a small update is admitted via the
// skip (no exact re-encode needed), while an update that would actually breach the
// cap is still rejected by the exact path. The skip must never admit an update the
// exact check would have rejected, nor reject one it would have admitted.
func TestBudgetCheapSkipPreservesVerdict(t *testing.T) {
	room := newBareRoom(t)
	// A generous cap with lots of headroom so the cheap skip path is the one taken
	// for a small update once docBytes is established.
	room.cfg.Limits.MaxDocBytes = 1 << 20 // 1 MiB

	// Establish docBytes from the current (near-empty) doc via one exact check.
	small := updateBytesFor(t, "hello ")
	if room.applyWouldExceedMaxDocBytes(small) {
		t.Fatal("a tiny update under a 1 MiB cap must be admitted")
	}
	if room.docBytes == 0 {
		t.Fatal("the exact path must establish docBytes for the subsequent cheap skip")
	}

	// With docBytes established and huge headroom, a small update must be admitted.
	// (docBytes + 2*len(update) + slack is far below the 1 MiB cap, so this returns
	// via the cheap skip without a re-encode.)
	if room.applyWouldExceedMaxDocBytes(updateBytesFor(t, "world ")) {
		t.Fatal("a small update with ample headroom must be admitted (cheap skip)")
	}

	// An update whose v1 length alone blows past the cap must still be rejected:
	// docBytes + 2*L + slack > cap defeats the skip and the exact path measures the
	// oversized result. Build the oversized text in ONE pass (a growth loop that
	// re-encodes a growing doc each iteration would be O(n^2)).
	bigBytes := make([]byte, 0, (1<<20)+(1<<16))
	for len(bigBytes) <= (1 << 20) {
		bigBytes = append(bigBytes, "OVERSIZE-PAYLOAD-CHUNK "...)
	}
	huge := updateBytesFor(t, string(bigBytes))
	if !room.applyWouldExceedMaxDocBytes(huge) {
		t.Fatal("an update that would exceed the cap must be rejected even with the cheap skip in place")
	}
}

// TestBudgetSkipDisabledForTinyCap asserts the budgetSkipSlack guard keeps the
// short-circuit sound at tiny limits: with a cap below the slack, the cheap skip
// can never fire (docBytes + 2L + slack always exceeds the cap), so the exact
// pre-commit rejection used by the existing FR-024 tests is fully preserved.
func TestBudgetSkipDisabledForTinyCap(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 256 // below budgetSkipSlack: the skip can never fire

	// Seed docBytes as if a prior exact check ran, to prove the skip still does not
	// fire even when docBytes is established (slack alone, 1024, already exceeds the
	// 256-byte cap, so docBytes + 2L + slack <= cap is unsatisfiable).
	room.docBytes = 1

	// A genuinely oversized insert (matching the existing FR-024 disconnect test's
	// ~1200-char payload) so the exact path it is forced onto actually rejects.
	big := ""
	for i := 0; i < 200; i++ {
		big += "lorem "
	}
	if !room.applyWouldExceedMaxDocBytes(updateBytesFor(t, big)) {
		t.Fatal("a breaching update under a tiny cap must be rejected (cheap skip must not fire below budgetSkipSlack)")
	}

	// And a tiny in-bounds-shaped update is NOT spuriously rejected by being forced
	// onto the exact path — a near-empty memo encodes well under 256 bytes, so the
	// exact check returns false. This proves the tiny-cap behavior is the exact
	// check's, unchanged by the short-circuit.
	if room.applyWouldExceedMaxDocBytes(updateBytesFor(t, "hi")) {
		t.Fatal("a tiny update whose exact encode is under the cap must be admitted")
	}
}

// updateBytesFor builds a real v1 Yjs update that inserts s into a fresh memo doc,
// matching the shape applyWouldExceedMaxDocBytes receives from dispatchSync.
func updateBytesFor(t *testing.T, s string) []byte {
	t.Helper()
	peer := newRoomDoc("unit")
	insertText(peer, s)
	update, err := ycrdt.EncodeStateAsUpdate(peer, nil)
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	return update
}
