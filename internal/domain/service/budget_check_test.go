package service

import (
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"
)

// updateInserting builds a v1 update that inserts s into a scratch document, in
// the shape the budget check receives from a client.
func updateInserting(t *testing.T, s string) []byte {
	t.Helper()
	doc := ycrdt.NewDoc("client")
	insertText(doc, s)
	update, err := ycrdt.EncodeStateAsUpdate(doc, nil)
	if err != nil {
		t.Fatalf("encode update: %v", err)
	}
	return update
}

// TestBudgetCheckFailsOpenOnAServerFault is the branch worth defending, and the
// reason this function returns false in four separate error paths.
//
// A true here REJECTS the update and evicts the sender as an offender. When the
// reason is our own encode/apply failing rather than the client's update being
// too large, that punishes a client for a server fault — a correct edit is thrown
// away and the person who made it is disconnected, with nothing in the client's
// behaviour to explain it. So a measurement failure must fail OPEN and be loud in
// the logs (§VIII), never fail closed.
//
// Driven with bytes that are not a decodable update at all, which is what reaches
// the scratch-apply path.
func TestBudgetCheckFailsOpenOnAServerFault(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 64 // small, so the cheap skip cannot short-circuit
	room.docBytes = 0                // force the exact path

	if room.applyWouldExceedMaxDocBytes([]byte{0xff, 0xff, 0xff, 0xff}) {
		t.Fatal("an unmeasurable update was reported as over budget; a server-side measurement failure must fail OPEN, or a client is disconnected as an offender for our fault")
	}
}

// TestBudgetCheckDisabledWhenNoLimitConfigured covers the limit<=0 branch: zero
// means "no cap", not "cap of zero" — the latter would reject every edit.
func TestBudgetCheckDisabledWhenNoLimitConfigured(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 0

	big := updateInserting(t, strings.Repeat("x", 4096))
	if room.applyWouldExceedMaxDocBytes(big) {
		t.Fatal("MaxDocBytes=0 must disable the cap entirely; treating it as a zero-byte cap would reject every edit")
	}
}

// TestBudgetCheckSkipsTheExactMeasurementWhenItCannotPossiblyExceed covers the
// cheap-skip path — the one that keeps the common case off an O(document) encode.
//
// The skip must be SOUND: it may only skip when applying the update cannot reach
// the cap even in the worst case. It is asserted by observing that docBytes is
// left untouched, since the exact path always refreshes it.
func TestBudgetCheckSkipsTheExactMeasurementWhenItCannotPossiblyExceed(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 1 << 20
	room.docBytes = 100 // small known size, far from the cap

	small := updateInserting(t, "tiny")
	if room.applyWouldExceedMaxDocBytes(small) {
		t.Fatal("a small update against a large cap must not be over budget")
	}
	if room.docBytes != 100 {
		t.Fatalf("docBytes = %d, want it untouched at 100: the cheap skip must not have paid for the exact encode", room.docBytes)
	}
}

// TestBudgetCheckRejectsAnUpdateThatExceedsTheCap is the positive case, so the
// tests above cannot pass against a function that always returns false.
func TestBudgetCheckRejectsAnUpdateThatExceedsTheCap(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.Limits.MaxDocBytes = 512
	room.docBytes = 0

	big := updateInserting(t, strings.Repeat("x", 8192))
	if !room.applyWouldExceedMaxDocBytes(big) {
		t.Fatal("an update that grows the document past MaxDocBytes must be rejected")
	}
	if room.docBytes == 0 {
		t.Fatal("the exact path must refresh docBytes from the encode it just paid for, or the next cheap skip measures against a stale size")
	}
}
