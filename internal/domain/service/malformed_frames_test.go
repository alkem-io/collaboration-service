package service

import (
	"testing"
)

// TestMalformedFramesAreDroppedWithoutHarmingTheRoom covers the drop paths on
// both the client and the peer side.
//
// This is the offender-only property (FR-009c): a frame that cannot be parsed is
// discarded and nothing else changes. The alternatives are both worse than
// dropping — applying a partially-decoded frame corrupts the authoritative
// document for everyone, and tearing the room down turns one bad client (or one
// truncated peer delta) into an outage for every collaborator in it.
//
// The peer side matters more than it looks: peer frames arrive from another pod
// over the fan-out bus, so a malformed one is not attributable to any client in
// this room. Reacting to it by disconnecting people would punish an entirely
// uninvolved set of users for a bus-level fault.
func TestMalformedFramesAreDroppedWithoutHarmingTheRoom(t *testing.T) {
	garbage := [][]byte{
		nil,
		{},
		{0xff},
		{0xff, 0xff, 0xff, 0xff},
		{0x00}, // a sync frame header with no body
		{0x01}, // an awareness frame header with no body
	}

	t.Run("client frames", func(t *testing.T) {
		room := newBareRoom(t)
		// The sender MUST be a registered member. handleMessage drops frames from
		// unknown sources before it ever parses, so a bare room would make this test
		// pass by exercising that gate instead — asserting the drop-malformed
		// property while never reaching the parser. (It did, until this line.)
		room.members[1] = roomMember{id: 1, conn: &captureConn{}}
		insertText(room.doc, "existing content ")
		before := xmlText(room.doc)

		for _, frame := range garbage {
			// handleMessage returns "should tear down" — it must never be true for a
			// frame the room simply could not parse.
			if room.handleMessage(1, frame, false) {
				t.Fatalf("a malformed client frame (%v) asked for a room teardown; one bad client must not become an outage for every collaborator", frame)
			}
		}
		if got := xmlText(room.doc); got != before {
			t.Fatalf("a malformed frame changed the document: %q → %q", before, got)
		}
	})

	t.Run("peer frames", func(t *testing.T) {
		room := newBareRoom(t)
		insertText(room.doc, "existing content ")
		before := xmlText(room.doc)

		for _, frame := range garbage {
			room.handlePeer(frame, false)
			room.handlePeer(frame, true)
		}
		if got := xmlText(room.doc); got != before {
			t.Fatalf("a malformed peer frame changed the document: %q → %q", before, got)
		}
	})
}

// TestClientControlFramesAreIgnored covers the default arm of the client
// dispatch: control is server→client only, and a client sending one is ignored
// rather than treated as an error (y-protocols leniency).
//
// Ignoring rather than rejecting matters because the frame type space is shared
// with future protocol additions: a client speaking a newer dialect must degrade
// to "that message did nothing" rather than be disconnected.
func TestClientControlFramesAreIgnored(t *testing.T) {
	room := newBareRoom(t)
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}
	before := xmlText(room.doc)

	for _, msgType := range []byte{0x03, 0x7f} { // control, and an unassigned type
		if room.handleMessage(1, []byte{msgType, 0x00}, false) {
			t.Fatalf("a client-sent frame of type %#x asked for a teardown; unknown types must be ignored, not fatal", msgType)
		}
	}
	if got := xmlText(room.doc); got != before {
		t.Fatal("an ignored frame changed the document")
	}
}

// TestAwarenessTheDecoderRejectsIsNotRelayedToPeers is the regression for an
// offender-only violation found by independent review (FR-009c).
//
// A frame can be well-formed enough to pass the outer framing check and still be
// rejected by the awareness decoder. The room used to broadcast it anyway, on
// the reasoning that peers would "apply it against their own state" — but an
// awareness apply fails on the BYTES, not on the state, so a frame that fails
// here fails identically for every recipient.
//
// Relaying it therefore inverts offender-only: one client's bad frame costs
// every other client in the room, and every other pod on the bus, a failed
// decode. The sender is the only party that should bear its own malformed input.
//
// Non-vacuity: remove the `return false` after the failed apply and this fails —
// the frame reaches the other member and the peer bus.
func TestAwarenessTheDecoderRejectsIsNotRelayedToPeers(t *testing.T) {
	room := newBareRoom(t)
	sender := &captureConn{}
	observer := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: sender}
	room.members[2] = roomMember{id: 2, conn: observer}

	// A frame whose length-prefixed awareness body is present (so it clears the
	// framing check) but is not a decodable awareness update.
	frame := []byte{0x01, 0x04, 0xff, 0xff, 0xff, 0xff}

	before := observer.count()
	if room.handleMessage(1, frame, false) {
		t.Fatal("a rejected awareness frame must not ask for a room teardown")
	}

	if got := observer.count() - before; got != 0 {
		t.Fatalf("the other member received %d frame(s) the awareness decoder had already rejected; every recipient fails the same decode, so one client's bad frame becomes everyone's work (FR-009c)", got)
	}
}
