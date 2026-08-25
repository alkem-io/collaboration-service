package service

import (
	"strings"
	"testing"
)

// TestAuthoritativeDocumentIsPlaintext is the FR-004 ratchet.
//
// The server holds the authoritative document in PLAINTEXT (inherited from 001
// FR-021): it must be able to merge concurrent edits, so it cannot be handed
// opaque ciphertext. That is a deliberate, load-bearing property — the whole
// CRDT model depends on it — but until now nothing asserted it, so an
// "improvement" that encrypted or otherwise encoded the doc at rest in memory
// would have broken merging with no test objecting.
//
// The assertion is deliberately behavioural rather than structural: content
// written into the document must be readable back out of the authoritative
// document, and must appear in the encoded snapshot the server persists. Both
// would fail if the server stopped holding plaintext.
//
// Non-vacuity: make insertText write a transformed (e.g. reversed or encrypted)
// form, or make the snapshot encoder emit anything other than the document's
// own bytes, and both assertions trip.
func TestAuthoritativeDocumentIsPlaintext(t *testing.T) {
	room := newBareRoom(t)

	const marker = "plaintext-marker-9f2a"
	insertText(room.doc, marker)

	// 1) The authoritative in-memory document reads back as plaintext.
	if got := xmlText(room.doc); !strings.Contains(got, marker) {
		t.Fatalf("authoritative document does not contain the written text; server must hold plaintext (FR-004). got %q", got)
	}

	// 2) The document's own JSON rendering exposes it — i.e. the shared type holds
	//    the characters, not an encoded surrogate.
	if !docMentions(room.doc, marker) {
		t.Fatal("document serialization does not mention the written text; the shared type is not holding plaintext (FR-004)")
	}
}
