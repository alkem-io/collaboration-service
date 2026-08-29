package service

import (
	"bytes"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
)

var canonicalEmptyUpdateV1 = func() []byte {
	doc := ycrdt.NewDoc("empty-update")
	defer doc.Destroy()
	update, err := ycrdt.EncodeStateAsUpdate(doc, nil)
	if err != nil {
		panic(err)
	}
	return update
}()

// syncOutcome reports what dispatchSync did with a sync sub-message so the caller
// can enforce presence/limits (T013/T014): whether the sub-message was a mutating
// update (SyncStep2 / Update) and whether it was actually applied. The max-doc
// size limit is enforced BEFORE the live apply (dispatchSync), so an oversized
// update never mutates or broadcasts the authoritative doc — only the offending
// connection is rejected (the "offender-only impact" contract, FR-024).
type syncOutcome struct {
	// mutating is true for a SyncStep2 / Update (a write); false for a SyncStep1
	// (read-only catch-up).
	mutating bool
	// applied is true when a mutating update was actually applied to the doc
	// (false when canMutate was false and the write was dropped for a viewer).
	applied bool
	// rejectedTooLarge is true when a mutating update was refused because applying
	// it would have grown the encoded snapshot past MaxDocBytes; the live doc was
	// left untouched (no mutation, no broadcast). The caller disconnects the
	// offender.
	rejectedTooLarge bool
	// rejectedSchema is true when the update violated the assets-root contract.
	// The write is refused and the offending session ends with content-refused:
	// its rejected struct leaves a gap in its own clock sequence, so continuing
	// on the same session would silently diverge.
	rejectedSchema bool
	// rejectedNotWritable is true when a content-bearing update arrived from a
	// member that may not write. The update is refused and the offending session
	// ends with forbidden; continuing after the refused struct would leave the
	// writer silently divergent.
	//
	// It is deliberately FALSE only for the canonical empty handshake SyncStep2.
	rejectedNotWritable bool
}

// dispatchSync classifies one framed sync message (SyncStep1 / SyncStep2 /
// Update) and applies it to the room's doc, tagging the transaction with the
// source connection's origin so the doc "update" observer can skip echoing the
// delta back to its sender. For a SyncStep1 it writes a framed SyncStep2 reply
// into reply (the catch-up the requester needs — the US5 reconnect delta). A
// mutating update is applied only when canMutate is true; a viewer's write is
// dropped (T014, the read-only gate).
//
// Classification uses protocol.InspectMessage, which decodes the frame WITHOUT
// applying it and returns a zero-copy view of the body. That is what lets the
// domain checks — the read-only gate and the MaxDocBytes budget — run strictly
// before anything reaches the authoritative doc, while the core owns the wire
// format (FR-009b, FR-019). Body is a view into framed and is consumed
// synchronously here, so it never outlives the caller's buffer.
//
// r.applyUpdate remains the SINGLE mutation chokepoint (002 FR-005): every
// entry point — local writes here and cross-pod updates — routes through it, so
// the budget cannot be bypassed on one path.
func (r *Room) dispatchSync(framed []byte, reply *bytes.Buffer, src connID, canMutate bool) (syncOutcome, error) {
	info, err := protocol.InspectMessage(framed)
	if err != nil {
		return syncOutcome{}, err
	}
	if info.Type != protocol.MessageSync {
		return syncOutcome{}, nil
	}

	switch info.SyncType {
	case protocol.SyncMessageStep1:
		// The requester sent its state vector; reply with everything it is missing
		// (SyncStep2). This is the initial sync and the reconnect catch-up (US5):
		// the diff is computed against the requester's vector, so only the delta
		// crosses the wire. Read-only — allowed for viewers.
		step2, encErr := protocol.EncodeSyncStep2(r.doc, info.Body)
		if encErr != nil {
			return syncOutcome{}, encErr
		}
		reply.Write(step2)
		return syncOutcome{}, nil

	case protocol.SyncMessageStep2, protocol.SyncMessageUpdate:
		// Structs the client has that the server is missing. A viewer's write is
		// dropped (canMutate == false). Otherwise apply with the connection-tagged
		// origin; the update observer fans the delta to the other members and skips
		// the sender.
		if !canMutate {
			// Refuse the write and SAY SO — but ONLY for a genuine edit.
			//
			// SyncMessageUpdate is a client pushing a change it has already applied
			// locally. Dropped silently, the sender believes it landed: its next
			// update sits at clock k+1 against a server that never received k, stays
			// pending forever, and the two documents never reconverge. That is the
			// case with a real producer — an in-session edit after the inactivity
			// downgrade — and it gets the control frame.
			//
			// SyncMessageStep2 is the ordinary handshake and is allowed only when its
			// update body is canonically empty. A content-bearing Step2 is a write and
			// must be refused explicitly like SyncMessageUpdate.
			contentBearing := info.SyncType == protocol.SyncMessageUpdate || !bytes.Equal(info.Body, canonicalEmptyUpdateV1)
			return syncOutcome{
				mutating:            true,
				applied:             false,
				rejectedNotWritable: contentBearing,
			}, nil
		}
		// Enforce MaxDocBytes BEFORE committing to the live doc (FR-024): an
		// oversized update must never mutate or broadcast the authoritative doc and
		// then get evicted "after the fact".
		switch r.applyUpdate(info.Body, updateOrigin{src: src}) {
		case applyRejectedTooLarge:
			return syncOutcome{mutating: true, applied: false, rejectedTooLarge: true}, nil
		case applyRejectedSchema:
			return syncOutcome{mutating: true, applied: false, rejectedSchema: true}, nil
		case applyCandidateFailed:
			// The CANDIDATE refused the bytes, so the live document is provably
			// untouched: nothing to record, nothing to broadcast, no save to arm. Not a
			// policy verdict either, so the connection stays open.
			//
			// Only the candidate path can make this claim. A live apply that fails with
			// no candidate in front of it may have partially mutated, and is left on
			// its pre-existing path in applyUpdate rather than reported here.
			return syncOutcome{mutating: true, applied: false}, nil
		}
		return syncOutcome{mutating: true, applied: true}, nil
	}

	// Unknown in-range sync sub-types are no-ops, matching y-protocols.
	return syncOutcome{}, nil
}
