package service

import (
	"bytes"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
)

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
}

// dispatchSync decodes one framed sync message (SyncStep1 / SyncStep2 / Update)
// and applies it to the room's doc, tagging the transaction with the source
// connection's origin so the doc "update" observer can skip echoing the delta
// back to its sender. For a SyncStep1 it writes a framed SyncStep2 reply into
// reply (the catch-up the requester needs — the US5 reconnect delta). A mutating
// update (SyncStep2 / Update) is applied only when canMutate is true; a viewer's
// write is dropped (T014, the read-only gate).
//
// We dispatch the sub-message ourselves rather than via SyncHandler.HandleMessage
// because that helper applies updates with the handler as the transaction
// origin; the room needs the *connection* as origin so onDocUpdate can filter
// the echo. The protocol package stays the canonical reference for the wire
// shape (its EncodeSyncStep1/2 and framing are reused verbatim elsewhere).
func (r *Room) dispatchSync(framed []byte, reply *bytes.Buffer, src connID, canMutate bool) (syncOutcome, error) {
	in := bytes.NewBuffer(framed)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		return syncOutcome{}, err
	}
	if msgType != protocol.MessageSync {
		return syncOutcome{}, nil
	}

	dec := ycrdt.NewUpdateDecoderV1(payload)
	sub := ycrdt.ReadVarUint(dec.GetRestDecoder())

	switch sub {
	case ycrdt.MessageYjsSyncStep1:
		// The requester sent its state vector; reply with everything it is
		// missing (SyncStep2). This is the initial sync and the reconnect
		// catch-up (US5): the diff is computed against the requester's vector,
		// so only the delta crosses the wire. Read-only — allowed for viewers.
		sv, decErr := ycrdt.ReadVarUint8Array(dec.GetRestDecoder())
		if decErr != nil {
			return syncOutcome{}, decErr
		}
		enc := ycrdt.NewUpdateEncoderV1()
		if err := ycrdt.WriteSyncStep2(enc, r.doc, sv.([]byte)); err != nil {
			return syncOutcome{}, err
		}
		protocol.WriteMessage(reply, protocol.MessageSync, enc.ToUint8Array())
		return syncOutcome{}, nil

	case ycrdt.MessageYjsSyncStep2, ycrdt.MessageYjsUpdate:
		// Structs the client has that the server is missing. A viewer's write is
		// dropped (canMutate == false). Otherwise apply with the connection-tagged
		// origin; the update observer fans the delta to the other members and
		// skips the sender.
		data, decErr := ycrdt.ReadVarUint8Array(dec.GetRestDecoder())
		if decErr != nil {
			return syncOutcome{}, decErr
		}
		update := data.([]byte)
		if !canMutate {
			return syncOutcome{mutating: true, applied: false}, nil
		}
		// Enforce MaxDocBytes BEFORE committing to the live doc (FR-024): an
		// oversized update must never mutate or broadcast the authoritative doc and
		// then get evicted "after the fact". Scratch-apply the update onto a clone
		// of the current state and measure; reject without touching r.doc when the
		// result would exceed the limit.
		if !r.applyUpdate(update, updateOrigin{src: src}) {
			return syncOutcome{mutating: true, applied: false, rejectedTooLarge: true}, nil
		}
		return syncOutcome{mutating: true, applied: true}, nil
	}

	return syncOutcome{}, nil
}
