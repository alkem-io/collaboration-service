package service

import (
	"bytes"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
)

// dispatchSync decodes one framed sync message (SyncStep1 / SyncStep2 / Update)
// and applies it to the room's doc, tagging the transaction with the source
// connection's origin so the doc "update" observer can skip echoing the delta
// back to its sender. For a SyncStep1 it writes a framed SyncStep2 reply into
// reply (the catch-up the requester needs — the US5 reconnect delta).
//
// We dispatch the sub-message ourselves rather than via SyncHandler.HandleMessage
// because that helper applies updates with the handler as the transaction
// origin; the room needs the *connection* as origin so onDocUpdate can filter
// the echo. The protocol package stays the canonical reference for the wire
// shape (its EncodeSyncStep1/2 and framing are reused verbatim elsewhere).
func (r *Room) dispatchSync(framed []byte, reply *bytes.Buffer, src connID) error {
	in := bytes.NewBuffer(framed)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		return err
	}
	if msgType != protocol.MessageSync {
		return nil
	}

	dec := ycrdt.NewUpdateDecoderV1(payload)
	sub := ycrdt.ReadVarUint(dec.GetRestDecoder())

	switch sub {
	case ycrdt.MessageYjsSyncStep1:
		// The requester sent its state vector; reply with everything it is
		// missing (SyncStep2). This is the initial sync and the reconnect
		// catch-up (US5): the diff is computed against the requester's vector,
		// so only the delta crosses the wire.
		sv, decErr := ycrdt.ReadVarUint8Array(dec.GetRestDecoder())
		if decErr != nil {
			return decErr
		}
		enc := ycrdt.NewUpdateEncoderV1()
		ycrdt.WriteSyncStep2(enc, r.doc, sv.([]byte))
		protocol.WriteMessage(reply, protocol.MessageSync, enc.ToUint8Array())

	case ycrdt.MessageYjsSyncStep2, ycrdt.MessageYjsUpdate:
		// Structs the client has that the server is missing. Apply with the
		// connection-tagged origin; the update observer fans the delta to the
		// other members and skips the sender.
		data, decErr := ycrdt.ReadVarUint8Array(dec.GetRestDecoder())
		if decErr != nil {
			return decErr
		}
		ycrdt.ApplyUpdate(r.doc, data.([]byte), updateOrigin{src: src})
	}

	return nil
}
