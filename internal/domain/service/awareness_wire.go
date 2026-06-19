package service

import (
	"bytes"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
)

// Awareness wire framing — canonical y-protocols compatibility.
//
// The contract (contracts/ws-protocol.md) specifies the type-`1` awareness
// channel as "y-protocols awareness", and the wire protocol as y-protocols v1.
// Canonical y-protocols / y-websocket frame an awareness message as:
//
//	writeVarUint(messageType=1)
//	writeVarUint8Array(awarenessUpdateBody)   // a LENGTH-PREFIXED byte array
//
// and read it back with readVarUint8Array. The body itself
// ([count][clientID][clock][varString state]…) is identical to what the fork's
// ycrdt.EncodeAwarenessUpdate emits and ApplyAwarenessUpdate consumes — only the
// outer length prefix differs.
//
// The fork's protocol.EncodeAwarenessUpdateMessage frames the body WITHOUT that
// length prefix ([type][body]). That is server-to-server consistent but breaks
// interop with real yjs clients (proven by the JS-interop e2e harness: a
// canonical client's readVarUint8Array misreads the first body byte as a length
// and fails to decode). These helpers restore the canonical length-prefixed
// framing at the server's transport boundary, leaving the vendored CRDT core
// untouched (the fork bump is a separate, fuzz-gated WS-A concern).

// encodeAwarenessFrame frames a raw awareness-update body as a canonical
// y-protocols awareness message: [type=1][writeVarUint8Array(body)].
func encodeAwarenessFrame(body []byte) []byte {
	out := new(bytes.Buffer)
	ycrdt.WriteVarUint(out, uint64(protocol.MessageAwareness))
	ycrdt.WriteVarUint8Array(out, body)
	return out.Bytes()
}

// decodeAwarenessBody extracts the raw awareness-update body from the payload of
// a canonical awareness message (everything after the type byte, i.e. a
// length-prefixed array). It returns false when the payload is not a
// well-formed varUint8Array, so a malformed frame is dropped rather than
// misapplied.
func decodeAwarenessBody(payload []byte) ([]byte, bool) {
	dec := bytes.NewBuffer(payload)
	v, err := ycrdt.ReadVarUint8Array(dec)
	if err != nil {
		return nil, false
	}
	body, ok := v.([]byte)
	return body, ok
}
