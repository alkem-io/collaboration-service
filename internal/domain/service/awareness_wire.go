package service

import (
	"github.com/antst/go-yjs/protocol"
)

// Awareness wire framing — canonical y-protocols compatibility.
//
// The contract (contracts/ws-protocol.md) specifies the type-`1` awareness
// channel as "y-protocols awareness", framed as:
//
//	writeVarUint(messageType=1)
//	writeVarUint8Array(awarenessUpdateBody)   // LENGTH-PREFIXED
//
// This file used to hand-roll that framing. The old y-crdt fork's
// EncodeAwarenessUpdateMessage omitted the length prefix, which was
// server-to-server consistent but broke real Yjs clients (a canonical client's
// readVarUint8Array misread the first body byte as a length). go-yjs frames it
// canonically, so the workaround is gone: both helpers now delegate to the core
// rather than re-implementing its primitives (FR-007, §VIII).

// encodeAwarenessFrame frames a raw awareness-update body as a canonical
// y-protocols awareness message: [type=1][writeVarUint8Array(body)].
func encodeAwarenessFrame(body []byte) []byte {
	return protocol.EncodeAwarenessUpdateMessage(body)
}

// awarenessBody extracts the raw awareness-update body from a full framed
// awareness message. It returns false when the frame is not a well-formed
// canonical awareness message, so a malformed frame is dropped rather than
// misapplied.
//
// Strictness note: protocol.InspectMessage decodes the length-prefixed body but
// tolerates trailing bytes after it. A canonical awareness frame is exactly one
// length-prefixed array, so a tail means a malformed frame. We re-derive the
// canonical frame length from the exported FrameLength/BodyLength and reject any
// mismatch, preserving the guarantee the previous hand-rolled decoder gave.
func awarenessBody(frame []byte) ([]byte, bool) {
	info, err := protocol.InspectMessage(frame)
	if err != nil || info.Type != protocol.MessageAwareness {
		return nil, false
	}
	// [type=1 as a 1-byte varuint][body length as varuint][body]
	if info.BodyLength < 0 {
		return nil, false
	}
	if info.FrameLength != 1+varUintLen(uint64(info.BodyLength))+info.BodyLength {
		return nil, false
	}
	return info.Body, true
}

// varUintLen returns the encoded byte length of v as a lib0 varuint (7 payload
// bits per byte, continuation bit set on all but the last).
func varUintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
