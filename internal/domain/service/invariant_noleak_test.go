package service

import (
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvNoLeak — INV-NOLEAK (spec 002 FR-011). When a solo client trips the doc-size
// (or rate) limit, the self-disconnect empties the room — and the now-empty room MUST
// still be released, not leak its run-loop goroutine + Y.Doc. RED before 002: the
// cmdMessage dispatch never re-armed the idle timer after a self-disconnect, so a
// solo member that tripped a limit (and ignored the close control) pinned the room
// forever.
func TestInvNoLeak(t *testing.T) {
	cfg := RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: 50 * time.Millisecond, BackendTimeout: 5 * time.Second}
	cfg.Limits.MaxDocBytes = 1 // any non-trivial update blows the 1-byte cap
	m, _ := testManager(t, cfg)

	id := model.DocumentID("doc-noleak")
	c := newFakeClient(t)
	c.join(m, id, model.ContentTypeMemo)
	waitFor(t, "room live", func() bool { return m.RoomCount() == 1 })

	// Forward an oversized update → the server rejects it and disconnects the solo
	// member, emptying the room.
	peer := newRoomDoc("noleak")
	insertText(peer, "well past the one-byte cap ")
	update, err := ycrdt.EncodeStateAsUpdate(peer, nil)
	if err != nil {
		t.Fatalf("encode peer state: %v", err)
	}
	c.session.Forward(protocol.EncodeUpdate(update))

	// The emptied room must be released (idle timer re-armed after the disconnect),
	// not leaked.
	waitFor(t, "the emptied room to be released (no goroutine leak)", func() bool { return m.RoomCount() == 0 })
}
