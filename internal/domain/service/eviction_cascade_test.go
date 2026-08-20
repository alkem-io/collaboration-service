package service

import (
	"errors"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// failingConn is a service.Conn whose Send always fails — modelling a client whose
// outbound queue is full (the slow-consumer shed). It counts Send calls and, as a
// safety valve, stops failing after a high threshold so a regression that recurses
// unbounded cannot stack-overflow and kill the whole test binary before the
// assertion runs; a correct (bounded) cascade never approaches the valve.
type failingConn struct {
	mu    sync.Mutex
	calls int
}

// CloseAfterDrain implements service.Conn. This double models a connection whose
// writes always fail, so an end intent is simply discarded.
func (f *failingConn) CloseAfterDrain(_ model.SessionEnd) {}

const failingConnSafetyValve = 5000

func (f *failingConn) Send(_ []byte) error {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	if n >= failingConnSafetyValve {
		return nil // valve: prevent an unbounded-recursion regression from crashing the binary.
	}
	return errors.New("send queue full")
}

func (f *failingConn) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// announceAwareness registers a member in the room with a real awareness id so its
// eviction broadcasts a forced-removal frame (the path that re-enters the cascade).
func announceAwareness(r *Room, id connID, clientID ycrdt.Number, conn Conn) {
	// Seed a REMOTE client's awareness through the public path: a peer awareness
	// pinned to clientID publishes its state and the room merges it. Awareness
	// States/Meta are unexported in go-yjs, and going through encode/apply models
	// what actually crosses the wire instead of reaching into core internals — so
	// this test exercises the same code path production does.
	peerDoc := ycrdt.NewDoc("peer", ycrdt.WithClientID(clientID))
	peerAw := ycrdt.NewAwareness(peerDoc)
	if err := peerAw.SetLocalState(ycrdt.MakeObject("user", "test")); err != nil {
		panic(err)
	}
	update := ycrdt.EncodeAwarenessUpdate(peerAw, []ycrdt.Number{clientID}, nil)
	if err := ycrdt.ApplyAwarenessUpdate(r.awareness, update, nil); err != nil {
		panic(err)
	}
	r.members[id] = roomMember{id: id, conn: conn, awarenessID: clientID, hasAwareness: true}
}

// TestDropMemberBoundsCrossFailingCascade defends the dropMember deregister-before-
// evict ordering (room.go). Two members each have a failing Send and an announced
// awareness id. Dropping one evicts its awareness, whose broadcast Send fails for
// the OTHER member and re-enters dropMember for it; that eviction's broadcast Send
// then fails back for the first. With the fix (delete from r.members BEFORE
// evictAwareness), the re-entrant dropMember is a no-op (already gone) and the
// cascade is bounded to one drop per member. Both members must end up removed and
// the run loop must converge.
//
// Non-vacuity: move `delete(r.members, id)` to AFTER `r.evictAwareness(m)` in
// dropMember (the pre-fix order) and this test fails — the two members recurse into
// each other (sendMember→broadcast→evictAwareness→dropMember→…), each level adding
// a Send, until the failingConn safety valve trips; the Send count then blows far
// past the asserted bound (and, without the valve, the process would stack-overflow
// and crash).
func TestDropMemberBoundsCrossFailingCascade(t *testing.T) {
	room := newBareRoom(t)
	a, b := &failingConn{}, &failingConn{}
	announceAwareness(room, 1, 101, a)
	announceAwareness(room, 2, 202, b)

	done := make(chan bool, 1)
	go func() {
		// Dropping member 1 cascades into member 2 (its eviction broadcast fails for
		// 2) and, with the bug, back into 1.
		dropped := room.dropMember(1)
		done <- dropped
	}()

	select {
	case dropped := <-done:
		if !dropped {
			t.Fatal("dropMember(1) reported the member was absent")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dropMember did not converge: the cross-failing eviction cascade is recursing unbounded")
	}

	// Both members must be gone, and the cascade must have been bounded: with the
	// fix each conn's Send is hit only a handful of times (the initial shed plus the
	// peer's single eviction broadcast), nowhere near the safety valve.
	if _, ok := room.members[1]; ok {
		t.Error("member 1 still registered after dropMember")
	}
	if _, ok := room.members[2]; ok {
		t.Error("member 2 still registered after the cascade")
	}
	if total := a.count() + b.count(); total > 20 {
		t.Fatalf("Send was called %d times: the eviction cascade is not bounded (deregister-before-evict regressed)", total)
	}
}
