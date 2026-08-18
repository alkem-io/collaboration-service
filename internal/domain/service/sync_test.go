package service

import (
	"bytes"
	"sync"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// newBareRoom builds a memo room without starting its run loop, for unit-testing
// the sync primitives directly (no goroutine, no timers).
func newBareRoom(t *testing.T) *Room {
	t.Helper()
	deps := newTestDeps()
	deps.Contributor = noopContributor{}
	r := &Room{
		id:           "unit",
		content:      model.ContentTypeMemo,
		doc:          newRoomDoc("unit"),
		deps:         deps.Deps,
		cfg:          DefaultRoomConfig(),
		metrics:      NopMetrics{},
		logger:       zap.NewNop(),
		commands:     make(chan command, 8),
		done:         make(chan struct{}),
		members:      make(map[connID]roomMember),
		contributors: make(map[string]struct{}),
	}
	r.awareness = ycrdt.NewAwareness(r.doc)
	applyConvention(r.doc, model.ContentTypeMemo)
	return r
}

// TestDispatchSyncStep1ProducesDelta is the focused y-protocols handshake unit
// test against the vendored core: a SyncStep1 carrying a peer's state vector
// must yield a framed SyncStep2 whose application brings the peer to the server's
// state — the catch-up delta (the mechanism behind initial sync and US5
// reconnect). It uses only the protocol package's public framing.
func TestDispatchSyncStep1ProducesDelta(t *testing.T) {
	room := newBareRoom(t)
	insertText(room.doc, "server-only ")

	// A peer with an empty doc asks for what it is missing.
	peer := ycrdt.NewDoc("unit")
	step1 := protocol.EncodeSyncStep1(peer)

	var reply bytes.Buffer
	if _, err := room.dispatchSync(step1, &reply, 1, true); err != nil {
		t.Fatalf("dispatchSync(SyncStep1): %v", err)
	}
	if reply.Len() == 0 {
		t.Fatal("SyncStep1 produced no SyncStep2 reply")
	}

	// The reply is a framed MessageSync; apply it to the peer via a SyncHandler
	// (the canonical client path) and assert convergence.
	ph := protocol.NewSyncHandler(peer)
	var discard bytes.Buffer
	if _, err := ph.HandleMessage(reply.Bytes(), &discard); err != nil {
		t.Fatalf("peer HandleMessage(SyncStep2): %v", err)
	}
	if got := xmlText(peer); !contains(got, "server-only") {
		t.Fatalf("peer did not catch up via SyncStep2 delta: %q", got)
	}
}

// TestDispatchSyncUpdateAppliesToServer asserts a framed sync Update from a peer
// is applied to the room's authoritative doc (the server-side write path).
func TestDispatchSyncUpdateAppliesToServer(t *testing.T) {
	room := newBareRoom(t)

	// Build an update on a peer doc and frame it as a sync Update.
	peer := ycrdt.NewDoc("unit")
	insertText(peer, "from-peer ")
	update, err := ycrdt.EncodeStateAsUpdate(peer, nil)
	if err != nil {
		t.Fatalf("encode peer state: %v", err)
	}
	framed := protocol.EncodeUpdate(update)

	var reply bytes.Buffer
	if _, err := room.dispatchSync(framed, &reply, 7, true); err != nil {
		t.Fatalf("dispatchSync(Update): %v", err)
	}
	if got := xmlText(room.doc); !contains(got, "from-peer") {
		t.Fatalf("server did not apply the peer update: %q", got)
	}
}

// TestOnDocUpdateSkipsOriginator asserts the update observer fans an applied
// delta to every member except the one that produced it (echo filtering), and
// marks the room dirty for the debounce.
func TestOnDocUpdateSkipsOriginator(t *testing.T) {
	room := newBareRoom(t)

	originator := &captureConn{}
	other := &captureConn{}
	room.members[connID(1)] = roomMember{id: 1, conn: originator}
	room.members[connID(2)] = roomMember{id: 2, conn: other}

	// Apply an update tagged with connection 1's origin; the observer must fan
	// it to connection 2 only.
	room.doc.On("update", ycrdt.NewObserverHandler(room.onDocUpdate))
	peer := ycrdt.NewDoc("unit")
	insertText(peer, "x ")
	peerUpdate, err := ycrdt.EncodeStateAsUpdate(peer, nil)
	if err != nil {
		t.Fatalf("encode peer state: %v", err)
	}
	_ = ycrdt.ApplyUpdate(room.doc, peerUpdate, updateOrigin{src: 1})

	if originator.count() != 0 {
		t.Errorf("update echoed back to originator (%d frames)", originator.count())
	}
	if other.count() == 0 {
		t.Errorf("update not fanned out to the other member")
	}
	if !room.dirty {
		t.Errorf("room not marked dirty after a mutating update")
	}
}

// captureConn is a minimal service.Conn that counts the frames it receives.
type captureConn struct {
	mu     sync.Mutex
	frames int
}

func (c *captureConn) Send(_ []byte) error {
	c.mu.Lock()
	c.frames++
	c.mu.Unlock()
	return nil
}

func (c *captureConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frames
}
