package ws

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestSendInitialWaitsForQueueSpaceInsteadOfShedding is the deterministic ratchet
// on the handshake-send policy.
//
// Send sheds on a full queue, which is right for room broadcasts — it protects
// the room's single run loop from one slow client. It is wrong for the handshake
// batch: those are the server's own frames on the per-connection goroutine, and a
// full queue there only means the writer goroutine has not been scheduled yet.
// sendInitial must therefore WAIT for space rather than drop the joiner.
//
// Non-vacuity: this fails deterministically, not probabilistically, if
// sendInitial is replaced by Send — Send's default branch closes the connection
// and returns errConnClosed on the very first call, so both the "still pending"
// and the "not closed" assertions trip immediately. That determinism is the whole
// point: the end-to-end version of this property can only be observed by winning
// a scheduling race, which is what made the test this one replaces flaky.
func TestSendInitialWaitsForQueueSpaceInsteadOfShedding(t *testing.T) {
	server, _ := dialPair(t)
	// Depth-1 queue and NO writer goroutine, so nothing can drain except us.
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())

	if err := wc.sendInitial(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first handshake frame (fills the queue): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- wc.sendInitial(ctx, []byte{2}) }()

	// The second frame must still be pending: waiting, not shed.
	select {
	case err := <-second:
		t.Fatalf("second handshake frame returned %v instead of waiting for queue space; the batch must not be shed", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-wc.closed:
		t.Fatal("connection was closed while the handshake batch waited; the batch must not trip the slow-consumer shed")
	default:
	}

	// Make room the way the writer goroutine would, and the pending send lands.
	<-wc.send
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second handshake frame after the queue drained: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second handshake frame did not complete after the queue drained")
	}
}

// TestSendInitialShedsAPeerThatNeverDrains covers the other half of the policy:
// waiting is bounded. A peer that never makes room is not a scheduling artifact,
// it is a client that is not reading, and it is shed like any other — otherwise
// the handler goroutine would park on it forever.
func TestSendInitialShedsAPeerThatNeverDrains(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())

	if err := wc.sendInitial(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first handshake frame (fills the queue): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := wc.sendInitial(ctx, []byte{2}); err == nil {
		t.Fatal("expected the handshake send to give up once its deadline passed")
	}
	select {
	case <-wc.closed:
	default:
		t.Fatal("a peer that never drained the handshake batch must be shed, not left open")
	}
}

// TestSendInitialRefusesAnAlreadyClosedConnection pins the fast-path guard.
//
// The handshake batch is sent frame by frame after the join succeeds, and the
// connection can already be gone by then — the client disconnects during
// materialization, or an earlier frame in the same batch tripped the shed. Every
// later frame must refuse immediately and enqueue NOTHING: pushing onto the queue
// of a closed connection buries frames that no writer will ever drain, and the
// caller would keep feeding a batch that has no destination.
func TestSendInitialRefusesAnAlreadyClosedConnection(t *testing.T) {
	server, _ := dialPair(t)

	// Repeated, because the failure it guards against is a coin toss. Without the
	// fast-path check the send and the closed signal are BOTH ready, and Go's select
	// picks at random — so a single pass proves nothing: half the time the wrong
	// implementation would return errConnClosed anyway, having queued nothing.
	for i := 0; i < 200; i++ {
		wc := newWSConn(context.Background(), server, 4, zap.NewNop())
		wc.close()

		if err := wc.sendInitial(context.Background(), []byte{1}); !errors.Is(err, errConnClosed) {
			t.Fatalf("iteration %d: sendInitial on a closed connection = %v, want errConnClosed", i, err)
		}
		if n := len(wc.send); n != 0 {
			t.Fatalf("iteration %d: %d frame(s) were enqueued on a closed connection; nothing will ever drain them", i, n)
		}
	}
}

// TestSendInitialGivesUpWhenTheConnectionClosesWhileItWaits is the race the
// fast-path guard cannot cover.
//
// sendInitial deliberately BLOCKS on a full queue rather than shedding, because a
// full queue during the handshake usually just means the writer has not been
// scheduled yet. But the connection can die while it is parked there. It must then
// return promptly on the closed signal — not sit until the handshake deadline
// expires, holding the per-connection goroutine and delaying teardown for a
// connection that is already gone.
func TestSendInitialGivesUpWhenTheConnectionClosesWhileItWaits(t *testing.T) {
	server, _ := dialPair(t)
	// Depth-1 queue and no writer, so the second frame must block.
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())
	if err := wc.sendInitial(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first frame: %v", err)
	}

	// A deadline far longer than the test: if the close signal is ignored, this
	// fails by timing out rather than by passing slowly.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	blocked := make(chan error, 1)
	go func() { blocked <- wc.sendInitial(ctx, []byte{2}) }()

	// Let it park on the full queue, then close underneath it.
	time.Sleep(50 * time.Millisecond)
	wc.close()

	select {
	case err := <-blocked:
		if !errors.Is(err, errConnClosed) {
			t.Fatalf("a blocked sendInitial returned %v after the connection closed, want errConnClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a blocked sendInitial ignored the connection closing; it would hold the connection goroutine until the handshake deadline")
	}
}
