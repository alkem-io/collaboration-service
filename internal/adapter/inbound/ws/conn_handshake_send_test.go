package ws

import (
	"context"
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
