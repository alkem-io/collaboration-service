package ws

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestSlowConsumerShedDoesNotBlockRunLoop is the regression for PR #10 finding
// (a): the slow-consumer shed must NOT run coder/websocket's close HANDSHAKE on
// the caller's goroutine. close() is invoked from the room's single run loop
// (sendMember → Send → shed), and Conn.Close performs a close handshake that
// writes a close frame with an internal ~5s timeout and then waits on the
// connection goroutines. Against a stalled peer (here: a parked server side that
// never reads) that write blocks until the deadline fires, freezing the run loop
// — and N such sheds serialize into N×~5s of room-wide stall. The fix closes the
// socket via CloseNow (no handshake) on its own goroutine, so close() must return
// effectively immediately regardless of peer liveness.
//
// We assert the SHED PATH (Send → overflow → close) returns far below the ~5s
// handshake window the old synchronous Close could block for. The dialPair peer
// is parked (never reads), which is exactly the stalled-client condition.
func TestSlowConsumerShedDoesNotBlockRunLoop(t *testing.T) {
	server, _ := dialPair(t) // client side parked: dialPair's cleanup keeps it idle/non-reading
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())
	// No writer goroutine, so the second Send overflows the depth-1 queue and sheds
	// (which calls close()) — the run-loop-equivalent path.

	if err := wc.Send([]byte{0}); err != nil {
		t.Fatalf("first Send (fills buffer): %v", err)
	}

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_ = wc.Send([]byte{0}) // overflow → shed → close(); must not block on a handshake
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		// The old synchronous Close handshake would block ~5s here on a stalled peer.
		// CloseNow-on-a-goroutine returns essentially instantly; 1s is a generous
		// ceiling that still fails loudly if the handshake ever creeps back onto the
		// run loop.
		if elapsed > time.Second {
			t.Fatalf("shed/close blocked the caller for %v; the close handshake must not run on the run loop", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shed/close did not return within 3s: the close handshake is blocking the run loop (finding (a) regressed)")
	}
}

// TestCloseDoesNotBlockOnStalledPeer asserts close() itself returns promptly even
// when the peer is stalled — the off-loop, no-handshake teardown contract. This
// guards the same property directly at close(), independent of the Send shed path
// (e.g. the disconnect / size-limit / rate-limit eviction paths that also call
// close() from the run loop).
func TestCloseDoesNotBlockOnStalledPeer(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 4, zap.NewNop())

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		wc.close()
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		if elapsed > time.Second {
			t.Fatalf("close() blocked for %v on a stalled peer; teardown must be off-loop and handshake-free", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close() did not return within 3s on a stalled peer (finding (a) regressed)")
	}
}
