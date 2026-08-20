package ws

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestOfferInFlightAcrossTheBoundaryIsRefused is the linearization property.
//
// The dangerous shape is check-then-act: a sender that reads the terminal flag
// and THEN enqueues has two separable steps, and a close intent admitted between
// them puts the sender's frame behind a close it was just told had not happened.
// The sender is handed a nil error for a frame that will never be written, and
// the port's promise that nothing is admitted after the end becomes false.
//
// The test forces the dangerous ordering without sleeping and without a
// production hook: it takes the admission lock itself, so an offer issued from
// another goroutine is guaranteed to be waiting on the boundary; moves the
// boundary underneath it exactly as admitEnd would; and only then releases.
// Whenever that offer is scheduled, the end has already been admitted, so its
// only correct answer is refusal.
//
// Non-vacuity: release the lock between the check and the enqueue in offer and
// the frame is admitted behind the intent (see the ledger — the probe adds the
// scheduling point that makes it deterministic; the shipped tree has no hook).
func TestOfferInFlightAcrossTheBoundaryIsRefused(t *testing.T) {
	server, _ := dialPair(t)
	c := newWSConn(context.Background(), server, 4, zap.NewNop())

	c.admit.Lock()

	result := make(chan admission, 1)
	go func() { result <- c.offer(outgoing{frame: []byte("late")}) }()

	// While the boundary is HELD, no offer may make progress. This is the
	// discriminating assertion: an implementation that reads the terminal state
	// outside the lock — an atomic flag, say — sails past here and enqueues,
	// which is exactly the shape that lets a frame land behind the intent.
	select {
	case got := <-result:
		t.Fatalf("offer completed (%v) while the admission boundary was held; its terminal check and its enqueue are separable, so a close admitted between them puts this frame behind the intent", got)
	case <-time.After(100 * time.Millisecond):
	}

	// Move the boundary exactly as admitEnd does, while the offer waits on it.
	end := model.NewSessionEnd(model.CodeServerShutdown)
	c.ending = true
	c.send <- outgoing{end: &end}
	c.admit.Unlock()

	if got := <-result; got != refusedTerminal {
		t.Fatalf("an offer crossing the boundary was %v, want refusedTerminal; it would sit behind the close and never be written, having reported success", got)
	}

	// And the queue holds the intent alone: nothing landed behind it.
	if n := len(c.send); n != 1 {
		t.Fatalf("queue holds %d items, want 1 (the close intent alone)", n)
	}
	if item := <-c.send; item.end == nil {
		t.Fatal("the item ahead of the close is a frame; the intent must be last")
	}
}

// TestSendInitialRefusesOnceTheSessionIsEnding covers the half that consulted the
// boundary not at all.
//
// A teardown racing the handler's post-join batch could admit the close intent
// while sendInitial was parked waiting for space. The writer stops at the intent,
// so nothing drains again — and the handshake sat there for the full
// handshakeSendTimeout before shedding a connection whose session had already
// ended. It must refuse promptly instead.
//
// The queue is deliberately FULL and no writer runs, so the old code could only
// have left this waiting until its deadline.
func TestSendInitialRefusesOnceTheSessionIsEnding(t *testing.T) {
	server, _ := dialPair(t)
	// Depth 2: room for one frame AND the close intent, so the session ends with
	// the connection still OPEN. If the intent were shed instead, `closed` would
	// be closed and the wait below would return for that reason rather than
	// because the boundary was honoured — the test would pass without testing
	// anything.
	c := newWSConn(context.Background(), server, 2, zap.NewNop())

	if err := c.sendInitial(context.Background(), []byte{1}); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	c.CloseAfterDrain(model.NewSessionEnd(model.CodeDocumentDeleted))
	select {
	case <-c.closed:
		t.Fatal("the connection was hard-closed; this test needs the intent QUEUED so the boundary is the only thing that can refuse")
	default:
	}
	if len(c.send) != 2 {
		t.Fatalf("queue holds %d, want 2 (frame + intent): the queue must be full so sendInitial has to wait", len(c.send))
	}

	done := make(chan error, 1)
	go func() {
		// A generous deadline: if sendInitial ignored the boundary it would park
		// here for all of it, because nothing will ever drain this queue.
		ctx, cancel := context.WithTimeout(context.Background(), handshakeSendTimeout)
		defer cancel()
		done <- c.sendInitial(ctx, []byte{2})
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a handshake frame was admitted after the session ended")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendInitial parked on a queue that will never drain instead of honouring the terminal boundary")
	}
}

// TestShedAfterAQueuedIntentEndsTheConnection pins the close()/closeWith overlap.
//
// Once a close intent is queued, the writer normally performs the graceful close.
// But close() — the slow-consumer shed, and the handler's own deferred teardown —
// can still arrive first, and then the writer observes `closed` and exits without
// the handshake. That is INTENDED: the shed exists for a connection that is
// already going away, and running a close handshake at that point would block on
// a peer that is not reading.
//
// So the guaranteed outcome is not which close code the peer sees — that race is
// deliberately unresolved — but that the connection ends exactly once and nothing
// further is ever admitted. That is what this pins.
func TestShedAfterAQueuedIntentEndsTheConnection(t *testing.T) {
	server, _ := dialPair(t)
	c := newWSConn(context.Background(), server, 4, zap.NewNop())

	c.CloseAfterDrain(model.NewSessionEnd(model.CodeEditsNotSaved))
	c.close() // the shed wins

	select {
	case <-c.closed:
	default:
		t.Fatal("the connection did not end")
	}
	if err := c.Send([]byte{1}); err == nil {
		t.Fatal("a frame was admitted after the connection ended")
	}
	if err := c.sendInitial(context.Background(), []byte{2}); err == nil {
		t.Fatal("a handshake frame was admitted after the connection ended")
	}
	c.close() // idempotent: must not panic on the already-closed channel
}
