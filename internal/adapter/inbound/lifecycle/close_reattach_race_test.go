package lifecycle

import (
	"sync"
	"testing"
)

// TestAdoptRefusesAfterCloseSoAReattachCannotLeakAConnection is the regression
// for the one defect in this service that outlived the process.
//
// Shutdown during a broker outage is exactly when reattach is looping. Close
// closes c.closed, reads the CURRENT (dead, or nil) attachment and closes that.
// Meanwhile reattach is inside openSession, which is not cancellable; on return
// it used to adopt unconditionally, overwriting c.conn/c.ch with a brand new LIVE
// pair that Close had already walked past. Nothing ever closed that pair: the
// AMQP connection and its consume goroutine leaked past shutdown, the broker saw
// an abrupt disconnect rather than a clean close, and the consumer could keep
// dispatching document.deleted into a Manager that was already closing.
//
// The fix is a refusal, not a retry or a lock around openSession: adopt
// re-checks the shutdown signal under the same lock that publishes the fields,
// and reattach releases the session it just opened when the refusal comes back.
//
// Non-vacuity: make adopt return true unconditionally and this fails — the
// post-Close session is adopted and reported as installed.
func TestAdoptRefusesAfterCloseSoAReattachCannotLeakAConnection(t *testing.T) {
	c := &Consumer{closed: make(chan struct{})}

	// A session opened just before Close lands is adopted normally.
	first := &session{conn: &fakeConn{}, ch: &fakeChannel{}}
	if !c.adopt(first) {
		t.Fatal("adopt refused a session while the consumer was open")
	}

	// Shutdown begins.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A re-attach that was already inside openSession now returns. Adopting it
	// would install a live connection nothing will ever close.
	late := &session{conn: &fakeConn{}, ch: &fakeChannel{}}
	if c.adopt(late) {
		t.Fatal("adopt accepted a session established AFTER Close; that connection and its goroutine leak past shutdown")
	}
}

// TestAdoptIsSafeUnderConcurrentClose runs the real interleaving rather than the
// sequential approximation above, so the race detector has something to inspect.
// The assertion is the invariant that matters: whatever happens, a session that
// is refused is never left installed.
func TestAdoptIsSafeUnderConcurrentClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		c := &Consumer{closed: make(chan struct{})}
		late := &session{conn: &fakeConn{}, ch: &fakeChannel{}}

		var wg sync.WaitGroup
		var adopted bool
		wg.Add(2)
		go func() { defer wg.Done(); _ = c.Close() }()
		go func() { defer wg.Done(); adopted = c.adopt(late) }()
		wg.Wait()

		if !adopted {
			// Refused: the caller closes it. Nothing must remain installed.
			if got := c.live(); got == late.ch {
				t.Fatal("a REFUSED session was still installed as the live attachment")
			}
		}
	}
}
