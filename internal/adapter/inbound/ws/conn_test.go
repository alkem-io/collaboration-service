package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// dialPair stands up a trivial echo-less server that just accepts the upgrade
// and parks, returning the server-side and client-side connections so wsConn can
// be exercised against a real socket.
func dialPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		accepted <- c
		// Park until the test closes us.
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	url := "ws" + srv.URL[len("http"):]
	cl, resp, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = cl.Close(websocket.StatusNormalClosure, "") })

	select {
	case s := <-accepted:
		t.Cleanup(func() { _ = s.Close(websocket.StatusNormalClosure, "") })
		return s, cl
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted")
		return nil, nil
	}
}

// TestWSConnDeliversFrames asserts a frame queued via Send is written to the
// socket and read by the peer.
func TestWSConnDeliversFrames(t *testing.T) {
	server, client := dialPair(t)
	wc := newWSConn(context.Background(), server, 8, zap.NewNop())
	wc.startWriter()
	defer wc.close()

	if err := wc.Send([]byte{1, 2, 3}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	typ, data, err := client.Read(context.Background())
	if err != nil {
		t.Fatalf("client Read: %v", err)
	}
	if typ != websocket.MessageBinary || string(data) != string([]byte{1, 2, 3}) {
		t.Fatalf("got typ=%v data=%v", typ, data)
	}
}

// TestWSConnSlowConsumerDropped asserts that overflowing the send queue (no
// writer draining it) sheds the connection rather than blocking the caller.
func TestWSConnSlowConsumerDropped(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())
	// Do NOT start the writer, so nothing drains the queue.

	// First Send fills the depth-1 buffer.
	if err := wc.Send([]byte{0}); err != nil {
		t.Fatalf("first Send: %v", err)
	}
	// Second Send overflows → connection shed, error returned.
	if err := wc.Send([]byte{0}); err == nil {
		t.Fatal("expected error when send queue overflows")
	}
	// Subsequent Send after close also errors.
	if err := wc.Send([]byte{0}); err == nil {
		t.Fatal("expected error after connection closed")
	}
}

// TestWSConnCloseIdempotent asserts close can be called repeatedly safely.
func TestWSConnCloseIdempotent(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 4, zap.NewNop())
	wc.close()
	wc.close() // must not panic / double-close the channel
	if err := wc.Send([]byte{1}); err == nil {
		t.Fatal("Send after close should error")
	}
}

// TestSendAfterCloseIntentIsRefused pins the rule that nothing may be queued
// behind a close.
//
// The session-end control tells the client its session is over. A frame written
// after that would contradict it — the client is told "this is the end" and then
// handed more document traffic on the same socket. The guard has to live in Send
// rather than in the writer, because by the time the writer sees the frame it is
// already ordered behind the close and the ordering decision has been made.
//
// Non-vacuity: remove the c.ending check from Send and the second Send below
// succeeds.
func TestSendAfterCloseIntentIsRefused(t *testing.T) {
	server, _ := dialPair(t)
	c := newWSConn(context.Background(), server, 8, zap.NewNop())

	if err := c.Send([]byte{1}); err != nil {
		t.Fatalf("send before the end must succeed: %v", err)
	}
	c.CloseAfterDrain(model.NewSessionEnd(model.CodeServerShutdown))
	if err := c.Send([]byte{2}); err == nil {
		t.Fatal("a frame was queued BEHIND the close; the client would be told the session ended and then sent more traffic")
	}
}

// TestCloseAfterDrainDoesNotBlockOnAFullQueue pins the room-loop guarantee.
//
// CloseAfterDrain is called from the room's single run loop, which serializes
// every join, edit and leave for the whole document. If it blocked on a client
// that is not draining, one unreachable socket would stall the teardown of every
// other member — the exact head-of-line blocking the buffered-queue design
// exists to prevent.
//
// Non-vacuity: change the enqueue in CloseAfterDrain from a select/default to an
// unconditional send and this blocks until the deadline below fires.
func TestCloseAfterDrainDoesNotBlockOnAFullQueue(t *testing.T) {
	server, _ := dialPair(t)
	// Depth 1 and no writer started, so the queue is full after one frame and
	// nothing will ever drain it.
	c := newWSConn(context.Background(), server, 1, zap.NewNop())
	if err := c.Send([]byte{1}); err != nil {
		t.Fatalf("first send: %v", err)
	}

	done := make(chan struct{})
	go func() {
		c.CloseAfterDrain(model.NewSessionEnd(model.CodeDocumentDeleted))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseAfterDrain blocked on a client that is not draining; that stalls the whole room")
	}
}

// TestCloseStatusPerCode pins the transport mapping for every code, so a client
// that only inspects the close event still gets a value it can branch on, and so
// the two transient codes stay distinguishable (a restart is not a rate limit).
func TestCloseStatusPerCode(t *testing.T) {
	want := map[model.SessionEndCode]websocket.StatusCode{
		model.CodeServerShutdown:            websocket.StatusGoingAway,
		model.CodeUpdateRateExceeded:        websocket.StatusTryAgainLater,
		model.CodeDocumentSizeLimitExceeded: websocket.StatusPolicyViolation,
		model.CodeDocumentDeleted:           websocket.StatusPolicyViolation,
		model.CodeEditsNotSaved:             websocket.StatusPolicyViolation,
		model.CodeContentRefused:            websocket.StatusPolicyViolation,
		model.CodeForbidden:                 websocket.StatusPolicyViolation,
		// Member-scoped and transient, like the rate limit: the client should retry,
		// so it gets 1013 rather than a policy violation. The two remain
		// distinguishable by the close reason, which is always the code itself.
		model.CodeUpdateNotAccepted: websocket.StatusTryAgainLater,
	}
	codes := model.SessionEndCodes()
	if len(codes) != len(want) {
		t.Fatalf("%d codes exist but %d are mapped to a close status; an unmapped code falls through to a default", len(codes), len(want))
	}
	for _, code := range codes {
		status, reason := closeStatusFor(model.NewSessionEnd(code))
		if status != want[code] {
			t.Errorf("close status for %q = %d, want %d", code, status, want[code])
		}
		// The reason is the stable code, never prose: clients branch on it.
		if reason != code {
			t.Errorf("close reason for %q = %q, want the code itself", code, reason)
		}
		if len(reason) > 123 {
			t.Errorf("close reason for %q is %d bytes; the WebSocket limit is 123", code, len(reason))
		}
	}
}

// TestCloseStatusForDegradesOnDisposition covers the unreachable-by-construction
// default in closeStatusFor.
//
// NewSessionEnd rejects unknown codes, so production cannot reach it — but the
// default is what a future code lands on if someone adds it to the table and
// forgets the status switch here. It must degrade to something the client can
// act on, matching the disposition, rather than fall through to a status that
// contradicts it.
func TestCloseStatusForDegradesOnDisposition(t *testing.T) {
	transient := model.SessionEnd{Code: "future-code", Scope: model.ScopeDocument, Disposition: model.DispositionTransient}
	if status, _ := closeStatusFor(transient); status != websocket.StatusGoingAway {
		t.Errorf("unmapped transient code = %d, want StatusGoingAway (%d): a retryable end must not close as a policy violation", status, websocket.StatusGoingAway)
	}
	terminal := model.SessionEnd{Code: "future-code", Scope: model.ScopeDocument, Disposition: model.DispositionTerminal}
	if status, _ := closeStatusFor(terminal); status != websocket.StatusPolicyViolation {
		t.Errorf("unmapped terminal code = %d, want StatusPolicyViolation (%d)", status, websocket.StatusPolicyViolation)
	}
}

// TestCloseAfterDrainIsIdempotentAndSafeOnAClosedConn covers the two guards.
//
// The room can reach a connection twice — a per-member limit dropping it while a
// teardown walks the member map — and only the FIRST end may be queued: a second
// would sit behind a close that already ends the writer and never be delivered,
// or race it. A connection already torn down must simply absorb the call rather
// than panic on a closed channel.
func TestCloseAfterDrainIsIdempotentAndSafeOnAClosedConn(t *testing.T) {
	server, _ := dialPair(t)
	c := newWSConn(context.Background(), server, 4, zap.NewNop())

	c.CloseAfterDrain(model.NewSessionEnd(model.CodeDocumentDeleted))
	// A second end must not enqueue anything.
	c.CloseAfterDrain(model.NewSessionEnd(model.CodeServerShutdown))
	if got := len(c.send); got != 1 {
		t.Fatalf("%d items queued, want 1: only the first session end may be sent", got)
	}

	// And on an already-closed connection it is a no-op, not a panic.
	other, _ := dialPair(t)
	c2 := newWSConn(context.Background(), other, 4, zap.NewNop())
	c2.close()
	c2.CloseAfterDrain(model.NewSessionEnd(model.CodeEditsNotSaved))
}

// TestJoinRefusedDuringShutdownIsRetryable pins the mapping for a join that
// races a graceful shutdown.
//
// It used to close 1011 "join failed" — an internal-error code — so a client
// reconnecting into a rolling deploy was told the server was broken and kept
// hammering it. It is the same situation a already-joined client is told about
// with server-shutdown, and it gets the same retryable status.
func TestJoinRefusedDuringShutdownIsRetryable(t *testing.T) {
	status, reason := joinCloseStatus(service.ErrShuttingDown)
	if status != websocket.StatusGoingAway {
		t.Errorf("shutdown join status = %d, want StatusGoingAway (%d)", status, websocket.StatusGoingAway)
	}
	if reason != model.CodeServerShutdown {
		t.Errorf("shutdown join reason = %q, want %q", reason, model.CodeServerShutdown)
	}
}

// TestCloseWithMarksTheConnectionClosed pins the second half of the graceful
// close: after the writer performs it, the connection must report itself closed
// so a later Send is refused rather than writing into a socket that is gone.
//
// It is asserted directly because on the live handler path the deferred
// abnormal close often wins the race to closeOnce, leaving this branch to run
// only when the graceful close gets there first — which is the normal case in
// production and the one worth pinning.
func TestCloseWithMarksTheConnectionClosed(t *testing.T) {
	server, _ := dialPair(t)
	c := newWSConn(context.Background(), server, 4, zap.NewNop())

	c.closeWith(model.NewSessionEnd(model.CodeDocumentDeleted))

	select {
	case <-c.closed:
	default:
		t.Fatal("closeWith left the connection open; later frames would be written after the client was told the session ended")
	}
	if err := c.Send([]byte{1}); err == nil {
		t.Fatal("Send succeeded after a graceful close")
	}
}
