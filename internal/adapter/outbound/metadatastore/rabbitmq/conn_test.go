package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeChannel captures publishes and, when wired to a client, auto-replies on
// the correlation id so Call's full round-trip is exercised without a broker.
type fakeChannel struct {
	client    *Client
	reply     nestReply
	publishes []amqp.Publishing
	keys      []string
	pubErr    error
	closed    bool
}

func (f *fakeChannel) PublishWithContext(_ context.Context, _, key string, _, _ bool, msg amqp.Publishing) error {
	f.publishes = append(f.publishes, msg)
	f.keys = append(f.keys, key)
	if f.pubErr != nil {
		return f.pubErr
	}
	// Simulate the server replying: deliver the scripted reply on the request's
	// correlation id so the waiting Call unblocks.
	if msg.CorrelationId != "" && f.client != nil {
		r := f.reply
		r.ID = msg.CorrelationId
		go f.client.deliver(msg.CorrelationId, r)
	}
	return nil
}

func (f *fakeChannel) Close() error { f.closed = true; return nil }

func newFakeClient(reply any) (*Client, *fakeChannel) {
	c := &Client{
		replyQ:      "reply-q",
		serverQueue: "server-q",
		timeout:     2 * time.Second,
		pending:     make(map[string]chan nestReply),
	}
	raw, _ := json.Marshal(reply)
	ch := &fakeChannel{client: c, reply: nestReply{Response: raw, IsDisposed: true}}
	c.ch = ch
	return c, ch
}

// TestCallFailsFastAfterReplyConsumerClosed asserts that once the reply consumer
// has exited (failAllPending marks the client closed), a new Call returns
// immediately with an error instead of publishing and then waiting out its full
// timeout for a reply that can never arrive.
func TestCallFailsFastAfterReplyConsumerClosed(t *testing.T) {
	c, ch := newFakeClient(FetchReply{Found: true})
	c.failAllPending() // simulate the reply consumer exiting (broker/channel drop)

	err := c.Call(context.Background(), PatternFetch, FetchData{ID: "doc-1"}, nil)
	if err == nil {
		t.Fatal("Call after reply consumer closed: want error, got nil")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Call error = %v, want a reply-consumer-closed error", err)
	}
	// Failing fast must not put a request on the wire.
	if len(ch.publishes) != 0 {
		t.Fatalf("publishes = %d, want 0 (fail fast must not publish)", len(ch.publishes))
	}
}

func TestClientCallRoundTrip(t *testing.T) {
	c, ch := newFakeClient(FetchReply{Found: true, ContentType: "memo", ContentPointer: "ptr"})
	store := newWithRPC(c)

	meta, err := store.Load(context.Background(), "doc-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if meta.ContentPointer != "ptr" {
		t.Errorf("ContentPointer = %q", meta.ContentPointer)
	}
	// The request was published to the server queue with a reply-to + correlation.
	if len(ch.publishes) != 1 {
		t.Fatalf("publishes = %d, want 1", len(ch.publishes))
	}
	p := ch.publishes[0]
	if p.ReplyTo != "reply-q" || p.CorrelationId == "" || p.ContentType != "application/json" {
		t.Errorf("publish props = %+v", p)
	}
	if ch.keys[0] != "server-q" {
		t.Errorf("routing key = %q, want server-q", ch.keys[0])
	}
	// The envelope carries the pattern.
	var env envelope
	if err := json.Unmarshal(p.Body, &env); err != nil || env.Pattern != PatternFetch {
		t.Errorf("envelope = %s (%v)", p.Body, err)
	}
}

// TestCallFailsFastWhenReplyConsumerClosesMidFlight defends failAllPending's drain
// of an IN-FLIGHT waiter (the case the sibling fast-fail test does NOT cover: that
// one calls Call AFTER closed is already set). Here a Call is already blocked in
// its select{ctx.Done(); waiter} — published, no reply yet — when the reply
// consumer exits. failAllPending must push an error onto that registered waiter so
// the Call returns immediately, rather than each in-flight Call waiting out its
// full timeout while pending stays occupied.
//
// Non-vacuity: replace failAllPending's body with just `c.mu.Lock(); c.closed =
// true; c.pending = map[string]chan nestReply{}; c.mu.Unlock()` (mark closed +
// clear, WITHOUT sending on each waiter) and this test fails — the blocked Call
// gets nothing on its waiter and runs to the 10s timeout, tripping the 2s
// "did not fail fast" watchdog.
func TestCallFailsFastWhenReplyConsumerClosesMidFlight(t *testing.T) {
	// Long timeout: a correct fast-fail returns in well under it; a regression that
	// fails to drain the in-flight waiter would block on it (caught by the watchdog).
	c := &Client{
		replyQ: "r", serverQueue: "s", timeout: 10 * time.Second,
		pending: make(map[string]chan nestReply),
	}
	c.ch = &fakeChannel{} // no client wired → publish succeeds but never auto-replies.

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Call(context.Background(), PatternFetch, FetchData{ID: "in-flight"}, &FetchReply{})
	}()

	// Wait until the Call has registered its waiter and is blocked in the select
	// (published, awaiting a reply) — only then is the in-flight drain meaningful.
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.pending) == 1
	})

	// The reply consumer exits (broker/channel drop): every outstanding waiter must
	// be failed, including this in-flight one.
	c.failAllPending()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("in-flight Call returned nil after the reply consumer closed; want an error")
		}
		if !strings.Contains(err.Error(), "closed") {
			t.Fatalf("in-flight Call error = %v, want a reply-channel-closed error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight Call did not fail fast after the reply consumer closed (waited out its timeout — failAllPending did not drain the registered waiter)")
	}
}

// waitFor polls cond up to 2s, failing the test if it never holds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition never became true within 2s")
}

func TestClientCallPublishError(t *testing.T) {
	c, ch := newFakeClient(SaveReply{Success: true})
	ch.pubErr = errors.New("channel closed")
	if err := c.Call(context.Background(), PatternSave, SaveData{ID: "d"}, &SaveReply{}); err == nil {
		t.Error("expected Call to surface a publish error")
	}
}

func TestClientCallTimeout(t *testing.T) {
	// A client whose channel never replies: Call must time out (fail, not hang).
	c := &Client{
		replyQ: "r", serverQueue: "s", timeout: 50 * time.Millisecond,
		pending: make(map[string]chan nestReply),
	}
	c.ch = &fakeChannel{} // no client wired → no auto-reply.
	if err := c.Call(context.Background(), PatternFetch, FetchData{ID: "d"}, &FetchReply{}); err == nil {
		t.Error("expected Call to time out without a reply")
	}
}

func TestClientCallServerError(t *testing.T) {
	c := &Client{
		replyQ: "r", serverQueue: "s", timeout: time.Second,
		pending: make(map[string]chan nestReply),
	}
	ch := &fakeChannel{client: c, reply: nestReply{Err: json.RawMessage(`"boom"`)}}
	c.ch = ch
	if err := c.Call(context.Background(), PatternSave, SaveData{ID: "d"}, &SaveReply{}); err == nil {
		t.Error("expected Call to surface the server err field")
	}
}

func TestConsumeRepliesMalformedBodySurfacesError(t *testing.T) {
	// A reply whose body is not a valid nestReply envelope must NOT be delivered as
	// a zero-value (empty) success — it must reach the waiter as an error, so Call
	// returns an error instead of nil-with-empty-data.
	c := &Client{
		replyQ: "r", serverQueue: "s", timeout: time.Second,
		pending: make(map[string]chan nestReply),
	}
	waiter := make(chan nestReply, 1)
	c.mu.Lock()
	c.pending["corr-1"] = waiter
	c.mu.Unlock()

	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{CorrelationId: "corr-1", Body: []byte("{not json")}
	close(deliveries)
	c.consumeReplies(deliveries)

	select {
	case r := <-waiter:
		if len(r.Err) == 0 {
			t.Fatalf("malformed reply must carry an error, got %+v", r)
		}
	default:
		t.Fatal("expected a reply to be delivered for the malformed body")
	}
}

func TestClientEmit(t *testing.T) {
	c, ch := newFakeClient(nil)
	if err := c.Emit(context.Background(), PatternContribution, ContributionData{ID: "d"}); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(ch.publishes) != 1 {
		t.Fatalf("publishes = %d, want 1", len(ch.publishes))
	}
	// Fire-and-forget: no reply-to / correlation.
	if ch.publishes[0].ReplyTo != "" || ch.publishes[0].CorrelationId != "" {
		t.Errorf("emit should not set reply-to/correlation: %+v", ch.publishes[0])
	}
}

func TestClientEmitError(t *testing.T) {
	c, ch := newFakeClient(nil)
	ch.pubErr = errors.New("down")
	if err := c.Emit(context.Background(), PatternContribution, ContributionData{ID: "d"}); err == nil {
		t.Error("expected Emit to surface a publish error")
	}
}

func TestClientClose(t *testing.T) {
	c, ch := newFakeClient(nil)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !ch.closed {
		t.Error("Close did not close the channel")
	}
}

func TestConsumeRepliesDispatches(t *testing.T) {
	c := &Client{pending: make(map[string]chan nestReply)}
	waiter := make(chan nestReply, 1)
	c.mu.Lock()
	c.pending["corr-1"] = waiter
	c.mu.Unlock()

	deliveries := make(chan amqp.Delivery, 1)
	body, _ := json.Marshal(nestReply{Response: json.RawMessage(`{"success":true}`)})
	deliveries <- amqp.Delivery{CorrelationId: "corr-1", Body: body}
	close(deliveries)

	c.consumeReplies(deliveries)
	select {
	case r := <-waiter:
		if string(r.Response) != `{"success":true}` {
			t.Errorf("dispatched reply = %s", r.Response)
		}
	default:
		t.Error("reply not dispatched to the waiter")
	}
}

func TestDeliverUnknownCorrelationIsDropped(t *testing.T) {
	c := &Client{pending: make(map[string]chan nestReply)}
	if c.deliver("nobody", nestReply{}) {
		t.Error("deliver to an unknown correlation id should report false")
	}
}

// TestConnectDialFailureSurfaces defends Connect's dial-error branch
// (conn.go:76): an unreachable broker must fail Connect with an error (so
// startup fails loudly) rather than return a half-built client. Reachable as a
// plain unit test — no broker needed.
func TestConnectDialFailureSurfaces(t *testing.T) {
	// Port 1 refuses connections; amqp.Dial fails fast. The credentials are a
	// throwaway literal for an unreachable host, not a real secret.
	_, _, err := Connect(Config{URL: "amqp://bad:bad@127.0.0.1:1/", Queue: "q", RequestTimeout: time.Second}) //nolint:gosec // fake creds for an unreachable broker
	if err == nil {
		t.Error("expected Connect to surface a dial failure against an unreachable broker")
	}
}

// TestClientCallMarshalErrorSurfaces defends Call's envelope-build branch
// (conn.go:137 → store.go:127): data that cannot be JSON-encoded (a func) must
// fail before any publish, surfaced as an error rather than publishing a
// malformed request.
func TestClientCallMarshalErrorSurfaces(t *testing.T) {
	c, ch := newFakeClient(nil)
	// A func value is not JSON-marshalable.
	if err := c.Call(context.Background(), PatternSave, func() {}, &SaveReply{}); err == nil {
		t.Error("expected Call to fail marshalling an unencodable payload")
	}
	if len(ch.publishes) != 0 {
		t.Error("Call must not publish when the envelope cannot be built")
	}
}

// TestClientCallUnmarshalReplyErrorSurfaces defends Call's reply-decode branch
// (conn.go:171): a reply whose Response is not valid JSON for the target type
// must surface as an error, never silently leave the caller's reply struct
// half-populated.
func TestClientCallUnmarshalReplyErrorSurfaces(t *testing.T) {
	c := &Client{
		replyQ: "r", serverQueue: "s", timeout: time.Second,
		pending: make(map[string]chan nestReply),
	}
	// Response is a JSON string, but the caller decodes into a struct → type error.
	ch := &fakeChannel{client: c, reply: nestReply{Response: json.RawMessage(`"not-an-object"`)}}
	c.ch = ch
	if err := c.Call(context.Background(), PatternFetch, FetchData{ID: "d"}, &FetchReply{}); err == nil {
		t.Error("expected Call to surface a reply-unmarshal error")
	}
}

// TestClientEmitMarshalErrorSurfaces defends Emit's envelope-build branch
// (conn.go:182): an unencodable event payload must fail before publishing.
func TestClientEmitMarshalErrorSurfaces(t *testing.T) {
	c, ch := newFakeClient(nil)
	if err := c.Emit(context.Background(), PatternContribution, func() {}); err == nil {
		t.Error("expected Emit to fail marshalling an unencodable payload")
	}
	if len(ch.publishes) != 0 {
		t.Error("Emit must not publish when the envelope cannot be built")
	}
}

// TestCloseWithNilConnIsSafe defends Close's nil-conn branch (conn.go:196): a
// Client built without a live connection (the channel set, conn nil) must Close
// without panicking and report no error — Close is called unconditionally on
// shutdown paths.
func TestCloseWithNilConnIsSafe(t *testing.T) {
	ch := &fakeChannel{}
	c := &Client{ch: ch} // conn is nil
	if err := c.Close(); err != nil {
		t.Errorf("Close with a nil conn should be a no-op, got %v", err)
	}
	if !ch.closed {
		t.Error("Close should still close the channel when conn is nil")
	}
}
