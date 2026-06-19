package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
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
