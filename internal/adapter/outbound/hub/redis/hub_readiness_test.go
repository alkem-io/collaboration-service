package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	yhub "github.com/antst/go-yjs/backend/hub"
	goredis "github.com/redis/go-redis/v9"
)

// TestSubscribeIsReadyBeforeItReturns is the readiness contract, asserted where it
// actually bites: a publish issued on the very next line.
//
// Redis pub/sub has no replay and no backlog. A message published before the
// server has registered the subscription is not delayed — it is GONE, and no
// deadline on the receiving side can wait it out. go-redis makes that easy to get
// wrong twice over: Client.Subscribe discards the error from pubsub.Subscribe, and
// even on success the SUBSCRIBE command has only been written, not acknowledged.
//
// Both channels are covered because they are separate subscriptions confirmed one
// at a time: waiting for only the first would leave awareness racing.
func TestSubscribeIsReadyBeforeItReturns(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind yhub.MessageKind
	}{
		{"document channel", yhub.DocumentUpdate},
		{"awareness channel", yhub.AwarenessUpdate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Repeated: a race that loses one time in ten passes a single run.
			for i := 0; i < 50; i++ {
				a, b := twoPods(t)
				var got recorder

				if _, err := b.Subscribe(context.Background(), "doc", "sub-on-b", got.handler); err != nil {
					t.Fatalf("subscribe on pod B: %v", err)
				}
				// No settling, no sleep: the next thing that happens is a publish.
				if err := a.Publish(context.Background(), yhub.Message{
					DocumentID: "doc", SourceID: "sub-on-a", Kind: tc.kind, Payload: []byte("x"),
				}); err != nil {
					t.Fatalf("publish from pod A: %v", err)
				}

				waitFor(t, "a message published immediately after Subscribe returned", func() bool {
					return got.count() == 1
				})
				if m := got.snapshot()[0]; m.Kind != tc.kind {
					t.Fatalf("kind = %v, want %v", m.Kind, tc.kind)
				}
			}
		})
	}
}

// TestSubscribeFailsWhenTheSubscriptionCannotBeEstablished asserts a failure to
// subscribe is REPORTED rather than swallowed.
//
// This is the error go-redis throws away. Without surfacing it, Subscribe hands
// back a Subscription that is wired to nothing: every cross-pod update for that
// document is silently missed for as long as the room lives, and the caller has no
// way to know — it looks exactly like a quiet document.
func TestSubscribeFailsWhenTheSubscriptionCannotBeEstablished(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "pod-a")
	t.Cleanup(func() { _ = h.Close() })

	// The server goes away before anyone subscribes.
	srv.Close()

	var got recorder
	sub, err := h.Subscribe(context.Background(), "doc", "sub", got.handler)
	if err == nil {
		t.Fatal("Subscribe reported success against a dead server; the caller believes it is receiving cross-pod updates and is receiving nothing")
	}
	if sub != nil {
		t.Fatal("Subscribe returned a subscription alongside its error")
	}
}

// TestAFailedSubscribeLeavesNoTraceBehind asserts the bookkeeping is unwound.
//
// Subscribe registers its subscriber before doing I/O. If a failed subscription
// left that entry in place, the document would carry a subscriber that can never
// receive anything — and because pump refs are derived from the live subscriber
// count, the next successful Subscribe would install a pump whose ref count never
// reaches zero, leaking the Redis subscription and its goroutine for the life of
// the pod.
func TestAFailedSubscribeLeavesNoTraceBehind(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "pod-a")
	t.Cleanup(func() { _ = h.Close() })
	srv.Close()

	var got recorder
	if _, err := h.Subscribe(context.Background(), "doc", "sub", got.handler); err == nil {
		t.Fatal("Subscribe against a dead server reported success")
	}

	h.mu.Lock()
	subs, pumps := len(h.subs["doc"]), len(h.pumps)
	h.mu.Unlock()
	if subs != 0 {
		t.Fatalf("a failed Subscribe left %d subscriber(s) registered; they receive nothing and hold a later pump's refs above zero", subs)
	}
	if pumps != 0 {
		t.Fatalf("a failed Subscribe left %d pump(s) behind", pumps)
	}
}

// TestSubscribeHonoursACancelledContextWhileWaiting asserts the readiness wait is
// bounded by the caller's context. A join whose client has already gone must not
// hold a goroutine waiting on a server that will never answer.
func TestSubscribeHonoursACancelledContextWhileWaiting(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "pod-a")
	t.Cleanup(func() { _ = h.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var got recorder
	done := make(chan error, 1)
	go func() {
		_, err := h.Subscribe(ctx, "doc", "sub", got.handler)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Subscribe succeeded on a cancelled context")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe ignored a cancelled context and kept waiting")
	}
}

// fakeReader is a scripted pub/sub connection. It drives the readiness handshake
// without a server, so the things a real Redis produces only occasionally — a
// message landing between two channel confirmations, a keep-alive during the
// wait — can be asserted on every run instead of hoped for.
type fakeReader struct {
	mu   sync.Mutex
	msgs []any
	errs error
	i    int
	ch   chan *goredis.Message
}

func (f *fakeReader) ReceiveTimeout(context.Context, time.Duration) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.i >= len(f.msgs) {
		if f.errs != nil {
			return nil, f.errs
		}
		return nil, errors.New("no more messages")
	}
	m := f.msgs[f.i]
	f.i++
	return m, nil
}

func (f *fakeReader) Channel(...goredis.ChannelOption) <-chan *goredis.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ch == nil {
		f.ch = make(chan *goredis.Message, 8)
	}
	return f.ch
}

func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ch != nil {
		close(f.ch)
		f.ch = nil
	}
	return nil
}

func (f *fakeReader) consumed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.i
}

// scriptedClient hands the hub a scripted pub/sub connection.
type scriptedClient struct {
	inner  client
	pubsub *fakeReader
}

func (c *scriptedClient) Publish(ctx context.Context, channel string, message any) *goredis.IntCmd {
	return c.inner.Publish(ctx, channel, message)
}
func (c *scriptedClient) Subscribe(context.Context, ...string) pubsubConn { return c.pubsub }
func (c *scriptedClient) Close() error                                    { return c.inner.Close() }

// TestAwaitSubscribedKeepsMessagesThatArriveBetweenConfirmations is the narrow
// case that would otherwise reintroduce the very loss this fix removes.
//
// SUBSCRIBE with two channels is confirmed one channel at a time, so a publish on
// the first can land before the second is confirmed. That message has already been
// taken off the connection by the wait — PubSub.Channel will never redeliver it —
// so dropping it here loses it exactly as the original race did, just in a smaller
// window.
func TestAwaitSubscribedKeepsMessagesThatArriveBetweenConfirmations(t *testing.T) {
	r := &fakeReader{msgs: []any{
		&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
		&goredis.Message{Channel: "doc:x", Payload: "early"},
		&goredis.Pong{},
		&goredis.Subscription{Kind: "subscribe", Channel: "awareness:x", Count: 2},
	}}

	early, err := awaitSubscribed(context.Background(), r, 2)
	if err != nil {
		t.Fatalf("awaitSubscribed: %v", err)
	}
	if len(early) != 1 || early[0].Payload != "early" {
		t.Fatalf("early = %v, want the one message that arrived between confirmations; it is already off the connection and Channel() will not redeliver it", early)
	}
}

// TestAwaitSubscribedSurfacesAReadFailure asserts a broken read is an error rather
// than a confirmation. Treating it as ready is how a subscription that never
// happened comes to look established.
func TestAwaitSubscribedSurfacesAReadFailure(t *testing.T) {
	r := &fakeReader{errs: errors.New("connection reset")}
	if _, err := awaitSubscribed(context.Background(), r, 2); err == nil {
		t.Fatal("awaitSubscribed reported the subscription confirmed after a read failure")
	}

	// And an answer that is not a confirmation at all.
	odd := &fakeReader{msgs: []any{"not a redis message"}}
	if _, err := awaitSubscribed(context.Background(), odd, 1); err == nil {
		t.Fatal("awaitSubscribed accepted an unrecognised reply as a confirmation")
	}
}

// TestAwaitSubscribedWaitsForEveryChannel asserts the wait is per-channel, not
// per-SUBSCRIBE.
//
// Redis confirms a two-channel SUBSCRIBE one channel at a time. Stopping after the
// first would leave the second — awareness, in this hub — exactly as raced as
// before the fix: a publish on it between the two confirmations is dropped and
// never redelivered. The call site derives the count from the channel list so the
// two cannot disagree; this pins the helper's half of that.
func TestAwaitSubscribedWaitsForEveryChannel(t *testing.T) {
	// Only one confirmation will ever arrive, and then the reader is exhausted.
	r := &fakeReader{msgs: []any{
		&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
	}}
	if _, err := awaitSubscribed(context.Background(), r, 2); err == nil {
		t.Fatal("awaitSubscribed returned ready after one confirmation of two; the second channel would still be racing a publish")
	}

	// Both present: ready, and both consumed.
	ok := &fakeReader{msgs: []any{
		&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
		&goredis.Subscription{Kind: "subscribe", Channel: "awareness:x", Count: 2},
	}}
	if _, err := awaitSubscribed(context.Background(), ok, 2); err != nil {
		t.Fatalf("awaitSubscribed with both confirmations: %v", err)
	}
	if ok.consumed() != 2 {
		t.Fatalf("consumed %d replies, want both confirmations", ok.consumed())
	}
}

// hubWithScriptedPubsub builds a Hub whose subscription handshake is scripted.
func hubWithScriptedPubsub(t *testing.T, script ...any) (*Hub, *fakeReader) {
	t.Helper()
	srv := miniredis.RunT(t)
	ps := &fakeReader{msgs: script}
	h := NewWithClient(&scriptedClient{
		inner:  goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})},
		pubsub: ps,
	}, "pod-a")
	t.Cleanup(func() { _ = h.Close() })
	return h, ps
}

// TestSubscribeWaitsForAConfirmationPerChannel asserts the hub waits for BOTH of
// its channels, driven from the connection rather than hoping a real server
// produces the interleaving.
//
// Returning after one confirmation leaves the second channel — awareness — as
// raced as it was before any of this: a publish on it before the server registers
// the subscription is dropped, permanently, with no error anywhere.
func TestSubscribeWaitsForAConfirmationPerChannel(t *testing.T) {
	t.Run("one confirmation is not enough", func(t *testing.T) {
		h, _ := hubWithScriptedPubsub(t,
			&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
			// and then nothing: the second channel is never confirmed
		)
		var got recorder
		if _, err := h.Subscribe(context.Background(), "x", "sub", got.handler); err == nil {
			t.Fatal("Subscribe reported ready after one of two channels was confirmed")
		}
	})

	t.Run("both confirmations, and both consumed", func(t *testing.T) {
		h, ps := hubWithScriptedPubsub(t,
			&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
			&goredis.Subscription{Kind: "subscribe", Channel: "awareness:x", Count: 2},
		)
		var got recorder
		if _, err := h.Subscribe(context.Background(), "x", "sub", got.handler); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if ps.consumed() != 2 {
			t.Fatalf("the handshake consumed %d replies, want one per channel", ps.consumed())
		}
	})
}

// TestAKeepAliveIsNotASubscriptionConfirmation asserts a Pong does not count.
//
// go-redis pings the pub/sub connection to keep it alive, so a Pong can land in
// the middle of the handshake. Counting it would declare the subscription ready
// one channel early — the awareness race again, reintroduced by a health check.
func TestAKeepAliveIsNotASubscriptionConfirmation(t *testing.T) {
	h, _ := hubWithScriptedPubsub(t,
		&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
		&goredis.Pong{},
		// the second channel is never confirmed
	)
	var got recorder
	if _, err := h.Subscribe(context.Background(), "x", "sub", got.handler); err == nil {
		t.Fatal("a keep-alive was counted as the second channel's confirmation")
	}
}

// TestAMessageArrivingDuringTheHandshakeIsStillDelivered is the narrow loss the
// handshake would otherwise introduce.
//
// A two-channel SUBSCRIBE is confirmed one channel at a time, so a publish on the
// first can land before the second is confirmed. That message has already been
// taken off the connection by the handshake — PubSub.Channel will never redeliver
// it — so dropping it loses the message exactly as the original race did, just in
// a smaller window. It has to be handed to the pump.
func TestAMessageArrivingDuringTheHandshakeIsStillDelivered(t *testing.T) {
	// Framed as a DIFFERENT pod would send it, so loopback suppression does not
	// swallow it and the assertion is about delivery rather than about filtering.
	payload := encode("other-pod", "far-pod", []byte("mid-handshake"))
	h, _ := hubWithScriptedPubsub(t,
		&goredis.Subscription{Kind: "subscribe", Channel: "doc:x", Count: 1},
		&goredis.Message{Channel: "doc:x", Payload: string(payload)},
		&goredis.Subscription{Kind: "subscribe", Channel: "awareness:x", Count: 2},
	)

	var got recorder
	if _, err := h.Subscribe(context.Background(), "x", "sub", got.handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitFor(t, "the message that arrived between confirmations", func() bool { return got.count() == 1 })
	if m := got.snapshot()[0]; string(m.Payload) != "mid-handshake" {
		t.Fatalf("payload = %q, want the message that arrived mid-handshake", m.Payload)
	}
}
