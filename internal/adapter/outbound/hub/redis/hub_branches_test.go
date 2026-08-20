package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/antst/go-yjs/backend"
	yhub "github.com/antst/go-yjs/backend/hub"
	goredis "github.com/redis/go-redis/v9"
)

func newTestHub(t *testing.T) *Hub {
	t.Helper()
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "instance-under-test")
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestInvalidSubscribeAndPublishAreRejected covers the argument guards.
//
// An empty document id or a nil handler would otherwise register a subscriber
// that can never be addressed or never be called — a silent no-op subscription
// rather than an error, which is the shape of bug that surfaces as "fan-out
// stopped working" with nothing in the logs.
func TestInvalidSubscribeAndPublishAreRejected(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()

	if _, err := h.Subscribe(ctx, "", "src", func(context.Context, yhub.Message) error { return nil }); !errors.Is(err, yhub.ErrInvalidMessage) {
		t.Fatalf("Subscribe with no document id = %v, want ErrInvalidMessage", err)
	}
	if _, err := h.Subscribe(ctx, "doc", "src", nil); !errors.Is(err, yhub.ErrInvalidMessage) {
		t.Fatalf("Subscribe with a nil handler = %v, want ErrInvalidMessage", err)
	}
	if err := h.Publish(ctx, yhub.Message{DocumentID: "", Kind: yhub.DocumentUpdate}); !errors.Is(err, yhub.ErrInvalidMessage) {
		t.Fatalf("Publish with no document id = %v, want ErrInvalidMessage", err)
	}
	if err := h.Publish(ctx, yhub.Message{DocumentID: "doc", Kind: yhub.MessageKind(99)}); !errors.Is(err, yhub.ErrInvalidMessage) {
		t.Fatalf("Publish with an unknown kind = %v, want ErrInvalidMessage; an unrecognised kind must not be routed to a default channel", err)
	}
}

// TestCancelledPublishIsRejected covers the context guard on the publish path.
func TestCancelledPublishIsRejected(t *testing.T) {
	h := newTestHub(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := h.Publish(ctx, yhub.Message{DocumentID: "doc", Kind: yhub.DocumentUpdate}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish with a cancelled context = %v, want context.Canceled", err)
	}
}

// TestGeneratedInstanceIDWhenNoneGiven covers the default. The id is what keeps
// a pod from re-delivering its own loopback, so an empty one shared by every pod
// would make each pod discard the OTHER pods' messages as if they were its own —
// fan-out would appear to work locally and silently deliver nothing across pods.
func TestGeneratedInstanceIDWhenNoneGiven(t *testing.T) {
	srv := miniredis.RunT(t)
	a := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "")
	b := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "")
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	if a.instance == "" || b.instance == "" {
		t.Fatal("an empty instance id must be replaced with a generated one")
	}
	if a.instance == b.instance {
		t.Fatal("two hubs generated the same instance id; each would discard the other's messages as its own loopback and cross-pod delivery would silently stop")
	}
}

// TestPublishSurfacesARedisFailure covers the transport-error path: a hub whose
// Redis is gone must report it rather than report success, or a pod would go on
// serving while silently isolated from every other pod.
func TestPublishSurfacesARedisFailure(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "instance")
	t.Cleanup(func() { _ = h.Close() })
	srv.Close() // the broker goes away

	err := h.Publish(context.Background(), yhub.Message{DocumentID: "doc", Kind: yhub.DocumentUpdate, Payload: []byte("x")})
	if err == nil {
		t.Fatal("a publish against a dead Redis must fail; reporting success would leave a pod serving while silently isolated")
	}
	if !strings.Contains(err.Error(), "redis publish") {
		t.Fatalf("error = %v, want it to name the failing stage", err)
	}
}

// TestClosingAnAlreadyClosedHubIsANoOp and closing twice must not panic —
// App.Close runs closers unconditionally and a double close is ordinary.
func TestClosingAnAlreadyClosedHubIsANoOp(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "instance")
	if err := h.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second Close must be a no-op: %v", err)
	}
}

// TestClosingASubscriptionOfAnUnknownDocumentIsSafe covers removeSubscriber's
// guards, which a double Close or a Close after hub shutdown reaches.
func TestClosingASubscriptionOfAnUnknownDocumentIsSafe(t *testing.T) {
	h := newTestHub(t)
	sub, err := h.Subscribe(context.Background(), "doc", "src", func(context.Context, yhub.Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// The document is gone from the registry; a second removal must not panic.
	h.removeSubscriber("doc", 999)
	h.removeSubscriber("never-existed", 1)
}

// TestMalformedWireFramesAreIgnored covers the decoder's guards.
//
// A frame this hub did not write — another service sharing the Redis instance,
// or a truncated message — must be dropped, not guessed at. Guessing would hand
// a subscriber a payload sliced at an arbitrary offset and let it be applied to
// a document as a CRDT update.
func TestMalformedWireFramesAreIgnored(t *testing.T) {
	for _, frame := range [][]byte{
		nil,
		{},
		{0x00},
		{0x00, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0xff},      // instance length overruns
		{0x00, 0x00, 0x00, 0x01, 'a'}, // no source length
		{0x00, 0x00, 0x00, 0x01, 'a', 0xff, 0xff, 0xff, 0xff}, // source length overruns
	} {
		if _, _, _, ok := decode(frame); ok {
			t.Fatalf("decode accepted a malformed frame %v; a guessed payload could be applied to a document as a CRDT update", frame)
		}
	}
}

// TestEncodeDecodeRoundTrip is the positive case, so the rejection test above
// cannot pass against a decoder that rejects everything.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	frame := encode("inst-1", backend.SourceID("src-1"), []byte("payload"))
	instance, source, payload, ok := decode(frame)
	if !ok {
		t.Fatal("a frame this hub wrote must decode")
	}
	if instance != "inst-1" || source != "src-1" || string(payload) != "payload" {
		t.Fatalf("round-trip = (%q, %q, %q)", instance, source, payload)
	}
}

// TestOversizeIdentifiersAreTruncatedNotCorrupted covers the length caps. The
// prefixes are uint32, so an identifier longer than the cap must be truncated
// deliberately rather than silently wrapping the length and producing a frame
// whose declared size does not match its contents.
func TestOversizeIdentifiersAreTruncatedNotCorrupted(t *testing.T) {
	huge := strings.Repeat("x", maxIDLen+100)
	frame := encode(huge, backend.SourceID(huge), []byte("body"))
	instance, source, payload, ok := decode(frame)
	if !ok {
		t.Fatal("a frame with oversize identifiers must still decode")
	}
	if len(instance) != maxIDLen || len(source) != maxIDLen {
		t.Fatalf("identifiers were not truncated to the cap: instance=%d source=%d", len(instance), len(source))
	}
	if string(payload) != "body" {
		t.Fatalf("payload corrupted by truncation: %q", payload)
	}
}

// TestSubscribeAfterCloseIsRejected covers the closed check on the subscribe
// path, which App.Close can race with a late room materialization.
func TestSubscribeAfterCloseIsRejected(t *testing.T) {
	srv := miniredis.RunT(t)
	h := NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "instance")
	if err := h.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := h.Subscribe(context.Background(), "doc", "src", func(context.Context, yhub.Message) error { return nil }); !errors.Is(err, yhub.ErrClosed) {
		t.Fatalf("Subscribe after Close = %v, want ErrClosed", err)
	}
}

// TestSecondSubscriberSharesTheDocumentsRedisSubscription covers the refcount
// path. Redis fan-out is per channel, so a second local subscriber needs no
// second connection — and one connection per subscriber would exhaust the
// broker's limit on a busy document.
func TestSecondSubscriberSharesTheDocumentsRedisSubscription(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()

	s1, err := h.Subscribe(ctx, "doc", "a", func(context.Context, yhub.Message) error { return nil })
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	s2, err := h.Subscribe(ctx, "doc", "b", func(context.Context, yhub.Message) error { return nil })
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	h.mu.Lock()
	pumps, refs := len(h.pumps), h.pumps["doc"].refs
	h.mu.Unlock()
	if pumps != 1 || refs != 2 {
		t.Fatalf("pumps=%d refs=%d, want one shared subscription with two references", pumps, refs)
	}

	// The pump survives the first unsubscribe and goes with the last.
	_ = s1.Close()
	h.mu.Lock()
	still := len(h.pumps)
	h.mu.Unlock()
	if still != 1 {
		t.Fatal("the shared subscription was torn down while a subscriber remained")
	}
	_ = s2.Close()
	h.mu.Lock()
	gone := len(h.pumps)
	h.mu.Unlock()
	if gone != 0 {
		t.Fatal("the shared subscription outlived its last subscriber")
	}
}

// TestPumpDoesNotOutliveItsSubscribersUnderRace is the regression for a leak
// found in adversarial review (T067).
//
// Subscribe registers the subscriber, then starts the document's Redis
// subscription OFF the lock (it does I/O), then installs it. A Close landing in
// that window found no pump to decrement — so nothing was stopped — and the
// install then registered a pump whose only subscriber was already gone.
//
// The leak is permanent and self-concealing: refs is stuck above zero, so no
// later Close can ever reach zero either. The pod holds a Redis subscription and
// a goroutine per affected document for the rest of its life, and the only
// symptom is connection count on the Redis side.
//
// Driven by opening and immediately closing many subscriptions, which is exactly
// the shape of a room that materializes and is torn down at once — a shutdown
// race, or the purge tombstone refusing a room mid-build.
//
// Non-vacuity: restore `p.refs = 1` and this fails with pumps left behind.
func TestPumpDoesNotOutliveItsSubscribersUnderRace(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 600 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doc := backend.DocumentID("doc-" + strconv.Itoa(i%2))
			sub, err := h.Subscribe(ctx, doc, backend.SourceID(strconv.Itoa(i)), func(context.Context, yhub.Message) error { return nil })
			if err != nil {
				return
			}
			_ = sub.Close()
		}(i)
	}
	wg.Wait()

	h.mu.Lock()
	pumps, subs := len(h.pumps), len(h.subs)
	h.mu.Unlock()

	if subs != 0 {
		t.Fatalf("%d documents still have subscribers after every subscription was closed", subs)
	}
	if pumps != 0 {
		t.Fatalf("%d Redis subscriptions outlived their last subscriber; refs is stuck above zero so no later Close can reach it either, and the pod holds a subscription and a goroutine per document for the rest of its life", pumps)
	}
}

// TestPumpRefsAlwaysMatchTheLiveSubscriberCount pins the invariant the leak
// above violated.
//
// removeSubscriber decrements refs and tears the pump down at zero, so its
// arithmetic only works if refs equals the number of live subscribers for that
// document. Deriving refs from the map rather than assuming a value is what
// makes the two sites agree by construction instead of by coincidence.
func TestPumpRefsAlwaysMatchTheLiveSubscriberCount(t *testing.T) {
	h := newTestHub(t)
	ctx := context.Background()

	subs := make([]yhub.Subscription, 0, 3)
	for i := range 3 {
		s, err := h.Subscribe(ctx, "doc", backend.SourceID(strconv.Itoa(i)), func(context.Context, yhub.Message) error { return nil })
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		subs = append(subs, s)

		h.mu.Lock()
		refs, live := h.pumps["doc"].refs, len(h.subs["doc"])
		h.mu.Unlock()
		if refs != live {
			t.Fatalf("after %d subscribes: refs=%d live=%d", i+1, refs, live)
		}
	}

	for i, s := range subs {
		_ = s.Close()
		h.mu.Lock()
		live := len(h.subs["doc"])
		p := h.pumps["doc"]
		h.mu.Unlock()

		if live == 0 {
			if p != nil {
				t.Fatalf("after closing subscription %d the last subscriber was gone but the pump remained", i)
			}
			continue
		}
		if p == nil {
			t.Fatalf("after closing subscription %d the pump was torn down with %d subscribers still live", i, live)
		}
		if p.refs != live {
			t.Fatalf("after closing subscription %d: refs=%d live=%d", i, p.refs, live)
		}
	}
}
