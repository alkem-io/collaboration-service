package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/antst/go-yjs/backend"
	yhub "github.com/antst/go-yjs/backend/hub"
	goredis "github.com/redis/go-redis/v9"
)

// recorder collects deliveries for assertions.
type recorder struct {
	mu   sync.Mutex
	msgs []yhub.Message
}

func (r *recorder) handler(_ context.Context, m yhub.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) snapshot() []yhub.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]yhub.Message(nil), r.msgs...)
}

func twoPods(t *testing.T) (*Hub, *Hub) {
	t.Helper()
	srv := miniredis.RunT(t)
	a := NewWithClient(goredis.NewClient(&goredis.Options{Addr: srv.Addr()}), "pod-a")
	b := NewWithClient(goredis.NewClient(&goredis.Options{Addr: srv.Addr()}), "pod-b")
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a, b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestAMessageReachesTheOtherPod is the reason this adapter exists: a client on
// pod A and a client on pod B must converge, which requires the update to cross.
func TestAMessageReachesTheOtherPod(t *testing.T) {
	a, b := twoPods(t)
	var got recorder

	if _, err := b.Subscribe(context.Background(), "doc", "sub-on-b", got.handler); err != nil {
		t.Fatalf("subscribe on pod B: %v", err)
	}

	if err := a.Publish(context.Background(), yhub.Message{
		DocumentID: "doc", SourceID: "sub-on-a", Kind: yhub.DocumentUpdate, Payload: []byte("edit"),
	}); err != nil {
		t.Fatalf("publish from pod A: %v", err)
	}

	waitFor(t, "the update to arrive on pod B", func() bool { return got.count() == 1 })
	m := got.snapshot()[0]
	if string(m.Payload) != "edit" {
		t.Fatalf("payload = %q, want the published bytes", m.Payload)
	}
	if m.SourceID != "sub-on-a" {
		t.Fatalf("SourceID = %q, want the publisher's; echo suppression on the far pod depends on it surviving the wire", m.SourceID)
	}
	if m.Kind != yhub.DocumentUpdate {
		t.Fatalf("Kind = %v, want DocumentUpdate; awareness and document updates must not be confusable on the wire", m.Kind)
	}
}

// TestAwarenessAndDocumentUpdatesStayOnSeparateChannels pins the kind separation
// (FR-009).
//
// Awareness is EPHEMERAL and must never reach durable storage. Keeping the two
// on distinct channels means a subscriber cannot mistake one for the other even
// if it ignored Kind entirely — the separation survives on the wire rather than
// depending on every consumer reading a flag correctly.
func TestAwarenessAndDocumentUpdatesStayOnSeparateChannels(t *testing.T) {
	a, b := twoPods(t)
	var got recorder

	if _, err := b.Subscribe(context.Background(), "doc", "sub-on-b", got.handler); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for _, kind := range []yhub.MessageKind{yhub.DocumentUpdate, yhub.AwarenessUpdate} {
		if err := a.Publish(context.Background(), yhub.Message{
			DocumentID: "doc", SourceID: "sub-on-a", Kind: kind, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("publish %v: %v", kind, err)
		}
	}

	waitFor(t, "both messages to arrive", func() bool { return got.count() == 2 })
	kinds := map[yhub.MessageKind]int{}
	for _, m := range got.snapshot() {
		kinds[m.Kind]++
	}
	if kinds[yhub.DocumentUpdate] != 1 || kinds[yhub.AwarenessUpdate] != 1 {
		t.Fatalf("kinds = %v, want one of each; a document update arriving as awareness (or vice versa) would route ephemeral state into durable storage", kinds)
	}
}

// TestAPodDoesNotDeliverItsOwnPublishTwice is the loopback rule.
//
// Redis has no per-publisher filtering, so a pod's own publish comes straight
// back to it. Local subscribers were already delivered to synchronously inside
// Publish, so re-delivering the loopback would double-apply every message for
// any local subscriber whose source id differs from the publisher's — silently,
// and only in multi-pod deployments.
func TestAPodDoesNotDeliverItsOwnPublishTwice(t *testing.T) {
	a, _ := twoPods(t)
	var got recorder

	// A local subscriber with a DIFFERENT source id: it must receive the publish
	// exactly once, not once locally and again off the wire.
	if _, err := a.Subscribe(context.Background(), "doc", "other-local", got.handler); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := a.Publish(context.Background(), yhub.Message{
		DocumentID: "doc", SourceID: "publisher", Kind: yhub.DocumentUpdate, Payload: []byte("once"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, "the local delivery", func() bool { return got.count() >= 1 })
	// Give the loopback every chance to arrive and be wrongly re-delivered.
	time.Sleep(200 * time.Millisecond)
	if n := got.count(); n != 1 {
		t.Fatalf("delivered %d times, want exactly 1; the pod re-delivered its own loopback on top of the synchronous local delivery", n)
	}
}

// TestClosingASubscriptionStopsDelivery covers unsubscribe, including that the
// document's Redis subscription is torn down with its last local subscriber
// rather than leaking a connection per document ever opened.
func TestClosingASubscriptionStopsDelivery(t *testing.T) {
	a, b := twoPods(t)
	var got recorder

	sub, err := b.Subscribe(context.Background(), "doc", "sub-on-b", got.handler)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second close must be a no-op: %v", err)
	}

	if err := a.Publish(context.Background(), yhub.Message{
		DocumentID: "doc", SourceID: "sub-on-a", Kind: yhub.DocumentUpdate, Payload: []byte("after unsubscribe"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := got.count(); n != 0 {
		t.Fatalf("received %d messages after unsubscribing", n)
	}

	b.mu.Lock()
	pumps := len(b.pumps)
	b.mu.Unlock()
	if pumps != 0 {
		t.Fatalf("%d Redis subscriptions left open after the last local subscriber went; one per document ever opened would accumulate for the life of the pod", pumps)
	}
}

// TestSubscriptionSourceIDIsReported covers the accessor the contract requires.
func TestSubscriptionSourceIDIsReported(t *testing.T) {
	a, _ := twoPods(t)
	sub, err := a.Subscribe(context.Background(), "doc", backend.SourceID("my-source"), func(context.Context, yhub.Message) error { return nil })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	if sub.SourceID() != "my-source" {
		t.Fatalf("SourceID() = %q, want my-source", sub.SourceID())
	}
}
