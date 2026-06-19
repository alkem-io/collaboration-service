package redis

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// newTestRedis spins up an in-process miniredis and returns two Broadcasters
// backed by it but tagged with distinct pod ids — a faithful stand-in for two
// service instances (pods) sharing one Redis (SC-007/SC-011 two-pod).
func newTestRedis(t *testing.T) (podA, podB *Broadcaster) {
	t.Helper()
	mr := miniredis.RunT(t)

	clientA := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	clientB := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = clientA.Close(); _ = clientB.Close() })

	return newWithClient(clientA, "pod-A"), newWithClient(clientB, "pod-B")
}

// collector captures payloads delivered to a Subscribe handler.
type collector struct {
	mu         sync.Mutex
	docs       [][]byte
	ephemerals [][]byte
}

func (c *collector) handle(payload []byte, ephemeral bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]byte(nil), payload...)
	if ephemeral {
		c.ephemerals = append(c.ephemerals, cp)
	} else {
		c.docs = append(c.docs, cp)
	}
}

func (c *collector) snapshot() (docs, ephemerals [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.docs...), append([][]byte(nil), c.ephemerals...)
}

// waitFor polls cond up to a deadline; subscription delivery is asynchronous.
func waitFor(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestPublishDocRoundTripsToOtherPod(t *testing.T) {
	podA, podB := newTestRedis(t)
	ctx := context.Background()
	const id = model.DocumentID("doc-1")

	var col collector
	cancel, err := podB.Subscribe(ctx, id, col.handle)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	// Wait until pod B's subscription is live before publishing (miniredis
	// drops messages with no subscriber).
	if !waitFor(t, func() bool { return podB.subscriberCount(id) > 0 }) {
		t.Fatal("subscription never became active")
	}

	want := []byte("update-bytes")
	if err := podA.Publish(ctx, id, want, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitFor(t, func() bool { docs, _ := col.snapshot(); return len(docs) == 1 }) {
		t.Fatal("pod B never received pod A's doc update")
	}
	docs, ephemerals := col.snapshot()
	if string(docs[0]) != string(want) {
		t.Errorf("payload = %q, want %q", docs[0], want)
	}
	if len(ephemerals) != 0 {
		t.Errorf("doc update misrouted to ephemeral channel: %v", ephemerals)
	}
}

func TestPublishEphemeralUsesAwarenessChannel(t *testing.T) {
	podA, podB := newTestRedis(t)
	ctx := context.Background()
	const id = model.DocumentID("doc-eph")

	var col collector
	cancel, err := podB.Subscribe(ctx, id, col.handle)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()
	if !waitFor(t, func() bool { return podB.subscriberCount(id) > 0 }) {
		t.Fatal("subscription never became active")
	}

	want := []byte("cursor-frame")
	if err := podA.Publish(ctx, id, want, true); err != nil {
		t.Fatalf("publish ephemeral: %v", err)
	}

	if !waitFor(t, func() bool { _, eph := col.snapshot(); return len(eph) == 1 }) {
		t.Fatal("pod B never received pod A's ephemeral")
	}
	docs, eph := col.snapshot()
	if string(eph[0]) != string(want) {
		t.Errorf("ephemeral payload = %q, want %q", eph[0], want)
	}
	if len(docs) != 0 {
		t.Errorf("ephemeral misrouted to doc channel: %v", docs)
	}
}

func TestPodDoesNotReceiveOwnPublishBack(t *testing.T) {
	podA, _ := newTestRedis(t)
	ctx := context.Background()
	const id = model.DocumentID("doc-self")

	var col collector
	cancel, err := podA.Subscribe(ctx, id, col.handle)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()
	if !waitFor(t, func() bool { return podA.subscriberCount(id) > 0 }) {
		t.Fatal("subscription never became active")
	}

	// Pod A publishes its own update; Redis echoes it to A's own subscription,
	// but the source-id tag must make the adapter drop it (no local double-apply).
	if err := podA.Publish(ctx, id, []byte("mine"), false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Give the echo a chance to (wrongly) arrive.
	time.Sleep(200 * time.Millisecond)
	docs, eph := col.snapshot()
	if len(docs) != 0 || len(eph) != 0 {
		t.Errorf("pod received its own publish back: docs=%v eph=%v", docs, eph)
	}
}

func TestCancelUnsubscribesIdempotently(t *testing.T) {
	podA, podB := newTestRedis(t)
	ctx := context.Background()
	const id = model.DocumentID("doc-cancel")

	var col collector
	cancel, err := podB.Subscribe(ctx, id, col.handle)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !waitFor(t, func() bool { return podB.subscriberCount(id) > 0 }) {
		t.Fatal("subscription never became active")
	}

	cancel()
	// Calling cancel again must not panic (idempotent contract).
	cancel()

	if !waitFor(t, func() bool { return podB.subscriberCount(id) == 0 }) {
		t.Fatal("subscription still active after cancel")
	}

	// A publish after cancel must not reach the (torn-down) handler.
	if err := podA.Publish(ctx, id, []byte("after-cancel"), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	docs, eph := col.snapshot()
	if len(docs) != 0 || len(eph) != 0 {
		t.Errorf("handler invoked after cancel: docs=%v eph=%v", docs, eph)
	}
}

func TestTwoInstancesConverge(t *testing.T) {
	// SC-011 two-pod: two instances sharing one Redis each see the other's
	// publishes on the same document, proving cross-instance fan-out.
	podA, podB := newTestRedis(t)
	ctx := context.Background()
	const id = model.DocumentID("doc-converge")

	var colA, colB collector
	cancelA, err := podA.Subscribe(ctx, id, colA.handle)
	if err != nil {
		t.Fatalf("subscribe A: %v", err)
	}
	defer cancelA()
	cancelB, err := podB.Subscribe(ctx, id, colB.handle)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	defer cancelB()

	if !waitFor(t, func() bool { return podA.subscriberCount(id) > 0 && podB.subscriberCount(id) > 0 }) {
		t.Fatal("subscriptions never became active")
	}

	if err := podA.Publish(ctx, id, []byte("from-A"), false); err != nil {
		t.Fatalf("publish A: %v", err)
	}
	if err := podB.Publish(ctx, id, []byte("from-B"), false); err != nil {
		t.Fatalf("publish B: %v", err)
	}

	// Each pod sees only the OTHER pod's update (origin filtering), so both
	// converge to having observed both updates across the cluster.
	if !waitFor(t, func() bool {
		da, _ := colA.snapshot()
		db, _ := colB.snapshot()
		return len(da) == 1 && len(db) == 1
	}) {
		da, _ := colA.snapshot()
		db, _ := colB.snapshot()
		t.Fatalf("cross-instance convergence failed: A saw %d, B saw %d", len(da), len(db))
	}
	da, _ := colA.snapshot()
	db, _ := colB.snapshot()
	if string(da[0]) != "from-B" {
		t.Errorf("pod A saw %q, want from-B", da[0])
	}
	if string(db[0]) != "from-A" {
		t.Errorf("pod B saw %q, want from-A", db[0])
	}
}

func TestNewFromURL(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := New("redis://"+mr.Addr(), "pod-x")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = b.Close() }()

	ctx := context.Background()
	if err := b.Publish(ctx, "doc", []byte("x"), false); err != nil {
		t.Errorf("publish after New: %v", err)
	}
}

func TestNewFromURLInvalid(t *testing.T) {
	if _, err := New("not-a-redis-url", "pod-x"); err == nil {
		t.Error("expected error for invalid REDIS_URL")
	}
}

// TestNewDefaultsEmptySource defends New's source-defaulting branch
// (broadcaster.go:58): an empty source id must be replaced with a generated one
// so echo suppression still works (a pod with an empty tag could not distinguish
// its own echoes). The broadcaster must be usable afterward.
func TestNewDefaultsEmptySource(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := New("redis://"+mr.Addr(), "") // empty source → generated uuid
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = b.Close() }()
	if b.source == "" {
		t.Error("New must generate a non-empty source id when none is supplied")
	}
}

// TestNewWithClientDefaultsEmptySource defends newWithClient's source-defaulting
// branch (broadcaster.go:67): the same invariant on the lower-level constructor.
func TestNewWithClientDefaultsEmptySource(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	b := newWithClient(client, "") // empty source → generated uuid
	if b.source == "" {
		t.Error("newWithClient must generate a non-empty source id when none is supplied")
	}
}

// TestPublishSurfacesClientError defends Publish's error branch
// (broadcaster.go:85): when the underlying client is closed the publish fails,
// and the broadcaster must surface that error rather than report a fan-out that
// never reached Redis.
func TestPublishSurfacesClientError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	b := newWithClient(client, "pod-x")
	_ = client.Close()
	if err := b.Publish(context.Background(), "doc", []byte("x"), false); err == nil {
		t.Error("expected Publish to surface a closed-client error")
	}
}

// TestSubscribeSurfacesReceiveError defends Subscribe's confirmation-error
// branch (broadcaster.go:102): if the subscription cannot be confirmed (closed
// client) Subscribe must return an error and a nil cancel, never a half-live
// subscription that silently drops frames.
func TestSubscribeSurfacesReceiveError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	b := newWithClient(client, "pod-x")
	_ = client.Close()
	cancel, err := b.Subscribe(context.Background(), "doc", func([]byte, bool) {})
	if err == nil {
		t.Error("expected Subscribe to fail confirming against a closed client")
	}
	if cancel != nil {
		t.Error("a failed Subscribe must not return a cancel func")
	}
}

// TestDecodeFrameRejectsTruncated defends decodeFrame's two malformed-frame
// guards (broadcaster.go:186 too-short, :190 source-tag overruns the buffer). A
// malformed frame must be reported invalid (ok=false) so the consumer drops it
// rather than applying garbage as a document update.
func TestDecodeFrameRejectsTruncated(t *testing.T) {
	if _, _, ok := decodeFrame([]byte{0x00}); ok {
		t.Error("a 1-byte frame (no length prefix) must be rejected")
	}
	// Length prefix claims 5 source bytes, but only 2 follow.
	if _, _, ok := decodeFrame([]byte{0x00, 0x05, 'a', 'b'}); ok {
		t.Error("a frame whose source length overruns the buffer must be rejected")
	}
}

// TestEncodeFrameRoundTrips defends the encode/decode contract and exercises
// encodeFrame's normal path; combined with the truncation test above it pins the
// frame codec both pods rely on for echo suppression.
func TestEncodeFrameRoundTrips(t *testing.T) {
	frame := encodeFrame("pod-A", []byte("payload-bytes"))
	src, payload, ok := decodeFrame(frame)
	if !ok || src != "pod-A" || string(payload) != "payload-bytes" {
		t.Errorf("round trip = (%q, %q, %v)", src, payload, ok)
	}
}

// TestEncodeFrameTruncatesOversizedSource defends encodeFrame's source-truncation
// branch (broadcaster.go:172): a source longer than the uint16 length prefix can
// hold is truncated defensively. Truncation must affect only the source tag
// (echo suppression), never the payload bytes, which must round-trip intact.
func TestEncodeFrameTruncatesOversizedSource(t *testing.T) {
	oversized := strings.Repeat("s", maxSourceLen+10)
	payload := []byte("doc-bytes-must-survive")
	frame := encodeFrame(oversized, payload)
	src, got, ok := decodeFrame(frame)
	if !ok {
		t.Fatal("encoded frame must decode")
	}
	if len(src) != maxSourceLen {
		t.Errorf("source truncated to %d bytes, want %d", len(src), maxSourceLen)
	}
	if string(got) != string(payload) {
		t.Errorf("payload corrupted by source truncation: got %q", got)
	}
}
