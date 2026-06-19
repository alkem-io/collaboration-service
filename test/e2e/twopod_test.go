//go:build e2e

package e2e

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	ycrdt "github.com/skyterra/y-crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/config"
)

// redisConfig is the standalone config with FANOUT_MODE=redis pointed at url.
// Two pods built from this share one Redis bus — the multi-pod topology (SC-007).
func redisConfig(url string) *config.Config {
	cfg := standaloneConfig()
	cfg.Fanout = config.FanoutRedis
	cfg.Redis.URL = url
	return cfg
}

// bootPod boots one service instance (a "pod") through the real app wiring with
// the redis fan-out config, on its own httptest server, returning its ws:// base.
// Each pod gets a fresh app.New, so each constructs its own redis client with a
// unique source id — exactly the production per-pod topology.
func bootPod(t *testing.T, url string) string {
	t.Helper()
	application, err := app.New(redisConfig(url), zap.NewNop())
	if err != nil {
		t.Fatalf("app.New (pod): %v", err)
	}
	srv := httptest.NewServer(application.Handler)
	t.Cleanup(func() {
		srv.Close()
		application.Close()
	})
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestTwoPodCrossInstanceConvergence is the SC-011 two-pod e2e: two SEPARATE
// service instances share one Redis fan-out bus; a client on pod A and a client
// on pod B converge on both the document and awareness via the redis fan-out,
// with NO code change vs single-pod (the only difference is FANOUT_MODE=redis).
// This is the full WS-level extension of the Wave-2 TestTwoInstancesConverge.
func TestTwoPodCrossInstanceConvergence(t *testing.T) {
	mr := miniredis.RunT(t) // in-process Redis shared by both pods (pub/sub).
	url := "redis://" + mr.Addr()

	baseA := bootPod(t, url)
	baseB := bootPod(t, url)

	const docID = "e2e-2pod-memo"
	a := dial(t, baseA, docID, "memo") // client on pod A
	b := dial(t, baseB, docID, "memo") // client on pod B
	time.Sleep(150 * time.Millisecond)

	// A (pod A) edits; the update must cross the Redis bus to B (pod B).
	a.insertMemo("from-pod-A ")
	if !eventually(func() bool { return contains(b.memoText(), "from-pod-A") }) {
		t.Fatalf("pod-B client never received pod-A's edit via redis fan-out; got %q", b.memoText())
	}

	// And symmetrically B → A.
	b.insertMemo("from-pod-B ")
	if !eventually(func() bool { return contains(a.memoText(), "from-pod-B") }) {
		t.Fatalf("pod-A client never received pod-B's edit via redis fan-out; got %q", a.memoText())
	}

	// Both clients converge to identical state across the two pods.
	if !eventually(func() bool { return a.memoText() == b.memoText() }) {
		t.Fatalf("cross-pod clients diverged:\n  a=%q\n  b=%q", a.memoText(), b.memoText())
	}
}

// TestTwoPodAwarenessCrossInstance proves the presence channel fans out across
// pods over the awareness:{id} bus: a client on pod A announces awareness and a
// client on pod B observes it (SC-009 cross-instance).
func TestTwoPodAwarenessCrossInstance(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr()

	baseA := bootPod(t, url)
	baseB := bootPod(t, url)

	const docID = "e2e-2pod-presence"
	a := dial(t, baseA, docID, "whiteboard")
	b := dial(t, baseB, docID, "whiteboard")
	time.Sleep(150 * time.Millisecond)

	a.setAwareness(ycrdt.Object{"user": "alice-on-A"})
	if !eventually(func() bool { return b.awarenessClientCount() >= 2 }) {
		t.Fatalf("pod-B client never saw pod-A's awareness; holds %d states", b.awarenessClientCount())
	}
}
