//go:build e2e

package e2e

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/config"
)

// redisConfig is the standalone config with HUB_MODE=redis pointed at url.
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
// with NO code change vs single-pod (the only difference is HUB_MODE=redis).
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

	a.setAwareness(ycrdt.MakeObject("user", "alice-on-A"))
	if !eventually(func() bool { return b.awarenessClientCount() >= 2 }) {
		t.Fatalf("pod-B client never saw pod-A's awareness; holds %d states", b.awarenessClientCount())
	}
}

// TestTwoPodConvergesUnderHostileDelivery is T051 / SC-007.
//
// The hub contract promises neither ordering nor single delivery, so a
// deployment must converge anyway. The two pods here are driven with interleaved,
// concurrent edits from both sides — which is what actually produces reordering
// on a real bus, since two publishers racing means neither pod sees a single
// well-ordered stream.
//
// Correct echo suppression is asserted at the same time, and it is the part that
// fails loudly if wrong: a pod that re-published the peer updates it received
// would loop with its counterpart forever, and the symptom in this test is that
// the documents never settle rather than that they settle wrongly.
func TestTwoPodConvergesUnderHostileDelivery(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr()

	baseA := bootPod(t, url)
	baseB := bootPod(t, url)

	const docID = "e2e-2pod-hostile"
	a := dial(t, baseA, docID, "memo")
	b := dial(t, baseB, docID, "memo")
	time.Sleep(150 * time.Millisecond)

	// Interleaved concurrent edits from both pods: neither side observes a
	// well-ordered stream, which is the reordering the contract permits.
	const rounds = 12
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range rounds {
			a.insertMemo(fmt.Sprintf("A%02d ", i))
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := range rounds {
			b.insertMemo(fmt.Sprintf("B%02d ", i))
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Wait()

	if !eventually(func() bool { return a.memoText() == b.memoText() }) {
		t.Fatalf("pods did not converge under interleaved edits:\n  a=%q\n  b=%q", a.memoText(), b.memoText())
	}

	// Every edit from both sides survived. Convergence to an IDENTICAL but
	// truncated document would satisfy the check above while having lost writes.
	final := a.memoText()
	for i := range rounds {
		for _, want := range []string{fmt.Sprintf("A%02d", i), fmt.Sprintf("B%02d", i)} {
			if !contains(final, want) {
				t.Fatalf("converged state is missing %q; the pods agreed on a document that lost an edit: %q", want, final)
			}
		}
	}
}

// TestTwoPodConvergenceBound is the T071/SC-002 bound for the multi-pod path.
//
// Convergence "eventually" is not a useful promise to a person typing: the
// requirement is that connected clients reach identical state within one second
// of edits settling. Asserting the bound rather than mere convergence is what
// catches a fan-out path that works but has acquired a multi-second debounce,
// retry backoff, or poll interval — all of which pass an unbounded check.
func TestTwoPodConvergenceBound(t *testing.T) {
	mr := miniredis.RunT(t)
	url := "redis://" + mr.Addr()

	baseA := bootPod(t, url)
	baseB := bootPod(t, url)

	const docID = "e2e-2pod-bound"
	a := dial(t, baseA, docID, "memo")
	b := dial(t, baseB, docID, "memo")
	time.Sleep(150 * time.Millisecond)

	a.insertMemo("bounded-cross-pod ")
	settled := time.Now()

	if !convergedWithin(time.Second, func() bool {
		return a.memoText() == b.memoText() && contains(b.memoText(), "bounded-cross-pod")
	}) {
		t.Fatalf("cross-pod clients did not converge within 1s of the edit settling (took >%v):\n  a=%q\n  b=%q",
			time.Since(settled), a.memoText(), b.memoText())
	}
}

// convergedWithin polls cond until it holds or the budget expires.
func convergedWithin(budget time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
