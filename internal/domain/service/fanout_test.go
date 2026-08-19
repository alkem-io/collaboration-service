package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/antst/go-yjs/backend/hub"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// The multi-pod bus in these tests is the CORE's shipped in-process hub, shared
// by several Managers. It replaced a hand-written sharedBus/podBroadcaster pair
// that modelled the same thing.
//
// Sharing the real implementation is strictly better than modelling it: echo
// suppression is now the hub's own SourceID logic rather than a fake's
// re-implementation of it, so these tests exercise the mechanism production uses
// instead of a second copy that could agree with the test while disagreeing with
// Redis. Each room generates its own source id, which is what makes two Managers
// over one hub behave as two pods.

// newPodManager builds a Manager wired to the shared bus as pod `source`, with
// its own in-process metastore/blobstore (separate per pod, as in production —
// only the fan-out bus and the durable stores are shared, and here we keep the
// stores private to prove fan-out, not persistence, drives convergence).
func newPodManager(t *testing.T, bus hub.Hub, _ string) *Manager {
	t.Helper()
	open := authopen.New()
	deps := Deps{
		Hub:        bus,
		Metadata:   metainmem.New(),
		Checkpoint: persistinprocess.New(),
		Auth:       open,
		AuthZ:      open,
	}
	return NewManager(deps, DefaultRoomConfig(), NopMetrics{}, zap.NewNop())
}

func TestTwoPodDocUpdateConverges(t *testing.T) {
	bus := hub.NewInProcess()
	mgrA := newPodManager(t, bus, "pod-A")
	mgrB := newPodManager(t, bus, "pod-B")
	defer mgrA.Close()
	defer mgrB.Close()

	const id = model.DocumentID("shared-doc")

	// One client on each pod, same document.
	clientA := newFakeClient(t)
	clientA.join(mgrA, id, model.ContentTypeMemo)
	clientA.observeUpdates()
	clientB := newFakeClient(t)
	clientB.join(mgrB, id, model.ContentTypeMemo)
	clientB.observeUpdates()

	// Client A (pod A) types; the edit must reach client B (pod B) via the bus.
	clientA.insertText("hello-from-A")

	if !eventually(func() bool { return contains(clientB.text(), "hello-from-A") }) {
		t.Fatalf("pod B client never received pod A's edit; got %q", clientB.text())
	}

	// And symmetrically B → A.
	clientB.insertText("hello-from-B")
	if !eventually(func() bool { return contains(clientA.text(), "hello-from-B") }) {
		t.Fatalf("pod A client never received pod B's edit; got %q", clientA.text())
	}
}

func TestTwoPodAwarenessConverges(t *testing.T) {
	bus := hub.NewInProcess()
	mgrA := newPodManager(t, bus, "pod-A")
	mgrB := newPodManager(t, bus, "pod-B")
	defer mgrA.Close()
	defer mgrB.Close()

	const id = model.DocumentID("shared-aware")

	clientA := newFakeClient(t)
	clientA.join(mgrA, id, model.ContentTypeWhiteboard)
	clientA.observeUpdates()
	clientB := newFakeClient(t)
	clientB.join(mgrB, id, model.ContentTypeWhiteboard)
	clientB.observeUpdates()

	// A sets a cursor (awareness); B must observe it via the awareness:{id} bus.
	clientA.setAwareness(ycrdt.MakeObject("user", "alice"))

	if !eventually(func() bool {
		return clientB.awarenessUserOf(clientA.aware.ClientID) != nil
	}) {
		t.Fatal("pod B never received pod A's awareness")
	}
}

func TestPeerUpdateNotEchoedBackToBus(t *testing.T) {
	// A peer-pod update applied locally must NOT be re-published to the bus, or
	// two pods would ping-pong an update forever. Proven by counting publishes:
	// pod B applying pod A's update produces no further doc publish from B.
	bus := hub.NewInProcess()
	var mu sync.Mutex
	publishes := map[string]int{}
	countingBus := &countingHub{inner: bus, mu: &mu, publishes: publishes, label: "pod-B"}

	mgrA := newManagerWithHub(t, bus)
	mgrB := newManagerWithHub(t, countingBus)
	defer mgrA.Close()
	defer mgrB.Close()

	const id = model.DocumentID("no-echo")
	clientA := newFakeClient(t)
	clientA.join(mgrA, id, model.ContentTypeMemo)
	clientA.observeUpdates()
	clientB := newFakeClient(t)
	clientB.join(mgrB, id, model.ContentTypeMemo)
	clientB.observeUpdates()

	clientA.insertText("x")
	if !eventually(func() bool { return contains(clientB.text(), "x") }) {
		t.Fatal("pod B never received the edit")
	}

	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	n := publishes["pod-B"]
	mu.Unlock()
	if n != 0 {
		t.Errorf("pod B re-published a peer update %d time(s); want 0 (would loop)", n)
	}
}

// --- helpers for the no-echo test ---

// countingHub wraps a hub.Hub and counts publishes per source, so the no-echo
// test can assert that a pod receiving a peer update does not re-publish it.
//
// Re-publishing is the failure that matters here: two pods each relaying the
// other's updates is an infinite loop that saturates the bus and the CPU of every
// pod in the cluster, and it only appears with more than one pod.
type countingHub struct {
	inner     hub.Hub
	mu        *sync.Mutex
	publishes map[string]int
	label     string
}

func (c *countingHub) Publish(ctx context.Context, msg hub.Message) error {
	c.mu.Lock()
	c.publishes[c.label]++
	c.mu.Unlock()
	return c.inner.Publish(ctx, msg)
}

func (c *countingHub) Subscribe(ctx context.Context, doc backend.DocumentID, src backend.SourceID, fn hub.Handler) (hub.Subscription, error) {
	return c.inner.Subscribe(ctx, doc, src, fn)
}

func (c *countingHub) Close() error { return c.inner.Close() }

func newManagerWithHub(t *testing.T, h hub.Hub) *Manager {
	t.Helper()
	open := authopen.New()
	deps := Deps{
		Hub:        h,
		Metadata:   metainmem.New(),
		Checkpoint: persistinprocess.New(),
		Auth:       open,
		AuthZ:      open,
	}
	return NewManager(deps, DefaultRoomConfig(), NopMetrics{}, zap.NewNop())
}

// eventually polls cond up to 2s; cross-pod delivery is asynchronous.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
