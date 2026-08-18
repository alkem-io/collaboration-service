package service

import (
	"context"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// sharedBus is an in-process ClusterBroadcaster shared by several managers in a
// test, modelling one Redis shared by N pods. Each manager wraps it in a
// podBroadcaster tagged with that pod's source id so a pod never receives its
// own publish back — exactly the contract the redis adapter implements, but
// without a Redis dependency in the domain test.
type sharedBus struct {
	mu   sync.Mutex
	subs map[model.DocumentID][]*busSub
}

type busSub struct {
	source  string
	handler func(payload []byte, ephemeral bool)
}

func newSharedBus() *sharedBus {
	return &sharedBus{subs: make(map[model.DocumentID][]*busSub)}
}

func (s *sharedBus) publish(source string, id model.DocumentID, payload []byte, ephemeral bool) {
	s.mu.Lock()
	subs := append([]*busSub(nil), s.subs[id]...)
	s.mu.Unlock()
	for _, sub := range subs {
		if sub.source == source {
			continue // origin filtering: a pod never gets its own echo.
		}
		sub.handler(append([]byte(nil), payload...), ephemeral)
	}
}

func (s *sharedBus) subscribe(source string, id model.DocumentID, handler func([]byte, bool)) func() {
	sub := &busSub{source: source, handler: handler}
	s.mu.Lock()
	s.subs[id] = append(s.subs[id], sub)
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		cur := s.subs[id]
		for i, c := range cur {
			if c == sub {
				s.subs[id] = append(cur[:i:i], cur[i+1:]...)
				break
			}
		}
	}
}

// podBroadcaster is one pod's view of the shared bus.
type podBroadcaster struct {
	bus    *sharedBus
	source string
}

func (p *podBroadcaster) Publish(_ context.Context, id model.DocumentID, payload []byte, ephemeral bool) error {
	p.bus.publish(p.source, id, payload, ephemeral)
	return nil
}

func (p *podBroadcaster) Subscribe(_ context.Context, id model.DocumentID, handler func(payload []byte, ephemeral bool)) (func(), error) {
	return p.bus.subscribe(p.source, id, handler), nil
}

// newPodManager builds a Manager wired to the shared bus as pod `source`, with
// its own in-process metastore/blobstore (separate per pod, as in production —
// only the fan-out bus and the durable stores are shared, and here we keep the
// stores private to prove fan-out, not persistence, drives convergence).
func newPodManager(t *testing.T, bus *sharedBus, source string) *Manager {
	t.Helper()
	open := authopen.New()
	deps := Deps{
		Broadcaster: &podBroadcaster{bus: bus, source: source},
		Metadata:    metainmem.New(),
		Checkpoint:  persistinprocess.New(),
		Auth:        open,
		AuthZ:       open,
	}
	return NewManager(deps, DefaultRoomConfig(), NopMetrics{}, zap.NewNop())
}

func TestTwoPodDocUpdateConverges(t *testing.T) {
	bus := newSharedBus()
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
	bus := newSharedBus()
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
	bus := newSharedBus()
	var mu sync.Mutex
	publishes := map[string]int{}
	countingBus := &countingSharedBus{inner: bus, mu: &mu, publishes: publishes}

	mgrA := newManagerWithBroadcaster(t, &podBroadcaster{bus: bus, source: "pod-A"})
	mgrB := newManagerWithBroadcaster(t, &countingPod{source: "pod-B", bus: countingBus})
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

type countingSharedBus struct {
	inner     *sharedBus
	mu        *sync.Mutex
	publishes map[string]int
}

type countingPod struct {
	source string
	bus    *countingSharedBus
}

func (p *countingPod) Publish(_ context.Context, id model.DocumentID, payload []byte, ephemeral bool) error {
	p.bus.mu.Lock()
	p.bus.publishes[p.source]++
	p.bus.mu.Unlock()
	p.bus.inner.publish(p.source, id, payload, ephemeral)
	return nil
}

func (p *countingPod) Subscribe(_ context.Context, id model.DocumentID, handler func(payload []byte, ephemeral bool)) (func(), error) {
	return p.bus.inner.subscribe(p.source, id, handler), nil
}

func newManagerWithBroadcaster(t *testing.T, b interface {
	Publish(context.Context, model.DocumentID, []byte, bool) error
	Subscribe(context.Context, model.DocumentID, func([]byte, bool)) (func(), error)
}) *Manager {
	t.Helper()
	open := authopen.New()
	deps := Deps{
		Broadcaster: b,
		Metadata:    metainmem.New(),
		Checkpoint:  persistinprocess.New(),
		Auth:        open,
		AuthZ:       open,
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
