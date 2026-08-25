package service

import (
	"context"
	"sync"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// frameSpy captures the document frames crossing the bus, so a test can redeliver
// a byte-identical copy of a real fan-out message rather than a hand-built
// approximation of one.
type frameSpy struct {
	mu     sync.Mutex
	frames [][]byte
}

func (s *frameSpy) handler(_ context.Context, m hub.Message) error {
	if m.Kind != hub.DocumentUpdate {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frames = append(s.frames, m.Payload)
	return nil
}

func (s *frameSpy) last() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.frames) == 0 {
		return nil
	}
	return s.frames[len(s.frames)-1]
}

// TestRedeliveredPeerUpdateIsAHarmlessNoOp is T052, and the hub contract's
// at-least-once posture made observable.
//
// The contract promises neither ordering nor single delivery — implementations
// "may duplicate or reorder". Redis pub/sub, a reconnecting subscriber, or a
// republishing pod can each deliver the same update twice. The room absorbs that
// because Yjs updates are idempotent: applying one twice is the same as applying
// it once.
//
// It is worth asserting precisely because the property is INHERITED rather than
// written. It holds through how the CRDT merges, not through anything in this
// service, so nothing here would notice if a later change made a second delivery
// observable — a version counter bumped per applied update, dedup bookkeeping, a
// "seen" set, an appended-rather-than-merged fast path. The symptom would be
// duplicated text, only in multi-pod deployments, and only after a redelivery.
func TestRedeliveredPeerUpdateIsAHarmlessNoOp(t *testing.T) {
	bus := hub.NewInProcess()
	mgrA := newManagerWithHub(t, bus)
	mgrB := newManagerWithHub(t, bus)
	t.Cleanup(mgrA.Close)
	t.Cleanup(mgrB.Close)

	const id = model.DocumentID("redelivered")

	// The spy subscribes first, so it sees the frame as it crosses.
	spy := &frameSpy{}
	sub, err := bus.Subscribe(context.Background(), backend.DocumentID(id), "spy", spy.handler)
	if err != nil {
		t.Fatalf("subscribe spy: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	a := newFakeClient(t)
	a.join(mgrA, id, model.ContentTypeMemo)
	a.observeUpdates()
	b := newFakeClient(t)
	b.join(mgrB, id, model.ContentTypeMemo)
	b.observeUpdates()

	a.insertText("exactly once ")
	if !eventually(func() bool { return contains(b.text(), "exactly once") }) {
		t.Fatalf("pod B never received the update: %q", b.text())
	}
	afterFirstDelivery := b.text()

	frame := spy.last()
	if frame == nil {
		t.Fatal("no document frame crossed the bus; there is nothing to redeliver")
	}

	// Redeliver the same frame several times, THROUGH THE BUS, as a hub is
	// permitted to. Going through the bus rather than calling the room's handler
	// directly is both faithful — a redelivery is a second publish — and required:
	// the room's document has a single writer, its run loop, so a test goroutine
	// applying an update behind its back is a data race rather than a scenario.
	for range 3 {
		if err := bus.Publish(context.Background(), hub.Message{
			DocumentID: backend.DocumentID(id),
			SourceID:   "redelivery",
			Kind:       hub.DocumentUpdate,
			Payload:    frame,
		}); err != nil {
			t.Fatalf("redeliver: %v", err)
		}
	}

	// Let the redeliveries drain through both rooms' loops.
	if !eventually(func() bool { return b.text() == afterFirstDelivery }) {
		t.Fatalf("document changed after redelivery: %q → %q", afterFirstDelivery, b.text())
	}

	if got := b.text(); got != afterFirstDelivery {
		t.Fatalf("redelivery changed the document: %q → %q; a duplicated update must be a no-op, or every redelivery duplicates text in multi-pod deployments", afterFirstDelivery, got)
	}
}
