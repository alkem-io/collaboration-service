package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// blockingSubHub mimics a redis-style fan-out where closing the subscription
// BLOCKS until the delivery goroutine exits (pubsub.Close semantics). The
// goroutine calls the room's handler exactly once (when triggered); if that
// handler parks because the decoupled peerUpdates queue is full, the close can
// only return once teardown cancels the room context and frees the parked
// write — the property under test.
type blockingSubHub struct {
	trigger        chan struct{}
	handlerEntered chan struct{}
	goroutineDone  chan struct{}
}

func (b *blockingSubHub) Publish(context.Context, hub.Message) error { return nil }

func (b *blockingSubHub) Close() error { return nil }

func (b *blockingSubHub) Subscribe(_ context.Context, doc backend.DocumentID, _ backend.SourceID, handler hub.Handler) (hub.Subscription, error) {
	subCtx := context.WithoutCancel(context.Background())
	b.trigger = make(chan struct{})
	b.handlerEntered = make(chan struct{})
	b.goroutineDone = make(chan struct{})
	go func() {
		defer close(b.goroutineDone)
		<-b.trigger
		close(b.handlerEntered)
		// May park on the full peerUpdates queue.
		_ = handler(subCtx, hub.Message{
			DocumentID: doc, Kind: hub.DocumentUpdate, Payload: []byte("peer-payload"),
		})
	}()
	return blockingSubscription{done: b.goroutineDone}, nil
}

// blockingSubscription's Close WAITS for the delivery goroutine to exit, which is
// what makes the teardown ordering observable.
type blockingSubscription struct{ done chan struct{} }

func (blockingSubscription) SourceID() backend.SourceID { return "" }

func (s blockingSubscription) Close() error { <-s.done; return nil }

// TestInvTeardownNoDeadlock — INV-TEARDOWN-NODEADLOCK (spec 002 FR-009). A subscribe
// goroutine parked writing a peer update must not deadlock teardown: teardown cancels
// the room context (freeing the parked decoupled write) BEFORE cancelSub, which for
// redis waits for that goroutine. Reverting the teardown order (cancelSub before the
// ctx cancel) deadlocks here — cancelSub waits on a goroutine that only the ctx
// cancel can free.
func TestInvTeardownNoDeadlock(t *testing.T) {
	deps := newTestDeps()
	bcast := &blockingSubHub{}
	deps.Hub = bcast

	room, err := newRoom(context.Background(), "doc-deadlock", model.ContentTypeMemo,
		deps.Deps, RoomConfig{SendBuffer: 16, BackendTimeout: 5 * time.Second}, NopMetrics{}, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	room.onReleased = func() {}

	// Fill the bounded peerUpdates queue so the next fan-out write parks (the room is
	// not started, so nothing drains it).
	for i := 0; i < cap(room.peerUpdates); i++ {
		room.peerUpdates <- peerUpdate{}
	}
	// Drive one fan-out delivery: the subscribe goroutine enters the room's handler
	// and parks on the full queue.
	close(bcast.trigger)
	<-bcast.handlerEntered
	time.Sleep(20 * time.Millisecond) // let the parked write settle into its select

	done := make(chan struct{})
	go func() { room.teardown(nil); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("teardown deadlocked: cancelSub waited on a subscribe goroutine parked in the fan-out write (FR-009)")
	}
}
