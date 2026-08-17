package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// blockingSubBroadcaster mimics a redis-style fan-out where cancelSub (pubsub.Close)
// BLOCKS until the subscribe goroutine exits. The subscribe goroutine calls the
// room's handler exactly once (when triggered); if that handler parks because the
// decoupled peerUpdates queue is full, cancelSub can only return once teardown
// cancels the room context and frees the parked write — the property under test.
type blockingSubBroadcaster struct {
	trigger        chan struct{}
	handlerEntered chan struct{}
	goroutineDone  chan struct{}
}

func (b *blockingSubBroadcaster) Publish(context.Context, model.DocumentID, []byte, bool) error {
	return nil
}

func (b *blockingSubBroadcaster) Subscribe(_ context.Context, _ model.DocumentID, handler func(payload []byte, ephemeral bool)) (func(), error) {
	b.trigger = make(chan struct{})
	b.handlerEntered = make(chan struct{})
	b.goroutineDone = make(chan struct{})
	go func() {
		defer close(b.goroutineDone)
		<-b.trigger
		close(b.handlerEntered)
		handler([]byte("peer-payload"), false) // may park on the full peerUpdates queue
	}()
	// cancelSub WAITS for the subscribe goroutine to exit (redis pubsub.Close semantics).
	return func() { <-b.goroutineDone }, nil
}

// TestInvTeardownNoDeadlock — INV-TEARDOWN-NODEADLOCK (spec 002 FR-009). A subscribe
// goroutine parked writing a peer update must not deadlock teardown: teardown cancels
// the room context (freeing the parked decoupled write) BEFORE cancelSub, which for
// redis waits for that goroutine. Reverting the teardown order (cancelSub before the
// ctx cancel) deadlocks here — cancelSub waits on a goroutine that only the ctx
// cancel can free.
func TestInvTeardownNoDeadlock(t *testing.T) {
	deps := newTestDeps()
	bcast := &blockingSubBroadcaster{}
	deps.Broadcaster = bcast

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
