package inmemory

import (
	"context"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// TestSinglePodBroadcasterIsInertButUniform pins the single-pod default, which
// had no test at all despite being the broadcaster every non-Redis deployment
// runs.
//
// "It does nothing" is exactly why it needs one. The room treats single-pod and
// multi-pod identically — it publishes every local edit and subscribes on every
// materialization — so this adapter's contract is that doing nothing must still
// be SAFE to call on those paths: Publish must not error (an error would surface
// to clients as a fan-out failure on a pod that has no peers), Subscribe must
// return a usable cancel (a nil one would panic in room teardown), and the
// handler must never be invoked (there is no peer, so any invocation would be the
// room re-applying its own edit).
func TestSinglePodBroadcasterIsInertButUniform(t *testing.T) {
	b := New()
	ctx := context.Background()

	if err := b.Publish(ctx, "doc", []byte("update"), false); err != nil {
		t.Fatalf("Publish on a single pod must succeed: %v — an error here surfaces to clients as a fan-out failure on a pod with no peers", err)
	}
	if err := b.Publish(ctx, "doc", []byte("awareness"), true); err != nil {
		t.Fatalf("Publish (ephemeral) on a single pod must succeed: %v", err)
	}

	called := false
	cancel, err := b.Subscribe(ctx, "doc", func([]byte, bool) { called = true })
	if err != nil {
		t.Fatalf("Subscribe on a single pod must succeed: %v", err)
	}
	if cancel == nil {
		t.Fatal("Subscribe must return a usable cancel; room teardown calls it unconditionally and would panic on nil")
	}
	cancel()
	cancel() // teardown can run twice; a no-op cancel must tolerate it

	if called {
		t.Fatal("the handler was invoked on a single-pod broadcaster; there is no peer, so any delivery would be the room re-applying its own edit")
	}
}

// TestSatisfiesTheClusterBroadcasterPort is the compile-time assertion made
// executable, so a port change that this adapter stops satisfying fails a test
// rather than only a build somewhere else.
func TestSatisfiesTheClusterBroadcasterPort(_ *testing.T) {
	var _ port.ClusterBroadcaster = New()
}
