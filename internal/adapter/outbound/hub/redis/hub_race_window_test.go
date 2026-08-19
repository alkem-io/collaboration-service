package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	yhub "github.com/antst/go-yjs/backend/hub"
	goredis "github.com/redis/go-redis/v9"
)

// blockingSubscribeClient holds Subscribe open until the test releases it,
// putting a goroutine deterministically inside the window where the pump has
// been started but not yet installed.
//
// No production seam is needed for this: the hub already takes its Redis client
// as an interface, so the window can be widened from the outside. That matters —
// a test hook in production code to prove a lock-window bug would be a change
// nobody can justify at review time.
type blockingSubscribeClient struct {
	inner   client
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingSubscribeClient) Publish(ctx context.Context, channel string, message any) *goredis.IntCmd {
	return c.inner.Publish(ctx, channel, message)
}

func (c *blockingSubscribeClient) Subscribe(ctx context.Context, channels ...string) *goredis.PubSub {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.inner.Subscribe(ctx, channels...)
}

func (c *blockingSubscribeClient) Close() error { return c.inner.Close() }

// TestSubscriberClosingInsideThePumpStartWindowLeavesNoPump is the deterministic
// proof for the leak found in adversarial review (T067 finding 2).
//
// Subscribe registers the subscriber, starts the document's Redis subscription
// OFF the lock (it does I/O), then installs it. This test parks a goroutine in
// exactly that window and closes the subscription while it is there.
//
// Before the fix, removeSubscriber found no pump to decrement — there was none
// yet — and the install then set refs=1 for a subscriber already gone. refs never
// returns to zero afterwards, so no later Close can tear it down either: the pod
// holds that Redis subscription and its goroutine for the rest of its life, and
// the only symptom is connection count climbing on the Redis side.
//
// A 600-iteration concurrent stress test never reproduced this; the window is
// only as wide as one client.Subscribe call. Widening it from the outside is what
// makes the guarantee provable rather than merely argued.
//
// Non-vacuity: restore `p.refs = 1` (and drop the live==0 branch) and this fails
// with one pump left behind.
func TestSubscriberClosingInsideThePumpStartWindowLeavesNoPump(t *testing.T) {
	srv := miniredis.RunT(t)
	blocking := &blockingSubscribeClient{
		inner:   goredis.NewClient(&goredis.Options{Addr: srv.Addr()}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	h := NewWithClient(blocking, "instance-under-test")
	t.Cleanup(func() { _ = h.Close() })

	subCh := make(chan yhub.Subscription, 1)
	errCh := make(chan error, 1)
	go func() {
		s, err := h.Subscribe(context.Background(), "doc", "src", func(context.Context, yhub.Message) error { return nil })
		if err != nil {
			errCh <- err
			return
		}
		subCh <- s
	}()

	// Wait until Subscribe is inside the window: the subscriber is registered,
	// the pump is being started, and nothing is installed yet.
	select {
	case <-blocking.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe never reached the pump-start window")
	}

	h.mu.Lock()
	registered := len(h.subs["doc"])
	pumps := len(h.pumps)
	h.mu.Unlock()
	if registered != 1 || pumps != 0 {
		t.Fatalf("precondition: expected the subscriber registered (%d) and no pump installed (%d)", registered, pumps)
	}

	// THE RACE: close the only subscriber while the pump is still starting. This
	// is what a room torn down mid-materialization does — a shutdown, or the purge
	// tombstone refusing it.
	h.removeSubscriber("doc", 1)

	close(blocking.release)

	select {
	case err := <-errCh:
		t.Fatalf("Subscribe failed: %v", err)
	case <-subCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe never returned")
	}

	h.mu.Lock()
	leftPumps, leftSubs := len(h.pumps), len(h.subs)
	h.mu.Unlock()

	if leftSubs != 0 {
		t.Fatalf("%d documents still have subscribers after the only one was closed", leftSubs)
	}
	if leftPumps != 0 {
		t.Fatalf("a Redis subscription was installed for a document with NO subscribers (%d pumps); refs can never reach zero from here, so nothing will ever tear it down and the pod holds that subscription and its goroutine for life", leftPumps)
	}
}
