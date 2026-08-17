package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// blockingManager hangs in Purge until the per-delivery context is cancelled,
// simulating a wedged backend call. It records whether the context's deadline
// (not an external cancel) is what released it.
type blockingManager struct {
	mu       sync.Mutex
	released bool
	ctxErr   error
}

func (b *blockingManager) Purge(ctx context.Context, _ model.DocumentID) error {
	<-ctx.Done() // block until the consumer-imposed deadline cancels us.
	b.mu.Lock()
	b.released = true
	b.ctxErr = ctx.Err()
	b.mu.Unlock()
	return ctx.Err()
}

func (b *blockingManager) ReEvaluate(_ context.Context, _ model.DocumentID) {}

func (b *blockingManager) PreRegister(_ context.Context, _ model.Metadata) error { return nil }

// TestHandleDeliveryBoundsAStuckHandler defends the per-delivery timeout context
// (conn.go handleDelivery). The consumer drains deliveries serially on a single
// goroutine, so a handler that blocks forever head-of-line-blocks every later
// lifecycle event. handleDelivery wraps handle in a WithTimeout(handlerTimeout)
// context, so a wedged Purge is cancelled and returns (surfacing as nackRequeue)
// rather than freezing the consumer.
//
// Non-vacuity: revert handleDelivery's body to `return c.handle(context.Background(),
// body)` (the pre-fix unbounded context) and this test hangs — the watchdog fires
// "handleDelivery did not return: a stuck handler is head-of-line-blocking the
// consumer (unbounded context)" — because context.Background() never cancels, so
// blockingManager.Purge blocks forever.
func TestHandleDeliveryBoundsAStuckHandler(t *testing.T) {
	mgr := &blockingManager{}
	c := &Consumer{mgr: mgr, logger: zap.NewNop(), handlerTimeout: 50 * time.Millisecond}

	done := make(chan ackAction, 1)
	start := time.Now()
	go func() {
		done <- c.handleDelivery(eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "wedged"}))
	}()

	select {
	case action := <-done:
		elapsed := time.Since(start)
		// The deadline must be what released the handler, and it must surface as a
		// requeue (a cancelled cascade is a transient failure worth redelivering).
		if action != nackRequeue {
			t.Fatalf("handleDelivery on a timed-out handler = %v, want nackRequeue", action)
		}
		mgr.mu.Lock()
		released, ctxErr := mgr.released, mgr.ctxErr
		mgr.mu.Unlock()
		if !released || !errors.Is(ctxErr, context.DeadlineExceeded) {
			t.Fatalf("handler released=%v ctxErr=%v, want released by context.DeadlineExceeded", released, ctxErr)
		}
		// Bounded: it returned around handlerTimeout, not instantly and not forever.
		if elapsed > time.Second {
			t.Fatalf("handleDelivery took %v, want ~handlerTimeout (50ms)", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handleDelivery did not return: a stuck handler is head-of-line-blocking the consumer (unbounded context)")
	}
}

// TestResolveHandlerTimeoutDefaults asserts a zero or negative configured timeout
// resolves to the positive DefaultHandlerTimeout, while an explicit positive value
// passes through. A non-positive deadline must never reach context.WithTimeout,
// which reads zero as "already expired" and would cancel every handler instantly.
func TestResolveHandlerTimeoutDefaults(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want time.Duration
	}{
		{0, DefaultHandlerTimeout},
		{-1, DefaultHandlerTimeout},
		{5 * time.Second, 5 * time.Second},
	} {
		if got := resolveHandlerTimeout(tc.in); got != tc.want {
			t.Errorf("resolveHandlerTimeout(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestResolvePrefetchDefaults asserts a zero or negative configured prefetch
// resolves to the positive DefaultPrefetch (AMQP reads a 0 prefetch as
// "unlimited" — the unbounded backlog the QoS guards against), while an explicit
// positive value passes through.
func TestResolvePrefetchDefaults(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want int
	}{
		{0, DefaultPrefetch},
		{-1, DefaultPrefetch},
		{4, 4},
	} {
		if got := resolvePrefetch(tc.in); got != tc.want {
			t.Errorf("resolvePrefetch(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
