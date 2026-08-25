package redis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/antst/go-yjs/backend/conformance"
	yhub "github.com/antst/go-yjs/backend/hub"
	goredis "github.com/redis/go-redis/v9"
)

// TestHubConformance runs the core's fan-out contract against the Redis
// implementation (T050, SC-007).
//
// The suite is the point of running it here rather than trusting the in-process
// result: it deliberately injects the conditions a distributed hub actually meets
// — reordering, duplication, redelivery — and pins the three obligations that are
// easy to get wrong over pub/sub. Echo suppression by SourceID, because Redis has
// no per-publisher filtering and a pod would otherwise re-apply its own update.
// Payload ownership, because a handler may retain what it receives while Publish
// only borrows. And backpressure being OBSERVABLE, which is the obligation that
// shapes this implementation: local subscribers are delivered to synchronously
// inside Publish precisely so a handler error reaches the publisher instead of
// vanishing into a fire-and-forget send.
func TestHubConformance(t *testing.T) {
	conformance.Hub(t, func() yhub.Hub {
		srv := miniredis.RunT(t)
		return NewWithClient(goredisClient{goredis.NewClient(&goredis.Options{Addr: srv.Addr()})}, "instance-under-test")
	})
}
