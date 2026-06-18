// Package redis is the multi-pod ClusterBroadcaster (port.ClusterBroadcaster):
// document updates are published on the doc:{id} channel and ephemeral/awareness
// frames on awareness:{id}, so clients connected to any pod converge
// transparently (R4, SC-007/SC-011). Each pod tags its publishes with a unique
// source id and drops frames carrying its own id, so a pod never double-applies
// its own update echoed back by Redis pub/sub.
package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// channel-name prefixes (contracts/ws-protocol.md; research.md encoding table).
const (
	docChannelPrefix       = "doc:"
	awarenessChannelPrefix = "awareness:"
)

// Broadcaster fans document and ephemeral/awareness frames across pods over
// Redis pub/sub. It is safe for concurrent use.
type Broadcaster struct {
	client redisClient
	// source uniquely identifies this pod so it can drop its own echoed
	// publishes (Redis pub/sub has no per-publisher filtering).
	source string

	mu   sync.Mutex
	subs map[model.DocumentID]int // live subscriptions per document (observability/tests)
}

// redisClient is the subset of *goredis.Client the broadcaster uses, narrowed so
// tests can assert behavior without a network and the adapter stays decoupled
// from the full client surface.
type redisClient interface {
	Publish(ctx context.Context, channel string, message any) *goredis.IntCmd
	Subscribe(ctx context.Context, channels ...string) *goredis.PubSub
	Close() error
}

// New constructs a Broadcaster from a redis:// URL (REDIS_URL). The source id is
// the pod identity used for echo suppression; pass a stable per-process value
// (a generated UUID is fine — uniqueness, not stability across restarts, is what
// matters).
func New(url, source string) (*Broadcaster, error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	if source == "" {
		source = uuid.NewString()
	}
	return newWithClient(goredis.NewClient(opts), source), nil
}

// newWithClient wires a Broadcaster over an existing client (tests, custom
// options). source must be non-empty.
func newWithClient(client redisClient, source string) *Broadcaster {
	if source == "" {
		source = uuid.NewString()
	}
	return &Broadcaster{
		client: client,
		source: source,
		subs:   make(map[model.DocumentID]int),
	}
}

// Close releases the underlying Redis client.
func (b *Broadcaster) Close() error { return b.client.Close() }

// Publish frames payload with this pod's source id and publishes it on the
// document's doc:{id} or awareness:{id} channel. The frame lets every other pod
// distinguish a peer's update from its own echo on receive.
func (b *Broadcaster) Publish(ctx context.Context, id model.DocumentID, payload []byte, ephemeral bool) error {
	frame := encodeFrame(b.source, payload)
	if err := b.client.Publish(ctx, channelName(id, ephemeral), frame).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

// Subscribe opens Redis subscriptions on both the doc:{id} and awareness:{id}
// channels for the document, dispatching each peer-pod payload to handler with
// the ephemeral flag set by the channel it arrived on. Frames carrying this
// pod's own source id are dropped (the room already applied them locally). The
// returned cancel tears the subscription down and is idempotent.
func (b *Broadcaster) Subscribe(ctx context.Context, id model.DocumentID, handler func(payload []byte, ephemeral bool)) (func(), error) {
	pubsub := b.client.Subscribe(ctx, channelName(id, false), channelName(id, true))

	// Wait for the subscription to be confirmed so a Publish that races
	// Subscribe is not silently dropped (miniredis and Redis both require an
	// active subscriber).
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}

	b.track(id, +1)

	ephemeralChannel := channelName(id, true)
	ch := pubsub.Channel()
	done := make(chan struct{})

	go func() {
		for msg := range ch {
			source, payload, ok := decodeFrame([]byte(msg.Payload))
			if !ok || source == b.source {
				continue // malformed, or our own echo — drop it.
			}
			handler(payload, msg.Channel == ephemeralChannel)
		}
		close(done)
	}()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			_ = pubsub.Close() // closes the goredis channel, ending the goroutine.
			<-done
			b.track(id, -1)
		})
	}
	return cancel, nil
}

// track adjusts the live-subscription counter for a document (observability and
// the two-pod tests; never negative).
func (b *Broadcaster) track(id model.DocumentID, delta int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.subs[id] + delta
	if n <= 0 {
		delete(b.subs, id)
		return
	}
	b.subs[id] = n
}

// subscriberCount reports this pod's live subscriptions for a document.
func (b *Broadcaster) subscriberCount(id model.DocumentID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subs[id]
}

// channelName builds the doc:{id} or awareness:{id} channel for a document.
func channelName(id model.DocumentID, ephemeral bool) string {
	if ephemeral {
		return awarenessChannelPrefix + string(id)
	}
	return docChannelPrefix + string(id)
}

// maxSourceLen bounds the framed source id (a pod id / UUID is ~36 bytes); the
// length prefix is a uint16, so a longer id is truncated defensively — it only
// affects echo suppression, never document bytes.
const maxSourceLen = 0xffff

// encodeFrame prefixes payload with the publisher's source id so receivers can
// drop their own echo: [uint16 sourceLen][source][payload].
func encodeFrame(source string, payload []byte) []byte {
	src := []byte(source)
	if len(src) > maxSourceLen {
		src = src[:maxSourceLen]
	}
	srcLen := uint16(len(src) & maxSourceLen) // masked: provably fits a uint16.
	frame := make([]byte, 2+len(src)+len(payload))
	binary.BigEndian.PutUint16(frame, srcLen)
	copy(frame[2:], src)
	copy(frame[2+len(src):], payload)
	return frame
}

// decodeFrame reverses encodeFrame, returning the source id and the inner
// payload. ok is false for a truncated/malformed frame.
func decodeFrame(frame []byte) (source string, payload []byte, ok bool) {
	if len(frame) < 2 {
		return "", nil, false
	}
	srcLen := int(binary.BigEndian.Uint16(frame))
	if len(frame) < 2+srcLen {
		return "", nil, false
	}
	return string(frame[2 : 2+srcLen]), frame[2+srcLen:], true
}

// compile-time assertion that Broadcaster satisfies the port.
var _ port.ClusterBroadcaster = (*Broadcaster)(nil)
