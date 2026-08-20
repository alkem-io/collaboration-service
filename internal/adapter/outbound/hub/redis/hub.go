// Package redis implements the core's hub.Hub over Redis pub/sub: the multi-pod
// fan-out path.
//
// It is a NATIVE implementation of the contract, not a wrapper around a
// pre-existing broadcaster — it speaks Redis directly and satisfies
// conformance.Hub, which injects reordering, duplication and redelivery.
package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/antst/go-yjs/backend"
	yhub "github.com/antst/go-yjs/backend/hub"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Channel prefixes. Durable document updates and ephemeral awareness travel on
// SEPARATE channels: they have different recovery semantics, and keeping them
// apart on the wire means a subscriber can never mistake one for the other and
// route awareness into durable storage (FR-009).
const (
	documentChannelPrefix  = "doc:"
	awarenessChannelPrefix = "awareness:"
)

// client is the slice of *goredis.Client this hub uses, narrowed so the wiring
// is testable without a server and the adapter is not coupled to the full
// client surface.
type client interface {
	Publish(ctx context.Context, channel string, message any) *goredis.IntCmd
	Subscribe(ctx context.Context, channels ...string) pubsubConn
	Close() error
}

// pubsubConn is the slice of *goredis.PubSub this hub uses. Subscribe returns it
// as an interface rather than the concrete type so the readiness handshake — how
// many confirmations are awaited, what happens to a message that arrives between
// them, what a keep-alive counts as — is reachable from a test. Those are the
// parts a real server produces only occasionally, or not at all.
type pubsubConn interface {
	ReceiveTimeout(ctx context.Context, timeout time.Duration) (any, error)
	Channel(opts ...goredis.ChannelOption) <-chan *goredis.Message
	Close() error
}

// goredisClient adapts *goredis.Client to client. The wrapper exists because
// Client.Subscribe returns the CONCRETE *goredis.PubSub, so the real type cannot
// satisfy an interface whose Subscribe returns an interface.
type goredisClient struct{ *goredis.Client }

// Subscribe opens a pub/sub connection and returns it as the narrow pubsubConn.
func (c goredisClient) Subscribe(ctx context.Context, channels ...string) pubsubConn {
	return c.Client.Subscribe(ctx, channels...)
}

// Hub fans document and awareness messages across pods over Redis pub/sub.
//
// DELIVERY MODEL, which the contract's backpressure obligation forces. Local
// subscribers are delivered to SYNCHRONOUSLY inside Publish, so a handler's
// error reaches the publisher and a slow handler backpressures the publisher
// rather than being dropped. Remote pods are reached through Redis, which is
// fire-and-forget by nature — nothing there can report a remote subscriber's
// failure, and the contract does not ask it to: Publish success means the hub
// accepted the message, not that everyone received it.
//
// The instance id is what keeps those two paths from double-delivering. Every
// message put on the wire carries it, and inbound messages carrying our own are
// dropped: they are the loopback of something already delivered locally.
type Hub struct {
	client   client
	instance string

	mu     sync.Mutex
	closed bool
	nextID uint64
	subs   map[backend.DocumentID]map[uint64]*subscription
	// pumps holds one Redis subscription per document, shared by every local
	// subscriber of that document. Redis fan-out is per channel, so a second
	// local subscriber needs no second connection.
	pumps map[backend.DocumentID]*pump
}

type subscription struct {
	hub    *Hub
	doc    backend.DocumentID
	id     uint64
	source backend.SourceID
	fn     yhub.Handler

	once sync.Once
}

// SourceID reports the identity this subscription publishes under, which the hub
// uses to suppress echoing a message back to its own sender.
func (s *subscription) SourceID() backend.SourceID { return s.source }

// Close unregisters this subscriber and, once it is the document's last, tears
// down the document's Redis subscription. Idempotent.
func (s *subscription) Close() error {
	s.once.Do(func() { s.hub.removeSubscriber(s.doc, s.id) })
	return nil
}

// pump owns one document's Redis subscription and the goroutine draining it.
type pump struct {
	pubsub pubsubConn
	cancel context.CancelFunc
	done   chan struct{}
	refs   int
}

// New constructs a Hub from a redis:// URL. instance identifies this process for
// loopback suppression; an empty value gets a generated one (uniqueness, not
// stability across restarts, is what matters).
func New(url, instance string) (*Hub, error) {
	opts, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	return NewWithClient(goredisClient{goredis.NewClient(opts)}, instance), nil
}

// NewWithClient constructs a Hub over an existing client (tests, and callers
// that own the connection).
func NewWithClient(c client, instance string) *Hub {
	if instance == "" {
		instance = uuid.NewString()
	}
	return &Hub{
		client:   c,
		instance: instance,
		subs:     map[backend.DocumentID]map[uint64]*subscription{},
		pumps:    map[backend.DocumentID]*pump{},
	}
}

// Subscribe registers a local handler for a document and ensures this pod is
// subscribed to the document's Redis channels.
func (h *Hub) Subscribe(ctx context.Context, doc backend.DocumentID, source backend.SourceID, fn yhub.Handler) (yhub.Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if doc == "" || fn == nil {
		return nil, yhub.ErrInvalidMessage
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, yhub.ErrClosed
	}
	h.nextID++
	sub := &subscription{hub: h, doc: doc, id: h.nextID, source: source, fn: fn}
	if h.subs[doc] == nil {
		h.subs[doc] = map[uint64]*subscription{}
	}
	h.subs[doc][sub.id] = sub

	existing := h.pumps[doc]
	if existing != nil {
		existing.refs++
		h.mu.Unlock()
		return sub, nil
	}
	h.mu.Unlock()

	// Start the document's Redis subscription off the lock: it does I/O and waits
	// for the server's confirmation, and holding the hub lock across that would
	// stall every other document.
	p, err := h.startPump(ctx, doc)
	if err != nil {
		// No subscription, so there is nothing to hand back. Drop the entry made
		// above, or the document keeps a subscriber that will never receive
		// anything and whose presence keeps a later pump's refs above zero.
		h.removeSubscriber(doc, sub.id)
		return nil, err
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		p.stop()
		return nil, yhub.ErrClosed
	}
	// refs is DERIVED from the live subscriber count, never assumed.
	//
	// startPump runs off the lock (it does I/O), and in that window this
	// subscriber may already have been closed — removeSubscriber found no pump to
	// decrement, because there was none yet. Installing a pump with refs=1 then
	// registers a Redis subscription whose only subscriber is gone, and because
	// refs never reaches zero afterwards no later Close can tear it down either:
	// the pod holds that subscription and its goroutine for the rest of its life.
	//
	// Counting the map is also what removeSubscriber's arithmetic already assumes,
	// so deriving it here makes the invariant hold by construction rather than by
	// the two sites happening to agree.
	live := len(h.subs[doc])

	if raced := h.pumps[doc]; raced != nil {
		// Another Subscribe for the same document won; keep theirs.
		raced.refs = live
		h.mu.Unlock()
		p.stop()
		if live == 0 {
			h.removeSubscriber(doc, sub.id)
		}
		return sub, nil
	}
	if live == 0 {
		// Every subscriber for this document went away while the pump was starting.
		h.mu.Unlock()
		p.stop()
		return sub, nil
	}
	p.refs = live
	h.pumps[doc] = p
	h.mu.Unlock()
	return sub, nil
}

// Publish delivers to local subscribers synchronously and forwards to Redis for
// remote pods.
//
// Local first, and its error wins: a local handler that fails or blocks is real
// backpressure the publisher must see, and the contract forbids silently
// discarding a message because a local queue is full. The Redis publish is
// attempted regardless — a local failure says nothing about the other pods, and
// withholding the message from them would turn one pod's problem into divergence
// across the cluster.
func (h *Hub) Publish(ctx context.Context, msg yhub.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if msg.DocumentID == "" || (msg.Kind != yhub.DocumentUpdate && msg.Kind != yhub.AwarenessUpdate) {
		return yhub.ErrInvalidMessage
	}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return yhub.ErrClosed
	}
	targets := make([]*subscription, 0, len(h.subs[msg.DocumentID]))
	for _, s := range h.subs[msg.DocumentID] {
		if s.source != msg.SourceID {
			targets = append(targets, s)
		}
	}
	h.mu.Unlock()

	localErr := deliver(ctx, targets, msg)

	if err := h.client.Publish(ctx, channelFor(msg.DocumentID, msg.Kind), encode(h.instance, msg.SourceID, msg.Payload)).Err(); err != nil {
		if localErr != nil {
			return localErr
		}
		return fmt.Errorf("redis publish: %w", err)
	}
	return localErr
}

// deliver invokes each handler with its OWN copy of the payload. The contract
// says a Handler owns what it receives and may retain it, while Publish only
// borrows the caller's slice — so handing the same backing array to two handlers,
// or retaining the caller's, would let one subscriber observe another's mutation.
func deliver(ctx context.Context, targets []*subscription, msg yhub.Message) error {
	var first error
	for _, s := range targets {
		owned := yhub.Message{
			DocumentID: msg.DocumentID,
			SourceID:   msg.SourceID,
			Kind:       msg.Kind,
			Payload:    append([]byte(nil), msg.Payload...),
		}
		if err := s.fn(ctx, owned); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Close tears down every pump and rejects future use.
func (h *Hub) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	pumps := make([]*pump, 0, len(h.pumps))
	for _, p := range h.pumps {
		pumps = append(pumps, p)
	}
	h.pumps = map[backend.DocumentID]*pump{}
	h.subs = map[backend.DocumentID]map[uint64]*subscription{}
	h.mu.Unlock()

	for _, p := range pumps {
		p.stop()
	}
	return h.client.Close()
}

// removeSubscriber drops one local subscriber and stops the document's pump once
// the last one goes.
func (h *Hub) removeSubscriber(doc backend.DocumentID, id uint64) {
	h.mu.Lock()
	subs := h.subs[doc]
	if subs == nil {
		h.mu.Unlock()
		return
	}
	if _, ok := subs[id]; !ok {
		h.mu.Unlock()
		return
	}
	delete(subs, id)
	if len(subs) == 0 {
		delete(h.subs, doc)
	}
	var stopping *pump
	if p := h.pumps[doc]; p != nil {
		p.refs--
		if p.refs <= 0 {
			stopping = p
			delete(h.pumps, doc)
		}
	}
	h.mu.Unlock()

	if stopping != nil {
		stopping.stop()
	}
}

// startPump subscribes to the document's two channels and drains them.
// subscribeConfirmTimeout bounds the wait for Redis to confirm a subscription. It
// is a network round-trip to a server this pod already has a connection to, so
// silence means something is wrong rather than slow — and a join must not hang on
// it indefinitely.
const subscribeConfirmTimeout = 10 * time.Second

// startPump opens this document's Redis subscription and returns only once the
// server has CONFIRMED it.
//
// Both halves of that matter, and neither is free with go-redis:
//
//   - Client.Subscribe discards the error from pubsub.Subscribe (`_ = ...`), so a
//     subscription that failed outright is indistinguishable from one that worked.
//     The pump would sit on a PubSub that never delivers anything, for the life of
//     the document.
//   - Even when it succeeds, pubsub.Subscribe has only WRITTEN the SUBSCRIBE
//     command. The server may not have registered it yet. A publish issued right
//     after Subscribe returns can be processed first, and Redis pub/sub has no
//     replay or backlog: that message is gone permanently. Cross-pod, that is a
//     document update or an awareness state that simply never arrives, and no
//     amount of waiting on the receiving side recovers it.
//
// Reading the confirmations fixes both: it is the only place the discarded error
// resurfaces, and it establishes a happens-before between "Subscribe returned" and
// "the server will deliver".
func (h *Hub) startPump(ctx context.Context, doc backend.DocumentID) (*pump, error) {
	pumpCtx, cancel := context.WithCancel(context.Background())
	// One list, used for both the SUBSCRIBE and the count of confirmations to wait
	// for. Document and awareness are separate subscriptions, confirmed one at a
	// time and each able to fail on its own; deriving the count from the same slice
	// makes "confirm every channel we subscribed" true by construction rather than
	// by two numbers agreeing.
	channels := []string{
		documentChannelPrefix + string(doc),
		awarenessChannelPrefix + string(doc),
	}
	pubsub := h.client.Subscribe(pumpCtx, channels...)

	early, err := awaitSubscribed(ctx, pubsub, len(channels))
	if err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, err
	}

	p := &pump{pubsub: pubsub, cancel: cancel, done: make(chan struct{})}
	ch := pubsub.Channel()
	go func() {
		defer close(p.done)
		// Anything that arrived between the two confirmations was consumed by the
		// wait above and will not be redelivered by Channel(); dispatch it first or
		// it is dropped, which is the very loss this function exists to prevent.
		for _, m := range early {
			h.dispatchRemote(pumpCtx, doc, m)
		}
		for {
			select {
			case <-pumpCtx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				h.dispatchRemote(pumpCtx, doc, m)
			}
		}
	}()
	return p, nil
}

// awaitSubscribed reads want subscription confirmations, returning any real
// messages that arrived among them.
//
// Interleaving is possible: SUBSCRIBE with two channels is confirmed one channel
// at a time, and a publish on the first can land before the second is confirmed.
// Those messages are already off the connection, so they are handed back rather
// than discarded.
func awaitSubscribed(ctx context.Context, ps pubsubConn, want int) ([]*goredis.Message, error) {
	var early []*goredis.Message
	for confirmed := 0; confirmed < want; {
		raw, err := ps.ReceiveTimeout(ctx, subscribeConfirmTimeout)
		if err != nil {
			return nil, fmt.Errorf("redis hub: subscription not confirmed: %w", err)
		}
		switch m := raw.(type) {
		case *goredis.Subscription:
			confirmed++
		case *goredis.Message:
			early = append(early, m)
		case *goredis.Pong:
			// Keep-alive; not a confirmation and not data.
		default:
			return nil, fmt.Errorf("redis hub: unexpected %T while waiting for a subscription confirmation", raw)
		}
	}
	return early, nil
}

// dispatchRemote routes one inbound Redis message to local subscribers.
func (h *Hub) dispatchRemote(ctx context.Context, doc backend.DocumentID, m *goredis.Message) {
	instance, source, payload, ok := decode([]byte(m.Payload))
	if !ok {
		return // a frame this hub did not write; ignore rather than guess
	}
	if instance == h.instance {
		// Our own publish looping back. It was already delivered to local
		// subscribers synchronously inside Publish; delivering again here would
		// double-apply it for every local subscriber whose source id differs from
		// the publisher's.
		return
	}

	kind := yhub.DocumentUpdate
	if len(m.Channel) >= len(awarenessChannelPrefix) && m.Channel[:len(awarenessChannelPrefix)] == awarenessChannelPrefix {
		kind = yhub.AwarenessUpdate
	}

	h.mu.Lock()
	targets := make([]*subscription, 0, len(h.subs[doc]))
	for _, s := range h.subs[doc] {
		if s.source != source {
			targets = append(targets, s)
		}
	}
	h.mu.Unlock()

	// A remote handler error has nowhere to go: the publisher is in another
	// process. It is deliberately not swallowed into a drop — the message WAS
	// delivered; the subscriber failed to use it, which is the subscriber's own
	// concern and recovers through state-vector catch-up.
	_ = deliver(ctx, targets, yhub.Message{DocumentID: doc, SourceID: source, Kind: kind, Payload: payload})
}

func (p *pump) stop() {
	p.cancel()
	_ = p.pubsub.Close()
	<-p.done
}

func channelFor(doc backend.DocumentID, kind yhub.MessageKind) string {
	if kind == yhub.AwarenessUpdate {
		return awarenessChannelPrefix + string(doc)
	}
	return documentChannelPrefix + string(doc)
}

// maxIDLen bounds the two length-prefixed header fields. Both are process-local
// identifiers — a UUID and a room's source id — so the cap is far above any real
// value; it exists so the uint32 length prefix cannot be handed something it
// cannot represent, rather than to constrain callers.
const maxIDLen = 1 << 16

// encode frames [instanceLen][instance][sourceLen][source][payload].
//
// Length-prefixed rather than delimited because a source id is caller-supplied
// and could contain any byte; a delimiter would let one subscriber's id forge
// another's.
func encode(instance string, source backend.SourceID, payload []byte) []byte {
	if len(instance) > maxIDLen {
		instance = instance[:maxIDLen]
	}
	if len(source) > maxIDLen {
		source = source[:maxIDLen]
	}
	out := make([]byte, 0, 8+len(instance)+len(source)+len(payload))
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(instance))) //nolint:gosec // bounded by maxIDLen above
	out = append(out, n[:]...)
	out = append(out, instance...)
	binary.BigEndian.PutUint32(n[:], uint32(len(source))) //nolint:gosec // bounded by maxIDLen above
	out = append(out, n[:]...)
	out = append(out, source...)
	return append(out, payload...)
}

func decode(frame []byte) (instance string, source backend.SourceID, payload []byte, ok bool) {
	if len(frame) < 4 {
		return "", "", nil, false
	}
	instLen := int(binary.BigEndian.Uint32(frame[:4]))
	if instLen < 0 || len(frame) < 4+instLen+4 {
		return "", "", nil, false
	}
	instance = string(frame[4 : 4+instLen])
	rest := frame[4+instLen:]
	srcLen := int(binary.BigEndian.Uint32(rest[:4]))
	if srcLen < 0 || len(rest) < 4+srcLen {
		return "", "", nil, false
	}
	source = backend.SourceID(rest[4 : 4+srcLen])
	payload = append([]byte(nil), rest[4+srcLen:]...)
	return instance, source, payload, true
}

var (
	_ yhub.Hub          = (*Hub)(nil)
	_ yhub.Subscription = (*subscription)(nil)
)
