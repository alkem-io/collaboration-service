package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// DefaultPrefetch bounds the number of unacknowledged lifecycle deliveries the
// broker may push to this consumer at once when Config.Prefetch is unset. The
// consumer is single-threaded and manual-ack (autoAck=false): without a QoS
// prefetch the broker would stream an unbounded backlog of unacked deliveries
// into memory the instant the consumer attaches, defeating backpressure (a burst
// of lifecycle events would all be in flight at once). A small prefetch lets the
// broker hold the queue and feed events only as the consumer acks them.
const DefaultPrefetch = 1

// DefaultHandlerTimeout bounds the processing of a single lifecycle delivery when
// Config.HandlerTimeout is unset. The consumer is single-threaded, so a handler
// that blocks indefinitely — a room that will not accept the close — would
// head-of-line-block every later event. The deadline both frees the consumer and
// paces redelivery: the delivery is requeued, and it cannot be retried faster than
// this bound.
const DefaultHandlerTimeout = 30 * time.Second

// Config carries the RabbitMQ lifecycle-consumer settings.
type Config struct {
	// URL is the amqp:// connection string (shared with the metadata-store bus).
	URL string
	// Queue is the DEDICATED lifecycle queue the consumer binds — its own queue,
	// distinct from the metadata-store RPC queue (config.RabbitMQ.LifecycleQueue,
	// default alkemio-collaboration-lifecycle). It MUST NOT be the metadata-store RPC
	// queue: RabbitMQ round-robins a queue across its consumers, so sharing one
	// queue would let this consumer steal metadata-store fetch/save RPCs and drop them.
	Queue string
	// HandlerTimeout bounds the per-delivery processing context so one stuck event
	// cannot freeze the single-threaded consumer. Zero falls back to
	// DefaultHandlerTimeout.
	HandlerTimeout time.Duration
	// ReattachBackoff paces the supervisor's re-attach attempts after the broker
	// drops a connection or session, so a broker that is down is retried at a
	// bounded rate rather than spun on. Zero uses DefaultReattachBackoff.
	ReattachBackoff time.Duration
	// DepthPollInterval paces the queue-depth poll. Zero uses
	// DefaultDepthPollInterval; a negative value disables polling.
	DepthPollInterval time.Duration
	// Observer receives the consumer's operational signals. Nil means NopObserver.
	Observer Observer
	// Prefetch caps the unacked deliveries the broker pushes at once (channel QoS),
	// providing backpressure for the manual-ack, single-threaded consumer. Zero
	// falls back to DefaultPrefetch.
	Prefetch int
}

// Manager is the domain dependency the consumer drives (the concrete
// service.Manager satisfies it). Declared here as the consumer's required
// behaviour so cmd/server wires the real Manager in.
type Manager interface {
	// CloseDeleted disconnects and evicts a live room for a deleted document.
	CloseDeleted(ctx context.Context, id model.DocumentID) error
}

// brokerChannel and brokerConn are the narrow slices of amqp091-go this consumer
// actually uses, so its wiring can be exercised without a live broker.
//
// They exist because amqp091-go exposes no interfaces and ships no test helpers,
// and the RabbitMQ team declined to add them (amqp091-go#17). Without a seam the
// dial/declare/qos/consume sequence — including every failure branch that must
// close what it already opened — is reachable only against a real broker, which
// means it is not reachable in the unit lane at all.
//
// Deliberately narrow: only the calls Connect makes. A wider interface would be a
// second definition of the AMQP API rather than a description of what this
// consumer needs.
type brokerChannel interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	Close() error
}

type brokerConn interface {
	Channel() (brokerChannel, error)
	Close() error
}

// amqpConn adapts *amqp.Connection to brokerConn. The wrapper is needed because
// amqp.Connection.Channel returns the CONCRETE *amqp.Channel, so the real type
// cannot satisfy an interface whose Channel returns an interface.
type amqpConn struct{ *amqp.Connection }

// Channel opens an AMQP channel and returns it as the narrow brokerChannel.
func (c amqpConn) Channel() (brokerChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// dialBroker is the connection factory. Tests replace it to drive Connect's
// wiring and its failure branches; production always dials RabbitMQ.
var dialBroker = func(url string) (brokerConn, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpConn{conn}, nil
}

// session is one live broker attachment: a connection, a channel already wired
// and the delivery stream from it. It is the
// unit the supervisor replaces — a dropped connection invalidates all of it at
// once, so re-attaching means rebuilding the whole thing, not patching a part.
type session struct {
	conn       brokerConn
	ch         brokerChannel
	deliveries <-chan amqp.Delivery
}

// openSession dials the broker and brings one attachment all the way up:
// topology, QoS, consume. Every failure closes what it opened.
func openSession(cfg Config, names queueNames) (*session, error) {
	conn, err := dialBroker(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	fail := func(err error) (*session, error) {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if err := declareTopology(ch, names); err != nil {
		return fail(fmt.Errorf("declare lifecycle topology: %w", err))
	}
	// Bound the unacked-delivery window BEFORE consuming (channel QoS). With manual
	// ack and a single-threaded consume loop, an unset prefetch lets the broker
	// stream the entire backlog into memory at once — no backpressure. A bounded
	// prefetch keeps the queue on the broker and feeds events only as they are
	// acked. Must precede Consume to take effect on the first deliveries.
	if err := ch.Qos(resolvePrefetch(cfg.Prefetch), 0, false); err != nil {
		return fail(fmt.Errorf("set lifecycle prefetch: %w", err))
	}
	// Manual ack (autoAck=false): a document.deleted is acknowledged only AFTER the
	// room is closed, so a crash between delivery and completion redelivers it
	// rather than dropping it. Auto-ack is at-most-once, and a lost delete leaves a
	// live room serving a document the owner removed.
	deliveries, err := ch.Consume(names.main, "", false, false, false, false, nil)
	if err != nil {
		return fail(fmt.Errorf("consume lifecycle queue: %w", err))
	}
	return &session{
		conn:       conn,
		ch:         ch,
		deliveries: deliveries,
	}, nil
}

// Connect dials RabbitMQ, declares the lifecycle topology, starts consuming, and
// routes each event to the Manager. Close it on shutdown.
//
// The first attachment must succeed: a misconfigured URL or an unreachable broker
// is a startup error, not something to retry into. After that the consumer
// supervises itself — a dropped connection or a broker restart is followed by
// re-attaching on a bounded backoff, indefinitely, until Close.
func Connect(cfg Config, mgr Manager, logger *zap.Logger) (*Consumer, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("lifecycle consumer: URL is required")
	}
	if cfg.Queue == "" {
		return nil, fmt.Errorf("lifecycle consumer: Queue is required")
	}
	names := namesFor(cfg.Queue)
	sess, err := openSession(cfg, names)
	if err != nil {
		return nil, fmt.Errorf("lifecycle consumer: %w", err)
	}

	obs := cfg.Observer
	if obs == nil {
		obs = NopObserver{}
	}
	c := &Consumer{
		mgr:             mgr,
		logger:          logger,
		obs:             obs,
		cfg:             cfg,
		handlerTimeout:  resolveHandlerTimeout(cfg.HandlerTimeout),
		names:           names,
		reattachBackoff: resolveReattachBackoff(cfg.ReattachBackoff),
		closed:          make(chan struct{}),
	}
	c.adopt(sess)
	go c.run(sess)
	if every := resolveDepthPollInterval(cfg.DepthPollInterval); every > 0 {
		go c.pollDepths(every)
	}
	return c, nil
}

// DefaultDepthPollInterval paces the queue-depth poll. Depth is a slow-moving
// level, and each poll is one cheap RPC per queue, so this only needs to be fast
// enough that a scrape never sees a stale level for long.
const DefaultDepthPollInterval = 30 * time.Second

// resolveDepthPollInterval maps a configured interval to its effective value:
// zero takes the default, negative disables polling entirely.
func resolveDepthPollInterval(d time.Duration) time.Duration {
	if d == 0 {
		return DefaultDepthPollInterval
	}
	return d
}

// pollDepths publishes each queue's READY message count until the consumer is
// closed. See lifecycle.Observer for what "ready" excludes.
//
// The re-declare is load-bearing beyond the count: it recreates any queue in the
// topology that has gone missing. That matters most for the DLQ — the main queue
// dead-letters into it, and while it is absent a rejected message parks in a state
// that is neither ready nor unacknowledged, invisible to the count below.
//
// The depth comes from re-declaring the queue rather than from a passive declare
// or the management API. An equivalent re-declaration is a no-op that returns the
// current count, and the arguments come from the same topologyFor list used to
// declare in the first place, so it cannot drift into an inequivalent declaration.
// A passive declare would take the channel down whenever a queue was missing —
// exactly the situation worth reporting rather than dying on.
func (c *Consumer) pollDepths(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
		}
		ch := c.live()
		if ch == nil {
			continue
		}
		for _, q := range topologyFor(c.names) {
			info, err := ch.QueueDeclare(q.name, true, false, false, false, q.args)
			if err != nil {
				// The channel is likely dead; the supervisor re-attaches when the
				// delivery stream ends. Report and give up on this round rather than
				// hammering a broken channel with the remaining queues.
				c.logger.Warn("lifecycle queue depth poll failed",
					zap.String("queue", q.name), zap.Error(err))
				break
			}
			// amqp.Queue.Messages is the READY count, not the total.
			c.obs.QueueReadyDepth(q.name, info.Messages)
		}
	}
}

// adopt installs a session as the live attachment.
func (c *Consumer) adopt(s *session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn, c.ch = s.conn, s.ch
}

// live reads the current attachment. The lock is for Close, which runs on the
// caller's goroutine; everything else touching it shares the run goroutine.
func (c *Consumer) live() brokerChannel {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch
}

// run is the supervisor. consume returns when the delivery stream ends — the
// broker closed the connection, or the channel faulted. Without this loop the
// first such failure would be terminal: the consume goroutine would exit and
// lifecycle events would stop being processed, silently and permanently, while
// the process stayed healthy in every other respect.
func (c *Consumer) run(sess *session) {
	for {
		c.consume(sess.deliveries)
		// Anything still open on the dead session is released before re-attaching,
		// so a broker-side channel fault does not leak the connection under it.
		_ = sess.ch.Close()
		_ = sess.conn.Close()

		next := c.reattach()
		if next == nil {
			return // Close was called.
		}
		sess = next
	}
}

// reattach re-opens a session, retrying on a bounded backoff until it succeeds or
// the consumer is closed (in which case it returns nil). It retries forever by
// design: the alternative is a live process that has silently stopped reacting to
// owner-delete events.
func (c *Consumer) reattach() *session {
	for {
		select {
		case <-c.closed:
			return nil
		case <-time.After(c.reattachBackoff):
		}
		sess, err := openSession(c.cfg, c.names)
		if err != nil {
			c.logger.Error("lifecycle consumer could not re-attach to the broker; retrying",
				zap.Duration("backoff", c.reattachBackoff), zap.Error(err))
			continue
		}
		c.adopt(sess)
		c.logger.Info("lifecycle consumer re-attached to the broker")
		return sess
	}
}

// DefaultReattachBackoff paces the supervisor's re-attach attempts after the
// broker drops a connection or session.
const DefaultReattachBackoff = 5 * time.Second

func resolveReattachBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultReattachBackoff
	}
	return d
}

// resolveHandlerTimeout maps a configured per-delivery timeout to its effective
// value: a zero or negative timeout falls back to DefaultHandlerTimeout. A
// non-positive deadline must never reach context.WithTimeout — zero is treated as
// "already expired" there, which would cancel every handler instantly.
func resolveHandlerTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultHandlerTimeout
	}
	return d
}

// resolvePrefetch maps a configured QoS prefetch to its effective value: a zero or
// negative prefetch falls back to DefaultPrefetch. A zero prefetch must never reach
// channel.Qos — AMQP reads 0 as "unlimited", the exact unbounded backlog this
// guards against.
func resolvePrefetch(n int) int {
	if n <= 0 {
		return DefaultPrefetch
	}
	return n
}

// consume routes each delivery through processOne until the delivery stream ends,
// which is how every attachment dies and where the supervisor takes over.
//
// Every delivery reaches exactly one of three terminal states: acked, requeued for
// redelivery, or rejected to the dead-letter queue. See processOne.
func (c *Consumer) consume(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		c.processOne(d)
	}
}

// processOne runs one delivery to a terminal state: acked when the close
// succeeded or was a no-op, requeued when a live room would not accept the close,
// or rejected to the DLQ when the envelope is one this service can never act on.
//
// Nothing is republished and no retry state is kept. The only failure left is a
// still-live room that will not take the close command, and that attempt is
// bounded by the handler deadline — which is what paces the requeue, so a
// persistently failing room cannot spin the consumer.
func (c *Consumer) processOne(d amqp.Delivery) {
	switch c.handleDelivery(d.Body) {
	case ackSuccess:
		if err := d.Ack(false); err != nil {
			// The close already happened and it is idempotent, so a failed ack costs
			// a duplicate delivery, never a lost one.
			c.logger.Warn("lifecycle ack failed; the event will be redelivered and re-closed idempotently",
				zap.Error(err))
		}

	case requeue:
		// Requeue rather than reject: this is a transient refusal by a live room, and
		// rejecting would route a recoverable event to the diagnostic DLQ as if it
		// were poison. The delivery stays broker-owned either way; requeue=true just
		// makes redelivery immediate instead of waiting for a channel recycle.
		if err := d.Nack(false, true); err != nil {
			c.logger.Warn("lifecycle requeue failed; the event stays broker-owned until the channel closes",
				zap.String("pattern", patternOf(d.Body)), zap.Error(err))
		}

	case rejectPoison:
		// Not retryable at any future time: an unparseable envelope or a pattern
		// outside the contract is a producer/consumer mismatch. requeue=false
		// dead-letters it via the main queue's DLX, so the mismatch shows up as DLQ
		// depth instead of vanishing. Nothing consumes that queue — it is diagnostic.
		if err := d.Nack(false, false); err != nil {
			c.logger.Warn("lifecycle reject failed; the poison event stays broker-owned",
				zap.Error(err))
		}
		c.logger.Error("lifecycle event rejected to the dead-letter queue: this service can never act on it",
			zap.String("queue", c.names.dlq),
			zap.String("pattern", patternOf(d.Body)))
		c.obs.EventDeadLettered(patternOf(d.Body))
	}
}

// handleDelivery processes one delivery body under a per-delivery timeout context
// (handlerTimeout). The consumer drains deliveries serially on a single goroutine,
// so a handler that blocks forever — a room that never accepts the close —
// would head-of-line-block every later event. Bounding the context guarantees the
// stuck handler returns (its room-close attempt is cancelled), surfacing as
// requeue so the consumer makes progress instead of freezing — and that same
// bound is what paces redelivery, since a room that stays stuck cannot be retried
// faster than the deadline.
func (c *Consumer) handleDelivery(body []byte) ackAction {
	ctx, cancel := context.WithTimeout(context.Background(), c.handlerTimeout)
	defer cancel()
	return c.handle(ctx, body)
}

// patternOf extracts an event's pattern for logging, so a rejected event can be
// identified without reading the raw body. Best effort: an unparseable envelope is
// one of the reasons we are here in the first place.
func patternOf(body []byte) string {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return "<unparseable>"
	}
	return env.Pattern
}

// Close tears down the channel and connection.
func (c *Consumer) Close() error {
	// Release any pending recycle first, so a backoff timer cannot close a channel
	// after shutdown has already closed it. Nil-safe: a Consumer built directly in
	// a test has no recycle channel and must still close cleanly.
	c.closeOnce.Do(func() {
		if c.closed != nil {
			close(c.closed)
		}
	})
	ch := c.live()
	if ch != nil {
		_ = ch.Close()
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
