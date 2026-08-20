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
// Config.HandlerTimeout is unset. The consumer is single-threaded (one goroutine
// draining the delivery channel serially), so a handler that blocks indefinitely
// — a wedged Purge backend call — would head-of-line-block
// every subsequent lifecycle event. A per-delivery deadline keeps a stuck handler
// from freezing the consumer: a handler that observes the context (PreRegister,
// abandoned at the deadline and treated as a failure. A Purge that hands the
// delete to a live room's run loop is instead bounded by that room's own
// BackendTimeout per queued command — still bounded, never indefinite.
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
	// ConfirmTimeout bounds the wait for a broker confirm/return on a transfer
	// publish. Zero uses DefaultConfirmTimeout.
	ConfirmTimeout time.Duration
	// RecycleBackoff delays the channel close after an unconfirmable transfer.
	// Zero uses DefaultRecycleBackoff.
	RecycleBackoff time.Duration
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
	// Purge runs the owner-delete cascade (disconnect, release, purge durable).
	Purge(ctx context.Context, id model.DocumentID) error
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
	// Confirm puts the channel in publisher-confirm mode. Every transfer out of the
	// main queue is an explicit publish whose broker acknowledgement decides whether
	// the original may be acked, so this MUST succeed before any transfer.
	Confirm(noWait bool) error
	// NotifyPublish and NotifyReturn are the two halves of "did it land". A confirm
	// says the exchange accepted the message; a return says nothing was routed to it.
	// Success requires the ack AND the absence of a return.
	NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(chan amqp.Return) chan amqp.Return
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type brokerConn interface {
	Channel() (brokerChannel, error)
	// ServerProperties carries the broker's reported version, which gates whether
	// the declared topology actually functions — see requireVersionFloor.
	ServerProperties() amqp.Table
	Close() error
}

// amqpConn adapts *amqp.Connection to brokerConn. The wrapper is needed because
// amqp.Connection.Channel returns the CONCRETE *amqp.Channel, so the real type
// cannot satisfy an interface whose Channel returns an interface.
type amqpConn struct{ *amqp.Connection }

// ServerProperties exposes the broker's advertised properties (server version).
func (c amqpConn) ServerProperties() amqp.Table { return c.Properties }

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
// for publisher confirms and returns, and the delivery stream from it. It is the
// unit the supervisor replaces — a dropped connection invalidates all of it at
// once, so re-attaching means rebuilding the whole thing, not patching a part.
type session struct {
	conn       brokerConn
	ch         brokerChannel
	confirms   chan amqp.Confirmation
	returns    chan amqp.Return
	deliveries <-chan amqp.Delivery
}

// openSession dials the broker and brings one attachment all the way up: version
// floor, topology, confirms, QoS, consume. Every failure closes what it opened.
func openSession(cfg Config, names queueNames) (*session, error) {
	conn, err := dialBroker(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	// The version floor is checked after connecting and BEFORE declaring anything.
	// Below it the declarations still succeed and the retry tiers silently never
	// expire, so refusing here is the only point at which the failure is visible.
	if err := requireVersionFloor(conn.ServerProperties()); err != nil {
		_ = conn.Close()
		return nil, err
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
	// Publisher confirms must be enabled before any transfer: the ack/reject
	// decision for every delivery depends on the broker's answer to a publish.
	if err := ch.Confirm(false); err != nil {
		return fail(fmt.Errorf("enable publisher confirms: %w", err))
	}
	// Bound the unacked-delivery window BEFORE consuming (channel QoS). With manual
	// ack and a single-threaded consume loop, an unset prefetch lets the broker
	// stream the entire backlog into memory at once — no backpressure. A bounded
	// prefetch keeps the queue on the broker and feeds events only as they are
	// acked. Must precede Consume to take effect on the first deliveries.
	if err := ch.Qos(resolvePrefetch(cfg.Prefetch), 0, false); err != nil {
		return fail(fmt.Errorf("set lifecycle prefetch: %w", err))
	}
	// Manual ack (autoAck=false): a lifecycle event (e.g. document.deleted) must be
	// acknowledged only AFTER its idempotent purge succeeds, so a crash or a backend
	// failure between delivery and completion redelivers the event rather than
	// silently dropping it (auto-ack is at-most-once; the cascade is a correctness
	// requirement — no orphan documents).
	deliveries, err := ch.Consume(names.main, "", false, false, false, false, nil)
	if err != nil {
		return fail(fmt.Errorf("consume lifecycle queue: %w", err))
	}
	return &session{
		conn:       conn,
		ch:         ch,
		confirms:   ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:    ch.NotifyReturn(make(chan amqp.Return, 1)),
		deliveries: deliveries,
	}, nil
}

// Connect dials RabbitMQ, declares the lifecycle topology, starts consuming, and
// routes each event to the Manager. Close it on shutdown.
//
// The first attachment must succeed: a misconfigured URL or a broker below the
// version floor is a startup error, not something to retry into. After that the
// consumer supervises itself — a dropped connection, a broker restart, or a
// deliberate recycle after an unconfirmable transfer is followed by re-attaching
// on a bounded backoff, indefinitely, until Close.
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
		mgr:            mgr,
		logger:         logger,
		obs:            obs,
		cfg:            cfg,
		handlerTimeout: resolveHandlerTimeout(cfg.HandlerTimeout),
		names:          names,
		confirmTimeout: resolveConfirmTimeout(cfg.ConfirmTimeout),
		recycleBackoff: resolveRecycleBackoff(cfg.RecycleBackoff),
		closed:         make(chan struct{}),
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
// topology that has gone missing. A deleted queue therefore comes back within one
// poll interval without anyone intervening, which is also what releases a message
// the broker has parked for a dead-letter hop into it.
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
		ch, _, _ := c.live()
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
	c.confirms, c.returns = s.confirms, s.returns
}

// live reads the current attachment as one consistent triple. The lock is for
// Close, which runs on the caller's goroutine; transfer and the supervisor share
// the run goroutine and cannot interleave with each other.
func (c *Consumer) live() (brokerChannel, chan amqp.Confirmation, chan amqp.Return) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch, c.confirms, c.returns
}

// run is the supervisor. consume returns when the delivery stream ends — which is
// every way an attachment can die: the broker closed the connection, the channel
// faulted, or recycle deliberately closed it after an unconfirmable transfer.
// Without this loop that first recycle would be terminal: the consume goroutine
// would exit and lifecycle events would stop being processed, silently and
// permanently, while the process stayed healthy in every other respect.
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
// design: the alternative is a live process that has silently stopped applying
// deletions and revocations.
func (c *Consumer) reattach() *session {
	for {
		select {
		case <-c.closed:
			return nil
		case <-time.After(c.recycleBackoff):
		}
		sess, err := openSession(c.cfg, c.names)
		if err != nil {
			c.logger.Error("lifecycle consumer could not re-attach to the broker; retrying",
				zap.Duration("backoff", c.recycleBackoff), zap.Error(err))
			continue
		}
		c.adopt(sess)
		c.logger.Info("lifecycle consumer re-attached to the broker")
		return sess
	}
}

// DefaultConfirmTimeout bounds the wait for a broker confirm/return on a transfer.
// It is short: the publish is to a queue on the same broker the delivery came
// from, so silence means something is wrong rather than slow.
const DefaultConfirmTimeout = 10 * time.Second

// DefaultRecycleBackoff paces channel recycles after an unconfirmable transfer.
const DefaultRecycleBackoff = 5 * time.Second

func resolveRecycleBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultRecycleBackoff
	}
	return d
}

func resolveConfirmTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultConfirmTimeout
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
// Nothing here is ever nacked or rejected. A successful event is acked; anything
// else is republished — down the retry ladder, or to the DLQ — and acked only once
// the broker has confirmed the republish. A transfer that is not confirmed leaves
// the delivery untouched, so it stays broker-owned and is redelivered.
func (c *Consumer) consume(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		c.processOne(d)
	}
}

// processOne runs one delivery to a terminal state: acked after its work is done,
// acked after its successor has been CONFIRMED onto another queue, or left
// untouched so the broker redelivers it.
//
// The delivery is never rejected. Rejecting would hand it to Q1's dead-letter
// route, which is an unconfirmed internal republish — "it will reach the DLQ"
// would be an assumption, and a transient publishing failure would become
// terminal handling.
func (c *Consumer) processOne(d amqp.Delivery) {
	action := c.handleDelivery(d.Body)

	if action == ackSuccess {
		if err := d.Ack(false); err != nil {
			c.logger.Warn("lifecycle ack failed", zap.Error(err))
		}
		return
	}

	// Terminal and retryable failures differ only in WHERE the event is sent; both
	// are an explicit confirmed publish followed by acking the original, so an
	// unactionable envelope is recorded in the DLQ rather than silently swallowed.
	attempt := attemptOf(d.Headers)
	var target string
	if action == ackTerminal {
		target = c.names.dlq
	} else {
		target = c.nextTarget(attempt)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.confirmTimeout)
	err := c.transfer(ctx, target, d, attempt+1)
	cancel()
	c.obs.EventTransferred(target, err == nil)

	if err == nil {
		if aerr := d.Ack(false); aerr != nil {
			// The successor is durable; a failed ack only means this event is
			// redelivered and transferred again. Handlers are idempotent, so a
			// duplicate is survivable — losing the original would not be.
			c.logger.Warn("lifecycle ack after transfer failed; the event will be redelivered and retried",
				zap.String("target", target), zap.Error(aerr))
		}
		if target == c.names.dlq {
			// replays distinguishes "this failed for the first time" from "a person
			// has already sent this round the ladder N times and it failed again",
			// which the attempt count cannot say: a replay clears it by design.
			replays := replaysOf(d.Headers)
			c.logger.Error("lifecycle event moved to the dead-letter queue; it will not be retried again without an operator replay",
				zap.String("queue", target),
				zap.String("pattern", patternOf(d.Body)),
				zap.Int32("attempt", attempt),
				zap.Int32("replays", replays))
			c.obs.EventDeadLettered(patternOf(d.Body), replays)
		}
		return
	}

	// The transfer did NOT happen. Leave the delivery unacknowledged so it stays
	// broker-owned, then recycle the channel after a bounded backoff so Rabbit
	// redelivers it. Not an immediate requeue: on a serial consumer that spins.
	c.logger.Error("lifecycle transfer was not confirmed; the event remains broker-owned and will be redelivered",
		zap.String("target", target),
		zap.String("pattern", patternOf(d.Body)),
		zap.Error(err))
	c.recycle()
}

// recycle closes the channel after a bounded delay. Every unacked delivery on it
// returns to the queue and is redelivered to the next channel, which is how an
// unconfirmable transfer retries without spinning.
func (c *Consumer) recycle() {
	select {
	case <-time.After(c.recycleBackoff):
	case <-c.closed:
		return
	}
	ch, _, _ := c.live()
	if ch == nil {
		return
	}
	// Closing the channel ends the delivery stream, which returns consume and hands
	// control to the supervisor, which re-attaches. The unacked delivery goes back
	// to the broker and is redelivered on the new attachment.
	if err := ch.Close(); err != nil {
		c.logger.Warn("closing the lifecycle channel after a failed transfer", zap.Error(err))
	}
}

// handleDelivery processes one delivery body under a per-delivery timeout context
// (handlerTimeout). The consumer drains deliveries serially on a single goroutine,
// so a handler that blocks forever — a wedged Purge backend call —
// would head-of-line-block every later event. Bounding the context guarantees the
// stuck handler returns (its backend call is cancelled), surfacing as retryLater
// so the consumer makes progress instead of freezing.
func (c *Consumer) handleDelivery(body []byte) ackAction {
	ctx, cancel := context.WithTimeout(context.Background(), c.handlerTimeout)
	defer cancel()
	return c.handle(ctx, body)
}

// patternOf extracts an event's pattern for logging, so a transferred event can be
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
	ch, _, _ := c.live()
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
