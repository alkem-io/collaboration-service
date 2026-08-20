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
// — a wedged Purge/ReEvaluate backend call — would head-of-line-block
// every subsequent lifecycle event. A per-delivery deadline keeps a stuck handler
// from freezing the consumer: a handler that observes the context (PreRegister,
// ReEvaluate) is abandoned (nack/requeue) at the deadline. A Purge that hands the
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
	// ReEvaluate re-runs per-document authZ for a live room's members.
	ReEvaluate(ctx context.Context, id model.DocumentID)
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

// Connect dials RabbitMQ, declares the durable lifecycle queue, starts consuming,
// and routes each event to the Manager. Close it on shutdown. The returned
// Consumer keeps running until Close or a connection drop.
func Connect(cfg Config, mgr Manager, logger *zap.Logger) (*Consumer, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("lifecycle consumer: URL is required")
	}
	if cfg.Queue == "" {
		return nil, fmt.Errorf("lifecycle consumer: Queue is required")
	}
	conn, err := dialBroker(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	// The version floor is checked after connecting and BEFORE declaring anything.
	// Below it the declarations still succeed and the retry tiers silently never
	// expire, so refusing here is the only point at which the failure is visible.
	if err := requireVersionFloor(conn.ServerProperties()); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("lifecycle consumer: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	names := namesFor(cfg.Queue)
	if err := declareTopology(ch, names); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare lifecycle topology: %w", err)
	}
	// Publisher confirms must be enabled before any transfer: the ack/reject
	// decision for every delivery depends on the broker's answer to a publish.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	// Bound the unacked-delivery window BEFORE consuming (channel QoS). With manual
	// ack and a single-threaded consume loop, an unset prefetch lets the broker
	// stream the entire backlog into memory at once — no backpressure. A bounded
	// prefetch keeps the queue on the broker and feeds events only as they are
	// acked. Must precede Consume to take effect on the first deliveries.
	if err := ch.Qos(resolvePrefetch(cfg.Prefetch), 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("set lifecycle prefetch: %w", err)
	}
	// Manual ack (autoAck=false): a lifecycle event (e.g. document.deleted) must be
	// acknowledged only AFTER its idempotent purge succeeds, so a crash or a backend
	// failure between delivery and completion redelivers the event rather than
	// silently dropping it (auto-ack is at-most-once; the cascade is a correctness
	// requirement — no orphan documents).
	deliveries, err := ch.Consume(cfg.Queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("consume lifecycle queue: %w", err)
	}

	c := &Consumer{
		mgr:            mgr,
		logger:         logger,
		conn:           conn,
		ch:             ch,
		handlerTimeout: resolveHandlerTimeout(cfg.HandlerTimeout),
		names:          names,
		confirms:       ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:        ch.NotifyReturn(make(chan amqp.Return, 1)),
		confirmTimeout: resolveConfirmTimeout(cfg.ConfirmTimeout),
		recycleBackoff: resolveRecycleBackoff(cfg.RecycleBackoff),
		closed:         make(chan struct{}),
	}
	go c.consume(deliveries)
	return c, nil
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

// consume routes each delivery to handle and acks/nacks per the outcome, until
// the channel closes. A successful (or idempotent / unactionable) event is acked;
// a genuine processing failure is nacked with a bounded requeue — requeued once,
// then dropped (nack without requeue) on the redelivery so a permanently failing
// "poison" message cannot loop forever.
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

	if err == nil {
		if aerr := d.Ack(false); aerr != nil {
			// The successor is durable; a failed ack only means this event is
			// redelivered and transferred again. Handlers are idempotent, so a
			// duplicate is survivable — losing the original would not be.
			c.logger.Warn("lifecycle ack after transfer failed; the event will be redelivered and retried",
				zap.String("target", target), zap.Error(aerr))
		}
		if target == c.names.dlq {
			c.logger.Error("lifecycle event moved to the dead-letter queue; it will not be retried again without an operator replay",
				zap.String("queue", target),
				zap.String("pattern", patternOf(d.Body)),
				zap.Int32("attempt", attempt))
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
	if err := c.ch.Close(); err != nil {
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

// patternOf extracts an event's pattern for logging, so a dropped event can be
// identified without reading the raw body. Best effort: an unparseable envelope
// is why we are here in the first place.
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
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
