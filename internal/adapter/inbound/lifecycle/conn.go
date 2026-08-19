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
const DefaultPrefetch = 16

// DefaultHandlerTimeout bounds the processing of a single lifecycle delivery when
// Config.HandlerTimeout is unset. The consumer is single-threaded (one goroutine
// draining the delivery channel serially), so a handler that blocks indefinitely
// — a wedged Purge/PreRegister/ReEvaluate backend call — would head-of-line-block
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
	// PreRegister writes an initial metadata row ahead of first connect.
	PreRegister(ctx context.Context, meta model.Metadata) error
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
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare lifecycle queue: %w", err)
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

	c := &Consumer{mgr: mgr, logger: logger, conn: conn, ch: ch, handlerTimeout: resolveHandlerTimeout(cfg.HandlerTimeout)}
	go c.consume(deliveries)
	return c, nil
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
		switch c.handleDelivery(d.Body) {
		case ackSuccess:
			if err := d.Ack(false); err != nil {
				c.logger.Warn("lifecycle ack failed", zap.Error(err))
			}
		case nackRequeue:
			requeue := shouldRequeue(d.Redelivered)
			if !requeue {
				// ERROR, not Warn, and it names the event. Dropping a lifecycle event
				// is not a routine retry outcome: a discarded document.deleted leaves
				// the owner's content durable and joinable after they deleted it, and
				// nothing downstream will ever notice — this log line is the ONLY
				// record that it happened.
				//
				// The bound itself is a known limitation, not a design: two attempts is
				// whatever a transient backend outage happens to outlast. Correct
				// handling needs a dead-letter queue or a retry schedule, which is a
				// broker-topology change shared with `server`. Until then this must at
				// least be alertable.
				c.logger.Error("DROPPING a lifecycle event after its redelivery also failed; if this was document.deleted, the owner's content is still stored and still joinable",
					zap.String("pattern", patternOf(d.Body)),
					zap.String("body", string(d.Body)))
			}
			if err := d.Nack(false, requeue); err != nil {
				c.logger.Warn("lifecycle nack failed", zap.Error(err))
			}
		}
	}
}

// handleDelivery processes one delivery body under a per-delivery timeout context
// (handlerTimeout). The consumer drains deliveries serially on a single goroutine,
// so a handler that blocks forever — a wedged Purge/PreRegister backend call —
// would head-of-line-block every later event. Bounding the context guarantees the
// stuck handler returns (its backend call is cancelled), surfacing as nackRequeue
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

// shouldRequeue implements the bounded-requeue rule for a failed lifecycle event:
// requeue only on the first attempt; on a redelivery, drop (no requeue) so a
// permanently failing "poison" message cannot loop forever. The purge/pre-register
// is idempotent, so re-processing on the single requeue is safe.
func shouldRequeue(redelivered bool) bool {
	return !redelivered
}

// Close tears down the channel and connection.
func (c *Consumer) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
