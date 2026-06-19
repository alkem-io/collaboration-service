package lifecycle

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// Config carries the RabbitMQ lifecycle-consumer settings.
type Config struct {
	// URL is the amqp:// connection string (shared with the metastore bus).
	URL string
	// Queue is the DEDICATED lifecycle queue the consumer binds — its own queue,
	// distinct from the metastore RPC queue (config.RabbitMQ.LifecycleQueue,
	// default alkemio-collaboration-lifecycle). It MUST NOT be the metastore RPC
	// queue: RabbitMQ round-robins a queue across its consumers, so sharing one
	// queue would let this consumer steal metastore fetch/save RPCs and drop them.
	Queue string
}

// Manager is the domain dependency the consumer drives (the concrete
// service.Manager satisfies it). Declared here as the consumer's required
// behaviour so cmd/server wires the real Manager in.
type Manager interface {
	// Purge runs the owner-delete cascade (disconnect, release, purge durable).
	Purge(ctx context.Context, id model.DocumentID) error
	// ReEvaluate re-runs per-document authZ for a live room's members.
	ReEvaluate(id model.DocumentID)
	// PreRegister writes an initial metadata row ahead of first connect.
	PreRegister(ctx context.Context, meta model.Metadata) error
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
	conn, err := amqp.Dial(cfg.URL)
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

	c := &Consumer{mgr: mgr, logger: logger, conn: conn, ch: ch}
	go c.consume(deliveries)
	return c, nil
}

// consume routes each delivery to handle and acks/nacks per the outcome, until
// the channel closes. A successful (or idempotent / unactionable) event is acked;
// a genuine processing failure is nacked with a bounded requeue — requeued once,
// then dropped (nack without requeue) on the redelivery so a permanently failing
// "poison" message cannot loop forever.
func (c *Consumer) consume(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		switch c.handle(context.Background(), d.Body) {
		case ackSuccess:
			if err := d.Ack(false); err != nil {
				c.logger.Warn("lifecycle ack failed", zap.Error(err))
			}
		case nackRequeue:
			requeue := shouldRequeue(d.Redelivered)
			if !requeue {
				c.logger.Warn("lifecycle event still failing on redelivery; dropping to avoid a poison loop")
			}
			if err := d.Nack(false, requeue); err != nil {
				c.logger.Warn("lifecycle nack failed", zap.Error(err))
			}
		}
	}
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
