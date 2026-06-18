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
	// Queue is the queue the server publishes lifecycle events to. When it
	// matches the metastore RPC queue the consumer ignores non-lifecycle traffic.
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
	deliveries, err := ch.Consume(cfg.Queue, "", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("consume lifecycle queue: %w", err)
	}

	c := &Consumer{mgr: mgr, logger: logger, conn: conn, ch: ch}
	go c.consume(deliveries)
	return c, nil
}

// consume routes each delivery to handle until the channel closes.
func (c *Consumer) consume(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		c.handle(context.Background(), d.Body)
	}
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
