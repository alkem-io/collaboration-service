package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/google/uuid"
)

// Config carries the RabbitMQ connection settings (env: RABBITMQ_* + the server
// queue name).
type Config struct {
	// URL is the amqp:// connection string (assembled from RABBITMQ_HOST/PORT/
	// USER/PASSWORD by the caller).
	URL string
	// Queue is the Alkemio server queue the collaboration patterns are routed to
	// (the @MessagePattern consumer's queue).
	Queue string
	// RequestTimeout bounds a request/reply RPC (legacy default 5s).
	RequestTimeout time.Duration
}

// amqpChannel is the publish surface the Client uses, narrowed so Call/Emit can
// be unit-tested with a fake channel (the real *amqp.Channel satisfies it).
type amqpChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// Client is the amqp091-backed rpc transport: it publishes NestJS-style RPC
// requests to the server queue with correlationId + replyTo and matches replies
// on a per-connection reply queue. It satisfies the store's rpc interface.
type Client struct {
	conn        *amqp.Connection
	ch          amqpChannel
	replyQ      string
	serverQueue string
	timeout     time.Duration

	mu      sync.Mutex
	pending map[string]chan nestReply
}

// deliver routes a reply to the goroutine waiting on its correlation id (the
// shared body of the live consumer loop and the unit-test fake). It returns
// false when no waiter is registered for the id.
func (c *Client) deliver(corrID string, reply nestReply) bool {
	c.mu.Lock()
	waiter, ok := c.pending[corrID]
	if ok {
		delete(c.pending, corrID)
	}
	c.mu.Unlock()
	if !ok {
		return false
	}
	waiter <- reply
	return true
}

// Connect dials RabbitMQ, opens a channel, declares an exclusive reply queue,
// and starts consuming replies. The returned Client is the rpc transport a Store
// is built over; close it on shutdown.
func Connect(cfg Config) (*Client, *Store, error) {
	if cfg.URL == "" {
		return nil, nil, fmt.Errorf("rabbitmq metadata store: URL is required")
	}
	if cfg.Queue == "" {
		return nil, nil, fmt.Errorf("rabbitmq metadata store: Queue is required")
	}
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}
	// Exclusive, auto-delete reply queue (the NestJS replyTo target).
	replyQ, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("declare reply queue: %w", err)
	}
	// Ensure the server queue exists (durable, matching the legacy services).
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("declare server queue: %w", err)
	}

	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	c := &Client{
		conn:        conn,
		ch:          ch,
		replyQ:      replyQ.Name,
		serverQueue: cfg.Queue,
		timeout:     timeout,
		pending:     make(map[string]chan nestReply),
	}

	deliveries, err := ch.Consume(replyQ.Name, "", true, true, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("consume reply queue: %w", err)
	}
	go c.consumeReplies(deliveries)

	return c, newWithRPC(c), nil
}

// consumeReplies dispatches each reply to the goroutine waiting on its
// correlation id.
func (c *Client) consumeReplies(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		var reply nestReply
		_ = json.Unmarshal(d.Body, &reply)
		c.deliver(d.CorrelationId, reply)
	}
}

// Call publishes an RPC request and waits for the correlated reply.
func (c *Client) Call(ctx context.Context, pattern string, data, reply any) error {
	corrID := uuid.NewString()
	body, err := marshalEnvelope(pattern, corrID, data)
	if err != nil {
		return err
	}

	waiter := make(chan nestReply, 1)
	c.mu.Lock()
	c.pending[corrID] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, corrID)
		c.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.ch.PublishWithContext(ctx, "", c.serverQueue, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       c.replyQ,
		Body:          body,
	}); err != nil {
		return fmt.Errorf("publish request: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("rpc timeout for %s: %w", pattern, ctx.Err())
	case r := <-waiter:
		if len(r.Err) > 0 && string(r.Err) != "null" {
			return fmt.Errorf("server error: %s", string(r.Err))
		}
		if reply != nil && len(r.Response) > 0 {
			if err := json.Unmarshal(r.Response, reply); err != nil {
				return fmt.Errorf("unmarshal reply: %w", err)
			}
		}
		return nil
	}
}

// Emit publishes a fire-and-forget event (no reply) to the server queue.
func (c *Client) Emit(ctx context.Context, pattern string, data any) error {
	body, err := marshalEnvelope(pattern, uuid.NewString(), data)
	if err != nil {
		return err
	}
	return c.ch.PublishWithContext(ctx, "", c.serverQueue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

// Close tears down the channel and connection.
func (c *Client) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
