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
	// closed is set once the reply consumer exits (failAllPending): the reply
	// queue is gone, so a later Call would publish but never receive its reply.
	// Check it at waiter registration so such a Call fails fast, not on timeout.
	closed bool
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
// correlation id. When deliveries closes — a broker/channel drop — it fails every
// outstanding request so in-flight Calls unblock immediately with an error rather
// than each waiting out its full timeout while pending stays occupied.
func (c *Client) consumeReplies(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		var reply nestReply
		if err := json.Unmarshal(d.Body, &reply); err != nil {
			// A malformed reply must not be delivered as a zero-value (empty)
			// success — that would make Call return nil with no data and mask the
			// protocol error (e.g. a Load mapping to ErrNotFound). Surface it as a
			// server error on the reply so the waiter's Call returns an error.
			reply = nestReply{Err: json.RawMessage(`"malformed reply envelope"`)}
		}
		c.deliver(d.CorrelationId, reply)
	}
	c.failAllPending()
}

// failAllPending drains every outstanding waiter with an error reply and marks
// the client closed, used when the reply consumer exits (channel/connection drop)
// so no Call is left hanging until its timeout. Each waiter channel is buffered
// (cap 1) and removed from pending under the lock, so the send never blocks and a
// late reply cannot double-deliver. Setting closed under the same lock fails any
// subsequent Call fast — its reply could never arrive.
func (c *Client) failAllPending() {
	c.mu.Lock()
	c.closed = true
	waiters := make([]chan nestReply, 0, len(c.pending))
	for corrID, waiter := range c.pending {
		waiters = append(waiters, waiter)
		delete(c.pending, corrID)
	}
	c.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- nestReply{Err: json.RawMessage(`"rabbitmq reply channel closed"`)}
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
	if c.closed {
		// Reply consumer has exited (broker/channel drop): a publish now would never
		// get its reply and would only block until the timeout — fail fast instead.
		c.mu.Unlock()
		return fmt.Errorf("rabbitmq reply consumer closed")
	}
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
		ContentType: "application/json",
		// Persistent marks the request durable so an ACCEPTED message survives a
		// broker restart (the server queue is durable). Broker ACCEPTANCE itself is
		// confirmed end-to-end by the correlated reply below: a request the broker
		// never queued yields no reply and surfaces as an rpc timeout error, so a
		// lost request is never a silent success — no publisher confirms needed.
		DeliveryMode:  amqp.Persistent,
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
		// Persistent marks the event durable so an ACCEPTED message survives a broker
		// restart. Emit is fire-and-forget (no reply, no publisher confirm), so broker
		// acceptance is NOT separately confirmed — delivery of the contribution event
		// is best-effort. Guaranteed delivery would need publisher confirms, a
		// deliberate change not implied by Persistent.
		DeliveryMode: amqp.Persistent,
		Body:         body,
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
