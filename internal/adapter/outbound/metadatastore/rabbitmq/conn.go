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
	// ReattachBackoff paces the supervisor's re-attach attempts after the broker
	// drops the connection or the channel faults. Zero uses
	// DefaultReattachBackoff.
	ReattachBackoff time.Duration
}

// DefaultReattachBackoff paces re-attach attempts. It matches the sibling
// lifecycle consumer's default: long enough not to hammer a broker that is still
// coming back, short enough that a restart is absorbed well inside the flush
// escalation window (5 failures on a 2/4/8/16/30s ladder, ~60s) that would
// otherwise discard unsaved edits.
const DefaultReattachBackoff = 2 * time.Second

// amqpChannel is the publish surface the Client uses, narrowed so Call/Emit can
// be unit-tested with a fake channel (the real *amqp.Channel satisfies it).
type amqpChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// setupChannel adds the calls Connect makes during wiring, on top of the publish
// surface the Client keeps afterwards. Split in two because they have different
// lifetimes: the Client holds amqpChannel for the life of the connection, while
// these are used once, during setup, and are what the failure branches must clean
// up after.
type setupChannel interface {
	amqpChannel
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// rpcConn is the connection surface Connect uses, so the dial/declare/consume
// wiring is reachable without a live broker. amqp091-go exposes no interfaces
// (amqp091-go#17), so the seam has to be declared here.
type rpcConn interface {
	Channel() (setupChannel, error)
	Close() error
}

// amqpRPCConn adapts *amqp.Connection, whose Channel returns the CONCRETE
// *amqp.Channel and therefore cannot satisfy rpcConn directly.
type amqpRPCConn struct{ *amqp.Connection }

// Channel opens an AMQP channel and returns it as the narrow setupChannel.
func (c amqpRPCConn) Channel() (setupChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// dialRPC is the connection factory. Tests replace it to drive Connect's wiring
// and its failure branches; production always dials RabbitMQ.
var dialRPC = func(url string) (rpcConn, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpRPCConn{conn}, nil
}

// Client is the amqp091-backed rpc transport: it publishes NestJS-style RPC
// requests to the server queue with correlationId + replyTo and matches replies
// on a per-connection reply queue. It satisfies the store's rpc interface.
type Client struct {
	cfg             Config
	serverQueue     string
	timeout         time.Duration
	reattachBackoff time.Duration

	// shutdown is closed by Close. It stops the supervisor and makes the
	// detached state terminal; without it, Close during a broker outage would
	// race the re-attach loop and leave a live connection behind.
	shutdown     chan struct{}
	shutdownOnce sync.Once

	mu      sync.Mutex
	pending map[string]chan nestReply
	// conn, ch and replyQ are REPLACED on every re-attach, so they are guarded by
	// mu and must be snapshotted under it rather than read directly.
	conn   rpcConn
	ch     amqpChannel
	replyQ string
	// detached is set when the reply consumer exits (failAllPending): the reply
	// queue is gone, so a Call would publish but never receive its reply. Check it
	// at waiter registration so such a Call fails fast, not on timeout. The
	// supervisor CLEARS it on a successful re-attach — it marks "no reply path
	// right now", not "dead forever".
	detached bool
}

// session is one live attachment: a connection, its channel, the exclusive reply
// queue declared on it, and the delivery stream. Re-attaching replaces all four
// together, which is why they travel as a unit.
type session struct {
	conn       rpcConn
	ch         setupChannel
	replyQ     string
	deliveries <-chan amqp.Delivery
}

// openSession dials, opens a channel, declares the exclusive reply queue and the
// durable server queue, and starts consuming replies. Every failure path closes
// what it had already opened, so a failed attempt leaks nothing.
func openSession(cfg Config) (*session, error) {
	conn, err := dialRPC(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}
	// Exclusive, auto-delete reply queue (the NestJS replyTo target).
	replyQ, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare reply queue: %w", err)
	}
	// Ensure the server queue exists (durable, matching the legacy services).
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare server queue: %w", err)
	}
	deliveries, err := ch.Consume(replyQ.Name, "", true, true, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("consume reply queue: %w", err)
	}
	return &session{conn: conn, ch: ch, replyQ: replyQ.Name, deliveries: deliveries}, nil
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

// Connect opens the first session and starts the supervisor that keeps it open.
// The returned Client is the rpc transport a Store is built over; close it on
// shutdown.
func Connect(cfg Config) (*Client, *Store, error) {
	if cfg.URL == "" {
		return nil, nil, fmt.Errorf("rabbitmq metadata store: URL is required")
	}
	if cfg.Queue == "" {
		return nil, nil, fmt.Errorf("rabbitmq metadata store: Queue is required")
	}
	sess, err := openSession(cfg)
	if err != nil {
		return nil, nil, err
	}

	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	backoff := cfg.ReattachBackoff
	if backoff <= 0 {
		backoff = DefaultReattachBackoff
	}
	c := &Client{
		cfg:             cfg,
		conn:            sess.conn,
		ch:              sess.ch,
		replyQ:          sess.replyQ,
		serverQueue:     cfg.Queue,
		timeout:         timeout,
		reattachBackoff: backoff,
		shutdown:        make(chan struct{}),
		pending:         make(map[string]chan nestReply),
	}
	go c.run(sess)

	return c, newWithRPC(c), nil
}

// run supervises the attachment. consumeReplies returns when the deliveries
// channel closes — the broker dropped the connection, or the channel faulted.
// Without this loop that first failure is terminal: the client marks itself
// detached and no Call can ever succeed again, so no client can join any
// document and every live room escalates to discarding its unsaved edits about a
// minute later, while the process stays healthy in every other respect.
//
// This mirrors the lifecycle consumer's supervisor deliberately: two AMQP
// clients in one binary should not have two different answers to "the broker
// restarted".
func (c *Client) run(sess *session) {
	for {
		c.consumeReplies(sess.deliveries)
		// Release whatever is still open on the dead session before re-attaching,
		// so a channel fault does not leak the connection under it.
		_ = sess.ch.Close()
		_ = sess.conn.Close()

		next := c.reattach()
		if next == nil {
			return // Close was called.
		}
		sess = next
	}
}

// reattach re-opens a session on a bounded backoff until it succeeds or the
// client is closed. It retries forever by design: the alternative is a live
// process whose metadata store is permanently dead.
func (c *Client) reattach() *session {
	for {
		select {
		case <-c.shutdown:
			return nil
		case <-time.After(c.reattachBackoff):
		}
		sess, err := openSession(c.cfg)
		if err != nil {
			continue
		}
		// Close can land while openSession is running, and openSession is not
		// cancellable. Adopting unconditionally would install a live connection
		// that Close has already walked past, leaking it and its consumer goroutine
		// past shutdown — so adopt re-checks under the same lock and refuses.
		if !c.adopt(sess) {
			_ = sess.ch.Close()
			_ = sess.conn.Close()
			return nil
		}
		return sess
	}
}

// adopt installs a new session as the live one and clears the detached flag, or
// refuses and reports false when Close has already run. The shutdown check and
// the field writes happen under ONE lock acquisition: split across two, Close
// could observe the old session, and the adopt could then publish the new one.
func (c *Client) adopt(sess *session) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.shutdown:
		return false
	default:
	}
	c.conn = sess.conn
	c.ch = sess.ch
	c.replyQ = sess.replyQ
	c.detached = false
	return true
}

// live snapshots the current publish surface. conn/ch/replyQ are replaced on
// every re-attach, so a Call must take them together under the lock rather than
// reading fields that can change between the publish and the reply-to it names.
func (c *Client) live() (amqpChannel, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch, c.replyQ, c.detached
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
// the client DETACHED, used when the reply consumer exits (channel/connection drop)
// so no Call is left hanging until its timeout. Each waiter channel is buffered
// (cap 1) and removed from pending under the lock, so the send never blocks and a
// late reply cannot double-deliver. Setting detached under the same lock fails
// any subsequent Call fast — its reply could never arrive on the dead session.
// The supervisor clears the flag once it re-attaches.
func (c *Client) failAllPending() {
	c.mu.Lock()
	c.detached = true
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
	if c.detached {
		// No reply path right now (broker/channel drop, supervisor re-attaching): a
		// publish would never get its reply and would only block until the timeout —
		// fail fast instead. This is transient; the caller retries.
		c.mu.Unlock()
		return fmt.Errorf("rabbitmq reply consumer closed")
	}
	// Snapshot the publish surface with the registration, under ONE lock: a
	// re-attach between them would publish on the new channel while naming the old
	// reply queue, and the reply would be delivered to a queue nobody consumes.
	ch, replyQ := c.ch, c.replyQ
	c.pending[corrID] = waiter
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, corrID)
		c.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := ch.PublishWithContext(ctx, "", c.serverQueue, false, false, amqp.Publishing{
		ContentType: "application/json",
		// Persistent marks the request durable so an ACCEPTED message survives a
		// broker restart (the server queue is durable). Broker ACCEPTANCE itself is
		// confirmed end-to-end by the correlated reply below: a request the broker
		// never queued yields no reply and surfaces as an rpc timeout error, so a
		// lost request is never a silent success — no publisher confirms needed.
		DeliveryMode:  amqp.Persistent,
		CorrelationId: corrID,
		ReplyTo:       replyQ,
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
	ch, _, _ := c.live()
	if ch == nil {
		return fmt.Errorf("rabbitmq channel unavailable")
	}
	return ch.PublishWithContext(ctx, "", c.serverQueue, false, false, amqp.Publishing{
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

// Close stops the supervisor and tears down the live channel and connection.
//
// Closing `shutdown` BEFORE taking the lock is what makes this safe against an
// in-flight re-attach: adopt re-checks shutdown under the same lock, so either
// this call sees the new session and closes it, or adopt refuses and reattach
// closes it itself. Neither order leaks the connection.
func (c *Client) Close() error {
	// A Client built directly (unit tests exercising Call/Emit without a
	// supervisor) has no shutdown channel; closing nil would panic. Guarding here
	// keeps the zero value usable rather than forcing every such test to know
	// about the supervisor.
	c.shutdownOnce.Do(func() {
		if c.shutdown != nil {
			close(c.shutdown)
		}
	})
	c.mu.Lock()
	ch, conn := c.ch, c.conn
	// A Call racing Close must fail fast rather than publish onto a channel this
	// is about to close.
	c.detached = true
	c.mu.Unlock()
	if ch != nil {
		_ = ch.Close()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}
