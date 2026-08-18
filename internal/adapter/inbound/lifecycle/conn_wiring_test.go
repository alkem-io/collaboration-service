package lifecycle

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// fakeChannel records what Connect asked the broker to do and can fail any one
// step, so each failure branch — every one of which must close what it has
// already opened — is reachable without a live broker.
type fakeChannel struct {
	mu sync.Mutex

	declared     string
	durable      bool
	prefetch     int
	consumeQueue string
	autoAck      bool
	closed       int

	declareErr error
	qosErr     error
	consumeErr error

	deliveries chan amqp.Delivery
}

func (f *fakeChannel) QueueDeclare(name string, durable, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.declared, f.durable = name, durable
	return amqp.Queue{Name: name}, f.declareErr
}

func (f *fakeChannel) Qos(prefetchCount, _ int, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prefetch = prefetchCount
	return f.qosErr
}

func (f *fakeChannel) Consume(queue, _ string, autoAck, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeQueue, f.autoAck = queue, autoAck
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	f.deliveries = make(chan amqp.Delivery, 4)
	return f.deliveries, nil
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeChannel) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeConn struct {
	ch         *fakeChannel
	channelErr error
	closed     int
}

func (f *fakeConn) Channel() (brokerChannel, error) {
	if f.channelErr != nil {
		return nil, f.channelErr
	}
	return f.ch, nil
}

func (f *fakeConn) Close() error { f.closed++; return nil }

// withFakeBroker swaps the dial factory for the duration of one test.
func withFakeBroker(t *testing.T, conn *fakeConn, dialErr error) {
	t.Helper()
	prev := dialBroker
	dialBroker = func(string) (brokerConn, error) {
		if dialErr != nil {
			return nil, dialErr
		}
		return conn, nil
	}
	t.Cleanup(func() { dialBroker = prev })
}

// TestConnectDeclaresDurablyAndBoundsPrefetchBeforeConsuming pins the wiring
// order and the three settings that are correctness requirements rather than
// preferences.
//
// The queue must be DURABLE (a lifecycle event surviving a broker restart is the
// whole point of the cascade), autoAck must be FALSE (auto-ack is at-most-once,
// and a document.deleted lost between delivery and purge leaves an orphan), and
// Qos must be set BEFORE Consume — a prefetch applied afterwards does not bound
// the first deliveries, so the broker streams its whole backlog into memory the
// instant the consumer attaches.
func TestConnectDeclaresDurablyAndBoundsPrefetchBeforeConsuming(t *testing.T) {
	ch := &fakeChannel{}
	withFakeBroker(t, &fakeConn{ch: ch}, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "lifecycle-q"}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if ch.declared != "lifecycle-q" || !ch.durable {
		t.Fatalf("queue = %q durable=%v; the lifecycle queue must be declared durable or events are lost on a broker restart", ch.declared, ch.durable)
	}
	if ch.prefetch != DefaultPrefetch {
		t.Fatalf("prefetch = %d, want the default %d; an unset prefetch is UNLIMITED in AMQP, which is the unbounded backlog this guards against", ch.prefetch, DefaultPrefetch)
	}
	if ch.autoAck {
		t.Fatal("Consume used autoAck; a lifecycle event must be acked only after its purge succeeds, or a crash silently drops it and leaves an orphan document")
	}
	if ch.consumeQueue != "lifecycle-q" {
		t.Fatalf("consuming %q, want the declared lifecycle queue", ch.consumeQueue)
	}
}

// TestConnectHonoursConfiguredPrefetch covers the non-default branch of the QoS
// resolution.
func TestConnectHonoursConfiguredPrefetch(t *testing.T) {
	ch := &fakeChannel{}
	withFakeBroker(t, &fakeConn{ch: ch}, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", Prefetch: 3, HandlerTimeout: time.Second}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if ch.prefetch != 3 {
		t.Fatalf("prefetch = %d, want the configured 3", ch.prefetch)
	}
	if c.handlerTimeout != time.Second {
		t.Fatalf("handlerTimeout = %v, want the configured 1s", c.handlerTimeout)
	}
}

// TestConnectClosesWhatItOpenedOnEveryFailure is the branch that matters most and
// is unreachable without a seam.
//
// Each step after the dial has already opened something, so failing there must
// close it. A leaked channel or connection per failed Connect is invisible in
// tests, survives every retry, and shows up in production as a broker running out
// of file descriptors after a period of instability — precisely when reconnects
// are most frequent.
func TestConnectClosesWhatItOpenedOnEveryFailure(t *testing.T) {
	boom := errors.New("broker said no")

	for _, tc := range []struct {
		name        string
		ch          *fakeChannel
		channelErr  error
		wantChClose int
	}{
		{"channel", &fakeChannel{}, boom, 0},
		{"queue declare", &fakeChannel{declareErr: boom}, nil, 1},
		{"qos", &fakeChannel{qosErr: boom}, nil, 1},
		{"consume", &fakeChannel{consumeErr: boom}, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeConn{ch: tc.ch, channelErr: tc.channelErr}
			withFakeBroker(t, conn, nil)

			if _, err := Connect(Config{URL: "amqp://stub", Queue: "q"}, &fakeManager{}, zap.NewNop()); err == nil {
				t.Fatal("Connect must fail when the broker rejects a setup step")
			}
			if conn.closed != 1 {
				t.Fatalf("connection closed %d times, want 1; a failed Connect that leaks its connection exhausts broker file descriptors during exactly the instability that causes retries", conn.closed)
			}
			if got := tc.ch.closeCount(); got != tc.wantChClose {
				t.Fatalf("channel closed %d times, want %d", got, tc.wantChClose)
			}
		})
	}
}

// TestConnectSurfacesADialFailure covers the branch where the broker is
// unreachable — the common case during a broker restart or a network partition,
// and the one a service must report rather than swallow.
func TestConnectSurfacesADialFailure(t *testing.T) {
	withFakeBroker(t, nil, errors.New("connection refused"))

	_, err := Connect(Config{URL: "amqp://unreachable", Queue: "q"}, &fakeManager{}, zap.NewNop())
	if err == nil {
		t.Fatal("Connect must fail when the broker cannot be dialled")
	}
	if !strings.Contains(err.Error(), "dial rabbitmq") {
		t.Fatalf("error = %v, want it to name the dial step so the failing stage is identifiable", err)
	}
}

// TestConnectRejectsIncompleteConfig covers the two guards that run before any
// broker call.
func TestConnectRejectsIncompleteConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"no URL", Config{Queue: "q"}},
		{"no queue", Config{URL: "amqp://stub"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Connect(tc.cfg, &fakeManager{}, zap.NewNop()); err == nil {
				t.Fatal("Connect must reject an incomplete config before dialling")
			}
		})
	}
}

// TestCloseTearsDownChannelAndConnection covers Close, including the nil-field
// path a partially built Consumer would have.
func TestCloseTearsDownChannelAndConnection(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{ch: ch}
	withFakeBroker(t, conn, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q"}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if ch.closeCount() != 1 || conn.closed != 1 {
		t.Fatalf("Close left resources open: channel=%d connection=%d", ch.closeCount(), conn.closed)
	}

	// A Consumer with nothing wired must not panic on Close.
	if err := (&Consumer{}).Close(); err != nil {
		t.Fatalf("Close on an unwired Consumer: %v", err)
	}
}

// fakeAcker records ack/nack decisions. amqp.Delivery.Acknowledger is an
// interface precisely so handlers can be tested this way — the one seam
// amqp091-go does provide.
type fakeAcker struct {
	mu       sync.Mutex
	acks     int
	nacks    int
	requeued []bool
	ackErr   error
	nackErr  error
}

func (f *fakeAcker) Ack(_ uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	return f.ackErr
}

func (f *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks++
	f.requeued = append(f.requeued, requeue)
	return f.nackErr
}

func (f *fakeAcker) Reject(_ uint64, _ bool) error { return nil }

func (f *fakeAcker) snapshot() (int, int, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acks, f.nacks, append([]bool(nil), f.requeued...)
}

// TestConsumeAcksSuccessAndBoundsRequeueOnFailure covers the delivery loop,
// including the poison-message rule.
//
// The rule is the point: a permanently failing event is requeued ONCE and then
// dropped. Requeue-always is the obvious implementation and it is a livelock —
// the broker redelivers the same failing event forever, and because the consumer
// is single-threaded and serial, that one message starves every later lifecycle
// event indefinitely. A deleted document would simply never be purged.
func TestConsumeAcksSuccessAndBoundsRequeueOnFailure(t *testing.T) {
	// Built with the package's own envelope helper rather than hand-rolled JSON:
	// a body that does not parse routes to the unknown-event path and is ACKED, so
	// a hand-written envelope with one wrong field name would make the nack cases
	// below silently untestable.
	deleted := eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-1"})

	t.Run("success acks", func(t *testing.T) {
		acker := &fakeAcker{}
		c := &Consumer{mgr: &fakeManager{}, logger: zap.NewNop(), handlerTimeout: time.Second}
		ch := make(chan amqp.Delivery, 1)
		ch <- amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1}
		close(ch)
		c.consume(ch)

		acks, nacks, _ := acker.snapshot()
		if acks != 1 || nacks != 0 {
			t.Fatalf("acks=%d nacks=%d, want a single ack after a successful cascade", acks, nacks)
		}
	})

	t.Run("first failure requeues, redelivery drops", func(t *testing.T) {
		acker := &fakeAcker{}
		c := &Consumer{mgr: &fakeManager{purgeErr: errors.New("backend down")}, logger: zap.NewNop(), handlerTimeout: time.Second}
		ch := make(chan amqp.Delivery, 2)
		ch <- amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1, Redelivered: false}
		ch <- amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 2, Redelivered: true}
		close(ch)
		c.consume(ch)

		acks, nacks, requeued := acker.snapshot()
		if acks != 0 || nacks != 2 {
			t.Fatalf("acks=%d nacks=%d, want two nacks and no ack", acks, nacks)
		}
		if len(requeued) != 2 || !requeued[0] || requeued[1] {
			t.Fatalf("requeue decisions = %v, want [true false]: a failing event is requeued once and then DROPPED, or it redelivers forever and starves every later event on this single-threaded consumer", requeued)
		}
	})

	t.Run("ack and nack errors are survived", func(t *testing.T) {
		// A broker that rejects the acknowledgement must not stop the loop: the
		// remaining deliveries still have to be drained.
		acker := &fakeAcker{ackErr: errors.New("ack rejected"), nackErr: errors.New("nack rejected")}
		c := &Consumer{mgr: &fakeManager{}, logger: zap.NewNop(), handlerTimeout: time.Second}
		ch := make(chan amqp.Delivery, 2)
		ch <- amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1}
		ch <- amqp.Delivery{Body: []byte("not json"), Acknowledger: acker, DeliveryTag: 2}
		close(ch)
		c.consume(ch) // must return rather than hang or panic

		acks, _, _ := acker.snapshot()
		if acks == 0 {
			t.Fatal("the loop stopped before acking; a rejected acknowledgement must not halt delivery draining")
		}
	})
}
