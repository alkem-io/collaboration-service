package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// setupChan is a fake setupChannel that records the wiring calls and can fail any
// one of them.
type setupChan struct {
	mu sync.Mutex

	declared   []declareCall
	consumed   string
	autoAck    bool
	exclusive  bool
	closed     int
	deliveries chan amqp.Delivery

	declareErrAt int // 1-based index of the QueueDeclare call that should fail; 0 = none
	declareCalls int
	consumeCalls int
	consumeErr   error
}

type declareCall struct {
	name      string
	durable   bool
	exclusive bool
}

func (s *setupChan) QueueDeclare(name string, durable, _, exclusive, _ bool, _ amqp.Table) (amqp.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.declareCalls++
	if s.declareErrAt == s.declareCalls {
		return amqp.Queue{}, errors.New("declare rejected")
	}
	s.declared = append(s.declared, declareCall{name: name, durable: durable, exclusive: exclusive})
	if name == "" {
		name = "amq.gen-reply"
	}
	return amqp.Queue{Name: name}, nil
}

func (s *setupChan) Consume(queue, _ string, autoAck, exclusive, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumed, s.autoAck, s.exclusive = queue, autoAck, exclusive
	s.consumeCalls++
	if s.consumeErr != nil {
		return nil, s.consumeErr
	}
	s.deliveries = make(chan amqp.Delivery, 1)
	return s.deliveries, nil
}

// consumeCount reports how many times the client has attached a reply consumer.
// A second attach is the observable signature of the supervisor re-attaching.
func (s *setupChan) consumeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumeCalls
}

// dropDeliveries closes the live deliveries channel, which is exactly what
// amqp091 does when the broker drops the connection or the channel faults.
func (s *setupChan) dropDeliveries() {
	s.mu.Lock()
	d := s.deliveries
	s.deliveries = nil
	s.mu.Unlock()
	if d != nil {
		close(d)
	}
}

func (s *setupChan) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func (s *setupChan) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

func (s *setupChan) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *setupChan) declares() []declareCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]declareCall(nil), s.declared...)
}

type rpcFakeConn struct {
	ch         *setupChan
	channelErr error
	closed     int
}

func (c *rpcFakeConn) Channel() (setupChannel, error) {
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	return c.ch, nil
}

func (c *rpcFakeConn) Close() error { c.closed++; return nil }

func withFakeRPCBroker(t *testing.T, conn *rpcFakeConn, dialErr error) {
	t.Helper()
	prev := dialRPC
	dialRPC = func(string) (rpcConn, error) {
		if dialErr != nil {
			return nil, dialErr
		}
		return conn, nil
	}
	t.Cleanup(func() { dialRPC = prev })
}

// TestConnectDeclaresAnExclusiveReplyQueueAndADurableServerQueue pins the two
// queue declarations, which differ deliberately and would both be wrong if
// swapped.
//
// The REPLY queue is exclusive and auto-delete: it belongs to this connection
// alone, and a durable shared one would let another pod consume this pod's RPC
// replies — every Call would then time out nondeterministically depending on who
// got the reply. The SERVER queue is durable and shared, matching the queue the
// Alkemio server consumes from; declaring it non-durable would silently create a
// second, transient queue that the server never reads, so every request would
// vanish and every Call would time out.
func TestConnectDeclaresAnExclusiveReplyQueueAndADurableServerQueue(t *testing.T) {
	ch := &setupChan{}
	withFakeRPCBroker(t, &rpcFakeConn{ch: ch}, nil)

	client, store, err := Connect(Config{URL: "amqp://stub", Queue: "alkemio-collaboration"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if store == nil {
		t.Fatal("Connect returned no store")
	}

	got := ch.declares()
	if len(got) != 2 {
		t.Fatalf("declared %d queues, want 2 (reply + server): %+v", len(got), got)
	}
	if got[0].name != "" || !got[0].exclusive || got[0].durable {
		t.Fatalf("reply queue = %+v; it must be server-named, exclusive and non-durable, or another pod can consume this pod's replies", got[0])
	}
	if got[1].name != "alkemio-collaboration" || !got[1].durable || got[1].exclusive {
		t.Fatalf("server queue = %+v; it must be the durable shared queue the Alkemio server consumes, or requests go to a queue nobody reads", got[1])
	}
	if !ch.autoAck || !ch.exclusive {
		t.Fatalf("reply consume autoAck=%v exclusive=%v; replies are consumed auto-ack and exclusively on this connection", ch.autoAck, ch.exclusive)
	}
}

// TestConnectClosesWhatItOpenedOnEveryFailure covers each failure branch. Every
// step after the dial has already opened something; a leak here accumulates one
// channel and one connection per failed attempt, during exactly the broker
// instability that makes attempts frequent.
func TestConnectClosesWhatItOpenedOnEveryFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ch          *setupChan
		channelErr  error
		wantChClose int
	}{
		{"channel", &setupChan{}, errors.New("no channel"), 0},
		{"reply queue declare", &setupChan{declareErrAt: 1}, nil, 1},
		{"server queue declare", &setupChan{declareErrAt: 2}, nil, 1},
		{"consume", &setupChan{consumeErr: errors.New("no consume")}, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &rpcFakeConn{ch: tc.ch, channelErr: tc.channelErr}
			withFakeRPCBroker(t, conn, nil)

			if _, _, err := Connect(Config{URL: "amqp://stub", Queue: "q"}); err == nil {
				t.Fatal("Connect must fail when the broker rejects a setup step")
			}
			if conn.closed != 1 {
				t.Fatalf("connection closed %d times, want 1", conn.closed)
			}
			if got := tc.ch.closeCount(); got != tc.wantChClose {
				t.Fatalf("channel closed %d times, want %d", got, tc.wantChClose)
			}
		})
	}
}

// TestConnectRejectsIncompleteConfigAndDialFailure covers the guards that run
// before any broker call, and the unreachable-broker case.
func TestConnectRejectsIncompleteConfigAndDialFailure(t *testing.T) {
	if _, _, err := Connect(Config{Queue: "q"}); err == nil {
		t.Fatal("Connect must reject a missing URL")
	}
	if _, _, err := Connect(Config{URL: "amqp://stub"}); err == nil {
		t.Fatal("Connect must reject a missing queue")
	}

	withFakeRPCBroker(t, nil, errors.New("connection refused"))
	if _, _, err := Connect(Config{URL: "amqp://unreachable", Queue: "q"}); err == nil {
		t.Fatal("Connect must surface a dial failure")
	}
}

// TestClientReattachesAfterTheBrokerDropsTheReplyConsumer is the regression for a
// permanent-outage bug: one broker blip bricked the metadata store for the whole
// life of the process.
//
// consumeReplies returns when the deliveries channel closes — a broker restart,
// failover, upgrade, or channel fault. failAllPending then marked the client
// closed and NOTHING ever re-dialled, because Connect started a bare
// `go c.consumeReplies(...)` with no supervisor. Every later Call returned
// "rabbitmq reply consumer closed" forever, so no client could join any document
// and every live room flushed, failed, escalated and DISCARDED its unsaved edits
// about a minute later.
//
// The sibling lifecycle consumer already supervises exactly this
// (lifecycle/conn.go run/reattach), so this is restoring an established pattern,
// not new machinery.
//
// Non-vacuity: delete the reattach loop from run() and this fails at the
// consumeCount check — the client never re-attaches. Keep the loop but forget to
// clear `detached` on adopt and it fails at the final Call, which would still be
// rejected fast as though the client were dead.
func TestClientReattachesAfterTheBrokerDropsTheReplyConsumer(t *testing.T) {
	ch := &setupChan{}
	withFakeRPCBroker(t, &rpcFakeConn{ch: ch}, nil)

	client, _, err := Connect(Config{
		URL:   "amqp://stub",
		Queue: "alkemio-collaboration",
		// Short so the test does not wait out a production backoff.
		ReattachBackoff: 5 * time.Millisecond,
		// Short so the post-reattach Call below surfaces as a TIMEOUT (proving the
		// request was published) rather than making the test slow.
		RequestTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if got := ch.consumeCount(); got != 1 {
		t.Fatalf("consume attaches before the drop = %d, want 1", got)
	}

	// The broker drops the connection.
	ch.dropDeliveries()

	// The supervisor must re-attach on its own.
	deadline := time.Now().Add(2 * time.Second)
	for ch.consumeCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("client never re-attached after the broker dropped the reply consumer (consume attaches = %d, want 2); one broker blip bricks the metadata store for the process lifetime", ch.consumeCount())
		}
		time.Sleep(time.Millisecond)
	}

	// And it must accept work again. A re-attached client PUBLISHES and then waits
	// for a reply the fake never sends, so the correct outcome is an rpc timeout.
	// A client still marked detached would instead reject the call outright,
	// without publishing.
	err = client.Call(context.Background(), PatternFetch, FetchData{ID: "doc-1"}, nil)
	if err == nil {
		t.Fatal("Call after re-attach: want an rpc timeout from the silent fake, got nil")
	}
	if strings.Contains(err.Error(), "consumer closed") {
		t.Fatalf("Call after re-attach failed fast as though still detached: %v", err)
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("Call after re-attach = %v, want an rpc timeout (proving it published)", err)
	}
}
