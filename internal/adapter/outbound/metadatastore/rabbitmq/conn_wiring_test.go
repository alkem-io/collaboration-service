package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	if s.consumeErr != nil {
		return nil, s.consumeErr
	}
	s.deliveries = make(chan amqp.Delivery, 1)
	return s.deliveries, nil
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
