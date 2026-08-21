package lifecycle

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// fakeChannel records what Connect asked the broker to do and can fail any one
// step, so each failure branch — every one of which must close what it has
// already opened — is reachable without a live broker.
type declaredQueue struct {
	name    string
	durable bool
	args    amqp.Table
}

type fakeChannel struct {
	mu sync.Mutex

	declared     string
	durable      bool
	prefetch     int
	consumeQueue string
	autoAck      bool
	closed       int

	declarations []declaredQueue
	// calls records setup verbs in order, so ordering claims are asserted rather
	// than merely asserted-to-have-happened.
	calls []string

	// declaredMessages is the message count QueueDeclare reports, per queue. Depths
	// differ per queue so a reading that is hardcoded, or taken from the wrong
	// queue, cannot pass.
	declaredMessages map[string]int
	declareErr       error
	qosErr           error
	consumeErr       error

	deliveries chan amqp.Delivery
}

func (f *fakeChannel) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeChannel) declaredQueues() []declaredQueue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]declaredQueue(nil), f.declarations...)
}

func (f *fakeChannel) QueueDeclare(name string, durable, _, _, _ bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	f.declarations = append(f.declarations, declaredQueue{name: name, durable: durable, args: args})
	f.calls = append(f.calls, "declare:"+name)
	msgs := f.declaredMessages[name]
	defer f.mu.Unlock()
	f.declared, f.durable = name, durable
	return amqp.Queue{Name: name, Messages: msgs}, f.declareErr
}

func (f *fakeChannel) Qos(prefetchCount, _ int, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prefetch = prefetchCount
	f.calls = append(f.calls, "qos")
	return f.qosErr
}

func (f *fakeChannel) Consume(queue, _ string, autoAck, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeQueue, f.autoAck = queue, autoAck
	f.calls = append(f.calls, "consume")
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	f.deliveries = make(chan amqp.Delivery, 4)
	return f.deliveries, nil
}

// deliver pushes a delivery onto whatever stream Consume most recently opened.
func (f *fakeChannel) deliver(d amqp.Delivery) {
	f.mu.Lock()
	ch := f.deliveries
	f.mu.Unlock()
	if ch != nil {
		ch <- d
	}
}

// errTestBroker stands in for any broker-side refusal.
var errTestBroker = errors.New("broker said no")

func (f *fakeChannel) Cancel(_ string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "cancel")
	return nil
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	// A real channel close ends the delivery stream; the consume loop's only exit
	// is that stream closing, so the fake must do the same or the supervisor is
	// never reached.
	if f.deliveries != nil {
		close(f.deliveries)
		f.deliveries = nil
	}
	return nil
}

func (f *fakeChannel) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeConn struct {
	ch *fakeChannel

	// The supervisor closes a dead session's connection from its own goroutine
	// while Close may be closing the same one from the caller's, exactly as a real
	// *amqp.Connection tolerates. The counter has to be safe for that.
	mu         sync.Mutex
	closed     int
	channelErr error
}

// refuseChannels makes every subsequent Channel call fail, as a broker that has
// gone away does. Guarded: the supervisor reads this from its own goroutine.
func (f *fakeConn) refuseChannels(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channelErr = err
}

func (f *fakeConn) Channel() (brokerChannel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channelErr != nil {
		return nil, f.channelErr
	}
	return f.ch, nil
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeConn) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// withFakeBroker swaps the dial factory for the duration of one test.
func withFakeBroker(t *testing.T, conn *fakeConn, dialErr error) *atomic.Int64 {
	t.Helper()
	var dials atomic.Int64
	prev := dialBroker
	dialBroker = func(string) (brokerConn, error) {
		dials.Add(1)
		if dialErr != nil {
			return nil, dialErr
		}
		return conn, nil
	}
	t.Cleanup(func() { dialBroker = prev })
	return &dials
}

// connectForTopologyTest connects against a fake broker and returns the channel
// it wired, so the topology assertions below each start from a real Connect.
func connectForTopologyTest(t *testing.T, queue string) *fakeChannel {
	t.Helper()
	ch := &fakeChannel{}
	withFakeBroker(t, &fakeConn{ch: ch}, nil)
	c, err := Connect(Config{URL: "amqp://stub", Queue: queue}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return ch
}

func declaredByName(t *testing.T, ch *fakeChannel) map[string]declaredQueue {
	t.Helper()
	byName := map[string]declaredQueue{}
	for _, d := range ch.declaredQueues() {
		byName[d.name] = d
	}
	return byName
}

// TestConnectDeclaresTheWholeTopologyDurablyAsQuorumQueues pins the queue set and
// the two properties that are correctness requirements rather than preferences:
// durability (a broker restart must not vaporise a pending document.deleted) and
// quorum type (classic dead-lettering is at-most-once, so the poison hop could
// silently vanish).
//
// The MAIN queue's argument table is a CROSS-REPO CONTRACT, and this is the test
// that fails first if it drifts. `server` declares the same queue (server:
// src/core/microservices/microservices.module.ts). An inequivalent redeclaration
// fails PRECONDITION_FAILED and whichever side declares second DOES NOT START —
// and because queue arguments are immutable, the fix is deleting and recreating
// the queue, not redeploying. Changing this table means changing both repos in
// lockstep.
func TestConnectDeclaresTheWholeTopologyDurablyAsQuorumQueues(t *testing.T) {
	byName := declaredByName(t, connectForTopologyTest(t, "lifecycle-q"))

	if len(byName) != 2 {
		t.Fatalf("declared %d queues (%v), want exactly 2: the main queue and its DLQ", len(byName), byName)
	}
	for _, want := range []string{"lifecycle-q", "lifecycle-q.dlq"} {
		d, ok := byName[want]
		if !ok {
			t.Fatalf("queue %q was never declared", want)
		}
		if !d.durable {
			t.Fatalf("queue %q is not durable; events would be lost on a broker restart", want)
		}
		if d.args["x-queue-type"] != "quorum" {
			t.Fatalf("queue %q is not a quorum queue (args=%v); classic dead-lettering is at-most-once", want, d.args)
		}
	}

	// The main queue: quorum + unlimited deliveries + a DLX pointing at the DLQ.
	//
	// The int32 assertions pin a CONVENTION, not a broker requirement: 4.0.5
	// normalizes integer widths. Pinning the width keeps the two repos writing the
	// same thing; it does not model PRECONDITION_FAILED.
	main := byName["lifecycle-q"].args
	if len(main) != 4 {
		t.Fatalf("main queue args = %v, want EXACTLY {x-queue-type, x-delivery-limit, x-dead-letter-exchange, x-dead-letter-routing-key} — every entry is mirrored by server", main)
	}
	if main["x-delivery-limit"] != int32(-1) {
		t.Fatalf("main x-delivery-limit = %v (%T), want int32(-1). RabbitMQ 4.0 defaults quorum queues to 20, and a transient close failure is a REQUEUE — every requeue is another delivery, so at the limit a document.deleted is dropped",
			main["x-delivery-limit"], main["x-delivery-limit"])
	}
	if main["x-dead-letter-exchange"] != "" {
		t.Fatalf("main x-dead-letter-exchange = %v, want the default exchange (empty string)", main["x-dead-letter-exchange"])
	}
	if main["x-dead-letter-routing-key"] != "lifecycle-q.dlq" {
		t.Fatalf("main x-dead-letter-routing-key = %v, want the DLQ. Without it Nack(requeue=false) DISCARDS the poison message instead of recording it",
			main["x-dead-letter-routing-key"])
	}

	// The DLQ: terminal. No TTL, no dead-lettering, and unlimited deliveries.
	dlq := byName["lifecycle-q.dlq"].args
	if len(dlq) != 2 {
		t.Fatalf("DLQ args = %v, want EXACTLY {x-queue-type, x-delivery-limit}; a TTL or a DLX here would move poison somewhere instead of holding it for a human", dlq)
	}
	if dlq["x-delivery-limit"] != int32(-1) {
		t.Fatalf("DLQ x-delivery-limit = %v (%T), want int32(-1). Nothing consumes this queue, but the management UI's Get-with-requeue issues basic.get, and that counts as a delivery — twenty operator inspections and the 21st drops the record, because the DLQ has no dead-letter route",
			dlq["x-delivery-limit"], dlq["x-delivery-limit"])
	}
}

// TestConnectBoundsPrefetchAndDeclaresBeforeConsuming pins the ORDER of the setup
// verbs, not merely that they happened. Qos applied after Consume does not bound
// the first deliveries, so the broker streams its whole backlog into memory the
// instant the consumer attaches; and consuming before the retry queues exist means
// the first failing event has nowhere to transfer to.
func TestConnectBoundsPrefetchAndDeclaresBeforeConsuming(t *testing.T) {
	ch := connectForTopologyTest(t, "lifecycle-q")

	log := ch.callLog()
	consumeAt := -1
	for i, call := range log {
		if call == "consume" {
			consumeAt = i
		}
	}
	if consumeAt < 0 {
		t.Fatalf("Consume was never called; call log = %v", log)
	}
	for _, must := range []string{
		"declare:lifecycle-q",
		"declare:lifecycle-q.dlq",
		"qos",
	} {
		at := -1
		for i, call := range log {
			if call == must {
				at = i
				break
			}
		}
		if at < 0 || at > consumeAt {
			t.Errorf("%q happened at %d, Consume at %d — it must precede Consume; call log = %v", must, at, consumeAt, log)
		}
	}

	// The invariant is that a prefetch is APPLIED at all — 0 means unlimited in
	// AMQP, so the broker streams its whole backlog into memory the instant the
	// consumer attaches. The exact default is an operational choice, covered by
	// TestConnectHonoursConfiguredPrefetch on the configured branch.
	if ch.prefetch <= 0 {
		t.Errorf("prefetch = %d; a zero/negative prefetch is UNLIMITED in AMQP, which is the unbounded backlog this guards against", ch.prefetch)
	}
	if ch.autoAck {
		t.Error("Consume used autoAck; a lifecycle event must be acked only after the room is closed, or a crash silently drops it and leaves a live room on a deleted document")
	}
	if ch.consumeQueue != "lifecycle-q" {
		t.Errorf("consuming %q, want the declared lifecycle queue", ch.consumeQueue)
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
			conn := &fakeConn{ch: tc.ch}
			conn.refuseChannels(tc.channelErr)
			withFakeBroker(t, conn, nil)

			if _, err := Connect(Config{URL: "amqp://stub", Queue: "q"}, &fakeManager{}, zap.NewNop()); err == nil {
				t.Fatal("Connect must fail when the broker rejects a setup step")
			}
			if conn.closeCount() != 1 {
				t.Fatalf("connection closed %d times, want 1; a failed Connect that leaks its connection exhausts broker file descriptors during exactly the instability that causes retries", conn.closeCount())
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

// TestCloseTearsDownChannelAndConnectionAndStopsSupervising covers Close on two
// counts. It must release the channel and connection — and it must STOP the
// supervisor. The supervisor's job is to re-attach whenever the delivery stream
// ends, and Close ends it by closing the channel; if Close did not also stand the
// supervisor down, shutdown would look exactly like a broker blip and the consumer
// would dial its way back up forever behind a process that is trying to exit.
func TestCloseTearsDownChannelAndConnectionAndStopsSupervising(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{ch: ch}
	dials := withFakeBroker(t, conn, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", ReattachBackoff: time.Millisecond}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("Connect dialled %d times, want 1", got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The supervisor closes the dead session too, so the counts are "at least once"
	// rather than exactly once — closing an already-closed AMQP channel is a no-op
	// error, and the invariant is release, not arithmetic.
	waitFor(t, "channel and connection released", func() bool {
		return ch.closeCount() >= 1 && conn.closeCount() >= 1
	})

	// Well past several backoffs: if the supervisor were still running it would have
	// re-dialled by now.
	time.Sleep(50 * time.Millisecond)
	if got := dials.Load(); got != 1 {
		t.Fatalf("dialled %d times after Close; the supervisor kept re-attaching through shutdown", got)
	}

	// A Consumer with nothing wired must not panic on Close.
	if err := (&Consumer{}).Close(); err != nil {
		t.Fatalf("Close on an unwired Consumer: %v", err)
	}
}

// waitFor polls cond until it holds or the test times out. Used where the
// supervisor goroutine, not the test goroutine, makes the state true.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// fakeAcker records ack/nack decisions. amqp.Delivery.Acknowledger is an
// interface precisely so handlers can be tested this way — the one seam
// amqp091-go does provide.
type fakeAcker struct {
	requeues []bool
	mu       sync.Mutex
	acks     int
	nacks    int
	ackErr   error
	nackErr  error
}

func (f *fakeAcker) Ack(_ uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	return f.ackErr
}

// Nack records the REQUEUE FLAG, not just the count. requeue=true (retry on the
// main queue) and requeue=false (dead-letter as poison) are opposite outcomes for
// an event, and a fake that counted only "a nack happened" would let them swap
// without a single test noticing.
func (f *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks++
	f.requeues = append(f.requeues, requeue)
	return f.nackErr
}

func (f *fakeAcker) Reject(_ uint64, _ bool) error { return nil }

func (f *fakeAcker) snapshot() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acks, f.nacks
}

// requeueFlags returns the requeue flag of every Nack, in order.
func (f *fakeAcker) requeueFlags() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.requeues...)
}

// consumerForTest builds a Consumer wired to a fake channel with the real
// topology names, so acknowledgement behaviour is exercised rather than stubbed.
func consumerForTest(t *testing.T, mgr Manager, ch *fakeChannel) *Consumer {
	t.Helper()
	c := &Consumer{
		mgr: mgr, logger: zap.NewNop(), ch: ch, obs: NopObserver{},
		handlerTimeout:  time.Second,
		names:           namesFor("lifecycle-q"),
		reattachBackoff: time.Millisecond,
		closed:          make(chan struct{}),
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestASuccessfulCloseIsAckedAndNothingElse is the happy path: the room was
// closed (or there was no room), so the event is done.
func TestASuccessfulCloseIsAckedAndNothingElse(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})
	acker := &fakeAcker{}
	c := consumerForTest(t, &fakeManager{}, &fakeChannel{})

	c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

	acks, nacks := acker.snapshot()
	if acks != 1 || nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want exactly one ack", acks, nacks)
	}
}

// TestATransientCloseFailureIsRequeuedNotDeadLettered is the discriminating test
// for how a transient failure is retried.
//
// A live room that will not accept the close is TRANSIENT: it may take the close
// on the next delivery. Rejecting it would route a recoverable event to the
// diagnostic DLQ, where nothing consumes it and the document keeps a live room
// forever. requeue=true is what makes the broker redeliver it.
//
// Non-vacuity: flip the flag to false and the requeue assertion fails while the
// nack count stays 1 — which is exactly the mistake a count-only fake would miss.
func TestATransientCloseFailureIsRequeuedNotDeadLettered(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})
	acker := &fakeAcker{}
	c := consumerForTest(t, &fakeManager{closeErr: errors.New("room busy")}, &fakeChannel{})

	c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

	acks, nacks := acker.snapshot()
	if acks != 0 || nacks != 1 {
		t.Fatalf("acks=%d nacks=%d, want exactly one nack and no ack; acking would drop a document that still has a live room", acks, nacks)
	}
	if flags := acker.requeueFlags(); len(flags) != 1 || !flags[0] {
		t.Fatalf("requeue flags = %v, want [true]; requeue=false dead-letters a recoverable event into a queue nothing consumes", flags)
	}
}

// TestAnUnactionableEnvelopeIsRejectedToTheDeadLetterQueue: an envelope this
// service can never act on is recorded, not swallowed and not retried forever.
//
// requeue=false is what dead-letters it via the main queue's DLX. Requeuing it
// instead would spin the same poison through the consumer indefinitely, and
// acking it would make a producer/consumer shape mismatch invisible.
func TestAnUnactionableEnvelopeIsRejectedToTheDeadLetterQueue(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"not an envelope at all", []byte("{{{")},
		{"a pattern outside the contract", eventBody(t, "some.other.event", map[string]any{"id": "x"})},
		{"the right pattern with a malformed payload", eventBody(t, PatternDocumentDeleted, map[string]any{"id": ""})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acker := &fakeAcker{}
			c := consumerForTest(t, &fakeManager{}, &fakeChannel{})

			c.processOne(amqp.Delivery{Body: tc.body, Acknowledger: acker, DeliveryTag: 1})

			acks, nacks := acker.snapshot()
			if acks != 0 || nacks != 1 {
				t.Fatalf("acks=%d nacks=%d, want exactly one nack; acking makes a producer/consumer mismatch vanish", acks, nacks)
			}
			if flags := acker.requeueFlags(); len(flags) != 1 || flags[0] {
				t.Fatalf("requeue flags = %v, want [false]; requeue=true spins poison through the consumer forever", flags)
			}
		})
	}
}

// TestTheConsumerReAttachesAfterTheDeliveryStreamEnds is the test for the whole
// point of the supervisor.
//
// Every way an attachment dies — the broker closing the connection, or a channel
// fault — ends the delivery stream, which returns the consume loop. Without the
// supervisor that is terminal: the goroutine exits and the process goes on looking
// perfectly healthy while no document is ever closed again.
//
// So: kill the stream, then assert an event delivered afterwards is still handled.
func TestTheConsumerReAttachesAfterTheDeliveryStreamEnds(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{ch: ch}
	dials := withFakeBroker(t, conn, nil)
	mgr := &fakeManager{}

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", ReattachBackoff: time.Millisecond}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	deliver := func(id string) {
		ch.mu.Lock()
		d := ch.deliveries
		ch.mu.Unlock()
		if d == nil {
			t.Fatalf("no delivery stream open when sending %s", id)
		}
		d <- amqp.Delivery{
			Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": id}),
			Acknowledger: &fakeAcker{},
			DeliveryTag:  1,
		}
	}

	deliver("before")
	waitFor(t, "the first event to be closed", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.closed) == 1
	})

	// Kill the attachment the way recycle does.
	_ = ch.Close()

	waitFor(t, "the supervisor to re-attach", func() bool { return dials.Load() >= 2 })
	// The dead session's connection is released before re-attaching. Nothing else
	// closes it — the test only closed the channel — so a supervisor that dropped
	// the reference would leak one connection per blip, during exactly the broker
	// instability that produces the blips.
	if conn.closeCount() == 0 {
		t.Fatal("the supervisor re-attached without releasing the dead session's connection")
	}
	waitFor(t, "a fresh delivery stream", func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		return ch.deliveries != nil
	})

	deliver("after")
	waitFor(t, "an event delivered after the re-attach to be closed", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.closed) == 2
	})
}

// recordingObserver captures the operational signals the consumer emits.
type recordingObserver struct {
	mu          sync.Mutex
	depths      map[string]int
	deadLetters []string
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{depths: map[string]int{}}
}

func (o *recordingObserver) QueueReadyDepth(queue string, ready int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.depths[queue] = ready
}

func (o *recordingObserver) EventDeadLettered(pattern string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deadLetters = append(o.deadLetters, pattern)
}

func (o *recordingObserver) deadLetterSnapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.deadLetters...)
}

func (o *recordingObserver) snapshot() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	d := map[string]int{}
	for k, v := range o.depths {
		d[k] = v
	}
	return d
}

// TestQueueDepthIsPolledForEveryQueueInTheTopology asserts the level signal covers
// the whole ladder, not just the DLQ.
//
// Depth is what a counter cannot give: it only goes up, so the increment that put
// events in the DLQ scrolls out of the alert window while the events stay there.
// Every tier is polled because per-tier depth is also how long things have been
// failing, and AMQP has no message-age reading short of the management API.
func TestQueueDepthIsPolledForEveryQueueInTheTopology(t *testing.T) {
	obs := newRecordingObserver()
	// Distinct depths per queue, so a reading that is hardcoded — or taken from the
	// wrong queue — cannot pass.
	depths := map[string]int{
		"lifecycle-q":     11,
		"lifecycle-q.dlq": 55,
	}
	ch := &fakeChannel{declaredMessages: depths}
	conn := &fakeConn{ch: ch}
	withFakeBroker(t, conn, nil)

	c, err := Connect(Config{
		URL: "amqp://stub", Queue: "lifecycle-q",
		DepthPollInterval: time.Millisecond, Observer: obs,
	}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	waitFor(t, "every queue in the topology to report a depth", func() bool {
		got := obs.snapshot()
		for q := range depths {
			if _, ok := got[q]; !ok {
				return false
			}
		}
		return true
	})

	got := obs.snapshot()
	for q, want := range depths {
		if got[q] != want {
			t.Errorf("depth for %s = %d, want the broker's reported %d", q, got[q], want)
		}
	}
}

// TestDepthPollIntervalResolution pins the three-way meaning of the configured
// interval. Asserted on the resolver rather than by waiting: a test that watches
// for polls over a short window cannot tell "disabled" from "every 30 seconds",
// and would pass against a resolver that quietly ignored the operator's choice.
func TestDepthPollIntervalResolution(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"unset takes the default", 0, DefaultDepthPollInterval},
		{"configured is honoured", 5 * time.Second, 5 * time.Second},
		{"negative disables polling", -1, -1},
	} {
		if got := resolveDepthPollInterval(tc.in); got != tc.want {
			t.Errorf("%s: resolveDepthPollInterval(%v) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
	}
	if resolveDepthPollInterval(-1) > 0 {
		t.Error("a negative interval resolved to a positive one; Connect starts the poller on >0, so polling could not be turned off")
	}
}

// TestDepthPollingCanBeDisabled asserts the resolved decision is acted on: a
// disabled poll starts no poller (and does not panic on a negative ticker).
func TestDepthPollingCanBeDisabled(t *testing.T) {
	obs := newRecordingObserver()
	ch := &fakeChannel{declaredMessages: map[string]int{"lifecycle-q": 4}}
	withFakeBroker(t, &fakeConn{ch: ch}, nil)

	c, err := Connect(Config{
		URL: "amqp://stub", Queue: "lifecycle-q",
		DepthPollInterval: -1, Observer: obs,
	}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	time.Sleep(20 * time.Millisecond)
	if depths := obs.snapshot(); len(depths) != 0 {
		t.Fatalf("polling was disabled but reported %v", depths)
	}
}

// TestOnlyTheDeadLetterQueueRaisesTheDeadLetterSignal asserts a hop onto the retry
// ladder is not reported as a dead-lettering. The DLQ signal is the escalation
// one — it means an event will not be retried again without a person — so raising
// it for an ordinary retry would make it worthless.
func TestOnlyTheDeadLetterQueueRaisesTheDeadLetterSignal(t *testing.T) {
	obs := newRecordingObserver()
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{closeErr: errors.New("backend down")}, ch)
	c.obs = obs

	// No attempt header: this is the first failure, so it goes to the 30s tier.
	c.processOne(amqp.Delivery{
		Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"}),
		Acknowledger: &fakeAcker{}, DeliveryTag: 1,
	})

	if got := obs.deadLetterSnapshot(); len(got) != 0 {
		t.Fatalf("a requeued transient failure was reported as a dead-lettering: %v", got)
	}
}

// TestTheSupervisorKeepsTryingWhileTheBrokerRefuses asserts a failed re-attach is
// retried rather than abandoned. A broker that is down for a minute during a
// rollout must not permanently stop lifecycle processing — that is the same
// silent-death failure the supervisor exists to prevent, just moved one step later.
func TestTheSupervisorKeepsTryingWhileTheBrokerRefuses(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{ch: ch}
	dials := withFakeBroker(t, conn, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", ReattachBackoff: time.Millisecond}, &fakeManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// The broker starts refusing channels, then the delivery stream ends.
	conn.refuseChannels(errTestBroker)
	_ = ch.Close()

	// Several attempts, all failing.
	waitFor(t, "the supervisor to retry a refusing broker", func() bool { return dials.Load() >= 4 })

	// It recovers when the broker does.
	conn.refuseChannels(nil)
	waitFor(t, "a fresh delivery stream once the broker recovers", func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		return ch.deliveries != nil
	})
}

// TestADepthPollFailureDoesNotKillTheConsumer asserts a broken poll is reported
// and dropped, not fatal. The poll is observability; a consumer that stopped
// applying deletions because it could not read a queue count would be trading the
// thing that matters for the thing that watches it.
func TestADepthPollFailureDoesNotKillTheConsumer(t *testing.T) {
	ch := &fakeChannel{}
	conn := &fakeConn{ch: ch}
	withFakeBroker(t, conn, nil)
	mgr := &fakeManager{}
	obs := newRecordingObserver()

	c, err := Connect(Config{
		URL: "amqp://stub", Queue: "q",
		DepthPollInterval: time.Millisecond, Observer: obs,
	}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Every re-declare now fails, so every poll fails.
	ch.mu.Lock()
	ch.declareErr = errTestBroker
	ch.mu.Unlock()
	time.Sleep(20 * time.Millisecond)

	// The consumer still applies events.
	ch.deliver(amqp.Delivery{
		Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-alive"}),
		Acknowledger: &fakeAcker{}, DeliveryTag: 1,
	})
	waitFor(t, "the consumer to keep applying events through a failing depth poll", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.closed) == 1
	})
}

// TestBackoffResolutionDefaults pins the reattach delay's resolver. It is the
// only remaining timing knob: the confirm timeout went with the transfer path.
func TestBackoffResolutionDefaults(t *testing.T) {
	if got := resolveReattachBackoff(0); got != DefaultReattachBackoff {
		t.Errorf("resolveReattachBackoff(0) = %v, want %v", got, DefaultReattachBackoff)
	}
	if got := resolveReattachBackoff(2 * time.Second); got != 2*time.Second {
		t.Errorf("resolveReattachBackoff(2s) = %v, want it honoured", got)
	}
}
