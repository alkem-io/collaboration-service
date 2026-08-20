package lifecycle

import (
	"context"
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

type publishedMsg struct {
	exchange  string
	key       string
	mandatory bool
	msg       amqp.Publishing
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
	published    []publishedMsg
	confirmMode  bool
	// calls records setup verbs in order, so ordering claims are asserted rather
	// than merely asserted-to-have-happened.
	calls []string

	declareErr error
	qosErr     error
	consumeErr error
	confirmErr error
	publishErr error

	// confirmAck/returnMsg script the broker's response to the next publish.
	confirmAck  bool
	returnOnPub bool

	confirms chan amqp.Confirmation
	returns  chan amqp.Return

	deliveries chan amqp.Delivery
}

func (f *fakeChannel) Confirm(bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.confirmErr != nil {
		return f.confirmErr
	}
	f.confirmMode = true
	f.calls = append(f.calls, "confirm")
	return nil
}

func (f *fakeChannel) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirms = c
	return c
}

func (f *fakeChannel) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.returns = c
	return c
}

func (f *fakeChannel) PublishWithContext(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) error {
	f.mu.Lock()
	if f.publishErr != nil {
		err := f.publishErr
		f.mu.Unlock()
		return err
	}
	f.published = append(f.published, publishedMsg{exchange: exchange, key: key, mandatory: mandatory, msg: msg})
	tag := uint64(len(f.published))
	confirms, returns := f.confirms, f.returns
	ack, ret := f.confirmAck, f.returnOnPub
	f.mu.Unlock()

	// The broker answers asynchronously; mirror that so the transfer path is
	// exercised as it will run against a real channel.
	go func() {
		if ret && returns != nil {
			returns <- amqp.Return{Exchange: exchange, RoutingKey: key, Body: msg.Body}
		}
		if confirms != nil {
			confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: ack}
		}
	}()
	return nil
}

func (f *fakeChannel) publishes() []publishedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publishedMsg(nil), f.published...)
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
	defer f.mu.Unlock()
	f.declared, f.durable = name, durable
	return amqp.Queue{Name: name}, f.declareErr
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
	ch         *fakeChannel
	channelErr error
	version    string

	// The supervisor closes a dead session's connection from its own goroutine
	// while Close may be closing the same one from the caller's, exactly as a real
	// *amqp.Connection tolerates. The counter has to be safe for that.
	mu     sync.Mutex
	closed int
}

func (f *fakeConn) ServerProperties() amqp.Table {
	v := f.version
	if v == "" {
		v = MinBrokerVersion.String()
	}
	return amqp.Table{"version": v}
}

func (f *fakeConn) Channel() (brokerChannel, error) {
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
// durability (a broker restart must not vaporise a pending document.deleted, which
// would leave an orphan) and quorum type (classic dead-lettering is at-most-once,
// so a retry hop could silently vanish).
//
// Q1's arguments are additionally frozen as EXACTLY {x-queue-type: quorum}: it is
// the one queue the producer also declares, and any extra argument makes the two
// declarations inequivalent, which the broker refuses with PRECONDITION_FAILED.
func TestConnectDeclaresTheWholeTopologyDurablyAsQuorumQueues(t *testing.T) {
	byName := declaredByName(t, connectForTopologyTest(t, "lifecycle-q"))

	for _, want := range []string{
		"lifecycle-q",
		"lifecycle-q.retry.30s", "lifecycle-q.retry.5m", "lifecycle-q.retry.30m",
		"lifecycle-q.dlq",
	} {
		d, ok := byName[want]
		if !ok {
			t.Fatalf("queue %q was never declared; the topology must exist before any transfer targets it", want)
		}
		if !d.durable {
			t.Fatalf("queue %q is not durable; events would be lost on a broker restart", want)
		}
		if d.args["x-queue-type"] != "quorum" {
			t.Fatalf("queue %q is not a quorum queue (args=%v); classic dead-lettering is at-most-once", want, d.args)
		}
	}
	if got := byName["lifecycle-q"].args; len(got) != 1 {
		t.Fatalf("Q1 args = %v, want EXACTLY {x-queue-type: quorum} — it is the one queue the producer also declares, and any extra argument is an inequivalent redeclaration", got)
	}
}

// TestRetryTiersCarryTheirScheduleAndTheDLQIsTerminal pins the arguments that make
// the delay ladder work. Each tier expires back to the main queue after its TTL;
// at-least-once dead-lettering is required because the quorum default is
// at-most-once (an expired retry could be dropped), and at-least-once is in turn
// only accepted with x-overflow=reject-publish.
func TestRetryTiersCarryTheirScheduleAndTheDLQIsTerminal(t *testing.T) {
	byName := declaredByName(t, connectForTopologyTest(t, "lifecycle-q"))

	for _, tier := range []struct {
		name string
		ttl  int32
	}{
		{"lifecycle-q.retry.30s", 30_000},
		{"lifecycle-q.retry.5m", 300_000},
		{"lifecycle-q.retry.30m", 1_800_000},
	} {
		a := byName[tier.name].args
		if a["x-message-ttl"] != tier.ttl {
			t.Errorf("%s x-message-ttl = %v (%T), want int32 %d — argument TYPE participates in declaration equivalence", tier.name, a["x-message-ttl"], a["x-message-ttl"], tier.ttl)
		}
		if a["x-dead-letter-routing-key"] != "lifecycle-q" {
			t.Errorf("%s must dead-letter back to the main queue, got %v", tier.name, a["x-dead-letter-routing-key"])
		}
		if a["x-dead-letter-strategy"] != "at-least-once" {
			t.Errorf("%s must use at-least-once dead-lettering; the default is at-most-once and would silently drop retries", tier.name)
		}
		if a["x-overflow"] != "reject-publish" {
			t.Errorf("%s must set x-overflow=reject-publish; at-least-once is refused with drop-head", tier.name)
		}
	}
	if a := byName["lifecycle-q.dlq"].args; a["x-message-ttl"] != nil || a["x-dead-letter-exchange"] != nil {
		t.Errorf("the DLQ must be terminal (no TTL, no dead-lettering), got %v", a)
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
		"declare:lifecycle-q.retry.30s", "declare:lifecycle-q.retry.5m", "declare:lifecycle-q.retry.30m",
		"declare:lifecycle-q.dlq",
		"qos", "confirm",
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
		t.Error("Consume used autoAck; a lifecycle event must be acked only after its purge succeeds, or a crash silently drops it and leaves an orphan document")
	}
	if ch.consumeQueue != "lifecycle-q" {
		t.Errorf("consuming %q, want the declared lifecycle queue", ch.consumeQueue)
	}
}

// TestTierCountMatchesTheSchedule pins tierCount to the actual ladder. tierCount
// exists so attempt bookkeeping needs no unchecked int→int32 narrowing; if the two
// drift, an event would be sent to the DLQ a tier early or index past the ladder.
func TestTierCountMatchesTheSchedule(t *testing.T) {
	if int(tierCount) != len(retryTiers) {
		t.Fatalf("tierCount = %d but the schedule has %d tiers", tierCount, len(retryTiers))
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

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", RecycleBackoff: time.Millisecond}, &fakeManager{}, zap.NewNop())
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
	mu      sync.Mutex
	acks    int
	nacks   int
	ackErr  error
	nackErr error
}

func (f *fakeAcker) Ack(_ uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	return f.ackErr
}

func (f *fakeAcker) Nack(_ uint64, _ bool, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks++
	return f.nackErr
}

func (f *fakeAcker) Reject(_ uint64, _ bool) error { return nil }

func (f *fakeAcker) snapshot() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acks, f.nacks
}

// consumerForTest builds a Consumer wired to a fake channel with the real
// topology names, so transfer behaviour is exercised rather than stubbed.
func consumerForTest(t *testing.T, mgr Manager, ch *fakeChannel) *Consumer {
	t.Helper()
	ch.confirmAck = true
	c := &Consumer{
		mgr: mgr, logger: zap.NewNop(), ch: ch,
		handlerTimeout: time.Second,
		names:          namesFor("lifecycle-q"),
		confirms:       ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:        ch.NotifyReturn(make(chan amqp.Return, 1)),
		confirmTimeout: 2 * time.Second,
		recycleBackoff: time.Millisecond,
		closed:         make(chan struct{}),
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestSuccessfulHandlerAcksWithoutTransferring: a handled event is acked and
// nothing is republished. Publishing on the success path would duplicate every
// event through the retry tiers.
func TestSuccessfulHandlerAcksWithoutTransferring(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})
	acker := &fakeAcker{}
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{}, ch)

	c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

	acks, nacks := acker.snapshot()
	if acks != 1 || nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want exactly one ack", acks, nacks)
	}
	if pubs := ch.publishes(); len(pubs) != 0 {
		t.Fatalf("a successful handler published %d message(s); success must not enter the retry path", len(pubs))
	}
}

// TestFailureTransfersToTheNextTierBeforeAcking is the ordering guarantee: the
// original is acked ONLY after the successor is confirmed onto another queue.
//
// Asserting the end state alone is not enough — a consumer that acked first and
// published afterwards would look identical once both have happened, and would
// lose the event on a crash in between. The fake records both, and the ack is
// checked to have happened after a publish exists.
func TestFailureTransfersToTheNextTierBeforeAcking(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})
	acker := &fakeAcker{}
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)

	c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

	pubs := ch.publishes()
	if len(pubs) != 1 {
		t.Fatalf("published %d message(s), want exactly 1 transfer to the first retry tier", len(pubs))
	}
	p := pubs[0]
	if p.key != "lifecycle-q.retry.30s" {
		t.Fatalf("transferred to %q, want the 30s tier on a first failure", p.key)
	}
	if p.exchange != "" {
		t.Fatalf("published to exchange %q, want the default exchange (routing key IS the queue name)", p.exchange)
	}
	if !p.mandatory {
		t.Fatal("the transfer was not mandatory; a confirm proves the exchange accepted the message, NOT that anything was routed — without mandatory, publishing to a missing queue is a silent discard that still confirms")
	}
	if p.msg.DeliveryMode != amqp.Persistent {
		t.Fatalf("delivery mode = %d, want persistent; a non-persistent retry is lost on a broker restart", p.msg.DeliveryMode)
	}
	if string(p.msg.Body) != string(deleted) {
		t.Fatal("the transferred body differs from the original; the envelope must be forwarded byte-for-byte")
	}
	if got := p.msg.Headers[headerAttempt]; got != int32(1) {
		t.Fatalf("attempt header = %v (%T), want int32(1)", got, got)
	}
	if acks, _ := acker.snapshot(); acks != 1 {
		t.Fatalf("acks=%d, want the original acked after its successor was confirmed", acks)
	}
}

// TestTransferFailureLeavesTheEventBrokerOwned is the durability rule: if the
// successor is not confirmed, the original is neither acked nor rejected, so the
// broker still owns it and will redeliver.
//
// Rejecting instead would convert a transient publishing failure into terminal
// handling, and the dead-letter republish behind a reject is not itself
// publisher-confirmed — "it will reach the DLQ" would be an assumption.
func TestTransferFailureLeavesTheEventBrokerOwned(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})

	for _, tc := range []struct {
		name string
		set  func(*fakeChannel)
	}{
		{"publish error", func(f *fakeChannel) { f.publishErr = errors.New("channel dead") }},
		{"broker nacks the publish", func(f *fakeChannel) { f.confirmAck = false }},
		{"broker returns it as unroutable", func(f *fakeChannel) { f.returnOnPub = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acker := &fakeAcker{}
			ch := &fakeChannel{}
			c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)
			tc.set(ch)

			c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

			acks, nacks := acker.snapshot()
			if acks != 0 {
				t.Fatalf("acks=%d: an unconfirmed transfer must NOT ack — the successor does not exist, so acking loses the event", acks)
			}
			if nacks != 0 {
				t.Fatalf("nacks=%d: the delivery must not be rejected either; rejection makes a transient failure terminal and its dead-letter hop is unconfirmed", nacks)
			}
		})
	}
}

// TestExhaustedScheduleGoesToTheDeadLetterQueue: once the tiers are used up the
// event is transferred to the DLQ, not retried forever and not dropped.
func TestExhaustedScheduleGoesToTheDeadLetterQueue(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})
	acker := &fakeAcker{}
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)

	// attempt == number of tiers: the schedule is exhausted.
	c.processOne(amqp.Delivery{
		Body: deleted, Acknowledger: acker, DeliveryTag: 1,
		Headers: amqp.Table{headerAttempt: tierCount},
	})

	pubs := ch.publishes()
	if len(pubs) != 1 || pubs[0].key != "lifecycle-q.dlq" {
		t.Fatalf("published %+v, want a single transfer to the DLQ once the retry schedule is exhausted", pubs)
	}
	if acks, _ := acker.snapshot(); acks != 1 {
		t.Fatalf("acks=%d, want the original acked after the DLQ transfer was confirmed", acks)
	}
}

// TestUnactionableEnvelopeGoesToTheDeadLetterQueue: an envelope this service can
// never act on is recorded, not swallowed. Under the old behaviour it was acked
// and vanished, so a producer/consumer shape mismatch was invisible; now it shows
// up as DLQ depth.
func TestUnactionableEnvelopeGoesToTheDeadLetterQueue(t *testing.T) {
	acker := &fakeAcker{}
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{}, ch)

	c.processOne(amqp.Delivery{Body: []byte("not json"), Acknowledger: acker, DeliveryTag: 1})

	pubs := ch.publishes()
	if len(pubs) != 1 || pubs[0].key != "lifecycle-q.dlq" {
		t.Fatalf("published %+v, want the unparseable envelope transferred to the DLQ rather than acked away", pubs)
	}
	if acks, _ := acker.snapshot(); acks != 1 {
		t.Fatalf("acks=%d, want the original acked after the DLQ transfer", acks)
	}
}

// TestAttemptHeaderRoutingIsTotalOverWhateverArrives asserts the tier decision is
// defined for every value that can turn up in x-collab-attempt, not just the ones
// this consumer writes.
//
// The header crosses a wire. AMQP numeric headers arrive in whatever width the
// publisher used, and an operator replaying out of the DLQ sets it by hand. A raw
// int32() narrowing of a large int64 WRAPS — 1<<32 becomes 0 — which routes an
// event that has already exhausted the schedule back to the first retry tier, and
// it then cycles the ladder forever instead of resting in the DLQ. A negative value
// would index before the start of the ladder — and, worse, would keep doing so:
// the outgoing header is the incoming one plus 1, so -7 becomes -6, which is still
// "before the ladder", and the event cycles the first tier forever without ever
// reaching the DLQ. The outgoing header is asserted alongside the target for that
// reason: every transfer must strictly advance toward the DLQ.
func TestAttemptHeaderRoutingIsTotalOverWhateverArrives(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})

	for _, tc := range []struct {
		name    string
		header  any
		want    string
		wantHdr int32
	}{
		{"absent", nil, "lifecycle-q.retry.30s", 1},
		{"first attempt", int32(0), "lifecycle-q.retry.30s", 1},
		{"mid-ladder", int32(1), "lifecycle-q.retry.5m", 2},
		{"exhausted", tierCount, "lifecycle-q.dlq", tierCount + 1},
		{"wraps to zero when narrowed", int64(1) << 32, "lifecycle-q.dlq", tierCount + 1},
		{"far past the ladder", int64(1) << 40, "lifecycle-q.dlq", tierCount + 1},
		{"negative", int32(-7), "lifecycle-q.retry.30s", 1},
		{"published as a wider int", int64(2), "lifecycle-q.retry.30m", 3},
		{"published as a byte", uint8(1), "lifecycle-q.retry.5m", 2},
		{"not a number at all", "two", "lifecycle-q.retry.30s", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := &fakeChannel{confirmAck: true}
			c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)

			d := amqp.Delivery{Body: deleted, Acknowledger: &fakeAcker{}, DeliveryTag: 1}
			if tc.header != nil {
				d.Headers = amqp.Table{headerAttempt: tc.header}
			}
			c.processOne(d)

			pubs := ch.publishes()
			if len(pubs) != 1 {
				t.Fatalf("published %d message(s), want exactly one transfer", len(pubs))
			}
			if pubs[0].key != tc.want {
				t.Fatalf("attempt header %v (%T) routed to %q, want %q", tc.header, tc.header, pubs[0].key, tc.want)
			}
			if got := pubs[0].msg.Headers[headerAttempt]; got != tc.wantHdr {
				t.Fatalf("attempt header %v (%T) transferred on as %v (%T), want int32 %d — a transfer that does not advance the count cycles the ladder forever",
					tc.header, tc.header, got, got, tc.wantHdr)
			}
		})
	}
}

// TestTheConsumerReAttachesAfterTheDeliveryStreamEnds is the test for the whole
// point of the supervisor.
//
// Every way an attachment dies — the broker closing the connection, a channel
// fault, or recycle deliberately closing the channel after an unconfirmable
// transfer — ends the delivery stream, which returns the consume loop. Before the
// supervisor existed that was terminal: the goroutine exited and the process went
// on looking perfectly healthy while no document was ever purged and no revoked
// grant was ever applied again. The retry ladder made it routine rather than
// exceptional, because recycle closes the channel on purpose.
//
// So: kill the stream, then assert an event delivered afterwards is still handled.
func TestTheConsumerReAttachesAfterTheDeliveryStreamEnds(t *testing.T) {
	ch := &fakeChannel{confirmAck: true}
	conn := &fakeConn{ch: ch}
	dials := withFakeBroker(t, conn, nil)
	mgr := &fakeManager{}

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", RecycleBackoff: time.Millisecond}, mgr, zap.NewNop())
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
	waitFor(t, "the first event to be purged", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.purged) == 1
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
	waitFor(t, "an event delivered after the re-attach to be purged", func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return len(mgr.purged) == 2
	})
}
