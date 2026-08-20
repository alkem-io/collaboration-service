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

	// declaredMessages is the message count QueueDeclare reports, per queue. Depths
	// differ per queue so a reading that is hardcoded, or taken from the wrong
	// queue, cannot pass.
	declaredMessages map[string]int
	declareErr       error
	qosErr           error
	consumeErr       error
	confirmErr       error
	publishErr       error

	// confirmAck/returnOnPub script the broker's response to the next publish.
	confirmAck  bool
	returnOnPub bool
	// nackFromPublish, when > 0, acks the first nackFromPublish-1 publishes and
	// nacks every one after — so a multi-publish run can fail at a chosen point
	// without the test racing the loop it is driving.
	nackFromPublish int
	// answersBeforePublishReturns delivers both answers into their channels
	// SYNCHRONOUSLY, before PublishWithContext returns — the state where the
	// connection reader has already dispatched both frames and the caller's select
	// sees two ready channels.
	answersBeforePublishReturns bool

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
	if f.nackFromPublish > 0 && len(f.published) >= f.nackFromPublish {
		ack = false
	}
	sync := f.answersBeforePublishReturns
	f.mu.Unlock()

	// A return always precedes its confirm — that is the frame order AMQP
	// guarantees and amqp091-go preserves, and the transfer path's correctness
	// depends on it.
	answer := func() {
		if ret && returns != nil {
			returns <- amqp.Return{Exchange: exchange, RoutingKey: key, Body: msg.Body}
		}
		if confirms != nil {
			confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: ack}
		}
	}
	if sync {
		answer()
		return nil
	}
	// Otherwise the broker answers asynchronously, as it usually will.
	go answer()
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
	ch      *fakeChannel
	version string

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

func (f *fakeConn) ServerProperties() amqp.Table {
	v := f.version
	if v == "" {
		v = MinBrokerVersion.String()
	}
	return amqp.Table{"version": v}
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
	// Q1's arguments are the frozen cross-repo literal, asserted exactly: it is the
	// one queue the producer also declares, and a difference in the SET or the
	// VALUES — an extra argument, a missing one, a changed number — is an
	// inequivalent redeclaration that stops whichever side declares second.
	//
	// The int32 assertions below pin a CONVENTION, not a broker requirement: 4.0.5
	// normalizes integer widths, so a plain Go int would be accepted by the broker.
	// Pinning the width keeps the two repos writing the same thing; it does not
	// model PRECONDITION_FAILED.
	q1 := byName["lifecycle-q"].args
	if len(q1) != 2 {
		t.Fatalf("Q1 args = %v, want EXACTLY {x-queue-type: quorum, x-delivery-limit: int32(-1)}", q1)
	}
	if q1["x-delivery-limit"] != int32(-1) {
		t.Fatalf("Q1 x-delivery-limit = %v (%T), want int32(-1). RabbitMQ 4.0 defaults quorum queues to 20, and Q1 has no dead-letter exchange, so at the limit a document.deleted is DROPPED rather than diverted. (int32 is this repo's convention; the broker compares the VALUE.)",
			q1["x-delivery-limit"], q1["x-delivery-limit"])
	}
	if dlq := byName["lifecycle-q.dlq"].args; dlq["x-delivery-limit"] != int32(-1) {
		t.Fatalf("DLQ x-delivery-limit = %v (%T), want int32(-1); a replay that fails and closes its channel is a delivery, so repeated failed replays would drop the message the DLQ exists to preserve",
			dlq["x-delivery-limit"], dlq["x-delivery-limit"])
	}
	for _, tier := range []string{"lifecycle-q.retry.30s", "lifecycle-q.retry.5m", "lifecycle-q.retry.30m"} {
		if _, present := byName[tier].args["x-delivery-limit"]; present {
			t.Fatalf("%s carries x-delivery-limit; the tiers keep the broker default deliberately — they have a dead-letter exchange, so the limit diverts rather than drops, and they have no consumer to increment it", tier)
		}
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
			t.Errorf("%s x-message-ttl = %v (%T), want int32 %d (the repo's convention; the VALUE is what the broker compares)", tier.name, a["x-message-ttl"], a["x-message-ttl"], tier.ttl)
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
		mgr: mgr, logger: zap.NewNop(), ch: ch, obs: NopObserver{},
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
		{"published as a short", int16(2), "lifecycle-q.retry.30m", 3},
		{"published as a plain int", int(1), "lifecycle-q.retry.5m", 2},
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

// recordingObserver captures the operational signals the consumer emits.
type recordingObserver struct {
	mu          sync.Mutex
	transfers   []transferSignal
	depths      map[string]int
	deadLetters []deadLetterSignal
}

type deadLetterSignal struct {
	pattern string
	replays int32
}

type transferSignal struct {
	queue     string
	confirmed bool
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{depths: map[string]int{}}
}

func (o *recordingObserver) EventTransferred(queue string, confirmed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transfers = append(o.transfers, transferSignal{queue, confirmed})
}

func (o *recordingObserver) QueueReadyDepth(queue string, ready int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.depths[queue] = ready
}

func (o *recordingObserver) EventDeadLettered(pattern string, replays int32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deadLetters = append(o.deadLetters, deadLetterSignal{pattern, replays})
}

func (o *recordingObserver) deadLetterSnapshot() []deadLetterSignal {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]deadLetterSignal(nil), o.deadLetters...)
}

func (o *recordingObserver) snapshot() ([]transferSignal, map[string]int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	d := map[string]int{}
	for k, v := range o.depths {
		d[k] = v
	}
	return append([]transferSignal(nil), o.transfers...), d
}

// TestEveryTransferIsReported asserts each ladder hop reports where it went and
// whether the broker took it — including the hop that did NOT happen.
//
// The unconfirmed case is the one worth stating. A transfer that is not confirmed
// leaves the delivery unacked and recycles the channel, so nothing else in the
// system records that anything went wrong; without this signal a broker that
// accepts publishes but routes none of them looks, from outside, exactly like a
// quiet day.
func TestEveryTransferIsReported(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})

	for _, tc := range []struct {
		name      string
		attempt   any
		body      []byte
		confirm   bool
		wantQueue string
		wantOK    bool
	}{
		{"first failure", nil, deleted, true, "lifecycle-q.retry.30s", true},
		{"exhausted", tierCount, deleted, true, "lifecycle-q.dlq", true},
		{"unactionable", nil, []byte("not json"), true, "lifecycle-q.dlq", true},
		{"broker nacked the transfer", nil, deleted, false, "lifecycle-q.retry.30s", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := newRecordingObserver()
			ch := &fakeChannel{}
			c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)
			c.obs = obs
			ch.mu.Lock()
			ch.confirmAck = tc.confirm
			ch.mu.Unlock()

			d := amqp.Delivery{Body: tc.body, Acknowledger: &fakeAcker{}, DeliveryTag: 1}
			if tc.attempt != nil {
				d.Headers = amqp.Table{headerAttempt: tc.attempt}
			}
			c.processOne(d)

			transfers, _ := obs.snapshot()
			if len(transfers) != 1 {
				t.Fatalf("reported %d transfers, want exactly 1: %v", len(transfers), transfers)
			}
			if transfers[0].queue != tc.wantQueue || transfers[0].confirmed != tc.wantOK {
				t.Fatalf("reported %+v, want {queue:%s confirmed:%v}", transfers[0], tc.wantQueue, tc.wantOK)
			}
		})
	}
}

// TestSuccessIsNotReportedAsATransfer asserts a handled event emits nothing.
// If success counted as a transfer, the "anything on the ladder" alert would fire
// continuously on a perfectly healthy service and be turned off within a day.
func TestSuccessIsNotReportedAsATransfer(t *testing.T) {
	obs := newRecordingObserver()
	ch := &fakeChannel{confirmAck: true}
	c := consumerForTest(t, &fakeManager{}, ch)
	c.obs = obs

	c.processOne(amqp.Delivery{
		Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"}),
		Acknowledger: &fakeAcker{}, DeliveryTag: 1,
	})

	if transfers, _ := obs.snapshot(); len(transfers) != 0 {
		t.Fatalf("a successfully handled event reported %v; the ladder alert would fire on a healthy service", transfers)
	}
}

// TestQueueDepthIsPolledForEveryQueueInTheTopology asserts the level signal covers
// the whole ladder, not just the DLQ.
//
// Depth is what a counter cannot give: it only goes up, so the increment that put
// events in the DLQ scrolls out of the alert window while the events stay there.
// Every tier is polled because per-tier depth is also how long things have been
// failing — an event in the 30m tier has already survived 30s + 5m — and AMQP has
// no message-age reading short of the management API.
func TestQueueDepthIsPolledForEveryQueueInTheTopology(t *testing.T) {
	obs := newRecordingObserver()
	depths := map[string]int{
		"lifecycle-q":           11,
		"lifecycle-q.retry.30s": 22,
		"lifecycle-q.retry.5m":  33,
		"lifecycle-q.retry.30m": 44,
		"lifecycle-q.dlq":       55,
	}
	ch := &fakeChannel{confirmAck: true, declaredMessages: depths}
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
		_, got := obs.snapshot()
		for q := range depths {
			if _, ok := got[q]; !ok {
				return false
			}
		}
		return true
	})

	_, got := obs.snapshot()
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
	ch := &fakeChannel{confirmAck: true, declaredMessages: map[string]int{"lifecycle-q": 4}}
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
	if _, depths := obs.snapshot(); len(depths) != 0 {
		t.Fatalf("polling was disabled but reported %v", depths)
	}
}

// TestAnAckNeverOutvotesAReturnThatIsAlreadyWaiting closes the race between the
// broker's two answers.
//
// transfer selects on the return channel and the confirm channel. Both may be
// ready at once: a mandatory publish to a missing queue produces basic.return
// followed by basic.ack, and by the time transfer reaches its select the
// connection reader may have dispatched both. Go's select picks at RANDOM between
// two ready cases — protocol ordering buys nothing there — so roughly half the
// time the confirm wins and an unroutable publish reports success. The event is
// then acked and gone: transferred nowhere, deleted from the only queue holding
// it. That is the single worst outcome the whole ladder exists to prevent, and it
// would be intermittent.
//
// The fake delivers both answers synchronously, before the publish call returns,
// which is exactly that state. Repeated, because a single pass could win the
// coin toss: without the drain this fails within a handful of iterations.
func TestAnAckNeverOutvotesAReturnThatIsAlreadyWaiting(t *testing.T) {
	deleted := eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"})

	for i := 0; i < 200; i++ {
		acker := &fakeAcker{}
		ch := &fakeChannel{}
		c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)
		ch.mu.Lock()
		// The broker ACKED the publish and RETURNED it as unroutable.
		ch.confirmAck = true
		ch.returnOnPub = true
		ch.answersBeforePublishReturns = true
		ch.mu.Unlock()

		c.processOne(amqp.Delivery{Body: deleted, Acknowledger: acker, DeliveryTag: 1})

		if acks, _ := acker.snapshot(); acks != 0 {
			t.Fatalf("iteration %d: the delivery was ACKED after an unroutable publish that the broker also confirmed. The event was removed from the only queue holding it and republished nowhere", i)
		}
	}
}

// TestTheDeadLetterSignalCarriesThePriorReplayCount asserts the DLQ signal reports
// how many times a person has already replayed the event.
//
// The attempt count cannot say this. A replay CLEARS it — deliberately, so the
// event gets the whole ladder again rather than returning to the DLQ on its first
// failure — which means every dead-lettering after a replay reports the same
// "attempt 3" as the first one did. Without the replay count, an event someone has
// now sent round the ladder three times into a backend that is still broken looks
// exactly like an event failing for the first time, and the operator repeats a fix
// that has already been shown not to work.
func TestTheDeadLetterSignalCarriesThePriorReplayCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers amqp.Table
		want    int32
	}{
		{"never replayed", amqp.Table{headerAttempt: tierCount}, 0},
		{"replayed once", amqp.Table{headerAttempt: tierCount, headerReplays: int32(1)}, 1},
		{"replayed many times", amqp.Table{headerAttempt: tierCount, headerReplays: int64(4)}, 4},
		{"count is garbage", amqp.Table{headerAttempt: tierCount, headerReplays: "lots"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := newRecordingObserver()
			ch := &fakeChannel{confirmAck: true}
			c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)
			c.obs = obs

			c.processOne(amqp.Delivery{
				Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"}),
				Headers:      tc.headers,
				Acknowledger: &fakeAcker{}, DeliveryTag: 1,
			})

			got := obs.deadLetterSnapshot()
			if len(got) != 1 {
				t.Fatalf("reported %d dead-letterings, want 1: %v", len(got), got)
			}
			if got[0].replays != tc.want {
				t.Fatalf("replays = %d, want %d", got[0].replays, tc.want)
			}
			if got[0].pattern != PatternDocumentDeleted {
				t.Fatalf("pattern = %q, want %q", got[0].pattern, PatternDocumentDeleted)
			}
		})
	}
}

// TestOnlyTheDeadLetterQueueRaisesTheDeadLetterSignal asserts a hop onto the retry
// ladder is not reported as a dead-lettering. The DLQ signal is the escalation
// one — it means an event will not be retried again without a person — so raising
// it for an ordinary retry would make it worthless.
func TestOnlyTheDeadLetterQueueRaisesTheDeadLetterSignal(t *testing.T) {
	obs := newRecordingObserver()
	ch := &fakeChannel{confirmAck: true}
	c := consumerForTest(t, &fakeManager{purgeErr: errors.New("backend down")}, ch)
	c.obs = obs

	// No attempt header: this is the first failure, so it goes to the 30s tier.
	c.processOne(amqp.Delivery{
		Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"}),
		Acknowledger: &fakeAcker{}, DeliveryTag: 1,
	})

	if got := obs.deadLetterSnapshot(); len(got) != 0 {
		t.Fatalf("a hop onto the retry ladder was reported as a dead-lettering: %v", got)
	}
}

// TestRecycleStandsDownWhenTheConsumerCloses asserts a pending recycle does not
// outlive shutdown. recycle waits out a backoff before closing the channel; if
// Close happened during that window and recycle went ahead anyway, it would close
// a channel the shutdown path had already released.
func TestRecycleStandsDownWhenTheConsumerCloses(t *testing.T) {
	ch := &fakeChannel{}
	c := consumerForTest(t, &fakeManager{}, ch)
	c.recycleBackoff = time.Hour // long enough that only Close can end the wait

	done := make(chan struct{})
	go func() { c.recycle(); close(done) }()

	_ = c.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recycle outlived Close; it is still waiting to close a channel shutdown has already released")
	}
}

// TestTheSupervisorKeepsTryingWhileTheBrokerRefuses asserts a failed re-attach is
// retried rather than abandoned. A broker that is down for a minute during a
// rollout must not permanently stop lifecycle processing — that is the same
// silent-death failure the supervisor exists to prevent, just moved one step later.
func TestTheSupervisorKeepsTryingWhileTheBrokerRefuses(t *testing.T) {
	ch := &fakeChannel{confirmAck: true}
	conn := &fakeConn{ch: ch}
	dials := withFakeBroker(t, conn, nil)

	c, err := Connect(Config{URL: "amqp://stub", Queue: "q", RecycleBackoff: time.Millisecond}, &fakeManager{}, zap.NewNop())
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
	ch := &fakeChannel{confirmAck: true}
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
		return len(mgr.purged) == 1
	})
}

// TestATransferGivesUpRatherThanWaitingForever asserts every way the broker can
// go quiet ends the wait as a FAILURE.
//
// Each of these leaves the delivery unacknowledged and recycles the channel, which
// is the safe outcome: the event stays broker-owned and comes back. Treating any
// of them as success would ack an event that was published nowhere; treating any
// as an indefinite wait would freeze the single-threaded consume loop and stop
// every later event behind it.
func TestATransferGivesUpRatherThanWaitingForever(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*Consumer, *fakeChannel)
	}{
		{"the broker never answers", func(c *Consumer, ch *fakeChannel) {
			ch.mu.Lock()
			ch.confirms, ch.returns = nil, nil // published, but nothing comes back
			ch.mu.Unlock()
			c.confirmTimeout = 30 * time.Millisecond
		}},
		{"the confirm channel closes", func(c *Consumer, ch *fakeChannel) {
			c.mu.Lock()
			confirms := c.confirms
			c.mu.Unlock()
			ch.mu.Lock()
			ch.confirms = nil
			ch.mu.Unlock()
			close(confirms)
		}},
		{"the return channel closes", func(c *Consumer, ch *fakeChannel) {
			c.mu.Lock()
			returns := c.returns
			c.mu.Unlock()
			ch.mu.Lock()
			ch.returns = nil
			ch.mu.Unlock()
			close(returns)
		}},
		{"the publish itself fails", func(_ *Consumer, ch *fakeChannel) {
			ch.mu.Lock()
			ch.publishErr = errTestBroker
			ch.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acker := &fakeAcker{}
			ch := &fakeChannel{}
			c := consumerForTest(t, &fakeManager{purgeErr: errTestBroker}, ch)
			tc.setup(c, ch)

			c.processOne(amqp.Delivery{
				Body:         eventBody(t, PatternDocumentDeleted, map[string]any{"id": "doc-1"}),
				Acknowledger: acker, DeliveryTag: 1,
			})

			acks, nacks := acker.snapshot()
			if acks != 0 {
				t.Fatalf("acks=%d: an unconfirmed transfer must not ack — the event would be gone", acks)
			}
			if nacks != 0 {
				t.Fatalf("nacks=%d: the delivery must be left alone, not rejected", nacks)
			}
		})
	}
}

// TestATransferHonoursACancelledContext asserts the per-delivery deadline reaches
// the wait. Without it a stuck transfer would outlive the handler timeout that is
// supposed to bound it.
func TestATransferHonoursACancelledContext(t *testing.T) {
	ch := &fakeChannel{}
	ch.mu.Lock()
	ch.confirms, ch.returns = nil, nil
	ch.mu.Unlock()
	c := consumerForTest(t, &fakeManager{}, ch)
	c.confirmTimeout = time.Hour // only the context can end this

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.transfer(ctx, "lifecycle-q.retry.30s", amqp.Delivery{Body: []byte("{}")}, 1)
	if err == nil {
		t.Fatal("transfer reported success on a cancelled context")
	}
	if !errors.Is(err, errTransferFailed) {
		t.Fatalf("transfer error = %v, want it to wrap errTransferFailed so the caller leaves the delivery alone", err)
	}
}

// TestRecycleIsSafeOnAConsumerWithNoChannel asserts recycle tolerates a Consumer
// that has no live attachment — the state between a failed re-attach and the next
// one. Panicking there would take down the supervisor that is trying to recover.
func TestRecycleIsSafeOnAConsumerWithNoChannel(_ *testing.T) {
	c := &Consumer{
		logger: zap.NewNop(), obs: NopObserver{},
		recycleBackoff: time.Millisecond,
		closed:         make(chan struct{}),
	}
	c.recycle() // no channel installed
	_ = c.Close()
}

// TestTimeoutResolutionDefaults pins the resolvers for the two broker timeouts.
// Zero must mean "the default", not "no timeout at all": a zero confirm timeout
// would make every transfer give up instantly, and a zero recycle backoff would
// spin the supervisor.
func TestTimeoutResolutionDefaults(t *testing.T) {
	if got := resolveConfirmTimeout(0); got != DefaultConfirmTimeout {
		t.Errorf("resolveConfirmTimeout(0) = %v, want %v", got, DefaultConfirmTimeout)
	}
	if got := resolveConfirmTimeout(3 * time.Second); got != 3*time.Second {
		t.Errorf("resolveConfirmTimeout(3s) = %v, want it honoured", got)
	}
	if got := resolveRecycleBackoff(0); got != DefaultRecycleBackoff {
		t.Errorf("resolveRecycleBackoff(0) = %v, want %v", got, DefaultRecycleBackoff)
	}
	if got := resolveRecycleBackoff(2 * time.Second); got != 2*time.Second {
		t.Errorf("resolveRecycleBackoff(2s) = %v, want it honoured", got)
	}
}
