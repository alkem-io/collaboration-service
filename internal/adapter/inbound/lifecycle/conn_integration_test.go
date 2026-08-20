//go:build integration

// Integration tests for the lifecycle consumer against a REAL RabbitMQ.
//
// The unit tests drive handle()/processOne() against fakes; those prove what this
// code does. These prove what the BROKER does with it, which is a separate
// question and the one that actually bit: RabbitMQ 3.9.13 accepts x-message-ttl
// and x-dead-letter-strategy on a quorum queue, echoes both back, and then never
// expires anything. Every argument in the topology is a promise made by the
// broker, and only a real broker can be asked whether it kept it.
//
// Run with: go test -tags=integration ./...
//
// Required env (skipped when unset):
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/
//
// The broker must be >= MinBrokerVersion. Against an older one Connect fails
// closed with an explanatory error, which is itself the correct outcome.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	nethttp "net/http"
	neturl "net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// recordingManager records the lifecycle calls the consumer routes to it.
type recordingManager struct {
	mu       sync.Mutex
	purged   []string
	purgeErr error
}

func (m *recordingManager) Purge(_ context.Context, id model.DocumentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purged = append(m.purged, string(id))
	return m.purgeErr
}

func (m *recordingManager) count(list *[]string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(*list)
}

func (m *recordingManager) has(list *[]string, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range *list {
		if v == id {
			return true
		}
	}
	return false
}

// brokerURL returns the configured broker, skipping the test when unset.
func brokerURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}
	return url
}

// uniqueName gives every run its own queue prefix, so a stale message from a
// previous or concurrent run cannot satisfy an assertion here.
func uniqueName(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// dialTest opens a connection and channel for the test's own use (publishing,
// declaring probes, counting), separate from the consumer's.
func dialTest(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(brokerURL(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("channel: %v", err)
	}
	t.Cleanup(func() {
		_ = ch.Close()
		_ = conn.Close()
	})
	return conn, ch
}

// deleteQueues removes the queues a test created, so a broker used for repeated
// runs does not accumulate them.
func deleteQueues(t *testing.T, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := amqp.Dial(brokerURL(t))
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		for _, n := range names {
			_, _ = ch.QueueDelete(n, false, false, false)
		}
	})
}

// messageCount reads a queue's message count by re-declaring it passively on a
// throwaway channel. A failed passive declare closes the channel it runs on, so
// it gets its own — and "the queue does not exist" is reported as 0 rather than
// as a test error, because several assertions below are about a MISSING queue.
func messageCount(t *testing.T, queue string) int {
	t.Helper()
	conn, err := amqp.Dial(brokerURL(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0
	}
	return q.Messages
}

// totalMessageCount reads a queue's TOTAL message count — ready plus unacked plus
// anything the broker is holding for a pending dead-letter hop.
//
// queue.declare-ok reports only the READY count, which is not the same number and
// is the wrong one here: a message retained by at-least-once dead-lettering while
// its target is unroutable is neither ready nor unacknowledged. Reading the ready
// count makes a broker doing exactly the right thing look like one that dropped
// the message. Only the management API exposes the total, so this is the one place
// the tests reach for it.
func totalMessageCount(t *testing.T, queue string) int {
	t.Helper()
	base := os.Getenv("RABBITMQ_MANAGEMENT_URL")
	if base == "" {
		t.Skip("RABBITMQ_MANAGEMENT_URL not set (e.g. http://guest:guest@localhost:15672)")
	}
	u, err := neturl.Parse(base)
	if err != nil {
		t.Fatalf("parse RABBITMQ_MANAGEMENT_URL: %v", err)
	}
	user, pass := "guest", "guest"
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			pass = p
		}
	}
	endpoint := fmt.Sprintf("%s://%s/api/queues/%s/%s", u.Scheme, u.Host, neturl.PathEscape("/"), neturl.PathEscape(queue))
	req, err := nethttp.NewRequest(nethttp.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("build management request: %v", err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := (&nethttp.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("management API: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == nethttp.StatusNotFound {
		return 0
	}
	if resp.StatusCode != nethttp.StatusOK {
		t.Fatalf("management API for %s: HTTP %d", queue, resp.StatusCode)
	}
	var q struct {
		Messages *int `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&q); err != nil {
		t.Fatalf("decode management response: %v", err)
	}
	if q.Messages == nil {
		return 0
	}
	return *q.Messages
}

// requireStableTotal waits for a queue's total message count to reach want and
// then STAY there for a settle window.
//
// The management API's queue statistics are eventually consistent (they refresh on
// collect_statistics_interval, 5s by default), so a single sample is flaky in both
// directions: it can read 0 for a queue that holds a message, and it can read a
// stale 1 for one that no longer does. Sampling once made this test pass two runs
// in three. A reading that holds steady across the refresh interval is the weakest
// claim that is actually true of the broker.
func requireStableTotal(t *testing.T, queue string, want int, why string) {
	t.Helper()
	const settle = 6 * time.Second
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if totalMessageCount(t, queue) != want {
			time.Sleep(time.Second)
			continue
		}
		stableUntil := time.Now().Add(settle)
		steady := true
		for time.Now().Before(stableUntil) {
			time.Sleep(time.Second)
			if got := totalMessageCount(t, queue); got != want {
				steady = false
				break
			}
		}
		if steady {
			return
		}
	}
	t.Fatalf("%s holds %d messages in total, want a steady %d. %s", queue, totalMessageCount(t, queue), want, why)
}

// envelope builds the NestJS event envelope { pattern, data, id } the producer
// sends.
func envelopeBody(t *testing.T, pattern string, data any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal event data: %v", err)
	}
	body, err := json.Marshal(map[string]any{"pattern": pattern, "data": json.RawMessage(raw), "id": "int"})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return body
}

// publishRaw publishes a body onto a queue through the default exchange.
func publishRaw(t *testing.T, ch *amqp.Channel, queue string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		t.Fatalf("publish to %s: %v", queue, err)
	}
}

func eventually(cond func() bool) bool {
	return eventuallyWithin(5*time.Second, cond)
}

func eventuallyWithin(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// TestConsumerConsumesLivePublishedEvents proves the consumer connects, declares
// its topology, consumes a real NestJS envelope, and tears down.
//
// The publisher declares Q1 with the frozen arguments, exactly as `server` does.
// That is not incidental: it is the cross-repo contract, and this test is the
// only place both halves of it run against the same broker.
func TestConsumerConsumesLivePublishedEvents(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-int")
	docID := uniqueName("live-doc")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	mgr := &recordingManager{}
	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	// The producer's own declaration of Q1 — the frozen contract.
	if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("the producer's declaration of Q1 was refused: %v", err)
	}
	publishRaw(t, ch, queue, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: docID}))

	if !eventually(func() bool { return mgr.has(&mgr.purged, docID) }) {
		t.Fatal("consumer never cascaded the published document.deleted")
	}
}

// TestQ1RejectsAnInequivalentProducerDeclaration proves the frozen contract is
// actually enforced by the broker rather than merely agreed in a document.
//
// If a producer declares Q1 with different arguments, RabbitMQ refuses with
// PRECONDITION_FAILED and closes the channel. That is the failure mode the frozen
// {x-queue-type: quorum} table exists to avoid, and it is why no dead-letter
// argument may be added to Q1 on this side.
func TestQ1RejectsAnInequivalentProducerDeclaration(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-precond")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, &recordingManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	// A classic queue, or any other argument set, is inequivalent.
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err == nil {
		t.Fatal("an argument-less redeclaration of Q1 was accepted; the frozen contract is not being enforced, so a producer/consumer mismatch would surface as silent misbehaviour instead of a startup failure")
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.PreconditionFailed {
		t.Fatalf("redeclaration failed with %v, want PRECONDITION_FAILED (406)", err)
	}
}

// TestAFailedEventLandsOnTheFirstRetryTier proves the transfer publish actually
// routes on a real broker.
//
// A confirm alone would not prove it: on the default exchange, publishing to a
// queue that does not exist is a silent discard that still confirms. Here the
// message is counted where it was sent.
func TestAFailedEventLandsOnTheFirstRetryTier(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-retry")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	mgr := &recordingManager{purgeErr: errors.New("backend down")}
	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	publishRaw(t, ch, queue, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-retry"}))

	if !eventually(func() bool { return mgr.count(&mgr.purged) >= 1 }) {
		t.Fatal("the consumer never attempted the purge")
	}
	if !eventually(func() bool { return messageCount(t, names.tiers[0]) == 1 }) {
		t.Fatalf("the failed event is not on the first retry tier (%s holds %d messages); it was either dropped or acked without being transferred",
			names.tiers[0], messageCount(t, names.tiers[0]))
	}
	// And it is no longer on Q1 — acked only after the transfer confirmed.
	if got := messageCount(t, names.main); got != 0 {
		t.Fatalf("%s still holds %d messages after a confirmed transfer", names.main, got)
	}
}

// TestAnUnactionableEventLandsInTheDeadLetterQueue proves an envelope nothing can
// act on is RECORDED rather than swallowed. No amount of redelivery makes an
// unparseable body actionable, so it skips the ladder — but dropping it would
// leave no trace that the producer is emitting something this service cannot read.
func TestAnUnactionableEventLandsInTheDeadLetterQueue(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-dlq")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, &recordingManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	publishRaw(t, ch, queue, []byte("not a lifecycle envelope"))

	if !eventually(func() bool { return messageCount(t, names.dlq) == 1 }) {
		t.Fatalf("the unactionable event is not in %s (it holds %d); it was swallowed", names.dlq, messageCount(t, names.dlq))
	}
	for _, tier := range names.tiers {
		if got := messageCount(t, tier); got != 0 {
			t.Errorf("%s holds %d messages; an unactionable envelope must not enter the retry ladder", tier, got)
		}
	}
}

// probeTier declares a main/tier pair with the production argument shape but a
// short TTL, so the broker's honouring of that shape can be tested in seconds
// rather than in the ladder's real 30s/5m/30m.
//
// dlRoutingKey is separate from mainQueue on purpose: pointing it at a queue that
// does not exist is how the missing-target case is set up.
func probeTier(t *testing.T, ch *amqp.Channel, mainQueue, tierQueue, dlRoutingKey string, ttlMS int32) {
	t.Helper()
	if _, err := ch.QueueDeclare(mainQueue, true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	}); err != nil {
		t.Fatalf("declare %s: %v", mainQueue, err)
	}
	if _, err := ch.QueueDeclare(tierQueue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-message-ttl":             ttlMS,
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": dlRoutingKey,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
	}); err != nil {
		t.Fatalf("declare %s: %v", tierQueue, err)
	}
}

// TestBrokerExpiresARetryTierBackToItsTarget is the assertion the whole ladder
// rests on, asked of the broker rather than of this code.
//
// RabbitMQ 3.9.13 accepts every one of these arguments, reports them back on
// inspection, and never expires anything — measured, not assumed. Against such a
// broker this test fails; against >= 3.13.2 it passes. Connect refuses to start
// below the floor for exactly this reason, and this is the test that keeps the
// floor honest as the ladder or the broker moves.
func TestBrokerExpiresARetryTierBackToItsTarget(t *testing.T) {
	_, ch := dialTest(t)
	base := uniqueName("collab-ttl-probe")
	main, tier := base, base+".tier"
	deleteQueues(t, main, tier)
	probeTier(t, ch, main, tier, main, 2000)

	publishRaw(t, ch, tier, []byte(`{"probe":true}`))
	if !eventually(func() bool { return messageCount(t, tier) == 1 }) {
		t.Fatalf("the probe message never landed in %s", tier)
	}

	if !eventuallyWithin(20*time.Second, func() bool { return messageCount(t, main) == 1 }) {
		t.Fatalf("after the TTL, %s holds %d messages and %s holds %d: the broker ACCEPTED x-message-ttl and x-dead-letter-strategy on a quorum queue and then expired nothing. Every retry would accumulate in its tier and never be redelivered, with no error anywhere",
			main, messageCount(t, main), tier, messageCount(t, tier))
	}
	if got := messageCount(t, tier); got != 0 {
		t.Fatalf("%s still holds %d messages after the dead-letter hop", tier, got)
	}
}

// TestAnExpiredRetryIsRetainedWhenItsTargetIsMissing is the at-least-once
// assertion, and the reason x-dead-letter-strategy is set at all.
//
// With the broker default (at-most-once) the dead-letter republish is internal and
// unconditional: if the message cannot be routed at expiry, it is gone. With
// at-least-once the hop is conditional on the target confirming it, so an
// unroutable dead-letter leaves the message in the SOURCE queue. The whole ladder
// is a promise of durability, and a delay tier that silently drops on expiry is
// the one failure that would make the promise a lie.
//
// Two queues, identical but for the strategy, run through one timeline. The
// control is what makes the result mean anything: if the default also retained,
// the argument could be dropped from the topology with no loss.
//
// The count must be the TOTAL, not the ready count. A message held for a pending
// dead-letter hop is neither ready nor unacknowledged, so queue.declare-ok reports
// zero for a message that is very much still there — an earlier version of this
// test read that number and "failed" against a broker doing exactly the right
// thing. Only the management API exposes the total, and it is eventually
// consistent, hence the settle window.
//
// What at-least-once does NOT do, measured on 3.13.2 over two minutes: retry the
// hop once the missing target is created. The message stays put. Retention is the
// guarantee; recovery is an operator action. That is why the topology declares
// every queue up front, and why a deleted tier queue is an incident rather than
// something that heals itself.
func TestAnExpiredRetryIsRetainedWhenItsTargetIsMissing(t *testing.T) {
	_, ch := dialTest(t)
	base := uniqueName("collab-retain-probe")
	atLeastOnce := base + ".at-least-once"
	atMostOnce := base + ".at-most-once"
	missing := base + ".missing" // deliberately never declared
	deleteQueues(t, atLeastOnce, atMostOnce, missing)

	// The TTL has to outlast the management API's statistics interval, or "the
	// message was here before it expired" is not an observable state and the
	// control proves nothing.
	const ttlMS = 30_000

	declare := func(name string, extra amqp.Table) {
		args := amqp.Table{
			"x-queue-type":              "quorum",
			"x-message-ttl":             int32(ttlMS),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": missing,
		}
		for k, v := range extra {
			args[k] = v
		}
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			t.Fatalf("declare %s: %v", name, err)
		}
	}
	declare(atLeastOnce, amqp.Table{
		"x-dead-letter-strategy": "at-least-once",
		"x-overflow":             "reject-publish",
	})
	declare(atMostOnce, nil) // broker default

	published := time.Now()
	publishRaw(t, ch, atLeastOnce, []byte(`{"probe":true}`))
	publishRaw(t, ch, atMostOnce, []byte(`{"probe":true}`))

	// Both hold their message before expiry. Without this the control's later
	// absence would be indistinguishable from a publish that never landed.
	requireStableTotal(t, atLeastOnce, 1, "The message never landed, so nothing below can be concluded.")
	requireStableTotal(t, atMostOnce, 1, "The control message never landed, so its later absence says nothing.")

	if until := time.Until(published.Add(ttlMS*time.Millisecond + 5*time.Second)); until > 0 {
		time.Sleep(until)
	}

	if got := totalMessageCount(t, missing); got != 0 {
		t.Fatalf("%s holds %d messages but was never declared", missing, got)
	}
	requireStableTotal(t, atLeastOnce, 1,
		"The expired message was DROPPED rather than retained. A delay tier that discards on expiry silently loses every retry it was built to protect.")
	requireStableTotal(t, atMostOnce, 0,
		"The at-most-once control retained its expired message too. If the broker default already retains, the retention above is not evidence that x-dead-letter-strategy does anything.")

	// The retained message is parked for the pending hop, not offered for delivery.
	if got := messageCount(t, atLeastOnce); got != 0 {
		t.Fatalf("%s reports %d READY messages; the retained message should be held for the dead-letter hop", atLeastOnce, got)
	}
}

// TestDepthPollingReportsRealBrokerCounts closes the loop on the depth gauge: the
// number it publishes is the broker's, on every queue in the topology.
func TestDepthPollingReportsRealBrokerCounts(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-depth")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	obs := &countingObserver{depths: map[string]int{}}
	mgr := &recordingManager{purgeErr: errors.New("backend down")}
	consumer, err := Connect(Config{
		URL: url, Queue: queue,
		DepthPollInterval: 200 * time.Millisecond, Observer: obs,
	}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	publishRaw(t, ch, queue, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-depth"}))

	if !eventually(func() bool { return obs.depth(names.tiers[0]) == 1 }) {
		t.Fatalf("the depth gauge never reported the failed event on %s (reported %d); the DLQ/backlog alert would read as empty while events sat in the ladder",
			names.tiers[0], obs.depth(names.tiers[0]))
	}
	if got := obs.depth(names.dlq); got != 0 {
		t.Fatalf("depth for %s = %d, want 0", names.dlq, got)
	}
}

type countingObserver struct {
	mu     sync.Mutex
	depths map[string]int
}

func (o *countingObserver) EventTransferred(string, bool) {}

func (o *countingObserver) EventDeadLettered(string, int32) {}

func (o *countingObserver) QueueReadyDepth(queue string, ready int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.depths[queue] = ready
}

func (o *countingObserver) depth(queue string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.depths[queue]
}

// TestTheRealLadderRedeliversAfterTheFirstTierExpires walks the PRODUCTION
// topology end to end, with the real 30-second first tier.
//
// The probe tests above prove the broker honours the argument shape; this proves
// the shape we actually ship is wired correctly — that a failing event leaves Q1,
// waits out the tier, comes back to Q1 on its own, is redelivered to this
// consumer, and (still failing) moves one rung further down the ladder.
//
// Nothing shorter covers it. Two things only appear here: RabbitMQ's dead-letter
// cycle detection, which drops a message that would return to a queue it has
// already been dead-lettered from and which the tier→Q1 hop looks superficially
// like; and the attempt header surviving the broker's own republish, which is what
// makes the second failure advance to the 5m tier instead of repeating the 30s one
// forever.
//
// It takes ~40s, so it is skipped under -short.
func TestTheRealLadderRedeliversAfterTheFirstTierExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("the first retry tier is 30s; skipped under -short")
	}
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-ladder")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	mgr := &recordingManager{purgeErr: errors.New("backend down")}
	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	publishRaw(t, ch, queue, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-ladder"}))

	if !eventually(func() bool { return mgr.count(&mgr.purged) == 1 }) {
		t.Fatal("the consumer never attempted the first purge")
	}
	if !eventually(func() bool { return messageCount(t, names.tiers[0]) == 1 }) {
		t.Fatalf("the failed event is not on %s", names.tiers[0])
	}

	// The tier's TTL is 30s. After it, the broker dead-letters the message back to
	// Q1, the consumer is redelivered it, fails again, and transfers it onward.
	if !eventuallyWithin(60*time.Second, func() bool { return mgr.count(&mgr.purged) == 2 }) {
		t.Fatalf("the event was never redelivered after the first tier expired (purge attempts: %d). Either the tier did not expire, or RabbitMQ's dead-letter cycle detection dropped the tier→Q1 hop — in which case every retry is silently discarded 30 seconds after it fails",
			mgr.count(&mgr.purged))
	}
	if !eventuallyWithin(15*time.Second, func() bool { return messageCount(t, names.tiers[1]) == 1 }) {
		t.Fatalf("the redelivered event did not advance to %s (it holds %d, and %s holds %d). The attempt header did not survive the broker's republish, so the event would cycle the first tier forever and never reach the DLQ",
			names.tiers[1], messageCount(t, names.tiers[1]), names.tiers[0], messageCount(t, names.tiers[0]))
	}
	if got := messageCount(t, names.tiers[0]); got != 0 {
		t.Errorf("%s still holds %d messages after the hop", names.tiers[0], got)
	}
	if got := messageCount(t, names.dlq); got != 0 {
		t.Errorf("%s holds %d messages; the schedule is not exhausted yet", names.dlq, got)
	}
}

// TestTheDepthPollRecreatesADeletedQueue is half of the recovery story, and the
// fast half.
//
// Every queue in the topology is re-declared on each depth poll and on each
// supervisor re-attach. So a queue an operator deletes — or that never survived a
// broker rebuild — comes back on its own within one poll interval, with nobody
// intervening. That matters beyond tidiness: a missing dead-letter target is the
// only way a retry can end up parked in the state the ready-depth gauge cannot
// see, and this is what closes that window.
func TestTheDepthPollRecreatesADeletedQueue(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-redeclare")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	consumer, err := Connect(Config{
		URL: url, Queue: queue, DepthPollInterval: 200 * time.Millisecond,
	}, &recordingManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	if _, err := ch.QueueDelete(names.tiers[2], false, false, false); err != nil {
		t.Fatalf("delete %s: %v", names.tiers[2], err)
	}
	if queueExists(t, names.tiers[2]) {
		t.Fatalf("%s still exists right after being deleted", names.tiers[2])
	}

	if !eventuallyWithin(15*time.Second, func() bool { return queueExists(t, names.tiers[2]) }) {
		t.Fatalf("%s was not recreated by the depth poll; a deleted tier stays missing, and anything dead-lettering into it parks in a state nothing reports", names.tiers[2])
	}
}

// queueExists reports whether a queue is present, using a throwaway channel
// because a failed passive declare closes the channel it runs on.
func queueExists(t *testing.T, queue string) bool {
	t.Helper()
	conn, err := amqp.Dial(brokerURL(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	return err == nil
}

// TestConnectReleasesAnEventParkedForAMissingTarget is the recovery, demonstrated
// end to end rather than described.
//
// An expired retry whose dead-letter target does not exist is retained by
// at-least-once in a state no consumer and no shovel can read: neither ready nor
// unacknowledged. The question is what gets it out. Measured on 3.13.2: creating
// the target is enough — the broker retries the hop on its own and releases the
// message about three minutes later. Nothing else is required, and nothing else
// works in the meantime.
//
// That makes the recovery action "declare the missing queue", which is precisely
// what this service does at startup and on every re-attach and depth poll. So the
// test drives it through the service: park an event for a Q1 that does not exist,
// then start the consumer and watch the event arrive and be applied.
//
// ~4 minutes; skipped under -short.
func TestConnectReleasesAnEventParkedForAMissingTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("the broker retries a parked dead-letter hop on a ~3 minute cycle; skipped under -short")
	}
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-parked")
	names := namesFor(queue)
	parking := queue + ".parking" // a probe tier, not part of the topology
	deleteQueues(t, append([]string{names.main, names.dlq, parking}, names.tiers...)...)

	_, ch := dialTest(t)
	// A delay tier shaped exactly like a real one, dead-lettering into a Q1 that
	// does not exist yet. Its TTL is short only so the parking happens promptly;
	// the tier duration has nothing to do with the release.
	if _, err := ch.QueueDeclare(parking, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-message-ttl":             int32(2000),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": names.main,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
	}); err != nil {
		t.Fatalf("declare %s: %v", parking, err)
	}
	docID := "doc-parked"
	publishRaw(t, ch, parking, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: docID}))

	// Park it: expire with the target absent.
	time.Sleep(8 * time.Second)
	if queueExists(t, names.main) {
		t.Fatalf("%s exists; the event cannot be parked and the test would prove nothing", names.main)
	}
	requireStableTotal(t, parking, 1, "The event was not parked, so the release below would prove nothing.")
	if got := messageCount(t, parking); got != 0 {
		t.Fatalf("%s reports %d ready; a parked message must be invisible to the ready count, or this is not the state under test", parking, got)
	}

	// Starting the consumer declares Q1 — and that is the whole recovery action.
	mgr := &recordingManager{}
	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	if !eventuallyWithin(5*time.Minute, func() bool { return mgr.has(&mgr.purged, docID) }) {
		t.Fatalf("the parked event never reached the consumer after %s was declared (parking queue still holds %d in total). Declaring the missing target is the documented recovery action; if it does not release the message, the runbook has no working recovery and the event is unreachable by any consumer or shovel",
			names.main, totalMessageCount(t, parking))
	}
	// And the source drained — the event was released, not delivered AND retained.
	// Stable, because the management statistics lag the release by an interval.
	requireStableTotal(t, parking, 0,
		"The event reached the consumer but its source still holds it, so a later poll would deliver it again.")
}

// TestReplayReturnsDeadLetteredEventsToTheLadder is the replay path, end to end
// against a real broker.
//
// Three properties, each of which a naive replay gets wrong:
//
//   - The attempt count is CLEARED, so the event gets the whole ladder again.
//     A shovel preserves headers, so a shovelled event arrives still marked
//     "attempt 3" and goes straight back to the DLQ on its first failure — which
//     looks exactly like a replay that worked.
//   - The replay count is INCREMENTED, because clearing the attempt count erases
//     the only evidence the event has been round before. Without it, an event a
//     person has replayed three times into a still-broken backend is
//     indistinguishable from one arriving for the first time.
//   - The dead-letter copy is released only after the republish is confirmed, so
//     a failed publish cannot lose the event.
func TestReplayReturnsDeadLetteredEventsToTheLadder(t *testing.T) {
	brokerURL(t)
	queue := uniqueName("collab-lifecycle-replay")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	_, ch := dialTest(t)
	if err := declareTopology(&amqpChannelShim{ch: ch}, names); err != nil {
		t.Fatalf("declare topology: %v", err)
	}

	// Seed the dead-letter queue exactly as an exhausted schedule leaves it: the
	// body a producer sent, carrying the attempt header from the end of the ladder.
	//
	// Deliberately WITHOUT running a consumer. An earlier version published to Q1
	// and closed the consumer to get the event moving, then purged Q1 before
	// seeding — and an unacknowledged delivery is REQUEUED when the consumer's
	// channel closes, which can land after the purge. The assertion below then read
	// the original event rather than the replayed one and saw no replay count. It
	// passed locally and failed in CI, which is exactly the shape of that race.
	docID := "doc-replayed"
	if err := ch.PublishWithContext(context.Background(), "", names.dlq, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Headers:      amqp.Table{headerAttempt: tierCount},
		Body:         envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: docID}),
	}); err != nil {
		t.Fatalf("seed the DLQ: %v", err)
	}
	if !eventually(func() bool { return messageCount(t, names.dlq) == 1 }) {
		t.Fatalf("the event never reached %s", names.dlq)
	}
	// Q1 must be empty, or the Get below could read something other than the
	// replayed event and the assertion would be about the wrong message.
	if got := messageCount(t, names.main); got != 0 {
		t.Fatalf("%s holds %d messages before the replay", names.main, got)
	}

	// Replay it.
	replayConn, replayCh := dialTest(t)
	_ = replayConn
	res, err := Replay(context.Background(), replayCh, queue, 0)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Moved != 1 {
		t.Fatalf("Replay moved %d event(s), want 1", res.Moved)
	}
	if !eventually(func() bool { return messageCount(t, names.dlq) == 0 }) {
		t.Fatalf("%s still holds %d messages; the dead-letter copy was not released", names.dlq, messageCount(t, names.dlq))
	}

	// Inspect what actually landed on the main queue.
	_, inspect := dialTest(t)
	var got amqp.Delivery
	if !eventually(func() bool {
		d, ok, err := inspect.Get(names.main, false)
		if err != nil || !ok {
			return false
		}
		got = d
		return true
	}) {
		t.Fatalf("nothing arrived on %s after the replay", names.main)
	}
	defer func() { _ = got.Ack(false) }()

	if _, present := got.Headers[headerAttempt]; present {
		t.Fatalf("the replayed event still carries %s = %v. It will fail once and go straight back to the dead-letter queue, which is indistinguishable from a replay that worked",
			headerAttempt, got.Headers[headerAttempt])
	}
	if got := replaysOf(got.Headers); got != 1 {
		t.Fatalf("%s = %d, want 1; clearing the attempt count erases the event's history, so without this a repeatedly-replayed event looks like a first arrival", headerReplays, got)
	}
}

// TestASecondReplayIsDistinguishableFromTheFirst asserts the replay counter
// accumulates. That is the whole reason it exists: after a replay the attempt
// count is deliberately zero again, so this is the only thing that says "the fix
// applied before the last replay did not work".
//
// Running three rounds on one connection also pins that Replay releases the
// dead-letter queue when it finishes. It does not: an earlier version left its
// consumer attached, so the next round's message was swallowed the instant it
// arrived — held unacknowledged on a channel nobody was reading, invisible to the
// ready depth and unreachable by a second replay. Rounds 2 and 3 fail without the
// cancel.
func TestASecondReplayIsDistinguishableFromTheFirst(t *testing.T) {
	brokerURL(t)
	queue := uniqueName("collab-lifecycle-replay2")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	_, ch := dialTest(t)
	if err := declareTopology(&amqpChannelShim{ch: ch}, names); err != nil {
		t.Fatalf("declare topology: %v", err)
	}

	body := envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-twice"})
	for round := int32(1); round <= 3; round++ {
		// Put it in the DLQ as an exhausted schedule would, carrying whatever
		// replay count the previous round produced.
		headers := amqp.Table{headerAttempt: tierCount}
		if round > 1 {
			headers[headerReplays] = round - 1
		}
		if err := ch.PublishWithContext(context.Background(), "", names.dlq, false, false, amqp.Publishing{
			ContentType: "application/json", DeliveryMode: amqp.Persistent,
			Headers: headers, Body: body,
		}); err != nil {
			t.Fatalf("round %d: seed the DLQ: %v", round, err)
		}
		if !eventually(func() bool { return messageCount(t, names.dlq) == 1 }) {
			t.Fatalf("round %d: the event never reached %s", round, names.dlq)
		}

		_, replayCh := dialTest(t)
		if _, err := Replay(context.Background(), replayCh, queue, 0); err != nil {
			t.Fatalf("round %d: Replay: %v", round, err)
		}

		_, inspect := dialTest(t)
		var d amqp.Delivery
		if !eventually(func() bool {
			got, ok, err := inspect.Get(names.main, false)
			if err != nil || !ok {
				return false
			}
			d = got
			return true
		}) {
			t.Fatalf("round %d: nothing arrived on %s", round, names.main)
		}
		if got := replaysOf(d.Headers); got != round {
			t.Fatalf("round %d: %s = %d, want %d; the counter must accumulate or repeated replays are invisible", round, headerReplays, got, round)
		}
		_ = d.Ack(false)
	}
}

// amqpChannelShim adapts *amqp.Channel to the narrow brokerChannel the topology
// helper takes, so the test declares exactly what production declares.
type amqpChannelShim struct{ ch *amqp.Channel }

func (c *amqpChannelShim) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return c.ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}
func (c *amqpChannelShim) Qos(prefetchCount, prefetchSize int, global bool) error {
	return c.ch.Qos(prefetchCount, prefetchSize, global)
}
func (c *amqpChannelShim) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}
func (c *amqpChannelShim) Cancel(consumer string, noWait bool) error {
	return c.ch.Cancel(consumer, noWait)
}
func (c *amqpChannelShim) Confirm(noWait bool) error { return c.ch.Confirm(noWait) }
func (c *amqpChannelShim) NotifyPublish(ch chan amqp.Confirmation) chan amqp.Confirmation {
	return c.ch.NotifyPublish(ch)
}
func (c *amqpChannelShim) NotifyReturn(ch chan amqp.Return) chan amqp.Return {
	return c.ch.NotifyReturn(ch)
}
func (c *amqpChannelShim) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}
func (c *amqpChannelShim) Close() error { return c.ch.Close() }

// TestAReplayThatCannotLandLeavesTheEventInTheDeadLetterQueue is the durability
// half of the replay path.
//
// The dead-letter queue is the last copy of the event. If a replay released it
// before the republish was confirmed — or trusted a confirm without checking for a
// return — a publish to a main queue that is not there would destroy the event
// outright: on the default exchange an unroutable publish is a silent discard that
// still gets confirmed. So the replay must fail loudly and change nothing.
func TestAReplayThatCannotLandLeavesTheEventInTheDeadLetterQueue(t *testing.T) {
	brokerURL(t)
	queue := uniqueName("collab-lifecycle-replay-fail")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	_, ch := dialTest(t)
	if err := declareTopology(&amqpChannelShim{ch: ch}, names); err != nil {
		t.Fatalf("declare topology: %v", err)
	}
	if err := ch.PublishWithContext(context.Background(), "", names.dlq, false, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		Headers: amqp.Table{headerAttempt: tierCount},
		Body:    envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-stranded"}),
	}); err != nil {
		t.Fatalf("seed the DLQ: %v", err)
	}
	if !eventually(func() bool { return messageCount(t, names.dlq) == 1 }) {
		t.Fatal("the event never reached the dead-letter queue")
	}

	// Take away the target.
	if _, err := ch.QueueDelete(names.main, false, false, false); err != nil {
		t.Fatalf("delete %s: %v", names.main, err)
	}

	_, replayCh := dialTest(t)
	res, err := Replay(context.Background(), replayCh, queue, 0)
	if err == nil {
		t.Fatal("Replay reported success with no queue to publish to; on the default exchange that publish is a silent discard that still confirms, so the event would be gone")
	}
	if res.Moved != 0 {
		t.Fatalf("Replay reported %d moved event(s) when none could land", res.Moved)
	}

	// The event is still in the dead-letter queue. Closing the replay channel
	// returns the unacknowledged copy.
	_ = replayCh.Close()
	if !eventually(func() bool { return messageCount(t, names.dlq) == 1 }) {
		t.Fatalf("%s holds %d messages after a failed replay, want 1: the only copy of the event was released before the republish landed", names.dlq, messageCount(t, names.dlq))
	}
}

// TestDeadLetterDepthReportsWhatIsWaiting covers the replay command's -dry-run
// reading. An operator uses it to decide whether to replay at all, so a count that
// silently read zero would end the investigation before it started.
func TestDeadLetterDepthReportsWhatIsWaiting(t *testing.T) {
	brokerURL(t)
	queue := uniqueName("collab-lifecycle-depth-read")
	names := namesFor(queue)
	deleteQueues(t, append([]string{names.main, names.dlq}, names.tiers...)...)

	_, ch := dialTest(t)
	shim := &amqpChannelShim{ch: ch}
	if err := declareTopology(shim, names); err != nil {
		t.Fatalf("declare topology: %v", err)
	}

	if got, err := DeadLetterDepth(shim, queue); err != nil || got != 0 {
		t.Fatalf("DeadLetterDepth on an empty queue = %d, %v; want 0, nil", got, err)
	}
	for i := 0; i < 3; i++ {
		publishRaw(t, ch, names.dlq, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: "d"}))
	}
	if !eventually(func() bool {
		n, err := DeadLetterDepth(shim, queue)
		return err == nil && n == 3
	}) {
		n, _ := DeadLetterDepth(shim, queue)
		t.Fatalf("DeadLetterDepth = %d, want 3; -dry-run would tell an operator there is nothing to replay", n)
	}
}
