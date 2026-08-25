//go:build integration

// Integration tests for the lifecycle consumer against a REAL RabbitMQ.
//
// The unit tests drive handle()/processOne() against fakes; those prove what this
// code does. These prove what the BROKER does with it, which is a separate
// question. Every argument in the topology is a promise made by the broker, and
// only a real broker can be asked whether it kept it — most of all the main
// queue's argument table, which `server` declares too and which fails
// PRECONDITION_FAILED if the two ever drift.
//
// Run with: go test -tags=integration ./...
//
// Required env (skipped when unset):
//
//	RABBITMQ_TEST_URL=amqp://user:pass@localhost:5672/
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
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
	closed   []string
	closeErr error
}

func (m *recordingManager) CloseDeleted(_ context.Context, id model.DocumentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = append(m.closed, string(id))
	return m.closeErr
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
	deleteQueues(t, names.main, names.dlq)

	mgr := &recordingManager{}
	consumer, err := Connect(Config{URL: url, Queue: queue, DepthPollInterval: -1}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	// A HAND-WRITTEN MIRROR of the producer's declaration of the main queue — the
	// frozen cross-repo contract, written here as `server` writes it
	// (server: src/core/microservices/microservices.module.ts). If the argument SET
	// or its VALUES drift from declareTopology's table, the broker refuses whichever
	// side declares second with PRECONDITION_FAILED and that side does not start.
	//
	// This table is a HAND-WRITTEN MIRROR of what `server` declares, not server's
	// code. It cannot detect a change made only on that side — server's own
	// declaration is pinned by its own test (server eb12d945, which landed the
	// matching dead-letter pair). What this proves is that the two tables, as
	// written, are equivalent to a real broker: if they were not, the declaration
	// below fails PRECONDITION_FAILED.
	//
	// Keeping the mirror truthful is a manual obligation on both sides, and queue
	// arguments are immutable, so a divergence is repaired by deleting and
	// recreating the queue rather than by redeploying. (Integer widths are
	// normalized by the broker, so it is the values that must match, not the Go
	// types.)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-delivery-limit":          int32(-1),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": queue + ".dlq",
	}); err != nil {
		t.Fatalf("the producer's declaration of the main queue was refused: %v", err)
	}
	publishRaw(t, ch, queue, envelopeBody(t, PatternDocumentDeleted, DeletedEvent{ID: docID}))

	if !eventually(func() bool { return mgr.has(&mgr.closed, docID) }) {
		t.Fatal("consumer never routed the published document.deleted to the Manager")
	}
}

// TestQ1RejectsAnInequivalentProducerDeclaration proves the frozen contract is
// actually enforced by the broker rather than merely agreed in a document.
//
// If a producer declares the main queue with ANY different argument set or value,
// RabbitMQ refuses with PRECONDITION_FAILED and closes the channel. That applies
// to every entry in the frozen table equally — queue type, delivery limit, and the
// dead-letter pair — so drift on either side is a startup failure rather than
// silent misbehaviour.
func TestQ1RejectsAnInequivalentProducerDeclaration(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-precond")
	names := namesFor(queue)
	deleteQueues(t, names.main, names.dlq)

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

// TestAnUnactionableEventLandsInTheDeadLetterQueue proves an envelope nothing can
// act on is RECORDED rather than swallowed. No amount of redelivery makes an
// unparseable body actionable, so it skips the ladder — but dropping it would
// leave no trace that the producer is emitting something this service cannot read.
func TestAnUnactionableEventLandsInTheDeadLetterQueue(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-dlq")
	names := namesFor(queue)
	deleteQueues(t, names.main, names.dlq)

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
	if got := messageCount(t, names.main); got != 0 {
		t.Errorf("the main queue holds %d messages; poison must be dead-lettered, not requeued to spin forever", got)
	}
}

// TestDepthPollingReportsRealBrokerCounts closes the loop on the depth gauge: the
// number it publishes is the broker's, on every queue in the topology.
func TestDepthPollingReportsRealBrokerCounts(t *testing.T) {
	url := brokerURL(t)
	queue := uniqueName("collab-lifecycle-depth")
	names := namesFor(queue)
	deleteQueues(t, names.main, names.dlq)

	obs := &countingObserver{depths: map[string]int{}}
	// A poison envelope: rejected to the DLQ, where it stays and is countable.
	consumer, err := Connect(Config{
		URL: url, Queue: queue,
		DepthPollInterval: 200 * time.Millisecond, Observer: obs,
	}, &recordingManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	publishRaw(t, ch, queue, []byte("not a lifecycle envelope"))

	if !eventually(func() bool { return obs.depth(names.dlq) == 1 }) {
		t.Fatalf("the depth gauge never reported the dead-lettered event on %s (reported %d); the DLQ alert would read as empty while poison sat there",
			names.dlq, obs.depth(names.dlq))
	}
	// The main queue drains: poison is rejected, not requeued.
	if !eventually(func() bool { return obs.depth(names.main) == 0 }) {
		t.Fatalf("depth for the main queue = %d, want 0; poison that requeues spins forever", obs.depth(names.main))
	}
}

type countingObserver struct {
	mu     sync.Mutex
	depths map[string]int
}

func (o *countingObserver) EventDeadLettered(string) {}

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
	deleteQueues(t, names.main, names.dlq)

	// A DELIBERATELY SLOW poll. The gap between deleting the queue and the poll
	// recreating it is the only window in which "it is really gone" is observable,
	// and at a 200ms interval that window is shorter than the round trip needed to
	// look — the poll would recreate the queue before the check ran, and the test
	// failed intermittently on a busy broker for that reason. Two seconds makes
	// both halves observable without changing what is being tested.
	const poll = 2 * time.Second
	consumer, err := Connect(Config{
		URL: url, Queue: queue, DepthPollInterval: poll,
	}, &recordingManager{}, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	_, ch := dialTest(t)
	if _, err := ch.QueueDelete(names.dlq, false, false, false); err != nil {
		t.Fatalf("delete %s: %v", names.dlq, err)
	}
	// Deleting a quorum queue is a Raft operation, so allow it to become visible
	// rather than assuming it is instant — but well inside one poll interval, so
	// the recreation below is still the poll's doing and not the delete failing.
	if !eventuallyWithin(poll/2, func() bool { return !queueExists(t, names.dlq) }) {
		t.Fatalf("%s was still present half a poll interval after being deleted", names.dlq)
	}

	if !eventuallyWithin(30*time.Second, func() bool { return queueExists(t, names.dlq) }) {
		t.Fatalf("%s was not recreated by the depth poll. A missing DLQ is not benign: the main queue dead-letters into it with the DEFAULT at-most-once strategy, so a rejected poison message is DISCARDED rather than recorded while its target is absent. The redeclare is what bounds that window", names.dlq)
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
