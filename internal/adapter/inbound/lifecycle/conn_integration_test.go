//go:build integration

// Integration test for the lifecycle consumer's live bus path (Connect / consume
// / Close) against a real RabbitMQ. The unit tests in consumer_test.go drive
// handle() directly; this one proves the consumer actually connects, declares its
// queue, consumes a published NestJS event envelope end to end, and tears down.
// Run with: go test -tags=integration ./...
//
// Required env (skipped when unset):
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/
package lifecycle

import (
	"context"
	"encoding/json"
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
	mu          sync.Mutex
	created     []string
	purged      []string
	reEvaluated []string
}

func (m *recordingManager) PreRegister(_ context.Context, meta model.Metadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created = append(m.created, string(meta.ID))
	return nil
}

func (m *recordingManager) Purge(_ context.Context, id model.DocumentID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purged = append(m.purged, string(id))
	return nil
}

func (m *recordingManager) ReEvaluate(_ context.Context, id model.DocumentID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reEvaluated = append(m.reEvaluated, string(id))
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

// TestConsumerConsumesLivePublishedEvents connects the consumer to a real broker
// and publishes document.created + document.deleted events onto its queue; the
// consumer must route both to the Manager (covering Connect / consume / Close).
func TestConsumerConsumesLivePublishedEvents(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}
	// Per-run unique queue + doc id so a stale message from a previous/concurrent
	// run cannot satisfy the assertions below (false positive).
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	queue := "collab-lifecycle-int-" + suffix
	docID := "live-doc-" + suffix

	mgr := &recordingManager{}
	consumer, err := Connect(Config{URL: url, Queue: queue}, mgr, zap.NewNop())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	publishEvent(t, url, queue, PatternDocumentDeleted, DeletedEvent{ID: docID})

	if !eventually(func() bool { return mgr.has(&mgr.created, docID) }) {
		t.Fatal("consumer never pre-registered the published document.created")
	}
	if !eventually(func() bool { return mgr.has(&mgr.purged, docID) }) {
		t.Fatal("consumer never cascaded the published document.deleted")
	}
}

// publishEvent publishes a NestJS event envelope { pattern, data, id } onto queue.
func publishEvent(t *testing.T, url, queue, pattern string, data any) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("publisher dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("publisher channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal event data: %v", err)
	}
	body, err := json.Marshal(map[string]any{"pattern": pattern, "data": json.RawMessage(raw), "id": "int"})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
