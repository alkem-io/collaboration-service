//go:build integration

// Integration test for the RabbitMQ metadata store against a real broker, with a
// local echo consumer standing in for the (not-yet-implemented) server consumer.
// Run with: go test -tags=integration ./...
//
// Required env (a local RabbitMQ works):
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/
package rabbitmq

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// startEchoConsumer mimics the server @MessagePattern consumer: it reads the
// NestJS request envelope, replies on replyTo with the correlationId, and serves
// a fixed save/fetch behavior so the adapter's full RPC path is exercised.
func startEchoConsumer(t *testing.T, url, queue string) func() {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	go func() {
		for d := range deliveries {
			var env envelope
			_ = json.Unmarshal(d.Body, &env)
			var resp any
			switch env.Pattern {
			case PatternSave, PatternDelete:
				resp = map[string]bool{"success": true}
			case PatternFetch:
				resp = FetchReply{Found: true, ContentType: "memo", Version: 1, ContentPointer: "ptr", CheckpointStore: "inline"}
			}
			if d.ReplyTo == "" {
				continue
			}
			body, _ := json.Marshal(nestReply{ID: env.ID, IsDisposed: true, Response: mustRaw(resp)})
			_ = ch.PublishWithContext(context.Background(), "", d.ReplyTo, false, false, amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          body,
			})
		}
	}()

	return func() { _ = ch.Close(); _ = conn.Close() }
}

func mustRaw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestRabbitMQRoundTrip(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}
	const queue = "collaboration-test"
	stop := startEchoConsumer(t, url, queue)
	defer stop()

	client, store, err := Connect(Config{URL: url, Queue: queue, RequestTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	if err := store.Save(ctx, model.Metadata{ID: "d", ContentType: model.ContentTypeMemo, ContentPointer: "ptr"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	meta, err := store.Load(ctx, "d")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if meta.ContentPointer != "ptr" {
		t.Errorf("Load = %+v", meta)
	}
	if err := store.Delete(ctx, "d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
