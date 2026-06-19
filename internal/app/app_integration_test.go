//go:build integration

// Integration tests for the composition root against real durable backends, so
// the adapter-selection branches that the hermetic e2e suite cannot reach
// (postgres metadata store, local blob store, and — when a bus is available —
// the rabbitmq metadata store + lifecycle consumer) are exercised through the
// REAL app.New wiring. Run with: go test -tags=integration ./...
//
// Required env (skipped when unset):
//
//	POSTGRES_TEST_DSN=postgres://user:pass@localhost:5432/collab_test?sslmode=disable
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/   (optional)
package app

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// TestNewPostgresLocalRoundTrip boots the full service through app.New with the
// Postgres metadata store and local-disk blob store selected, then drives a real
// WebSocket edit + persistence + reload round-trip — covering buildMetadata's
// postgres branch, buildBlob's local branch, blobKindFor, and New's durable-path
// happy path end to end.
func TestNewPostgresLocalRoundTrip(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN not set")
	}

	cfg := &config.Config{
		Port:          0,
		Fanout:        config.FanoutInMemory,
		MetaStore:     config.MetaStorePostgres,
		BlobStore:     config.BlobStoreLocal,
		AuthMode:      config.AuthModeOpen,
		Postgres:      config.PostgresConfig{DSN: dsn},
		LocalBlobRoot: t.TempDir(),
		Limits: config.LimitsConfig{
			MaxDocBytes: 32 << 20, MaxConnsPerRoom: 50,
			UpdateRatePerSec: 50, UpdateBurst: 50,
			CollaboratorInactivitySeconds: 120, ContributionWindowSeconds: 60,
		},
	}

	application, err := New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("app.New (postgres/local): %v", err)
	}
	t.Cleanup(application.Close)

	srv := httptest.NewServer(application.Handler)
	t.Cleanup(srv.Close)
	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http")

	docID := "app-int-" + time.Now().Format("150405.000")
	a := integDial(t, wsBase, docID)
	time.Sleep(80 * time.Millisecond)
	a.insert("postgres-local-durable ")

	// Let the debounce persist to local disk + the postgres index, then close so
	// the room idle-releases (final snapshot).
	time.Sleep(700 * time.Millisecond)
	a.close()
	time.Sleep(250 * time.Millisecond)

	// A fresh client rehydrates from the postgres index + local blob.
	b := integDial(t, wsBase, docID)
	if !integEventually(func() bool { return strings.Contains(b.text(), "postgres-local-durable") }) {
		t.Fatalf("reload from postgres/local did not converge: %q", b.text())
	}
}

// TestNewRabbitMQModeWires boots app.New in the Alkemio (rabbitmq) topology so
// buildMetadata's rabbitmq branch and startLifecycle (lifecycle.Connect) run
// against a real broker. A successful New + Close proves the bus wiring connects,
// declares its queue, and tears down cleanly.
func TestNewRabbitMQModeWires(t *testing.T) {
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("RABBITMQ_TEST_URL not set")
	}

	cfg := &config.Config{
		Port:      0,
		Fanout:    config.FanoutInMemory,
		MetaStore: config.MetaStoreRabbitMQ,
		BlobStore: config.BlobStoreInline,
		AuthMode:  config.AuthModeOpen,
		// Per-run unique queues so concurrent/previous runs cannot leak state. The
		// lifecycle consumer binds its OWN queue, distinct from the metastore RPC
		// queue (a shared queue round-robin-steals fetch/save RPCs).
		RabbitMQ: config.RabbitMQConfig{
			URL:            url,
			Queue:          "collab-app-int-" + strconv.FormatInt(time.Now().UnixNano(), 36),
			LifecycleQueue: "collab-app-int-lifecycle-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		},
		Limits: config.LimitsConfig{
			MaxDocBytes: 32 << 20, MaxConnsPerRoom: 50,
			UpdateRatePerSec: 50, UpdateBurst: 50,
			CollaboratorInactivitySeconds: 120, ContributionWindowSeconds: 60,
		},
	}

	application, err := New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("app.New (rabbitmq) should wire against a live broker: %v", err)
	}
	// Register teardown immediately so a failing assertion below does not leak the
	// lifecycle consumer + bus client.
	t.Cleanup(application.Close)
	if application.Manager == nil {
		t.Fatal("rabbitmq-mode app has no manager")
	}
}

// --- a minimal real WS client (the richer harness lives behind the e2e tag) ---

type integClient struct {
	conn    *websocket.Conn
	doc     *ycrdt.Doc
	handler *protocol.SyncHandler
	mu      sync.Mutex
}

func integDial(t *testing.T, base, documentID string) *integClient {
	t.Helper()
	conn, resp, err := websocket.Dial(context.Background(), base+"/collab/"+documentID+"?type=memo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })

	doc := ycrdt.NewDoc(documentID, true, ycrdt.DefaultGCFilter, nil, false)
	c := &integClient{conn: conn, doc: doc, handler: protocol.NewSyncHandler(doc)}
	doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		if len(v) < 1 {
			return
		}
		update, ok := v[0].([]uint8)
		if !ok {
			return
		}
		if len(v) > 1 && v[1] == c.handler {
			return
		}
		_ = conn.Write(context.Background(), websocket.MessageBinary, protocol.EncodeUpdate(update))
	}))
	go c.pump()
	_ = conn.Write(context.Background(), websocket.MessageBinary, protocol.EncodeSyncStep1(doc))
	return c
}

func (c *integClient) pump() {
	for {
		typ, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		var reply bytes.Buffer
		c.mu.Lock()
		_, herr := c.handler.HandleMessage(data, &reply)
		c.mu.Unlock()
		if herr == nil && reply.Len() > 0 {
			_ = c.conn.Write(context.Background(), websocket.MessageBinary, reply.Bytes())
		}
	}
}

func (c *integClient) insert(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	frag := c.doc.GetXmlFragment("default").(*ycrdt.YXmlFragment)
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, s, nil)
}

func (c *integClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.GetXmlFragment("default").(*ycrdt.YXmlFragment).ToString()
}

func (c *integClient) close() { _ = c.conn.Close(websocket.StatusNormalClosure, "bye") }

func integEventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
