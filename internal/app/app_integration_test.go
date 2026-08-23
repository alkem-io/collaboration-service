//go:build integration

// Integration tests for the composition root against real durable backends, so
// the adapter-selection branches the hermetic e2e suite cannot reach (the
// rabbitmq metadata store + lifecycle consumer) are exercised through the REAL
// app.New wiring. Run with: go test -tags=integration ./...
//
// Required env (skipped when unset):
//
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/
package app

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
)

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
		Port:            0,
		HubMode:         config.HubInMemory,
		MetadataStore:   config.MetadataStoreRabbitMQ,
		CheckpointStore: config.CheckpointStoreInline,
		AuthMode:        config.AuthModeOpen,
		// Per-run unique queues so concurrent/previous runs cannot leak state. The
		// lifecycle consumer binds its OWN queue, distinct from the metadata-store RPC
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

	doc := ycrdt.NewDoc(documentID)
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
	frag := c.doc.GetXMLFragment("default")
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, s, ycrdt.Object{})
}

func (c *integClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.GetXMLFragment("default").ToString()
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
