//go:build e2e

// Package e2e drives the collaboration-service end to end through its REAL
// composition root (internal/app.New) — the same wiring cmd/server boots — over
// an httptest server, with real WebSocket clients speaking canonical y-protocols.
// It is the Wave-4 proof (T017) of:
//
//   - single-pod convergence (memo + whiteboard), persistence round-trip, and
//     presence/awareness eviction (SC-002/SC-003/SC-009);
//   - two-pod cross-instance convergence over a shared Redis fan-out, with no
//     code change vs single-pod (SC-007/SC-011);
//   - canonical y-protocols interop against an ACTUAL yjs + y-protocols JS client
//     (test/e2e/jsinterop) — the highest-value compat signal;
//   - limits/authZ enforcement at the WS boundary (SC-008/SC-009).
//
// Build-tagged `e2e` so it is opt-in (`go test -tags e2e ./test/e2e/...`) and
// kept out of the fast unit lane. The Redis two-pod test uses an in-process
// miniredis as the shared bus, so the whole suite is hermetic — no external
// backends required (the durable-backend integration tests are the separate
// `integration` tag).
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/config"
)

// testApp boots the service through the real internal/app composition root for
// the given config and serves it on an httptest server, returning the ws:// base
// URL. The app and server are torn down on test cleanup (releasing every live
// room, persisting a final snapshot each).
func testApp(t *testing.T, cfg *config.Config) string {
	return "ws" + strings.TrimPrefix(testAppHTTP(t, cfg), "http")
}

// testAppHTTP is testApp returning the http:// base URL instead — used by tests
// that drive the REST API (pre-register) or assert a plain-HTTP handshake status
// (401), in addition to the WebSocket surface.
func testAppHTTP(t *testing.T, cfg *config.Config) string {
	t.Helper()
	application, err := app.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	srv := httptest.NewServer(application.Handler)
	t.Cleanup(func() {
		srv.Close()
		application.Close()
	})
	return srv.URL
}

// standaloneConfig is the zero-dependency single-pod config (open/inmemory/
// inline). It sets deliberately fast room cadences — a short save debounce and a
// short idle-release — so the persistence round-trip tests actually force a
// snapshot persist AND an idle-release (cold reload) within test time, rather
// than reconnecting to a still-live room. The production defaults (500ms / 30s)
// are exercised by the unit suite.
func standaloneConfig() *config.Config {
	return &config.Config{
		Port:            0,
		HubMode:         config.HubInMemory,
		MetadataStore:   config.MetadataStoreInMemory,
		CheckpointStore: config.CheckpointStoreInline,
		AuthMode:        config.AuthModeOpen,
		AuthZMode:       config.AuthZModeOpen,
		Limits: config.LimitsConfig{
			MaxDocBytes:                   32 << 20,
			MaxConnsPerRoom:               50,
			UpdateRatePerSec:              50,
			UpdateBurst:                   50,
			CollaboratorInactivitySeconds: 120,
			ContributionWindowSeconds:     60,
			SaveDebounceMillis:            20, // persist quickly so the round-trip is fast
			IdleReleaseSeconds:            0,  // release the room immediately on last leave
		},
	}
}

// --- a real WebSocket client speaking canonical y-protocols ---

// wsClient is a real WebSocket collaboration client: it dials /collab/{id},
// drives the y-protocols sync handshake over the socket, applies received sync
// frames to a local Y.Doc, decodes awareness frames with the canonical
// length-prefixed framing (mirroring a real yjs client), and forwards
// locally-originated edits as sync Update messages. It is the Go counterpart of
// the JS harness, used where a test needs a programmable client.
type wsClient struct {
	t         *testing.T
	conn      *websocket.Conn
	doc       *ycrdt.Doc
	awareness *ycrdt.Awareness
	handler   *protocol.SyncHandler
	mu        chan struct{} // 1-deep semaphore guarding doc/awareness access
	cancel    context.CancelFunc
}

func (c *wsClient) lock()   { c.mu <- struct{}{} }
func (c *wsClient) unlock() { <-c.mu }

// dial connects a wsClient to base for documentID with the given content type
// and starts its read pump (actor-less, for open-auth mode). It initiates
// SyncStep1 so the client receives the server's state.
func dial(t *testing.T, base, documentID, contentType string) *wsClient {
	return dialAsActor(t, base, documentID, contentType, "")
}

// e2eCreated remembers which (pod, document) pairs have been created, so two
// clients dialling the same document do not register it twice — a second create
// is an upsert that bumps the stored version, which the persistence round-trip
// tests read.
var e2eCreated sync.Map

// ensureDocument creates the document over the standalone REST API before the
// first client connects to it.
//
// A document must EXIST before it can be joined — the service refuses an id its
// metadata store has never heard of, which is what stops a deleted document from
// being resurrected by a reconnect. In Alkemio the memo/whiteboard row is that
// record and is created long before any socket opens; standalone, this REST call
// is the equivalent, so the e2e flow now mirrors the real one: create, then
// collaborate.
//
// It is keyed per POD as well as per document because each standalone pod owns
// its own in-memory index: registering on one says nothing about the other, and
// the two-pod tests dial both.
func ensureDocument(t *testing.T, wsBase, documentID, contentType string) {
	t.Helper()
	httpBase := "http" + strings.TrimPrefix(wsBase, "ws")
	if _, loaded := e2eCreated.LoadOrStore(httpBase+"|"+documentID, struct{}{}); loaded {
		return
	}
	// Every e2e document is registered WITH its storage bucket, because that is
	// the only place a bucket can come from: snapshots are written into the
	// document's own bucket and there is no deployment-wide fallback, so a
	// file-service save for a bucket-less row is refused. Harmless for the inline
	// checkpoint store, which never looks at it.
	body, err := json.Marshal(map[string]string{
		"contentType":     contentType,
		"storageBucketId": e2eStorageBucketID,
	})
	if err != nil {
		t.Fatalf("marshal create body: %v", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		httpBase+"/collab/"+documentID, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create %s: %v", documentID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create %s: status = %d, want 201", documentID, resp.StatusCode)
	}
}

// e2eStorageBucketID is the storage bucket every e2e document is registered
// under. In production `server` supplies the document's own bucket on the
// metadata row over RMQ; this raw-config fixture has no bus, so it supplies the
// same field through the create API instead. A file-service save for a row with
// no bucket is refused rather than misfiled into a shared bucket.
const e2eStorageBucketID = "11111111-1111-1111-1111-111111111111"

// e2eActorIDHeader is the dedicated, gateway-owned header the e2e configs name
// as AUTH_TOKEN_HEADER. It matches the Alkemio deployment. It deliberately is
// NOT "Authorization": `header` mode trusts the value verbatim as the actor id,
// so a client-controllable header is refused at startup.
const e2eActorIDHeader = "X-Alkemio-Actor-Id"

// dialAsActor is dial with the gateway-stamped actor id, so authzeval-mode tests
// can connect as a specific actor. An empty actorID sends no header at all,
// which is the open-mode (anonymous) shape.
func dialAsActor(t *testing.T, base, documentID, contentType, actorID string) *wsClient {
	t.Helper()
	var opts *websocket.DialOptions
	if actorID != "" {
		opts = &websocket.DialOptions{HTTPHeader: map[string][]string{e2eActorIDHeader: {actorID}}}
	}
	return dialWithDialOptions(t, base, documentID, contentType, opts)
}

// dialWithDialOptions is the shared dial body: it connects a wsClient with the
// given websocket.DialOptions (arbitrary handshake headers), starts its read
// pump, and initiates SyncStep1. dial and dialAsActor build on it.
func dialWithDialOptions(t *testing.T, base, documentID, contentType string, opts *websocket.DialOptions) *wsClient {
	t.Helper()
	ensureDocument(t, base, documentID, contentType)
	ctx, cancel := context.WithCancel(context.Background())
	url := base + "/collab/" + documentID + "?type=" + contentType
	conn, resp, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		cancel()
		t.Fatalf("dial %s: %v", url, err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	doc := ycrdt.NewDoc(documentID)
	c := &wsClient{
		t:         t,
		conn:      conn,
		doc:       doc,
		awareness: ycrdt.NewAwareness(doc),
		handler:   protocol.NewSyncHandler(doc),
		mu:        make(chan struct{}, 1),
		cancel:    cancel,
	}
	t.Cleanup(func() {
		cancel()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})

	// Forward locally-originated edits to the server as framed sync Updates,
	// skipping updates applied from the server (origin == handler).
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
		_ = c.conn.Write(context.Background(), websocket.MessageBinary, protocol.EncodeUpdate(update))
	}))

	go c.pump(ctx)

	if err := conn.Write(ctx, websocket.MessageBinary, protocol.EncodeSyncStep1(doc)); err != nil {
		t.Fatalf("write SyncStep1: %v", err)
	}
	return c
}

// pump reads frames until the context is cancelled, applying sync via the
// handler and awareness via the canonical length-prefixed decode.
func (c *wsClient) pump(ctx context.Context) {
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		c.dispatch(ctx, data)
	}
}

func (c *wsClient) dispatch(ctx context.Context, frame []byte) {
	in := bytes.NewBuffer(append([]byte(nil), frame...))
	msgType, _, err := protocol.ReadMessage(in)
	if err != nil {
		return
	}
	switch msgType {
	case protocol.MessageSync:
		var reply bytes.Buffer
		c.lock()
		_, herr := c.handler.HandleMessage(frame, &reply)
		c.unlock()
		if herr == nil && reply.Len() > 0 {
			_ = c.conn.Write(ctx, websocket.MessageBinary, reply.Bytes())
		}
	case protocol.MessageAwareness:
		// Canonical y-protocols awareness framing: the payload is a length-prefixed
		// body, which must be unwrapped before applying (a real yjs client does
		// readVarUint8Array → applyAwarenessUpdate).
		//
		// InspectMessage does that unwrapping as part of classifying the frame. The
		// core no longer exports the varint primitives this used to call directly,
		// and that is the right direction: a harness reimplementing the framing byte
		// layout can drift from the server it is supposed to be checking, which is
		// precisely the drift an interop harness exists to catch.
		info, derr := protocol.InspectMessage(frame)
		if derr != nil {
			c.t.Errorf("awareness frame failed canonical decode: %v", derr)
			return
		}
		c.lock()
		ycrdt.ApplyAwarenessUpdate(c.awareness, info.Body, c.handler)
		c.unlock()
	}
}

// insertMemo appends a YXmlText carrying s to the memo's "default" fragment.
func (c *wsClient) insertMemo(s string) {
	c.lock()
	defer c.unlock()
	frag := c.doc.GetXMLFragment("default")
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, s, ycrdt.Object{})
}

// memoText returns the memo's serialized text.
func (c *wsClient) memoText() string {
	c.lock()
	defer c.unlock()
	return c.doc.GetXMLFragment("default").ToString()
}

// addElement sets an id-keyed element on the whiteboard's "elements" Y.Map.
func (c *wsClient) addElement(id string, x float64) {
	c.lock()
	defer c.unlock()
	elements := c.doc.GetMap("elements")
	el := ycrdt.NewYMap(nil)
	elements.Set(id, el)
	el.Set("x", x)
}

// hasElement reports whether the whiteboard holds an element with id.
func (c *wsClient) hasElement(id string) bool {
	c.lock()
	defer c.unlock()
	return c.doc.GetMap("elements").Has(id)
}

// setAwareness sets the client's local awareness state and broadcasts it framed
// canonically (the same framing real yjs clients send).
func (c *wsClient) setAwareness(state ycrdt.Object) {
	c.lock()
	c.awareness.SetLocalState(state)
	update := ycrdt.EncodeAwarenessUpdate(c.awareness, []ycrdt.Number{c.awareness.ClientID}, nil)
	c.unlock()
	// Framed by the core rather than by hand, for the same reason the decode side
	// is: the harness must not carry its own copy of the wire layout.
	_ = c.conn.Write(context.Background(), websocket.MessageBinary,
		protocol.EncodeAwarenessUpdateMessage(update))
}

// awarenessClientCount returns how many awareness client states this client
// currently holds (its own + peers it has learned).
func (c *wsClient) awarenessClientCount() int {
	c.lock()
	defer c.unlock()
	return len(c.awareness.GetStates())
}

// close tears the client's socket down (to simulate a disconnect).
func (c *wsClient) close() {
	c.cancel()
	_ = c.conn.Close(websocket.StatusNormalClosure, "bye")
}

// eventually polls cond up to 5s; cross-pod and persistence delivery are async.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// contains is a readable alias for strings.Contains.
func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// secondConnectionRefused dials a raw second connection to documentID and reports
// whether the server refused it: either the dial fails, or the upgraded socket is
// closed by the server so the first read returns an error. Used to assert the
// connection-cap limit (FR-024) without the dial helper's t.Fatal.
func secondConnectionRefused(t *testing.T, base, documentID string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, base+"/collab/"+documentID, nil)
	if err != nil {
		// Some stacks surface the immediate server close as a dial error — a valid
		// "refused" outcome.
		return true
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	_, _, readErr := conn.Read(ctx)
	// A genuine server-side refusal closes the socket; a context-deadline timeout
	// means the read hung (the connection was NOT refused) — do not count that as
	// a refusal, or a regression that fails to shed the connection would pass.
	if readErr == nil || errors.Is(readErr, context.DeadlineExceeded) {
		return false
	}
	return true
}
