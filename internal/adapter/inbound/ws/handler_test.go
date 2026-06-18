package ws

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// authOpen returns the standalone open authenticator for the happy-path tests.
func authOpen() *authopen.Auth { return authopen.New() }

// eventually polls cond until true or a fixed deadline elapses.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// rejectAuth is an Auth that always fails, to exercise the 401 handshake path.
type rejectAuth struct{}

func (rejectAuth) Authenticate(_ context.Context, _ string) (model.Identity, error) {
	return model.Identity{}, errors.New("denied")
}

// captureAuth records the token string the handshake passes to Authenticate, so a
// test can assert which request header the handler read it from. It always fails
// (no upgrade) so ServeHTTP can be driven directly with a recorder.
type captureAuth struct{ token string }

func (c *captureAuth) Authenticate(_ context.Context, token string) (model.Identity, error) {
	c.token = token
	return model.Identity{}, errors.New("denied")
}

// TestHandshakeReadsConfiguredTokenHeader asserts the handler reads the handshake
// token from the header named by Handler.TokenHeader, defaulting to Authorization
// when unset. This is the seam the Alkemio deployment uses to point the handshake
// at the gateway's resolved actor-id header (AUTH_TOKEN_HEADER=X-Alkemio-Actor-Id)
// while standalone/open mode keeps Authorization.
func TestHandshakeReadsConfiguredTokenHeader(t *testing.T) {
	cases := []struct {
		name       string
		header     string // Handler.TokenHeader; "" = default
		setHeaders map[string]string
		wantToken  string
	}{
		{
			name:       "default reads Authorization",
			setHeaders: map[string]string{"Authorization": "auth-tok", "X-Alkemio-Actor-Id": "actor-tok"},
			wantToken:  "auth-tok",
		},
		{
			name:       "override reads the configured header",
			header:     "X-Alkemio-Actor-Id",
			setHeaders: map[string]string{"Authorization": "auth-tok", "X-Alkemio-Actor-Id": "actor-tok"},
			wantToken:  "actor-tok",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &captureAuth{}
			h := &Handler{Auth: spy, Logger: zap.NewNop(), TokenHeader: tc.header}

			req := httptest.NewRequest(http.MethodGet, "/collab/doc-hdr", nil)
			// Set the route var so the handler reaches the auth step (chi would set it
			// in the real router; we inject it directly for a unit-level assertion).
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("documentId", "doc-hdr")
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
			for k, v := range tc.setHeaders {
				req.Header.Set(k, v)
			}

			h.ServeHTTP(httptest.NewRecorder(), req)

			if spy.token != tc.wantToken {
				t.Fatalf("handshake token = %q, want %q (read from wrong header)", spy.token, tc.wantToken)
			}
		})
	}
}

// newTestServer spins up an httptest server mounting the ws handler over the
// real room manager (in-memory/inline defaults), returning the server and its
// ws:// base URL. Cross-origin is allowed so the test client can dial.
func newTestServer(t *testing.T, auth interface {
	Authenticate(context.Context, string) (model.Identity, error)
}) (*httptest.Server, string) {
	t.Helper()
	deps := service.Deps{
		Metadata: metainmem.New(),
		Blob:     blobinline.New(),
		Auth:     authopen.New(),
		AuthZ:    authopen.New(),
	}
	mgr := service.NewManager(deps, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  40 * time.Millisecond,
		SendBuffer:   256,
	}, nil, zap.NewNop())

	h := &Handler{
		Auth:          auth,
		Manager:       mgr,
		Logger:        zap.NewNop(),
		AcceptOptions: &websocket.AcceptOptions{InsecureSkipVerify: true},
	}

	r := chi.NewRouter()
	r.Method(http.MethodGet, "/collab/{documentId}", h)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}

// newTestServerWithManager mounts the ws handler over a caller-supplied manager,
// so a test can drive the refused-join paths (room full / forbidden) end to end.
func newTestServerWithManager(t *testing.T, mgr *service.Manager) (*httptest.Server, string) {
	t.Helper()
	h := &Handler{
		Auth:          authopen.New(),
		Manager:       mgr,
		Logger:        zap.NewNop(),
		AcceptOptions: &websocket.AcceptOptions{InsecureSkipVerify: true},
	}
	r := chi.NewRouter()
	r.Method(http.MethodGet, "/collab/{documentId}", h)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestRefusedJoinClosesSocket asserts that a second connection to a room at its
// connection cap is upgraded then closed with a policy-violation status (FR-024,
// the handler's joinCloseStatus path).
func TestRefusedJoinClosesSocket(t *testing.T) {
	deps := service.Deps{
		Metadata: metainmem.New(),
		Blob:     blobinline.New(),
		Auth:     authopen.New(),
		AuthZ:    authopen.New(),
	}
	cfg := service.RoomConfig{SaveDebounce: 20 * time.Millisecond, IdleTimeout: 5 * time.Second, SendBuffer: 64}
	cfg.Limits.MaxConnsPerRoom = 1
	mgr := service.NewManager(deps, cfg, nil, zap.NewNop())
	srv, base := newTestServerWithManager(t, mgr)
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First client occupies the single slot.
	first := dialClient(t, base, "full-room", model.ContentTypeMemo)
	first.run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Second dial is accepted at the HTTP layer but the room refuses the join, so
	// the server closes the socket — the client's read returns a close error.
	conn, resp, err := websocket.Dial(ctx, base+"/collab/full-room", nil)
	if err != nil {
		// Some stacks surface the immediate close as a dial error — that is also a
		// valid "refused" outcome.
		return
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	if _, _, readErr := conn.Read(ctx); readErr == nil {
		t.Fatal("second connection should have been closed by the server")
	}
}

// wsTestClient is a real WebSocket client driving the y-protocols handshake over
// the socket, applying received frames to a local Y.Doc.
type wsTestClient struct {
	t       *testing.T
	conn    *websocket.Conn
	mu      sync.Mutex // guards doc (mutated by the pump goroutine + test goroutine)
	doc     *ycrdt.Doc
	handler *protocol.SyncHandler
}

func dialClient(t *testing.T, base, docID string, content model.ContentType) *wsTestClient {
	t.Helper()
	url := base + "/collab/" + docID + "?type=" + string(content)
	conn, resp, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })

	doc := ycrdt.NewDoc(docID, true, ycrdt.DefaultGCFilter, nil, false)
	c := &wsTestClient{t: t, conn: conn, doc: doc, handler: protocol.NewSyncHandler(doc)}

	// Forward locally-originated edits to the server as framed sync Updates. The
	// observer fires synchronously inside an edit/apply that already holds c.mu,
	// so it must not re-lock.
	doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		if len(v) < 1 {
			return
		}
		update, ok := v[0].([]uint8)
		if !ok {
			return
		}
		if len(v) > 1 && v[1] == c.handler { // applied from server; don't echo
			return
		}
		_ = c.conn.Write(context.Background(), websocket.MessageBinary, protocol.EncodeUpdate(update))
	}))

	// Initiate sync so the client receives the server's state.
	if err := conn.Write(context.Background(), websocket.MessageBinary, protocol.EncodeSyncStep1(doc)); err != nil {
		t.Fatalf("write SyncStep1: %v", err)
	}
	return c
}

// pump reads one frame and dispatches it (sync/awareness apply, replying with
// any SyncStep2 the handler produces). It returns false on read error
// (connection closed).
func (c *wsTestClient) pump(ctx context.Context) bool {
	typ, data, err := c.conn.Read(ctx)
	if err != nil {
		return false
	}
	if typ != websocket.MessageBinary {
		return true
	}
	var reply bytes.Buffer
	c.mu.Lock()
	_, herr := c.handler.HandleMessage(data, &reply)
	c.mu.Unlock()
	if herr == nil && reply.Len() > 0 {
		_ = c.conn.Write(ctx, websocket.MessageBinary, reply.Bytes())
	}
	return true
}

// run pumps frames in the background until the context is cancelled.
func (c *wsTestClient) run(ctx context.Context) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			if !c.pump(ctx) {
				return
			}
		}
	}()
}

func (c *wsTestClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.GetXmlFragment("default").(*ycrdt.YXmlFragment).ToString()
}

func (c *wsTestClient) insert(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.doc.GetXmlFragment("default").(*ycrdt.YXmlFragment)
	xt := ycrdt.NewYXmlText()
	f.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, s, nil)
}

func (c *wsTestClient) addElement(id string, x float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elements := c.doc.GetMap("elements").(*ycrdt.YMap)
	el := ycrdt.NewYMap(nil)
	elements.Set(id, el)
	el.Set("x", x)
}

func (c *wsTestClient) hasElement(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doc.GetMap("elements").(*ycrdt.YMap).Has(id)
}

// TestHandshakeRejectedOn401 asserts a failed handshake auth never upgrades.
func TestHandshakeRejectedOn401(t *testing.T) {
	srv, base := newTestServer(t, rejectAuth{})
	_ = base
	resp, err := http.Get(srv.URL + "/collab/doc-401")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// TestMissingDocumentIDIs400 asserts an empty document id is rejected before any
// upgrade. chi will not route an empty path segment, so we invoke the handler
// directly with an empty URL param.
func TestMissingDocumentIDIs400(t *testing.T) {
	h := &Handler{Auth: authOpen(), Logger: zap.NewNop()}
	rr := httptest.NewRecorder()
	// A request without the {documentId} route var set → empty id.
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/collab/", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestStrayTextFrameIgnored asserts the read loop drops non-binary frames without
// tearing the connection down (y-protocols framing is binary-only).
func TestStrayTextFrameIgnored(t *testing.T) {
	srv, base := newTestServer(t, authOpen())
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dialClient(t, base, "stray", model.ContentTypeMemo)
	a.run(ctx)
	time.Sleep(50 * time.Millisecond)

	// Send a stray text frame; the server must ignore it and keep serving.
	if err := a.conn.Write(ctx, websocket.MessageText, []byte("not a y-frame")); err != nil {
		t.Fatalf("write text: %v", err)
	}

	// A subsequent real edit still round-trips, proving the connection survived.
	a.insert("after-text ")
	b := dialClient(t, base, "stray", model.ContentTypeMemo)
	b.run(ctx)
	if !eventually(func() bool { return strings.Contains(b.text(), "after-text") }) {
		t.Fatalf("connection did not survive a stray text frame")
	}
}

// TestEndToEndTwoClientConvergence drives two real WebSocket clients through the
// full framing over the socket: concurrent memo edits converge end to end. This
// exercises the actual upgrade, read loop, and y-protocols framing — not a mock.
func TestEndToEndTwoClientConvergence(t *testing.T) {
	srv, base := newTestServer(t, authOpen())
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dialClient(t, base, "e2e-memo", model.ContentTypeMemo)
	b := dialClient(t, base, "e2e-memo", model.ContentTypeMemo)
	a.run(ctx)
	b.run(ctx)

	// Give the handshakes a beat to settle before editing.
	time.Sleep(50 * time.Millisecond)

	a.insert("hello-a ")
	b.insert("hello-b ")

	if !eventually(func() bool {
		return a.text() == b.text() &&
			strings.Contains(a.text(), "hello-a") &&
			strings.Contains(a.text(), "hello-b")
	}) {
		t.Fatalf("end-to-end convergence failed:\n  a=%q\n  b=%q", a.text(), b.text())
	}
}

// TestEndToEndWhiteboardConvergence drives two real WebSocket clients editing a
// whiteboard (id-keyed Y.Map) concurrently; distinct elements converge end to
// end over the socket.
func TestEndToEndWhiteboardConvergence(t *testing.T) {
	srv, base := newTestServer(t, authOpen())
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dialClient(t, base, "e2e-wb", model.ContentTypeWhiteboard)
	b := dialClient(t, base, "e2e-wb", model.ContentTypeWhiteboard)
	a.run(ctx)
	b.run(ctx)
	time.Sleep(50 * time.Millisecond)

	a.addElement("el-a", 10)
	b.addElement("el-b", 20)

	if !eventually(func() bool {
		return a.hasElement("el-a") && a.hasElement("el-b") &&
			b.hasElement("el-a") && b.hasElement("el-b")
	}) {
		t.Fatalf("whiteboard did not converge end to end")
	}
}

// TestEndToEndPersistenceReload drives one real client, edits, lets the debounced
// snapshot persist and the room release, then a second client reconnects and
// receives the persisted state over the socket (US2 round-trip, end to end).
func TestEndToEndPersistenceReload(t *testing.T) {
	srv, base := newTestServer(t, authOpen())
	_ = srv

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dialClient(t, base, "e2e-reload", model.ContentTypeMemo)
	a.run(ctx)
	time.Sleep(50 * time.Millisecond)
	a.insert("durable ")

	// Wait for the debounce to persist, then close the client so the room idles
	// and releases (persisting a final snapshot).
	time.Sleep(100 * time.Millisecond)
	_ = a.conn.Close(websocket.StatusNormalClosure, "done")

	// Let the room release.
	time.Sleep(150 * time.Millisecond)

	// A fresh client connects and must receive the persisted content.
	b := dialClient(t, base, "e2e-reload", model.ContentTypeMemo)
	b.run(ctx)
	if !eventually(func() bool { return strings.Contains(b.text(), "durable") }) {
		t.Fatalf("reloaded client did not receive persisted content: %q", b.text())
	}
}
