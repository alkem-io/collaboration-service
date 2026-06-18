package service

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeClient is an in-process y-protocols peer driven through a room. It holds a
// local Y.Doc, a SyncHandler bound to it, and the room Session it joined. Frames
// the room fans out are applied to the local doc via the SyncHandler; frames the
// client wants to send are forwarded to the room. It is the test stand-in for a
// browser client, exercising the exact wire framing without a socket.
type fakeClient struct {
	t       *testing.T
	doc     *ycrdt.Doc
	aware   *ycrdt.Awareness
	handler *protocol.SyncHandler

	mu        sync.Mutex
	session   *Session
	received  [][]byte // every frame the room sent us, in order
	ephemeral [][]byte // type-2 ephemeral payloads (post-frame)
	control   []model.ControlMessage
}

func newFakeClient(t *testing.T) *fakeClient {
	t.Helper()
	doc := ycrdt.NewDoc("guid", true, ycrdt.DefaultGCFilter, nil, false)
	aw := ycrdt.NewAwareness(doc)
	h := protocol.NewSyncHandler(doc)
	h.SetAwareness(aw)
	return &fakeClient{t: t, doc: doc, aware: aw, handler: h}
}

// Send implements service.Conn: the room calls it (from the room's run loop
// goroutine) to deliver a frame. It applies sync/awareness frames to the local
// doc so the client converges, recording control/ephemeral frames for asserts.
// All local-doc access is guarded by c.mu so the room goroutine and the test
// goroutine never touch the doc concurrently (the doc is not thread-safe).
func (c *fakeClient) Send(frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]byte(nil), frame...)
	c.received = append(c.received, cp)

	in := bytes.NewBuffer(cp)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		return nil
	}
	switch model.WireMessageType(msgType) {
	case model.WireSync, model.WireAwareness:
		// Apply to the local doc/awareness; a SyncStep1 from the server yields a
		// SyncStep2 reply, which we forward back through the session.
		var reply bytes.Buffer
		if _, err := c.handler.HandleMessage(cp, &reply); err == nil && reply.Len() > 0 && c.session != nil {
			c.session.Forward(reply.Bytes())
		}
	case model.WireEphemeral:
		c.ephemeral = append(c.ephemeral, append([]byte(nil), payload...))
	case model.WireControl:
		var msg model.ControlMessage
		if err := json.Unmarshal(payload, &msg); err == nil {
			c.control = append(c.control, msg)
		}
	}
	return nil
}

// join attaches the client to the room manager, applies the server's initial
// frames (the server's SyncStep1 + awareness snapshot), and then drives the
// client's own SyncStep1 so the server replies with the structs the client is
// missing. This is the full y-websocket bidirectional handshake.
func (c *fakeClient) join(m *Manager, id model.DocumentID, content model.ContentType) {
	c.t.Helper()
	session, initial, err := m.Join(context.Background(), id, content, c)
	if err != nil {
		c.t.Fatalf("join: %v", err)
	}
	c.mu.Lock()
	c.session = session
	c.mu.Unlock()

	for _, f := range initial {
		_ = c.Send(f)
	}
	// The client initiates its own sync so it receives the server's state.
	c.withDoc(func(doc *ycrdt.Doc) {
		c.session.Forward(protocol.EncodeSyncStep1(doc))
	})
}

// withDoc runs fn with the local doc under the client lock so doc access is
// serialized with the room's Send goroutine.
func (c *fakeClient) withDoc(fn func(doc *ycrdt.Doc)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c.doc)
}

// observeUpdates registers a doc observer that forwards every locally-originated
// edit to the room, mirroring a live client. Registration mutates the doc's
// observer map, so it is done under c.mu (the room's Send goroutine reads that
// map when it emits update events). The observer itself fires synchronously
// inside an edit/apply that already holds c.mu, so it must not re-lock.
func (c *fakeClient) observeUpdates() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.doc.On("update", ycrdt.NewObserverHandler(func(v ...interface{}) {
		if len(v) == 0 {
			return
		}
		update, ok := v[0].([]uint8)
		if !ok {
			return
		}
		// Skip updates the client applied from the server (origin == handler);
		// only forward locally-originated edits (origin nil).
		if len(v) > 1 && v[1] == c.handler {
			return
		}
		c.session.Forward(protocol.EncodeUpdate(update))
	}))
}

func (c *fakeClient) ephemeralCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ephemeral)
}

func (c *fakeClient) controlKinds() []model.ControlKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	kinds := make([]model.ControlKind, 0, len(c.control))
	for _, m := range c.control {
		kinds = append(kinds, m.Kind)
	}
	return kinds
}

// testManager wires the in-memory/inline default adapters with a fast cadence so
// tests don't wait seconds for debounce/idle.
func testManager(t *testing.T, cfg RoomConfig) (*Manager, testDeps) {
	t.Helper()
	deps := newTestDeps()
	return NewManager(deps.Deps, cfg, nil, zap.NewNop()), deps
}

// testDeps is the bundle of shared adapters a test can inspect (metastore/blob).
type testDeps struct {
	Deps
	meta *metainmem.Store
	blob *blobinline.Store
}

func newTestDeps() testDeps {
	meta := metainmem.New()
	blob := blobinline.New()
	open := authopen.New()
	return testDeps{
		Deps: Deps{
			Metadata: meta,
			Blob:     blob,
			Auth:     open,
			AuthZ:    open,
		},
		meta: meta,
		blob: blob,
	}
}

// waitFor polls cond until true or the deadline, failing the test otherwise.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
