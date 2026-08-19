package service

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/hub"
	"github.com/antst/go-yjs/backend/persistence"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeClient is an in-process y-protocols peer driven through a room. It holds a
// local Y.Doc, a SyncHandler bound to it, and the room Session it joined. Frames
// the room fans out are applied to the local doc via the SyncHandler; frames the
// client wants to send are forwarded to the room. It is the test stand-in for a
// browser client, exercising the exact wire framing without a socket.
type fakeClient struct {
	t        *testing.T
	doc      *ycrdt.Doc
	aware    *ycrdt.Awareness
	handler  *protocol.SyncHandler
	identity model.Identity

	mu        sync.Mutex
	session   *Session
	received  [][]byte // every frame the room sent us, in order
	ephemeral [][]byte // type-2 ephemeral payloads (post-frame)
	control   []model.ControlMessage
	blocked   bool // simulates the inbound side of a network partition: Send drops frames
}

func newFakeClient(t *testing.T) *fakeClient {
	return newFakeClientWithIdentity(t, "")
}

// newFakeClientWithIdentity builds a fake client carrying an authenticated actor
// id, so presence/contribution/authZ paths that key off the actor are exercised.
func newFakeClientWithIdentity(t *testing.T, actorID string) *fakeClient {
	t.Helper()
	doc := ycrdt.NewDoc("guid")
	aw := ycrdt.NewAwareness(doc)
	h := protocol.NewSyncHandler(doc)
	h.SetAwareness(aw)
	return &fakeClient{t: t, doc: doc, aware: aw, handler: h, identity: model.Identity{ActorID: actorID}}
}

// Send implements service.Conn: the room calls it (from the room's run loop
// goroutine) to deliver a frame. It applies sync/awareness frames to the local
// doc so the client converges, recording control/ephemeral frames for asserts.
// All local-doc access is guarded by c.mu so the room goroutine and the test
// goroutine never touch the doc concurrently (the doc is not thread-safe).
// When the client is partitioned (partition()), frames are silently dropped.
func (c *fakeClient) Send(frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Drop all inbound frames while partitioned — simulates the client being
	// unreachable on the network (it cannot receive the server's fan-out).
	if c.blocked {
		return nil
	}
	cp := append([]byte(nil), frame...)
	c.received = append(c.received, cp)

	in := bytes.NewBuffer(cp)
	msgType, payload, err := protocol.ReadMessage(in)
	if err != nil {
		return nil
	}
	switch model.WireMessageType(msgType) {
	case model.WireSync:
		// Apply to the local doc; a SyncStep1 from the server yields a SyncStep2
		// reply, which we forward back through the session.
		var reply bytes.Buffer
		if _, err := c.handler.HandleMessage(cp, &reply); err == nil && reply.Len() > 0 && c.session != nil {
			c.session.Forward(reply.Bytes())
		}
	case model.WireAwareness:
		// Decode the canonical y-protocols awareness framing
		// ([type][writeVarUint8Array(body)], awareness_wire.go) and apply the body
		// — modelling a real y-protocols client, which reads readVarUint8Array
		// before applyAwarenessUpdate.
		if body, ok := awarenessBody(cp); ok {
			// Best-effort, mirroring a real client's receive path: a malformed
			// awareness body is dropped rather than crashing the fan-out goroutine
			// (Send runs on the room's goroutine, not the test's, so it must not
			// Fatal). awarenessBody already guarded the framing.
			_ = ycrdt.ApplyAwarenessUpdate(c.aware, body, c.handler)
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
	session, initial, err := m.Join(context.Background(), JoinRequest{
		ID: id, Content: content, Identity: c.identity, Conn: c,
	})
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
	// store is the in-process CheckpointStore backing these tests. It has the same
	// SHAPE as the deployed file-service store — one current state per document,
	// replaced on save — so a test never exercises a persistence model production
	// does not use.
	store *persistinprocess.Store
}

func newTestDeps() testDeps {
	meta := metainmem.New()
	store := persistinprocess.New()
	open := authopen.New()
	return testDeps{
		Deps: Deps{
			// The core's shipped in-process hub, as NewManager would supply. Tests
			// that build a Room directly bypass that default, and the room publishes
			// on every edit — a nil hub panics on the first one.
			Hub:        hub.NewInProcess(),
			Metadata:   meta,
			Checkpoint: store,
			Auth:       open,
			AuthZ:      open,
		},
		meta:  meta,
		store: store,
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

// --- stored-state helpers for tests -----------------------------------------
//
// The old BlobStore was addressed by content pointer; a CheckpointStore is
// addressed by document id and returns the document's whole current state. These
// keep the call sites readable and put the id conversion in one place.

// storedState returns a document's stored state.
func (d testDeps) storedState(ctx context.Context, id string) ([]byte, error) {
	cp, err := d.store.LoadCheckpoint(ctx, backend.DocumentID(id))
	if err != nil {
		return nil, err
	}
	return cp.Update, nil
}

// putState writes a document's state directly, for tests that need durable state
// to exist without driving a room.
func (d testDeps) putState(ctx context.Context, id string, update []byte) error {
	_, err := d.store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(id), Update: update, StateVector: []byte("derived-on-read"),
	})
	return err
}
