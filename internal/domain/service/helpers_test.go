package service

import (
	"bytes"
	"encoding/json"
	"strings"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/antst/go-yjs/protocol"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// --- memo (Y.XmlFragment) helpers ---

// insertText appends a YXmlText node carrying s to the memo's "default"
// fragment. Two clients each appending their own node converge to a document
// containing both (CRDT, no last-write-wins loss) — the property the convergence
// tests assert.
func insertText(doc *ycrdt.Doc, s string) {
	f := doc.GetXmlFragment("default")
	xt := ycrdt.NewYXmlText()
	f.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, s, ycrdt.Object{})
}

// xmlText renders the memo's "default" fragment to a plain string.
func xmlText(doc *ycrdt.Doc) string {
	return doc.GetXmlFragment("default").ToString()
}

// --- whiteboard (id-keyed Y.Map) helpers ---

// addElement inserts a per-element Y.Map keyed by id into the "elements" root
// map, mirroring the Excalidraw scene convention (data-model.md).
func addElement(doc *ycrdt.Doc, id string, props map[string]interface{}) {
	elements := doc.GetMap("elements")
	el := ycrdt.NewYMap(nil)
	elements.Set(id, el)
	for k, v := range props {
		el.Set(k, v)
	}
}

func elements(doc *ycrdt.Doc) *ycrdt.YMap { return doc.GetMap("elements") }

func elementsLen(doc *ycrdt.Doc) int { return elements(doc).GetSize() }

func hasElement(doc *ycrdt.Doc, id string) bool { return elements(doc).Has(id) }

func elementKeys(doc *ycrdt.Doc) []string { return elements(doc).Keys() }

// docMentions reports whether the document's JSON serialization contains needle —
// a coarse leak check used to assert ephemeral/awareness state never reaches the
// persisted snapshot.
func docMentions(doc *ycrdt.Doc, needle string) bool {
	return strings.Contains(ycrdtJSON(doc), needle)
}

// ycrdtJSON renders a doc's shared state as a JSON string (diagnostic helper).
// The core no longer exports a JSON stringifier, so this marshals the Object
// itself — a test-only diagnostic, and going through encoding/json avoids
// depending on an internal serializer for a leak assertion.
func ycrdtJSON(doc *ycrdt.Doc) string {
	b, err := json.Marshal(doc.ToJson())
	if err != nil {
		return ""
	}
	return string(b)
}

// --- wire-frame helpers ---

// encodeAwareness frames a full awareness update as a canonical y-protocols
// type-1 message ([type][writeVarUint8Array(body)]) — the same framing real yjs
// clients speak (awareness_wire.go), exercised end to end by the JS-interop e2e.
func encodeAwareness(update []byte) []byte {
	return encodeAwarenessFrame(update)
}

// encodeEphemeral frames an arbitrary ephemeral payload as a type-2 message
// (the custom whiteboard ephemeral channel).
func encodeEphemeral(payload []byte) []byte {
	var buf bytes.Buffer
	protocol.WriteMessage(&buf, uint8(model.WireEphemeral), payload)
	return buf.Bytes()
}

// --- locked client doc accessors ---
//
// Every doc read/edit a test performs goes through one of these so it is
// serialized (under c.mu) with the room's Send goroutine, which also mutates the
// doc. The underlying Y.Doc is not safe for concurrent access.

func (c *fakeClient) insertText(s string) {
	c.withDoc(func(doc *ycrdt.Doc) { insertText(doc, s) })
}

func (c *fakeClient) text() string {
	var out string
	c.withDoc(func(doc *ycrdt.Doc) { out = xmlText(doc) })
	return out
}

func (c *fakeClient) addElement(id string, props map[string]interface{}) {
	c.withDoc(func(doc *ycrdt.Doc) { addElement(doc, id, props) })
}

func (c *fakeClient) elementsLen() int {
	var n int
	c.withDoc(func(doc *ycrdt.Doc) { n = elementsLen(doc) })
	return n
}

func (c *fakeClient) hasElement(id string) bool {
	var ok bool
	c.withDoc(func(doc *ycrdt.Doc) { ok = hasElement(doc, id) })
	return ok
}

func (c *fakeClient) elementKeys() []string {
	var keys []string
	c.withDoc(func(doc *ycrdt.Doc) { keys = elementKeys(doc) })
	return keys
}

// setAwareness updates the client's local awareness state and forwards the
// framed awareness update to the room (cursor/presence), under the lock.
func (c *fakeClient) setAwareness(state ycrdt.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.aware.SetLocalState(state)
	update := ycrdt.EncodeAwarenessUpdate(c.aware, []ycrdt.Number{c.aware.ClientID}, nil)
	c.session.Forward(encodeAwareness(update))
}

// awarenessUserOf returns the "user" field of the awareness state this client
// holds for the given y client id, under the lock.
func (c *fakeClient) awarenessUserOf(clientID ycrdt.Number) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st := c.aware.GetStates()[clientID]; !st.IsNil() {
		return st.GetOr("user")
	}
	return nil
}

// --- fakeClient extensions ---

// partition simulates cutting the client's inbound network: Send drops all
// incoming frames until unpartition is called. The client's local doc and any
// outbound forwarding (once observeUpdates is registered) are unaffected —
// modelling a real offline client that keeps editing locally.
func (c *fakeClient) partition() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = true
}

// unpartition restores inbound delivery, modelling a reconnect.
func (c *fakeClient) unpartition() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = false
}

// pushBufferedAndResync simulates a reconnecting client (US5): it pushes its
// full local state up as a sync Update (so the server learns the client's
// offline-buffered edits and fans them to peers) and then drives SyncStep1 (so
// the server replies with the delta the client is missing). The combination
// converges both sides with no lost edits.
func (c *fakeClient) pushBufferedAndResync() {
	c.withDoc(func(doc *ycrdt.Doc) {
		// Send everything the client has; the server applies what it is missing.
		full, err := ycrdt.EncodeStateAsUpdate(doc, nil)
		if err != nil {
			c.t.Fatalf("encode client state: %v", err)
		}
		c.session.Forward(protocol.EncodeUpdate(full))
		// Ask the server for anything the client is missing.
		c.session.Forward(protocol.EncodeSyncStep1(doc))
	})
}

// hasControlKind reports whether the client has received a control message of
// the given kind.
func hasControlKind(c *fakeClient, kind model.ControlKind) bool {
	for _, k := range c.controlKinds() {
		if k == kind {
			return true
		}
	}
	return false
}

// releaseRoom returns a cleanup that tears a room down through the production
// funnel. It exists because teardown REQUIRES a session end — the argument that
// makes a silent teardown impossible to write — so tests name one too rather
// than reaching for a special no-reason entry point that production does not
// have.
func releaseRoom(r *Room) func() {
	return func() { r.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil) }
}

// hasControlCode reports whether the client received a session-end control
// carrying the given code.
func hasControlCode(c *fakeClient, code model.SessionEndCode) bool {
	for _, m := range c.controlMessages() {
		if m.Kind == model.ControlSessionEnd && m.Code == code {
			return true
		}
	}
	return false
}

// contains is a readable alias for strings.Contains used throughout the tests.
func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }
