// Package ws is the inbound WebSocket adapter: it terminates the raw WebSocket
// connection at wss://<host>/collab/<documentId> (one document per connection,
// y-websocket model), runs AuthN at the handshake, and carries y-protocols sync
// + awareness plus the custom ephemeral channel (contracts/ws-protocol.md). It
// translates wire messages to/from the domain core and never holds business
// logic itself.
//
// This is the Phase-1 (provisioning) stub: it accepts the upgrade, performs the
// handshake authentication via the Auth port, and closes with a "not yet
// implemented" status. The y-protocols sync/awareness/ephemeral handling and
// room wiring land with tasks T008–T012 of
// specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package ws

import (
	"net/http"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Handler upgrades /collab/{documentId} requests to WebSocket connections and
// (once implemented) drives the y-protocols sync + awareness exchange for the
// addressed document.
type Handler struct {
	// Auth resolves the connecting identity at the handshake (401 on failure).
	Auth port.Auth
	// Logger is the request-scoped structured logger.
	Logger *zap.Logger
}

// handshakeTokenHeader carries the Alkemio token/cookie surrogate. The real
// adapter (task T006) resolves identity from the Oathkeeper/Kratos cookie; the
// skeleton reads a bearer-style header so the Auth port is exercised end to end.
const handshakeTokenHeader = "Authorization"

// ServeHTTP authenticates the handshake, upgrades to WebSocket, and (for now)
// closes the connection with a "not implemented" status so the route, the
// upgrade path, and the Auth port are wired and testable ahead of the
// y-protocols implementation.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	documentID := model.DocumentID(chi.URLParam(r, "documentId"))
	if documentID == "" {
		http.Error(w, "missing document id", http.StatusBadRequest)
		return
	}

	if _, err := h.Auth.Authenticate(r.Context(), r.Header.Get(handshakeTokenHeader)); err != nil {
		// AuthN failure at the handshake → 401 (contracts/ws-protocol.md).
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// Accept already wrote the HTTP error response.
		h.Logger.Warn("websocket upgrade failed", zap.Error(err))
		return
	}

	// Prove the vendored CRDT core resolves and is callable: materialize the
	// authoritative Y.Doc for this document and its y-protocols sync handler.
	// The room lifecycle (load snapshot, register the connection, debounced
	// persistence) and the actual SyncStep1/SyncStep2/Update + awareness
	// exchange land with tasks T007–T012; the Phase-1 skeleton only constructs
	// the core to verify the binding, then closes cleanly.
	doc := newRoomDoc(string(documentID))
	_ = protocol.NewSyncHandler(doc)

	_ = conn.Close(websocket.StatusInternalError, "collab transport not yet implemented")
}

// newRoomDoc constructs the authoritative, garbage-collected Y.Doc that backs a
// room. The collaboration service holds plaintext docs (FR-021); GC is enabled
// (the configurable GC policy is FR-025, refined in the y-crdt fork).
func newRoomDoc(guid string) *ycrdt.Doc {
	return ycrdt.NewDoc(guid, true, nil, nil, false)
}
