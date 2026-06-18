// Package ws is the inbound WebSocket adapter: it terminates the raw WebSocket
// connection at wss://<host>/collab/<documentId> (one document per connection,
// y-websocket model), runs AuthN at the handshake, and carries y-protocols sync
// + awareness plus the custom ephemeral/control channels
// (contracts/ws-protocol.md). It translates wire messages to/from the domain
// core (service.Manager / service.Room) and holds no business logic itself.
package ws

import (
	"context"
	"errors"
	"net/http"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// Handler upgrades /collab/{documentId} requests to WebSocket connections and
// drives the y-protocols sync + awareness + ephemeral/control exchange for the
// addressed document via the room manager.
type Handler struct {
	// Auth resolves the connecting identity at the handshake (401 on failure).
	Auth port.Auth
	// Manager owns the room lifecycle the connection joins.
	Manager *service.Manager
	// Logger is the request-scoped structured logger.
	Logger *zap.Logger
	// AcceptOptions are passed to websocket.Accept. Tests set
	// InsecureSkipVerify to dial the httptest server cross-origin; production
	// leaves origin checking on.
	AcceptOptions *websocket.AcceptOptions
	// TokenHeader is the request header the handshake reads the Alkemio
	// token/identity surrogate from. Empty selects the default
	// (defaultTokenHeader). The Alkemio deployment terminates auth at the gateway
	// and forwards the resolved actor id in a header (e.g. X-Alkemio-Actor-Id),
	// while standalone/open mode keeps a bearer-style Authorization header; this
	// field lets the deployment point the handshake at whichever header carries
	// the identity (AUTH_TOKEN_HEADER).
	TokenHeader string
}

// defaultTokenHeader carries the Alkemio token/cookie surrogate when no header is
// configured. The open adapter reads a bearer-style header so the Auth port is
// exercised end to end; the Alkemio deployment overrides this (via the Handler's
// TokenHeader) with the gateway's resolved actor-id header.
const defaultTokenHeader = "Authorization"

// tokenHeader returns the configured handshake header, or the default when unset.
func (h *Handler) tokenHeader() string {
	if h.TokenHeader != "" {
		return h.TokenHeader
	}
	return defaultTokenHeader
}

// ServeHTTP authenticates the handshake, upgrades to WebSocket, joins the room
// for the addressed document, and runs the per-connection read loop until the
// client disconnects.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	documentID := model.DocumentID(chi.URLParam(r, "documentId"))
	if documentID == "" {
		http.Error(w, "missing document id", http.StatusBadRequest)
		return
	}

	identity, err := h.Auth.Authenticate(r.Context(), r.Header.Get(h.tokenHeader()))
	if err != nil {
		// AuthN failure at the handshake → 401 (contracts/ws-protocol.md).
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	content := contentTypeFromRequest(r)

	conn, err := websocket.Accept(w, r, h.AcceptOptions)
	if err != nil {
		// Accept already wrote the HTTP error response.
		h.Logger.Warn("websocket upgrade failed", zap.Error(err))
		return
	}

	h.serve(r.Context(), conn, documentID, content, identity)
}

// serve wires the accepted connection to a room and pumps frames until the read
// loop ends, then leaves the room and closes the socket.
func (h *Handler) serve(ctx context.Context, conn *websocket.Conn, id model.DocumentID, content model.ContentType, identity model.Identity) {
	// net/http cancels the request context once ServeHTTP returns, so we detach
	// the connection's lifetime from it: the hijacked WebSocket outlives the
	// handler return, and the read loop's error (client close / network) is the
	// termination signal.
	connCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	wc := newWSConn(connCtx, conn, h.Manager.SendBuffer(), h.Logger)
	defer wc.close()

	session, initial, err := h.Manager.Join(connCtx, service.JoinRequest{
		ID: id, Content: content, Identity: identity, Conn: wc,
	})
	if err != nil {
		// A refused join (full room / access denied) closes the upgraded socket
		// with a policy-violation status so the client does not retry blindly; a
		// fail-closed authZ error closes with an internal status.
		status, reason := joinCloseStatus(err)
		h.Logger.Warn("room join refused", zap.String("doc", string(id)), zap.Error(err))
		_ = conn.Close(status, reason)
		return
	}
	defer session.Leave()

	// Start the writer before enqueuing any frames so the bounded send queue is
	// drained as it fills: the initial handshake batch then cannot overflow a
	// small SendBuffer and trip the slow-consumer eviction before delivery.
	wc.startWriter()

	// Drive the handshake: the server sends SyncStep1 (+ awareness snapshot) so
	// the client replies with SyncStep2 and its own SyncStep1.
	for _, frame := range initial {
		if err := wc.Send(frame); err != nil {
			return
		}
	}

	h.readLoop(connCtx, conn, session)
}

// readLoop reads framed binary messages off the socket and forwards each to the
// room for serialized handling, until the client closes or an error occurs.
func (h *Handler) readLoop(ctx context.Context, conn *websocket.Conn, session *service.Session) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); status != -1 && status != websocket.StatusNormalClosure {
				h.Logger.Debug("connection closed", zap.Int("status", int(status)))
			}
			return
		}
		if typ != websocket.MessageBinary {
			// y-protocols framing is binary; ignore stray text frames.
			continue
		}
		session.Forward(data)
	}
}

// joinCloseStatus maps a refused-join error to a WebSocket close status and
// reason. A full room or access denial is a policy violation (the client should
// not retry blindly); any other error (a fail-closed authZ failure) is internal.
func joinCloseStatus(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, service.ErrRoomFull):
		return websocket.StatusPolicyViolation, "room full"
	case errors.Is(err, service.ErrForbidden):
		return websocket.StatusPolicyViolation, "forbidden"
	default:
		return websocket.StatusInternalError, "join failed"
	}
}

// contentTypeFromRequest resolves the document content type from the optional
// ?type= query param (memo|whiteboard), defaulting to memo. The persisted
// metadata's content type wins once a document has been saved; this only seeds a
// brand-new document's convention (T010). The authzeval adapter (T006) will
// instead source it from the metadata index.
func contentTypeFromRequest(r *http.Request) model.ContentType {
	switch model.ContentType(r.URL.Query().Get("type")) {
	case model.ContentTypeWhiteboard:
		return model.ContentTypeWhiteboard
	default:
		return model.ContentTypeMemo
	}
}

// compile-time assertion that the handler is a plain http.Handler.
var _ http.Handler = (*Handler)(nil)
