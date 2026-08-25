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
	"fmt"
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
	// ActorIDHeader is the request header the `header` AuthN adapter reads the
	// gateway-stamped actor id from (AUTH_TOKEN_HEADER, e.g. X-Alkemio-Actor-Id).
	//
	// EMPTY MEANS EMPTY — there is no default. `header` mode requires config.Load
	// to have supplied a dedicated gateway-owned name, and `open` mode ignores the
	// credential entirely, so an unset name yields an empty credential and open
	// resolves anonymous. A bearer-style "Authorization" default existed only to
	// carry credentials into a direct-validation adapter that has been removed,
	// and header mode rejects that name outright as client-controllable.
	ActorIDHeader string
	// ReadLimitBytes caps a single inbound WebSocket message. It must exceed
	// MaxDocBytes because a SyncStep2 carries the full v2 snapshot and a single
	// update can add nearly that much (e.g. pasting an image); the 32 KiB
	// coder/websocket default would close the socket with StatusMessageTooBig on
	// any real document. Set via ReadLimitFor at construction; ≤ 0 keeps the
	// coder/websocket default (only tests that exchange small fixtures leave it
	// unset).
	ReadLimitBytes int64
}

// readHeadroomBytes is the slack added to MaxDocBytes when sizing the per-message
// WebSocket read limit. A SyncStep2 carries the full v2 snapshot (≈MaxDocBytes)
// plus a few bytes of y-protocols framing, and awareness/ephemeral frames share
// the same socket; 4 MiB comfortably covers all of it.
const readHeadroomBytes = 4 << 20

// ReadLimitFor sizes the per-message WebSocket read limit from the document size
// bound. A single inbound message — notably a full-doc SyncStep2, but also a
// single update that pastes a large image — can be nearly as large as the encoded
// snapshot, so the limit MUST exceed MaxDocBytes. coder/websocket defaults the
// read limit to 32 KiB, which closes the socket with StatusMessageTooBig on any
// non-trivial document and traps the client in a reconnect loop; this raises it
// to MaxDocBytes + framing headroom. The room still enforces MaxDocBytes on apply,
// so an oversized update is rejected at commit, not silently accepted.
func ReadLimitFor(maxDocBytes int) int64 {
	return int64(maxDocBytes) + readHeadroomBytes
}

// credential reads the one handshake credential off the request and hands it to
// the Auth port (T018.3). The WS adapter only TRANSPORTS it; the selected adapter
// decides what it means, so the Auth port stays infra-free (§I).
//
// An unconfigured ActorIDHeader yields "", which is correct for both modes: `open`
// ignores it, and `header` never reaches here with one unset because config.Load
// refuses to start without a dedicated header name.
func (h *Handler) credential(r *http.Request) string {
	if h.ActorIDHeader == "" {
		return ""
	}
	return r.Header.Get(h.ActorIDHeader)
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

	identity, err := h.Auth.Authenticate(r.Context(), h.credential(r))
	if err != nil {
		// AuthN failure at the handshake → 401: a credential was PRESENTED but is
		// invalid, or a dependency was unreachable (contracts/ws-protocol.md, §V).
		// Whether ABSENCE is a failure is the adapter's call: `header` treats an
		// empty credential as the gateway not having run (401), while `open`
		// resolves it to anonymous and never reaches here.
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	content, err := contentTypeFromRequest(r)
	if err != nil {
		// An EXPLICIT but invalid ?type= is rejected pre-upgrade with a 400, mirroring
		// the REST create contract (resolveContentType → 400 on unknown). Silently
		// defaulting an unknown type to memo would, for a brand-new (no-snapshot)
		// document, seed the wrong convention root and diverge the two creation paths.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, h.AcceptOptions)
	if err != nil {
		// Accept already wrote the HTTP error response.
		h.Logger.Warn("websocket upgrade failed", zap.Error(err))
		return
	}

	// Raise the per-message read limit above the 32 KiB coder/websocket default so
	// a full-doc SyncStep2 or a large update (e.g. an image paste) is not truncated
	// into a connection-closing StatusMessageTooBig that traps the client in a
	// reconnect loop (see ReadLimitFor). MaxDocBytes is still enforced on apply.
	if h.ReadLimitBytes > 0 {
		conn.SetReadLimit(h.ReadLimitBytes)
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
	// drained as it fills rather than after the batch is complete.
	wc.startWriter()

	// Drive the handshake: the server sends SyncStep1 (+ awareness snapshot) so
	// the client replies with SyncStep2 and its own SyncStep1.
	//
	// sendInitial, not Send: starting the writer above does not mean it has been
	// SCHEDULED, so on a small SendBuffer the batch can still fill the queue and
	// Send's slow-consumer shed would drop a client that has done nothing wrong.
	// The batch waits for space instead, bounded by handshakeSendTimeout.
	sendCtx, cancelSend := context.WithTimeout(connCtx, handshakeSendTimeout)
	defer cancelSend()
	for _, frame := range initial {
		if err := wc.sendInitial(sendCtx, frame); err != nil {
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
// The close reason for a full room is the canonical room-capacity-reached code
// (OPEN-1) so the client preserves its read-only/collaborator-mode UX granularity
// — the join is refused (no control frame is sent on a refused join), so the code
// rides the close reason instead.
func joinCloseStatus(err error) (websocket.StatusCode, string) {
	switch {
	case errors.Is(err, service.ErrRoomFull):
		return websocket.StatusPolicyViolation, model.ReasonRoomCapacityReached
	case errors.Is(err, service.ErrForbidden), errors.Is(err, service.ErrDocumentUnknown):
		// ONE refusal for both. A separate status or reason for "no such document"
		// would let anyone holding a socket enumerate which document ids exist by
		// reading the close code, so the two are deliberately identical on the
		// wire and separable only in the server's own logs.
		return websocket.StatusPolicyViolation, "forbidden"
	case errors.Is(err, service.ErrShuttingDown):
		// The pod is going away mid-join. That is not an internal error, and
		// closing 1011 told the client it was one — so a client reconnecting into
		// a rolling deploy saw a server fault rather than "come back". It matches
		// the session-end shutdown case a joined client would have received.
		return websocket.StatusGoingAway, model.CodeServerShutdown
	default:
		return websocket.StatusInternalError, "join failed"
	}
}

// contentTypeFromRequest resolves the document content type from the optional
// ?type= query param (memo|whiteboard). An ABSENT/empty value defaults to memo;
// an EXPLICIT but unknown value is an error (a 400 at the handshake), mirroring the
// REST create contract (resolveContentType) so the two creation paths agree rather
// than one silently seeding the wrong convention. The persisted metadata's content
// type still wins once a document has been saved; this only seeds a brand-new
// document's convention (T010). The authzeval adapter (T006) will instead source it
// from the metadata index.
func contentTypeFromRequest(r *http.Request) (model.ContentType, error) {
	switch t := model.ContentType(r.URL.Query().Get("type")); t {
	case "", model.ContentTypeMemo:
		return model.ContentTypeMemo, nil
	case model.ContentTypeWhiteboard:
		return model.ContentTypeWhiteboard, nil
	default:
		return "", fmt.Errorf("type must be one of memo, whiteboard")
	}
}

// compile-time assertion that the handler is a plain http.Handler.
var _ http.Handler = (*Handler)(nil)
