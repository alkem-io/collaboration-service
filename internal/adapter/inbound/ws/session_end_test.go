package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/antst/go-yjs/protocol"
	"github.com/coder/websocket"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// readControl reads frames off a live socket until it finds a control message of
// the wanted kind, returning it. A close arriving first is reported as such, so
// a test can tell "the reason never came" from "the reason came late".
func readControl(t *testing.T, conn *websocket.Conn, kind model.ControlKind) (model.ControlMessage, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return model.ControlMessage{}, err
		}
		msgType, payload, perr := protocol.ReadMessage(bytes.NewBuffer(data))
		if perr != nil || model.WireMessageType(msgType) != model.WireControl {
			continue
		}
		var msg model.ControlMessage
		if json.Unmarshal(payload, &msg) != nil {
			continue
		}
		if msg.Kind == kind {
			return msg, nil
		}
	}
}

// managerForEnd builds a manager over a REAL metadata store (no openDocs double)
// so document existence behaves exactly as it does in production.
func managerForEnd(t *testing.T, docs ...model.DocumentID) (*service.Manager, string) {
	t.Helper()
	meta := metainmem.New()
	mgr := service.NewManager(service.Deps{
		Metadata:   meta,
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authopen.New(),
	}, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
	}, nil, zap.NewNop())
	t.Cleanup(mgr.Close)
	for _, doc := range docs {
		if err := mgr.PreRegister(context.Background(), model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
			t.Fatalf("pre-register %s: %v", doc, err)
		}
	}
	return mgr, newTestServerWithManager(t, mgr)
}

// TestSessionEndReachesTheClientBeforeTheClose is the ordering guarantee on the
// REAL handler path — a genuine socket, the real writer goroutine, a real close
// handshake — rather than against a test double.
//
// The frame and the close travel the same per-connection queue, so the client
// cannot observe the close first. Before this, a torn-down room left the socket
// OPEN and silently dropped whatever the client sent next; the client learned
// nothing, and eventually saw an abnormal 1006 with no reason attached.
//
// Non-vacuity: make CloseAfterDrain call close() directly instead of queueing the
// intent, and the read below returns a close error instead of the control frame.
func TestSessionEndReachesTheClientBeforeTheClose(t *testing.T) {
	const doc model.DocumentID = "end-order"
	mgr, base := managerForEnd(t, doc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, base+"/collab/"+string(doc)+"?type=memo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	// Wait until the session actually exists before shutting down. Dial returns
	// once the HTTP upgrade completes, which is BEFORE the handler has joined the
	// room — closing here would race the join and measure a refused join instead
	// of the teardown this test is about.
	readCtx0, cancel0 := context.WithTimeout(context.Background(), 5*time.Second)
	if _, _, err := conn.Read(readCtx0); err != nil {
		cancel0()
		t.Fatalf("handshake frame never arrived: %v", err)
	}
	cancel0()

	// Graceful shutdown: the retryable case, which used to carry nothing at all.
	go mgr.Close()

	msg, err := readControl(t, conn, model.ControlSessionEnd)
	if err != nil {
		t.Fatalf("the socket closed before the reason arrived: %v", err)
	}
	if msg.Code != model.CodeServerShutdown {
		t.Errorf("code = %q, want %q", msg.Code, model.CodeServerShutdown)
	}
	if msg.Disposition != model.DispositionTransient {
		t.Errorf("disposition = %q, want %q", msg.Disposition, model.DispositionTransient)
	}
	if msg.Scope != model.ScopeDocument {
		t.Errorf("scope = %q, want %q", msg.Scope, model.ScopeDocument)
	}

	// AND the close follows, carrying the same code as its reason so a client that
	// only inspects the close event still gets something it can branch on.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	for {
		_, _, rerr := conn.Read(readCtx)
		if rerr == nil {
			continue
		}
		if got := websocket.CloseStatus(rerr); got != websocket.StatusGoingAway {
			t.Fatalf("close status = %d, want StatusGoingAway (%d): %v", got, websocket.StatusGoingAway, rerr)
		}
		return
	}
}

// TestUnknownDocumentClosesExactlyLikeForbidden pins the non-enumeration
// property at the wire.
//
// A distinct status or reason for "no such document" would let anyone with a
// socket discover which document ids exist simply by reading close codes. The
// two refusals are therefore byte-identical, and only the server's own logs tell
// them apart.
//
// Non-vacuity: give ErrDocumentUnknown its own case in joinCloseStatus and the
// two results below diverge.
func TestUnknownDocumentClosesExactlyLikeForbidden(t *testing.T) {
	const known model.DocumentID = "exists-but-denied"

	// Denied, but the document EXISTS.
	deniedMgr := service.NewManager(service.Deps{
		Metadata:   openDocs(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      fixedAuthZ{allowed: false},
	}, service.RoomConfig{SaveDebounce: 20 * time.Millisecond, IdleTimeout: time.Second, SendBuffer: 256}, nil, zap.NewNop())
	t.Cleanup(deniedMgr.Close)
	deniedBase := newTestServerWithManager(t, deniedMgr)

	// Allowed, but the document does NOT exist.
	_, unknownBase := managerForEnd(t)

	deniedStatus, deniedReason := closeOf(t, deniedBase, string(known))
	unknownStatus, unknownReason := closeOf(t, unknownBase, "no-such-document")

	if deniedStatus != unknownStatus || deniedReason != unknownReason {
		t.Fatalf("an unknown document is distinguishable from a forbidden one: unknown=(%d,%q) forbidden=(%d,%q); that is a document-id oracle",
			unknownStatus, unknownReason, deniedStatus, deniedReason)
	}
	if deniedStatus != websocket.StatusPolicyViolation {
		t.Errorf("refusal status = %d, want StatusPolicyViolation (%d)", deniedStatus, websocket.StatusPolicyViolation)
	}
}

// closeOf dials and returns the status AND reason the server closed with.
func closeOf(t *testing.T, base, doc string) (websocket.StatusCode, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, base+"/collab/"+doc+"?type=memo", nil)
	if err != nil {
		t.Fatalf("dial %s: %v", doc, err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	for {
		_, _, rerr := conn.Read(ctx)
		if rerr == nil {
			continue
		}
		var ce websocket.CloseError
		if errors.As(rerr, &ce) {
			return ce.Code, ce.Reason
		}
		t.Fatalf("read %s: %v", doc, rerr)
	}
}
