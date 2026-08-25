package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// --- conn.go: newWSConn buffer default ---

// TestNewWSConnDefaultsBufferWhenNonPositive asserts a non-positive buffer is
// floored to the 64-deep default, so a misconfigured (zero) send buffer still
// gives the room a real outbound queue rather than an unbuffered channel that
// would block the run loop on the first Send.
func TestNewWSConnDefaultsBufferWhenNonPositive(t *testing.T) {
	// No socket is touched by newWSConn, so a nil conn is safe here.
	wc := newWSConn(context.Background(), nil, 0, zap.NewNop())
	if got := cap(wc.send); got != 64 {
		t.Fatalf("send buffer cap = %d, want the 64 default", got)
	}
	wcNeg := newWSConn(context.Background(), nil, -5, zap.NewNop())
	if got := cap(wcNeg.send); got != 64 {
		t.Fatalf("negative buffer cap = %d, want the 64 default", got)
	}
}

// TestStartWriterClosesOnSocketWriteFailure drives startWriter's write-error
// branch: once the peer end of the socket is gone, the writer's Write fails and
// it must close the connection (shedding the member) rather than spin. The
// observable effect is that a subsequent Send reports the connection closed.
func TestStartWriterClosesOnSocketWriteFailure(t *testing.T) {
	server, client := dialPair(t)
	wc := newWSConn(context.Background(), server, 8, zap.NewNop())
	wc.startWriter()

	// Tear down the peer so the server-side Write fails on the next frame.
	_ = client.Close(websocket.StatusNormalClosure, "")

	// Keep sending until the writer observes the broken socket and closes the conn
	// (Send then returns errConnClosed). Polling avoids depending on exact TCP
	// teardown timing.
	if !eventually(func() bool {
		for i := 0; i < 16; i++ {
			if err := wc.Send([]byte{0xaa}); err != nil {
				return true
			}
		}
		return false
	}) {
		t.Fatal("writer did not close the connection after a socket write failure")
	}
}

// viewerAuthZ grants read but denies update-content, so every join resolves to a
// read-only viewer — which receives a SyncStep1 PLUS a read-only-state control as
// its initial frames (two frames), enough to overflow a depth-1 send buffer.
type viewerAuthZ struct{}

func (viewerAuthZ) Evaluate(_ context.Context, _ model.Identity, _ model.DocumentID, p model.Privilege) (model.AuthDecision, error) {
	if p == model.PrivilegeRead {
		return model.AuthDecision{Allowed: true}, nil
	}
	return model.AuthDecision{Allowed: false}, nil
}

// TestHandshakeBatchIsNotShedOnASmallSendBuffer asserts the joiner-facing
// outcome of the handshake-send policy: a viewer whose join yields two initial
// frames (SyncStep1 + the read-only control frame) receives BOTH over a
// connection whose outbound queue holds only one, and is not dropped.
//
// This replaces an earlier test that asserted the opposite — that the second
// frame overflowed and the server shed the connection. That was never the
// intended behavior (serve starts the writer precisely so the batch drains as it
// fills) and the test only passed when it won a scheduling race: starting the
// writer goroutine does not mean it has RUN, so whether the queue had space was
// luck. Shedding a client for the server's own handshake frames is a defect, and
// sendInitial is the fix; this test pins the outcome that matters to a joiner.
func TestHandshakeBatchIsNotShedOnASmallSendBuffer(t *testing.T) {
	deps := service.Deps{
		Metadata:   openDocs(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      viewerAuthZ{},
	}
	// SendBuffer 1 is a valid (>0) config, so SendBuffer() returns it verbatim —
	// one slot for a two-frame batch.
	mgr := service.NewManager(deps, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  5 * time.Second,
		SendBuffer:   1,
	}, nil, zap.NewNop())

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
	base := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, base+"/collab/small-buffer", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	for i := range 2 {
		if _, _, readErr := conn.Read(ctx); readErr != nil {
			t.Fatalf("handshake frame %d not delivered over a depth-1 queue: %v", i+1, readErr)
		}
	}
}

// TestReadLoopLogsAbnormalClose drives readLoop's abnormal-close-status branch:
// the client closes with a non-normal status (going-away), so the server's Read
// returns a CloseStatus that is neither -1 nor NormalClosure, exercising the
// debug-log path. The now-empty room idle-releases, proving the read loop
// returned cleanly and the connection was left.
func TestReadLoopLogsAbnormalClose(t *testing.T) {
	deps := service.Deps{
		Metadata:   openDocs(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authopen.New(),
	}
	mgr := service.NewManager(deps, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  40 * time.Millisecond,
		SendBuffer:   256,
	}, nil, zap.NewNop())
	base := newTestServerWithManager(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dialClient(t, base, "abnormal-close", model.ContentTypeMemo)
	a.run(ctx)
	if !eventually(func() bool { return mgr.RoomCount() == 1 }) {
		t.Fatal("room never materialized")
	}

	// Close with a non-normal status code → server-side Read returns that status,
	// hitting the abnormal-close debug-log branch.
	_ = a.conn.Close(websocket.StatusGoingAway, "leaving")

	if !eventually(func() bool { return mgr.RoomCount() == 0 }) {
		t.Fatal("room did not release after an abnormal client close")
	}
}
