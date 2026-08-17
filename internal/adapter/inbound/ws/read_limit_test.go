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
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// newReadLimitServer mounts the ws handler with an explicit per-message read limit
// so the limit can be exceeded with a small test frame (ReadLimitFor's production
// sizing is MaxDocBytes + 4 MiB, too large to drive in a unit test).
func newReadLimitServer(t *testing.T, readLimit int64) string {
	t.Helper()
	deps := service.Deps{
		Metadata: metainmem.New(),
		Blob:     blobinline.New(),
		Auth:     authopen.New(),
		AuthZ:    authopen.New(),
	}
	mgr := service.NewManager(deps, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  5 * time.Second,
		SendBuffer:   64,
	}, nil, zap.NewNop())
	h := &Handler{
		Auth:           authopen.New(),
		Manager:        mgr,
		Logger:         zap.NewNop(),
		AcceptOptions:  &websocket.AcceptOptions{InsecureSkipVerify: true},
		ReadLimitBytes: readLimit,
	}
	r := chi.NewRouter()
	r.Method(http.MethodGet, "/collab/{documentId}", h)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// readEvent is one outcome from the background client reader: a binary frame, or a
// terminal read error (a close, including a StatusMessageTooBig close).
type readEvent struct {
	data []byte
	err  error
}

// startReader runs the single continuous client read loop coder/websocket requires
// (it does not support resuming after a context-cancelled Read, so the test must
// never cancel a Read mid-flight). It streams every frame and the terminal error
// onto a channel. ctx bounds the whole test; the loop exits on the first error.
func startReader(ctx context.Context, conn *websocket.Conn) <-chan readEvent {
	ch := make(chan readEvent, 32)
	go func() {
		defer close(ch)
		for {
			_, data, err := conn.Read(ctx)
			ch <- readEvent{data: data, err: err}
			if err != nil {
				return
			}
		}
	}()
	return ch
}

// TestReadLimitAcceptsWithinLimitFrame asserts a single inbound frame within
// ReadLimitBytes is read normally and does NOT close the socket — the lower half
// of the SetReadLimit contract (handler.go ServeHTTP). A within-limit binary frame
// is not a valid y-frame, so the room ignores it, but the connection stays open.
func TestReadLimitAcceptsWithinLimitFrame(t *testing.T) {
	const readLimit = 4096
	base := newReadLimitServer(t, readLimit)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, base+"/collab/rl-ok?type=memo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	conn.SetReadLimit(1 << 20)
	events := startReader(ctx, conn)

	small := make([]byte, readLimit/2)
	if err := conn.Write(ctx, websocket.MessageBinary, small); err != nil {
		t.Fatalf("write within-limit frame: %v", err)
	}

	// Watch the reader for a short window: the within-limit frame must NOT cause a
	// StatusMessageTooBig close. Handshake frames may arrive (fine); a too-big close
	// must not.
	deadline := time.After(400 * time.Millisecond)
	for {
		select {
		case ev := <-events:
			if ev.err != nil {
				if status := websocket.CloseStatus(ev.err); status == websocket.StatusMessageTooBig {
					t.Fatalf("within-limit frame was rejected as too big: %v", ev.err)
				}
				t.Fatalf("socket closed unexpectedly after a within-limit frame: %v", ev.err)
			}
		case <-deadline:
			return // stayed open with no too-big close — correct.
		}
	}
}

// TestReadLimitClosesOversizedFrame defends the SetReadLimit call in
// Handler.ServeHTTP: a single inbound frame larger than ReadLimitBytes trips
// coder/websocket's read limit, which closes the socket with StatusMessageTooBig.
//
// Non-vacuity: comment out `if h.ReadLimitBytes > 0 { conn.SetReadLimit(...) }` in
// ServeHTTP and this test fails — the read limit falls back to coder/websocket's
// 32 KiB default, which admits the 16 KiB frame, so no too-big close arrives and
// the reader stays quiet to the deadline, reporting "oversized frame was not
// rejected with StatusMessageTooBig".
func TestReadLimitClosesOversizedFrame(t *testing.T) {
	const readLimit = 4096
	base := newReadLimitServer(t, readLimit)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, base+"/collab/rl-big?type=memo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	conn.SetReadLimit(1 << 20)
	events := startReader(ctx, conn)

	over := make([]byte, readLimit*4) // 16 KiB > 4 KiB server read limit
	if err := conn.Write(ctx, websocket.MessageBinary, over); err != nil {
		// Some stacks surface the peer's reset as the write error — still a rejection.
		return
	}

	// Drain frames until the terminal close. The server's read of the oversized
	// frame trips SetReadLimit and closes with StatusMessageTooBig.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.err == nil {
				continue // a handshake/data frame; keep draining toward the close.
			}
			if status := websocket.CloseStatus(ev.err); status != -1 && status != websocket.StatusMessageTooBig {
				t.Fatalf("close status = %v, want StatusMessageTooBig", status)
			}
			return // got the terminal close (StatusMessageTooBig or an abrupt reset).
		case <-deadline:
			t.Fatal("oversized frame was not rejected with StatusMessageTooBig (server kept reading)")
		}
	}
}
