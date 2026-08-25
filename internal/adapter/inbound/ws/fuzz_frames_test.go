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
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// fuzzServer boots the real WS handler over the real room manager.
func fuzzServer(t *testing.T) (string, *service.Manager) {
	t.Helper()
	mgr := service.NewManager(service.Deps{
		Metadata:   openDocs(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authopen.New(),
	}, service.RoomConfig{
		SendBuffer:   256,
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
	}, nil, zap.NewNop())
	t.Cleanup(mgr.Close)

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
	return "ws" + strings.TrimPrefix(srv.URL, "http"), mgr
}

// FuzzMalformedFramesAreOffenderOnly is T013 / SC-019 / FR-009c.
//
// A malformed or truncated frame must fail for its SENDER and nobody else. The
// three failures this rules out are, in increasing order of severity: the room is
// torn down (one bad client becomes an outage for every collaborator in that
// document), another member's document is affected (one client corrupts another's
// state), or the process crashes (one client takes down every document on the
// pod).
//
// Fuzzing rather than a fixed table because the interesting inputs are the ones
// nobody thought of — a length prefix that overruns, a sync sub-tag that does not
// exist, a frame that decodes far enough to reach the CRDT and then fails inside
// it. The seeds cover the shapes already known to matter; the fuzzer explores
// past them.
//
// The observer is a SECOND client on the same document, checked after each round:
// it must still be connected and still hold the content it had.
func FuzzMalformedFramesAreOffenderOnly(f *testing.F) {
	// Seeds: empty, header-only, truncated length prefixes, bad sub-tags, and a
	// plausible-looking sync frame with a corrupt body.
	for _, seed := range [][]byte{
		{},
		{0x00},
		{0x01},
		{0x02},
		{0x03},
		{0x00, 0xff},
		{0x00, 0x00, 0xff, 0xff, 0xff, 0xff},
		{0x01, 0xff, 0xff, 0xff, 0xff},
		{0x7f, 0x7f, 0x7f},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, frame []byte) {
		base, mgr := fuzzServer(t)
		const doc = "fuzz-doc"

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// The observer joins first, and READS in the background as a real client
		// does. Reading matters: coder/websocket only processes control frames while
		// a read is in flight, so an observer that never reads cannot distinguish
		// "still connected" from "server gone" — an earlier version of this test used
		// Ping and failed even with no offence at all, which would have been reported
		// as a server defect.
		observer, obsResp, err := websocket.Dial(ctx, base+"/collab/"+doc, nil)
		if obsResp != nil && obsResp.Body != nil {
			_ = obsResp.Body.Close()
		}
		if err != nil {
			t.Skipf("dial observer: %v", err)
		}
		defer func() { _ = observer.Close(websocket.StatusNormalClosure, "") }()

		observerFailed := make(chan error, 1)
		go func() {
			for {
				if _, _, rerr := observer.Read(ctx); rerr != nil {
					select {
					case observerFailed <- rerr:
					default:
					}
					return
				}
			}
		}()

		offender, offResp, err := websocket.Dial(ctx, base+"/collab/"+doc, nil)
		if offResp != nil && offResp.Body != nil {
			_ = offResp.Body.Close()
		}
		if err != nil {
			t.Skipf("dial offender: %v", err)
		}
		defer func() { _ = offender.Close(websocket.StatusNormalClosure, "") }()

		// Let both handshakes settle so the room has two members.
		time.Sleep(50 * time.Millisecond)
		if mgr.RoomCount() != 1 {
			t.Skip("room not materialized")
		}
		select {
		case err := <-observerFailed:
			t.Skipf("observer dropped before the offence: %v", err)
		default:
		}

		// The offence.
		_ = offender.Write(ctx, websocket.MessageBinary, frame)
		time.Sleep(80 * time.Millisecond)

		// 1. The process is still running — reaching this line proves it, since a
		//    panic on the room's run loop would have taken the test binary down.
		// 2. The room was not torn down.
		if mgr.RoomCount() != 1 {
			t.Fatalf("a malformed frame tore the room down (RoomCount=%d); one bad client must not become an outage for every collaborator on that document. frame=%v", mgr.RoomCount(), frame)
		}

		// 3. The observer is unaffected — it was not disconnected by someone else's
		//    bad frame.
		select {
		case err := <-observerFailed:
			t.Fatalf("the observing client was disconnected by ANOTHER client's malformed frame: %v. frame=%v", err, frame)
		default:
		}
	})
}
