package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"
)

// dialPair stands up a trivial echo-less server that just accepts the upgrade
// and parks, returning the server-side and client-side connections so wsConn can
// be exercised against a real socket.
func dialPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		accepted <- c
		// Park until the test closes us.
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	url := "ws" + srv.URL[len("http"):]
	cl, resp, err := websocket.Dial(context.Background(), url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = cl.Close(websocket.StatusNormalClosure, "") })

	select {
	case s := <-accepted:
		t.Cleanup(func() { _ = s.Close(websocket.StatusNormalClosure, "") })
		return s, cl
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted")
		return nil, nil
	}
}

// TestWSConnDeliversFrames asserts a frame queued via Send is written to the
// socket and read by the peer.
func TestWSConnDeliversFrames(t *testing.T) {
	server, client := dialPair(t)
	wc := newWSConn(context.Background(), server, 8, zap.NewNop())
	wc.startWriter()
	defer wc.close()

	if err := wc.Send([]byte{1, 2, 3}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	typ, data, err := client.Read(context.Background())
	if err != nil {
		t.Fatalf("client Read: %v", err)
	}
	if typ != websocket.MessageBinary || string(data) != string([]byte{1, 2, 3}) {
		t.Fatalf("got typ=%v data=%v", typ, data)
	}
}

// TestWSConnSlowConsumerDropped asserts that overflowing the send queue (no
// writer draining it) sheds the connection rather than blocking the caller.
func TestWSConnSlowConsumerDropped(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 1, zap.NewNop())
	// Do NOT start the writer, so nothing drains the queue.

	// First Send fills the depth-1 buffer.
	if err := wc.Send([]byte{0}); err != nil {
		t.Fatalf("first Send: %v", err)
	}
	// Second Send overflows → connection shed, error returned.
	if err := wc.Send([]byte{0}); err == nil {
		t.Fatal("expected error when send queue overflows")
	}
	// Subsequent Send after close also errors.
	if err := wc.Send([]byte{0}); err == nil {
		t.Fatal("expected error after connection closed")
	}
}

// TestWSConnCloseIdempotent asserts close can be called repeatedly safely.
func TestWSConnCloseIdempotent(t *testing.T) {
	server, _ := dialPair(t)
	wc := newWSConn(context.Background(), server, 4, zap.NewNop())
	wc.close()
	wc.close() // must not panic / double-close the channel
	if err := wc.Send([]byte{1}); err == nil {
		t.Fatal("Send after close should error")
	}
}
