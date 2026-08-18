package http

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// hijackableRecorder is an httptest.ResponseRecorder that also implements
// http.Hijacker, standing in for the real connection a WebSocket upgrade takes
// over. httptest.ResponseRecorder deliberately does not implement Hijacker, so
// without this the property under test is unobservable.
type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

// TestStatusWriterUnwrapsToTheHijacker is the regression for the WebSocket
// upgrade path.
//
// statusWriter wraps the ResponseWriter to record the status code for access
// logging. That wrapper SHADOWS every interface the underlying writer implements
// but statusWriter does not — including http.Hijacker, which coder/websocket
// needs to take over the connection. Without Unwrap, every request to
// /collab/{documentId} fails the upgrade with 501 "does not implement
// http.Hijacker": the service starts, serves /healthz and /metrics perfectly, and
// cannot accept a single collaboration session.
//
// The assertion goes through http.ResponseController rather than calling Unwrap
// directly, because the convention is what actually matters — the controller is
// how coder/websocket reaches the Hijacker, and a correct-looking Unwrap with the
// wrong signature would satisfy a direct call while leaving the upgrade broken.
//
// Non-vacuity: delete Unwrap and the Hijack below fails with the same error the
// real upgrade would give.
func TestStatusWriterUnwrapsToTheHijacker(t *testing.T) {
	rec := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	ww := &statusWriter{ResponseWriter: rec, status: http.StatusOK}

	if _, _, err := http.NewResponseController(ww).Hijack(); err != nil {
		t.Fatalf("Hijack through the status-recording wrapper: %v — every WebSocket upgrade would fail with 501 and the service could not accept a single collaboration session", err)
	}
	if !rec.hijacked {
		t.Fatal("the hijack did not reach the underlying writer")
	}
}

// TestStatusWriterStillRecordsTheStatus guards the wrapper's own reason for
// existing, so a change made to fix the unwrapping cannot quietly break the
// access-log status.
func TestStatusWriterStillRecordsTheStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	ww := &statusWriter{ResponseWriter: rec, status: http.StatusOK}

	ww.WriteHeader(http.StatusTeapot)

	if ww.status != http.StatusTeapot {
		t.Fatalf("recorded status = %d, want %d", ww.status, http.StatusTeapot)
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("underlying writer got %d, want the status to pass through", rec.Code)
	}
}
