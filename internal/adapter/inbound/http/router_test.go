package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	InitMetrics()
	return NewRouter(Deps{
		CollabHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Logger: zap.NewNop(),
	})
}

func TestHealthzOK(t *testing.T) {
	rr := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"status":"ok"`) {
		t.Errorf("/healthz body = %q, want status ok", rr.Body.String())
	}
}

func TestMetricsExposed(t *testing.T) {
	rr := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "collaboration_rooms_active") {
		t.Errorf("/metrics body missing collaboration_rooms_active gauge")
	}
}

func TestCollabRouteMounted(t *testing.T) {
	rr := httptest.NewRecorder()
	newTestRouter(t).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/collab/doc-1", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("/collab/{id} status = %d, want 200 (handler reached)", rr.Code)
	}
}
