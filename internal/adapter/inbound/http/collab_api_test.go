package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeLifecycle records the create/delete calls the HTTP handlers make.
type fakeLifecycle struct {
	mu         sync.Mutex
	registered []model.Metadata
	purged     []model.DocumentID
	purgeErr   error
	createErr  error
}

func (f *fakeLifecycle) PreRegister(_ context.Context, meta model.Metadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, meta)
	return f.createErr
}

func (f *fakeLifecycle) Purge(_ context.Context, id model.DocumentID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, id)
	return f.purgeErr
}

func newCollabRouter(h *CollabAPIHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/collab/{documentId}", h.Create)
	r.Delete("/collab/{documentId}", h.Delete)
	return r
}

// TestCreateDocumentPreRegisters asserts POST /collab/{id} pre-registers the
// document with the content type from the body and returns 201 with the id
// (FR-020/FR-023, T016).
func TestCreateDocumentPreRegisters(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc}
	r := newCollabRouter(h)

	body := strings.NewReader(`{"contentType":"whiteboard","ownerRef":"callout-1"}`)
	req := httptest.NewRequest(http.MethodPost, "/collab/doc-1", body)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}
	var resp CreateDocumentResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID != "doc-1" || resp.ContentType != "whiteboard" {
		t.Fatalf("response mismatch: %+v", resp)
	}

	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.registered) != 1 || lc.registered[0].ID != "doc-1" ||
		lc.registered[0].ContentType != model.ContentTypeWhiteboard ||
		lc.registered[0].OwnerRef != "callout-1" {
		t.Fatalf("pre-register mismatch: %+v", lc.registered)
	}
}

// TestCreateDocumentDefaultsContentType asserts an empty/absent content type
// defaults to memo (the document convention default).
func TestCreateDocumentDefaultsContentType(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-2", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rr.Code)
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.registered[0].ContentType != model.ContentTypeMemo {
		t.Fatalf("content type = %q, want memo", lc.registered[0].ContentType)
	}
}

// TestCreateDocumentRejectsBadContentType asserts an unknown content type is a
// 400 (no silent default for an explicit, invalid value).
func TestCreateDocumentRejectsBadContentType(t *testing.T) {
	h := &CollabAPIHandler{Lifecycle: &fakeLifecycle{}}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-3", strings.NewReader(`{"contentType":"spreadsheet"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestCreateDocumentRejectsMalformedBody asserts a malformed JSON body is a 400.
func TestCreateDocumentRejectsMalformedBody(t *testing.T) {
	h := &CollabAPIHandler{Lifecycle: &fakeLifecycle{}}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-4", strings.NewReader(`{not json`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestDeleteDocumentCascades asserts DELETE /collab/{id} runs the same cascade
// purge as the bus event and returns 200 (FR-023, T016).
func TestDeleteDocumentCascades(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodDelete, "/collab/doc-5", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp DeleteCollabResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID != "doc-5" || !resp.Deleted {
		t.Fatalf("response mismatch: %+v", resp)
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.purged) != 1 || lc.purged[0] != "doc-5" {
		t.Fatalf("purge mismatch: %v", lc.purged)
	}
}

// TestDeleteDocumentPropagatesError asserts a cascade error surfaces as a 500
// (the durable purge genuinely failed — not an idempotent absent doc).
func TestDeleteDocumentPropagatesError(t *testing.T) {
	lc := &fakeLifecycle{purgeErr: context.DeadlineExceeded}
	h := &CollabAPIHandler{Lifecycle: lc}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodDelete, "/collab/doc-6", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

// TestCreateDocumentPropagatesError asserts a pre-register failure is a 500.
func TestCreateDocumentPropagatesError(t *testing.T) {
	lc := &fakeLifecycle{createErr: context.DeadlineExceeded}
	h := &CollabAPIHandler{Lifecycle: lc}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-7", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

// TestMissingDocumentIDRejected asserts the create/delete handlers reject an empty
// document id with a 400 when invoked without the route param set.
func TestMissingDocumentIDRejected(t *testing.T) {
	h := &CollabAPIHandler{Lifecycle: &fakeLifecycle{}}

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/collab/", strings.NewReader(`{}`))
		switch method {
		case http.MethodPost:
			h.Create(rr, req)
		case http.MethodDelete:
			h.Delete(rr, req)
		}
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s empty id: status = %d, want 400", method, rr.Code)
		}
	}
}
