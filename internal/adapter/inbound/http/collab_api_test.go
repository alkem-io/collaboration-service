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
	createErr  error
}

func (f *fakeLifecycle) PreRegister(_ context.Context, meta model.Metadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, meta)
	return f.createErr
}

func newCollabRouter(h *CollabAPIHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/collab/{documentId}", h.Create)
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

// TestCreateRejectsEmptyPolicyInAuthZEvalMode asserts that with
// RequireAuthorizationPolicy set (authzeval mode), a create with an empty
// authorizationPolicyId is rejected (400) and NOT persisted — otherwise it would
// register a document that fails every later authorization evaluation (CR Major).
func TestCreateRejectsEmptyPolicyInAuthZEvalMode(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc, RequireAuthorizationPolicy: true}
	r := newCollabRouter(h)

	// No authorizationPolicyId in the body.
	req := httptest.NewRequest(http.MethodPost, "/collab/doc-noauth", strings.NewReader(`{"contentType":"memo"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (empty policy in authzeval mode); body=%s", rr.Code, rr.Body.String())
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.registered) != 0 {
		t.Fatalf("a document with no authorization policy was persisted: %+v", lc.registered)
	}
}

// TestCreateAcceptsPolicyInAuthZEvalMode asserts that with
// RequireAuthorizationPolicy set, a create that DOES carry an authorizationPolicyId
// is accepted and persisted with that policy id.
func TestCreateAcceptsPolicyInAuthZEvalMode(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc, RequireAuthorizationPolicy: true}
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-auth",
		strings.NewReader(`{"contentType":"memo","authorizationPolicyId":"policy-9"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.registered) != 1 || lc.registered[0].AuthorizationPolicyID != "policy-9" {
		t.Fatalf("expected the carried policy id to be persisted: %+v", lc.registered)
	}
}

// TestCreateAllowsEmptyPolicyInOpenMode asserts that in open/standalone mode
// (RequireAuthorizationPolicy false), an empty authorizationPolicyId is fine —
// authZ grants everything there, so the policy id is genuinely optional.
func TestCreateAllowsEmptyPolicyInOpenMode(t *testing.T) {
	lc := &fakeLifecycle{}
	h := &CollabAPIHandler{Lifecycle: lc} // RequireAuthorizationPolicy defaults false
	r := newCollabRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/collab/doc-open", strings.NewReader(`{"contentType":"memo"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (open mode); body=%s", rr.Code, rr.Body.String())
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

func TestCreateDocumentRejectsUnknownField(t *testing.T) {
	h := &CollabAPIHandler{Lifecycle: &fakeLifecycle{}}
	r := newCollabRouter(h)

	// An unknown field must be rejected (DisallowUnknownFields) so client mistakes
	// surface instead of being silently ignored.
	req := httptest.NewRequest(http.MethodPost, "/collab/doc-x",
		strings.NewReader(`{"contentType":"memo","bogusField":"x"}`))
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown field", rr.Code)
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

// TestMissingDocumentIDRejected asserts the create handler rejects an empty
// document id with a 400 when invoked without the route param set.
func TestMissingDocumentIDRejected(t *testing.T) {
	h := &CollabAPIHandler{Lifecycle: &fakeLifecycle{}}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/collab/", strings.NewReader(`{}`))
	h.Create(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty id: status = %d, want 400", rr.Code)
	}
}

// TestCreateDocumentCarriesTheStorageBucket asserts the create body's
// storageBucketId reaches the metadata row.
//
// The field is create-path METADATA PARITY with the bus pre-register, which
// carries the same bucket: production gets it from `server` over RMQ, and this
// surface is what the raw-config file-service e2e fixture uses. It is NOT a
// supported standalone file-service deployment — config.Load rejects
// CHECKPOINT_STORE=file-service with METADATA_STORE=inmemory, so that pairing is
// reachable only by building a Config value directly, as the e2e does.
//
// Dropping the field silently on the way through would leave that fixture's rows
// bucket-less, and a file-service save for a bucket-less row is refused rather
// than misfiled into a shared bucket where the blob would outlive the document as
// an orphan (the delete cascade walks the document's OWN bucket).
//
// Non-vacuity: remove `StorageBucketID: req.StorageBucketID` from the meta
// literal in Create and this fails on an empty bucket.
func TestCreateDocumentCarriesTheStorageBucket(t *testing.T) {
	lc := &fakeLifecycle{}
	r := newCollabRouter(&CollabAPIHandler{Lifecycle: lc})

	body := strings.NewReader(`{"contentType":"memo","storageBucketId":"bucket-42"}`)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/collab/doc-b", body))

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if len(lc.registered) != 1 {
		t.Fatalf("registered %d documents, want 1", len(lc.registered))
	}
	if got := lc.registered[0].StorageBucketID; got != "bucket-42" {
		t.Errorf("StorageBucketID = %q, want %q — without it a file-service save for this row is refused", got, "bucket-42")
	}
}
