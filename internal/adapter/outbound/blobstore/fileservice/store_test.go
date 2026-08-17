package fileservice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// stubFileService is a faithful in-memory stand-in for file-service's
// /internal/file API: POST stores bytes (content-addressed for dedup) and
// returns a UUID + externalID; GET /{id}/content serves them; DELETE removes
// them. It asserts the multipart fields the collab adapter is contractually
// required to send.
type stubFileService struct {
	mu       sync.Mutex
	byID     map[string][]byte
	byHash   map[string]string // externalID -> id (dedup)
	nextID   int
	bucketID string
	authID   string

	// captured from the last create, for assertions.
	lastBucket     string
	lastDisplay    string
	lastReused     bool
	lastAuthSent   bool // whether an authorizationId field was present at all
	lastAuthNonEmp bool // whether that field carried a non-empty value
}

func newStub() *stubFileService {
	return &stubFileService{
		byID:     map[string][]byte{},
		byHash:   map[string]string{},
		bucketID: "11111111-1111-1111-1111-111111111111",
		authID:   "22222222-2222-2222-2222-222222222222",
	}
}

func (s *stubFileService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/file", s.create)
	mux.HandleFunc("GET /internal/file/{id}/content", s.content)
	mux.HandleFunc("DELETE /internal/file/{id}", s.delete)
	return mux
}

func (s *stubFileService) create(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "bad multipart", http.StatusBadRequest)
		return
	}
	var fileBytes []byte
	fields := map[string]string{}
	for {
		part, perr := mr.NextPart()
		if errors.Is(perr, io.EOF) {
			break
		}
		if perr != nil {
			http.Error(w, "read part", http.StatusBadRequest)
			return
		}
		b, _ := io.ReadAll(part)
		if part.FormName() == "file" {
			fileBytes = b
		} else {
			fields[part.FormName()] = string(b)
		}
	}

	if fields["storageBucketId"] == "" {
		http.Error(w, "missing storageBucketId", http.StatusBadRequest)
		return
	}
	// authorizationId is OPTIONAL (mirrors the real file-service): a snapshot is
	// uploaded WITHOUT it so the row's authz column is NULL. The stub must accept
	// its absence rather than 400 — this is exactly the behavior the fix relies on.
	authVal, authSent := fields["authorizationId"]

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBucket = fields["storageBucketId"]
	s.lastAuthSent = authSent
	s.lastAuthNonEmp = authVal != ""
	s.lastDisplay = fields["displayName"]

	// Content-addressed dedup keyed on the raw bytes (a stand-in for SHA3-256).
	ext := hashOf(fileBytes)
	if existing, ok := s.byHash[ext]; ok {
		s.lastReused = true
		writeJSON(w, createResponse{ID: existing, ExternalID: ext, Size: int64(len(fileBytes)), Reused: true})
		return
	}
	s.nextID++
	id := idFromInt(s.nextID)
	s.byID[id] = append([]byte(nil), fileBytes...)
	s.byHash[ext] = id
	s.lastReused = false
	writeJSON(w, createResponse{ID: id, ExternalID: ext, Size: int64(len(fileBytes)), Reused: false})
}

func (s *stubFileService) content(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	data, ok := s.byID[id]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (s *stubFileService) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	_, ok := s.byID[id]
	delete(s.byID, id)
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"authorizationId": s.authID})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func hashOf(b []byte) string { return "sha-" + strings.ToLower(string(b)) }

func idFromInt(n int) string {
	return "00000000-0000-0000-0000-" + padLeft(n)
}

func padLeft(n int) string {
	s := ""
	for i := 0; i < 12; i++ {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func newTestStore(t *testing.T) (*Store, *stubFileService) {
	t.Helper()
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)
	store, err := New(Config{
		BaseURL:         srv.URL,
		StorageBucketID: stub.bucketID,
		MaxUploadSize:   32 << 20,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store, stub
}

func TestPutGetRoundTrip(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	want := []byte("snapshot-payload")
	// On first save the hint is the document id; file-service assigns its own
	// UUID, which the adapter returns as the content pointer.
	pointer, err := store.Put(ctx, "doc-1", "", want)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if pointer == "doc-1" {
		t.Error("expected the returned pointer to be the file-service UUID, not the doc id")
	}
	got, err := store.Get(ctx, pointer)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %q, want %q", got, want)
	}
}

// With no per-document bucket, the upload falls back to the configured bucket,
// sends a displayName, and — critically — sends NO authorizationId so the
// file-service row gets a NULL authz column (UNIQUE permits many NULLs, so every
// snapshot persists). Sending a fixed authz would collide after the first row.
func TestPutFallsBackToConfiguredBucketAndOmitsAuth(t *testing.T) {
	store, stub := newTestStore(t)
	if _, err := store.Put(context.Background(), "doc-x", "", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stub.lastBucket != stub.bucketID {
		t.Errorf("storageBucketId = %q, want fallback %q", stub.lastBucket, stub.bucketID)
	}
	if stub.lastAuthSent {
		t.Errorf("authorizationId must NOT be sent (NULL authz); got sent=%v nonEmpty=%v", stub.lastAuthSent, stub.lastAuthNonEmp)
	}
	if stub.lastDisplay == "" {
		t.Error("displayName not sent")
	}
}

// The per-document bucket passed to Put overrides the configured fallback, so a
// snapshot lands in the document's OWN storage bucket (the core of the fix).
func TestPutUsesPerDocumentBucket(t *testing.T) {
	store, stub := newTestStore(t)
	docBucket := "99999999-9999-9999-9999-999999999999"
	if _, err := store.Put(context.Background(), "doc-y", docBucket, []byte("y")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stub.lastBucket != docBucket {
		t.Errorf("storageBucketId = %q, want per-document %q (not the configured fallback)", stub.lastBucket, docBucket)
	}
	if stub.lastAuthSent {
		t.Error("authorizationId must NOT be sent even on the per-document path")
	}
}

func TestPutDoesNotDeletePreviousSnapshot(t *testing.T) {
	// 002 FR-002 (delete-after-commit): on re-save the adapter uploads a NEW object
	// (new UUID) but must NOT delete the previous one — deleting before the caller
	// commits the new pointer would strand the metadata row on a missing blob if that
	// commit then failed. The caller (room.persist) deletes the superseded pointer
	// only AFTER the metadata commit succeeds.
	store, stub := newTestStore(t)
	ctx := context.Background()

	p1, err := store.Put(ctx, "doc-stable", "", []byte("v1"))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	p2, err := store.Put(ctx, p1, "", []byte("v2-different"))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if p1 == p2 {
		t.Error("expected a new pointer for new content")
	}

	// The OLD object must STILL exist: a failed commit of p2 would then leave the row
	// safely pointing at the still-present p1, never stranded.
	stub.mu.Lock()
	_, oldExists := stub.byID[p1]
	stub.mu.Unlock()
	if !oldExists {
		t.Error("Put deleted the previous snapshot — delete-before-commit can strand the metadata row (002 FR-002)")
	}
	if got, err := store.Get(ctx, p1); err != nil || string(got) != "v1" {
		t.Errorf("previous snapshot no longer retrievable: got %q err %v", got, err)
	}
	got, err := store.Get(ctx, p2)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2-different" {
		t.Errorf("Get after overwrite = %q, want v2-different", got)
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	_, err := store.Get(context.Background(), "never-put")
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get(absent) err = %v, want ErrNotFound", err)
	}
}

func TestDeleteThenGetIsNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	pointer, err := store.Put(ctx, "doc-del", "", []byte("x"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, pointer); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, pointer); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrNotFound", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	// Deleting a never-put pointer must be a no-op (idempotent cascade).
	if err := store.Delete(ctx, "absent"); err != nil {
		t.Errorf("Delete(absent) = %v, want nil", err)
	}
}

// TestGetDeleteEscapePathSignificantPointer defends the url.PathEscape on the
// pointer in Get/Delete. The stub routes GET/DELETE /internal/file/{id}/... via a
// single-segment {id} wildcard (which URL-decodes the segment). A pointer
// containing a '/' must be escaped to "%2F" so it stays ONE path segment and
// reaches the intended object; unescaped it would split into an extra segment,
// missing the route entirely (and could re-target a different resource).
//
// Non-vacuity: drop url.PathEscape in Get and Delete and these calls hit
// /internal/file/has/slash/content — an unmatched route → 404 → ErrNotFound for
// Get, and a non-2xx (404 here maps to a no-op) so the Delete then fails to remove
// the object and the trailing Get still finds it, failing the assertions.
func TestGetDeleteEscapePathSignificantPointer(t *testing.T) {
	store, stub := newTestStore(t)
	ctx := context.Background()
	const weird = "has/slash"
	want := []byte("escaped-bytes")
	stub.mu.Lock()
	stub.byID[weird] = want
	stub.mu.Unlock()

	got, err := store.Get(ctx, weird)
	if err != nil {
		t.Fatalf("Get(%q) = %v, want the seeded bytes (pointer must be PathEscaped)", weird, err)
	}
	if string(got) != string(want) {
		t.Fatalf("Get(%q) = %q, want %q", weird, got, want)
	}

	if err := store.Delete(ctx, weird); err != nil {
		t.Fatalf("Delete(%q) = %v, want nil", weird, err)
	}
	stub.mu.Lock()
	_, stillThere := stub.byID[weird]
	stub.mu.Unlock()
	if stillThere {
		t.Fatalf("Delete(%q) did not remove the object — the pointer was not PathEscaped to the right route", weird)
	}
}

func TestPutRejectsOversize(t *testing.T) {
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)
	store, _ := New(Config{
		BaseURL:         srv.URL,
		StorageBucketID: stub.bucketID,
		MaxUploadSize:   8, // tiny ceiling
	})
	_, err := store.Put(context.Background(), "big", "", []byte("this is definitely more than eight bytes"))
	if err == nil {
		t.Error("expected oversize Put to be rejected")
	}
}

func TestNewValidates(t *testing.T) {
	cases := []Config{
		{StorageBucketID: "b"}, // missing BaseURL
		{BaseURL: "http://x"},  // missing fallback bucket
	}
	for i, c := range cases {
		if _, err := New(c); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}

// New must NOT require an authorizationId — snapshots are uploaded with NULL
// authz, so a valid config needs only BaseURL + a fallback bucket.
func TestNewSucceedsWithoutAuthorizationID(t *testing.T) {
	if _, err := New(Config{BaseURL: "http://x", StorageBucketID: "b"}); err != nil {
		t.Errorf("New without an authorizationId must succeed, got %v", err)
	}
}

func TestServerErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to surface a 500")
	}
}

func TestUploadEmptyIDSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, createResponse{ID: ""}) // server returns no id
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to fail when the server returns an empty id")
	}
}

func TestUploadBadJSONSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to fail on a malformed response body")
	}
}

func TestGetNetworkErrorSurfaces(t *testing.T) {
	// A closed server: requests fail at the transport layer.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	srv.Close()
	if _, err := store.Get(context.Background(), "id"); err == nil {
		t.Error("expected Get to surface a transport error")
	}
	if err := store.Delete(context.Background(), "id"); err == nil {
		t.Error("expected Delete to surface a transport error")
	}
}

func TestGetNon200Surfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if _, err := store.Get(context.Background(), "id"); err == nil {
		t.Error("expected Get to surface a 500")
	}
}

func TestDeleteNon200Surfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if err := store.Delete(context.Background(), "id"); err == nil {
		t.Error("expected Delete to surface a 502")
	}
}

// TestPutSucceedsWhenPreviousDeleteFails defends Put's best-effort cleanup
// branch (store.go:102): the new snapshot upload succeeds, but deleting the
// superseded one fails. The save MUST still succeed — the new snapshot is
// already durable and recorded, and the orphan is reclaimable. A failed cleanup
// must never fail the save.
func TestPutSucceedsWhenPreviousDeleteFails(t *testing.T) {
	var nextID int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			nextID++
			writeJSON(w, createResponse{ID: idFromInt(nextID)})
		case http.MethodDelete:
			// The cleanup of the previous snapshot fails hard.
			http.Error(w, "delete unavailable", http.StatusInternalServerError)
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})

	// prevPointer differs from the assigned id, so Put will attempt (and fail)
	// the cleanup delete — yet must still return the new pointer with no error.
	got, err := store.Put(context.Background(), "00000000-0000-0000-0000-000000000099", "", []byte("v2"))
	if err != nil {
		t.Fatalf("Put must succeed despite a failed previous-snapshot cleanup: %v", err)
	}
	if got == "" {
		t.Error("expected the new content pointer to be returned")
	}
}

// TestUploadBadBaseURLFailsRequestBuild defends upload's request-build branch
// (store.go:136): a BaseURL carrying an illegal control character makes
// http.NewRequestWithContext fail, which Put must surface rather than panic or
// silently no-op. New does not validate URL syntax, so this is reachable.
func TestUploadBadBaseURLFailsRequestBuild(t *testing.T) {
	store, err := New(Config{BaseURL: "http://bad\x7fhost:4003", StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to fail building a request to a malformed BaseURL")
	}
}

// TestUploadTransportErrorSurfaces defends upload's client.Do branch
// (store.go:142): a closed server makes the POST fail at the transport layer,
// which Put must surface.
func TestUploadTransportErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	srv.Close() // transport now fails
	if _, err := store.Put(context.Background(), "doc", "", []byte("x")); err == nil {
		t.Error("expected Put to surface a transport error on upload")
	}
}

// TestGetBadBaseURLFailsRequestBuild defends Get's request-build branch
// (store.go:165): a malformed BaseURL makes http.NewRequestWithContext fail.
func TestGetBadBaseURLFailsRequestBuild(t *testing.T) {
	store, err := New(Config{BaseURL: "http://bad\x7fhost:4003", StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := store.Get(context.Background(), "id"); err == nil {
		t.Error("expected Get to fail building a request to a malformed BaseURL")
	}
}

// TestGetTruncatedBodySurfaces defends Get's read-body branch (store.go:181): a
// 200 whose declared Content-Length exceeds the bytes actually delivered (the
// connection is cut short) must surface as a read error, not return a silently
// truncated snapshot that a reload would apply.
func TestGetTruncatedBodySurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("short"))
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close() // cut the connection mid-body
		}
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if _, err := store.Get(context.Background(), "id"); err == nil {
		t.Error("expected Get to surface a truncated-body read error")
	}
}

// TestDeleteBadBaseURLFailsRequestBuild defends Delete's request-build branch
// (store.go:190): a malformed BaseURL makes http.NewRequestWithContext fail.
func TestDeleteBadBaseURLFailsRequestBuild(t *testing.T) {
	store, err := New(Config{BaseURL: "http://bad\x7fhost:4003", StorageBucketID: "b", MaxUploadSize: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.Delete(context.Background(), "id"); err == nil {
		t.Error("expected Delete to fail building a request to a malformed BaseURL")
	}
}
