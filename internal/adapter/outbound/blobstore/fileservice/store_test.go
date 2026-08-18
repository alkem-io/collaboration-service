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

	// creates/rewrites count how the adapter reached the store, so a test can
	// assert it REWRITES a stable file rather than creating a new one per save.
	creates  int
	rewrites int
}

// replace models PUT /internal/file/{id}/content ("store-and-link"): the file id
// is a stable identifier, so the content is swapped behind it and the id is
// unchanged. 404 when the file does not exist; 409 when the new content's hash
// already belongs to ANOTHER file in the bucket (content dedup).
func (s *stubFileService) replace(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	body, _ := io.ReadAll(r.Body)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	hash := hashOf(body)
	if owner, ok := s.byHash[hash]; ok && owner != id {
		w.WriteHeader(http.StatusConflict)
		return
	}
	delete(s.byHash, hashOf(s.byID[id]))
	s.byID[id] = body
	s.byHash[hash] = id
	s.rewrites++
	w.WriteHeader(http.StatusOK)
	writeJSON(w, createResponse{ID: id, ExternalID: hash, Size: int64(len(body))})
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
	mux.HandleFunc("PUT /internal/file/{id}/content", s.replace)
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
	s.creates++
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

// TestPutNeverLeavesTheRowStranded is the restructured 002 FR-002 guard.
//
// The original asserted that re-saving produced a NEW pointer and that the OLD
// object survived, so a failed metadata commit could still fall back to it. That
// premise is gone: a file id is a stable identifier, so re-saving REWRITES the
// same file and the pointer never changes. The property it was defending —
// the metadata row is never left pointing at nothing — is now structural rather
// than something Put has to be careful about, and this asserts exactly that.
func TestPutNeverLeavesTheRowStranded(t *testing.T) {
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
	if p1 != p2 {
		t.Fatalf("pointer changed on re-save (%q -> %q); a stable id is what removes the stranding window", p1, p2)
	}

	// The pointer the row holds resolves to live content at every moment: there is
	// no interval in which it names a deleted object.
	stub.mu.Lock()
	_, exists := stub.byID[p1]
	stub.mu.Unlock()
	if !exists {
		t.Error("the pointer stored in the metadata row does not resolve — the row is stranded (002 FR-002)")
	}
	got, err := store.Get(ctx, p1)
	if err != nil {
		t.Fatalf("Get after re-save: %v", err)
	}
	if string(got) != "v2-different" {
		t.Fatalf("Get = %q, want the rewritten content", got)
	}
}

// TestRewriteServerErrorDoesNotForkTheDocument pins a hazard the fallback
// introduces: only a 404 (no such file) may fall back to creating a file. A
// transient 500 on the rewrite must SURFACE, because creating a second file
// would fork the document — the row keeps the old pointer while fresh content
// lands somewhere it will never be read from.
func TestRewriteServerErrorDoesNotForkTheDocument(t *testing.T) {
	var creates int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			http.Error(w, "backend unavailable", http.StatusInternalServerError)
		case http.MethodPost:
			creates++
			writeJSON(w, createResponse{ID: idFromInt(creates)})
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})

	if _, err := store.Put(context.Background(), "00000000-0000-0000-0000-000000000099", "", []byte("v2")); err == nil {
		t.Fatal("a server error on rewrite must surface, not fall back to creating a second file")
	}
	if creates != 0 {
		t.Fatalf("created %d files after a failed rewrite; that forks the document", creates)
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
	// No prior pointer: exercise the CREATE path directly, with no rewrite attempt.
	if _, err := store.Put(context.Background(), "", "", []byte("x")); err == nil {
		t.Error("expected Put to fail when the server returns an empty id")
	}
}

func TestUploadBadJSONSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", MaxUploadSize: 1 << 20})
	// No prior pointer: exercise the CREATE path directly, with no rewrite attempt.
	if _, err := store.Put(context.Background(), "", "", []byte("x")); err == nil {
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

// TestPutRewritesInPlaceKeepingAStablePointer is the regression test for the
// create-new-every-save defect.
//
// A file id is a STABLE identifier — a filename, not a version. Saving a
// document repeatedly must REWRITE its file (PUT /internal/file/{id}/content)
// and return the SAME pointer, not create a new file each time and leave the
// previous one to be reaped. The old behaviour churned a fresh id per flush,
// which is what forced the pointer-update-and-delete dance in room.persist.
//
// Non-vacuity: restore the old Put (always POST /internal/file) and the pointer
// changes between saves while creates climbs to 3, tripping both assertions.
func TestPutRewritesInPlaceKeepingAStablePointer(t *testing.T) {
	store, stub := newTestStore(t)
	ctx := context.Background()

	// First save: no file exists yet, so the adapter creates one.
	first, err := store.Put(ctx, "doc-1", "bucket-1", []byte("snapshot-v1"))
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}

	// Subsequent saves must reuse that id.
	second, err := store.Put(ctx, first, "bucket-1", []byte("snapshot-v2"))
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	third, err := store.Put(ctx, second, "bucket-1", []byte("snapshot-v3"))
	if err != nil {
		t.Fatalf("third Put: %v", err)
	}

	if second != first || third != first {
		t.Fatalf("pointer churned across saves: %q then %q then %q; a file id is stable", first, second, third)
	}

	stub.mu.Lock()
	creates, rewrites := stub.creates, stub.rewrites
	stub.mu.Unlock()
	if creates != 1 {
		t.Fatalf("created %d files, want exactly 1; later saves must rewrite, not create", creates)
	}
	if rewrites != 2 {
		t.Fatalf("rewrote %d times, want 2", rewrites)
	}

	// The latest content is what a reader gets back.
	got, err := store.Get(ctx, first)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "snapshot-v3" {
		t.Fatalf("Get = %q, want the most recent snapshot", got)
	}
}

// TestPutCreatesWhenTheFileIsGone covers the self-healing path: a pointer whose
// file was removed out of band must not wedge saving forever. The rewrite 404s
// and the adapter falls back to creating a fresh file.
func TestPutCreatesWhenTheFileIsGone(t *testing.T) {
	store, stub := newTestStore(t)
	ctx := context.Background()

	first, err := store.Put(ctx, "doc-1", "bucket-1", []byte("snapshot-v1"))
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := store.Delete(ctx, first); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	again, err := store.Put(ctx, first, "bucket-1", []byte("snapshot-v2"))
	if err != nil {
		t.Fatalf("Put after the file vanished: %v", err)
	}
	if again == "" {
		t.Fatal("expected a fresh pointer after the file was removed")
	}
	stub.mu.Lock()
	creates := stub.creates
	stub.mu.Unlock()
	if creates != 2 {
		t.Fatalf("created %d files, want 2 (initial + recreate after deletion)", creates)
	}
}

// TestPutSurfacesDedupConflict pins the 409 path. file-service deduplicates on
// content hash within a bucket (unique(externalID, storageBucketID)), so writing
// bytes that already belong to ANOTHER file in the same bucket is refused. That
// is a permanent condition for those bytes, not a transient fault, so it must
// surface as an error rather than be retried as one.
func TestPutSurfacesDedupConflict(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Two distinct files in the same bucket.
	a, err := store.Put(ctx, "doc-a", "bucket-1", []byte("content-a"))
	if err != nil {
		t.Fatalf("Put a: %v", err)
	}
	b, err := store.Put(ctx, "doc-b", "bucket-1", []byte("content-b"))
	if err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if a == b {
		t.Fatal("expected two distinct files")
	}

	// Rewriting b with a's exact content collides on the content hash.
	if _, err := store.Put(ctx, b, "bucket-1", []byte("content-a")); err == nil {
		t.Fatal("a dedup conflict must surface as an error, not be silently accepted")
	} else if !strings.Contains(err.Error(), "dedup conflict") {
		t.Fatalf("error = %v, want it to name the dedup conflict so it is not retried as transient", err)
	}
}
