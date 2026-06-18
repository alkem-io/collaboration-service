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
	lastBucket  string
	lastAuth    string
	lastDisplay string
	lastReused  bool
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
	if fields["authorizationId"] == "" {
		http.Error(w, "missing authorizationId", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBucket = fields["storageBucketId"]
	s.lastAuth = fields["authorizationId"]
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
		AuthorizationID: stub.authID,
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
	pointer, err := store.Put(ctx, "doc-1", want)
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

func TestPutSendsRequiredMultipartFields(t *testing.T) {
	store, stub := newTestStore(t)
	if _, err := store.Put(context.Background(), "doc-x", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stub.lastBucket != stub.bucketID {
		t.Errorf("storageBucketId = %q, want %q", stub.lastBucket, stub.bucketID)
	}
	if stub.lastAuth != stub.authID {
		t.Errorf("authorizationId = %q, want %q", stub.lastAuth, stub.authID)
	}
	if stub.lastDisplay == "" {
		t.Error("displayName not sent")
	}
}

func TestPutOverwriteDeletesPreviousSnapshot(t *testing.T) {
	// On re-save, the adapter uploads a new object (new UUID) and deletes the
	// previous one (latest-only, R7), so old snapshots do not accumulate. The
	// caller passes the previous pointer as the hint.
	store, stub := newTestStore(t)
	ctx := context.Background()

	p1, err := store.Put(ctx, "doc-stable", []byte("v1"))
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	p2, err := store.Put(ctx, p1, []byte("v2-different"))
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if p1 == p2 {
		t.Error("expected a new pointer for new content")
	}

	// The old object must be gone; the new one serves v2.
	stub.mu.Lock()
	_, oldExists := stub.byID[p1]
	stub.mu.Unlock()
	if oldExists {
		t.Error("previous snapshot not deleted on overwrite (orphan)")
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
	pointer, err := store.Put(ctx, "doc-del", []byte("x"))
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

func TestPutRejectsOversize(t *testing.T) {
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)
	store, _ := New(Config{
		BaseURL:         srv.URL,
		StorageBucketID: stub.bucketID,
		AuthorizationID: stub.authID,
		MaxUploadSize:   8, // tiny ceiling
	})
	_, err := store.Put(context.Background(), "big", []byte("this is definitely more than eight bytes"))
	if err == nil {
		t.Error("expected oversize Put to be rejected")
	}
}

func TestNewValidates(t *testing.T) {
	cases := []Config{
		{StorageBucketID: "b", AuthorizationID: "a"}, // missing BaseURL
		{BaseURL: "http://x", AuthorizationID: "a"},  // missing bucket
		{BaseURL: "http://x", StorageBucketID: "b"},  // missing auth id
	}
	for i, c := range cases {
		if _, err := New(c); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}

func TestServerErrorSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", []byte("x")); err == nil {
		t.Error("expected Put to surface a 500")
	}
}

func TestUploadEmptyIDSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, createResponse{ID: ""}) // server returns no id
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", []byte("x")); err == nil {
		t.Error("expected Put to fail when the server returns an empty id")
	}
}

func TestUploadBadJSONSurfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
	if _, err := store.Put(context.Background(), "doc", []byte("x")); err == nil {
		t.Error("expected Put to fail on a malformed response body")
	}
}

func TestGetNetworkErrorSurfaces(t *testing.T) {
	// A closed server: requests fail at the transport layer.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
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
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
	if _, err := store.Get(context.Background(), "id"); err == nil {
		t.Error("expected Get to surface a 500")
	}
}

func TestDeleteNon200Surfaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	store, _ := New(Config{BaseURL: srv.URL, StorageBucketID: "b", AuthorizationID: "a", MaxUploadSize: 1 << 20})
	if err := store.Delete(context.Background(), "id"); err == nil {
		t.Error("expected Delete to surface a 502")
	}
}
