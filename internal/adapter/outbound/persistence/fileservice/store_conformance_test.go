package fileservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/conformance"
	"github.com/antst/go-yjs/backend/persistence"
	"github.com/antst/go-yjs/crdt"
)

// stubFileService is a faithful stand-in for the subset of file-service this
// store uses: create assigns a stable id, PUT rewrites content behind that id,
// GET serves it. It enforces the real unique(externalID, storageBucketID)
// content dedup, so the 409 path is exercised rather than assumed.
type stubFileService struct {
	mu      sync.Mutex
	byID    map[string][]byte
	bucket  map[string]string // file id -> bucket
	byHash  map[string]string // bucket|hash -> file id
	nextID  int
	creates int
	writes  int
}

func newStub() *stubFileService {
	return &stubFileService{byID: map[string][]byte{}, bucket: map[string]string{}, byHash: map[string]string{}}
}

func hashKey(bucket string, b []byte) string {
	sum := sha256.Sum256(b)
	return bucket + "|" + hex.EncodeToString(sum[:])
}

func (s *stubFileService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/file", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<20)
		//nolint:gosec // G120: the body is already bounded by MaxBytesReader above,
		// and this is a test stub for file-service, not a served endpoint.
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			http.Error(w, "bad multipart", http.StatusBadRequest)
			return
		}
		f, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "no file part", http.StatusBadRequest)
			return
		}
		defer func() { _ = f.Close() }()
		body, _ := io.ReadAll(f)
		bucket := r.FormValue("storageBucketId")

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, clash := s.byHash[hashKey(bucket, body)]; clash {
			w.WriteHeader(http.StatusConflict)
			return
		}
		s.nextID++
		s.creates++
		id := "file-" + strconv.Itoa(s.nextID)
		s.byID[id] = append([]byte(nil), body...)
		s.bucket[id] = bucket
		s.byHash[hashKey(bucket, body)] = id
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	})
	mux.HandleFunc("PUT /internal/file/{id}/content", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		body, _ := io.ReadAll(r.Body)

		s.mu.Lock()
		defer s.mu.Unlock()
		prev, ok := s.byID[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		bucket := s.bucket[id]
		if owner, clash := s.byHash[hashKey(bucket, body)]; clash && owner != id {
			w.WriteHeader(http.StatusConflict)
			return
		}
		delete(s.byHash, hashKey(bucket, prev))
		s.byID[id] = append([]byte(nil), body...)
		s.byHash[hashKey(bucket, body)] = id
		s.writes++
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /internal/file/{id}/content", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		body, ok := s.byID[r.PathValue("id")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	})
	mux.HandleFunc("DELETE /internal/file/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		s.mu.Lock()
		body, ok := s.byID[id]
		if ok {
			delete(s.byHash, hashKey(s.bucket[id], body))
			delete(s.bucket, id)
			delete(s.byID, id)
		}
		s.mu.Unlock()
		if !ok {
			// file-service answers 404 for an id it does not hold. The adapter
			// treats that as success, because a retried cascade must not fail on
			// the second attempt.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// count reports how many files the stub currently holds, so a test can assert
// erasure actually happened rather than trusting a nil error.
func (s *stubFileService) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// mapResolver is the DocumentID -> file pointer map the real wiring keeps in the
// Alkemio metadata index. Written once per document, read on load.
type mapResolver struct {
	mu       sync.Mutex
	pointers map[backend.DocumentID]string
	bucket   string
}

func (m *mapResolver) Pointer(_ context.Context, id backend.DocumentID) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pointers[id]
	if !ok {
		return "", m.bucket, ErrNoPointer
	}
	return p, m.bucket, nil
}

func (m *mapResolver) Record(_ context.Context, id backend.DocumentID, pointer string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pointers[id] = pointer
	return nil
}

func newStoreForTest(t *testing.T) (*Store, *stubFileService) {
	t.Helper()
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)
	// Per-document buckets in production; one shared bucket here makes the dedup
	// path REACHABLE, so the 409 branch is genuinely exercised.
	res := &mapResolver{pointers: map[backend.DocumentID]string{}, bucket: "bucket-test"}
	store, err := New(Config{BaseURL: srv.URL, FallbackBucketID: "bucket-test"}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store, stub
}

// TestCheckpointConformance runs the core's adversarial checkpoint contract
// against the store that actually carries production documents (FR-008, SC-006).
//
// The suite is mutation-tested upstream — aliasing on save, aliasing on load,
// frozen revisions, silent missing, ignored cancellation and unchecked fences are
// each planted and required to fail — so passing is evidence, not absence of
// evidence. Both alias directions matter here in particular: file-service hands
// us a response body we do not own, and we hand it a buffer it must not retain.
func TestCheckpointConformance(t *testing.T) {
	conformance.CheckpointPersistence(t, func() persistence.CheckpointStore {
		store, _ := newStoreForTest(t)
		return store
	})
}

// --- behaviours the contract cannot express, but this medium requires ---

// realUpdate builds a genuine Yjs update carrying text. Opaque bytes will not do
// here: this store DERIVES the state vector by parsing the stored blob, so a
// non-Yjs fixture is correctly rejected as ErrCorrupt. (The upstream suite had
// the same bug and fixed it for the same reason.)
func realUpdate(t *testing.T, text string) []byte {
	t.Helper()
	// Pin the client id so the same text yields BYTE-IDENTICAL bytes. Without it
	// every call gets a random client id, so two "identical" updates hash
	// differently and the content-dedup path is unreachable — the 409 test would
	// pass vacuously by never colliding.
	doc := crdt.NewDoc("fixture", crdt.WithClientID(1))
	doc.GetText("t").Insert(0, text, crdt.Object{})
	update, err := crdt.EncodeStateAsUpdate(doc, nil)
	if err != nil {
		t.Fatalf("encode fixture update: %v", err)
	}
	return update
}

func save(t *testing.T, s *Store, id string, update []byte) persistence.Revision {
	t.Helper()
	rev, err := s.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(id), Update: update, StateVector: []byte("sv"),
	})
	if err != nil {
		t.Fatalf("SaveCheckpoint(%s): %v", id, err)
	}
	return rev
}

// TestSaveRewritesOneStableFile is the core shape assertion: a document owns ONE
// file for its lifetime, rewritten on every save. If this regresses to a file
// per save, the pointer churns, the metadata row needs updating on every flush,
// and superseded files accumulate — the exact behaviour fixed in fb32d05.
func TestSaveRewritesOneStableFile(t *testing.T) {
	store, stub := newStoreForTest(t)
	save(t, store, "doc-1", realUpdate(t, "state-1"))
	save(t, store, "doc-1", realUpdate(t, "state-2"))
	save(t, store, "doc-1", realUpdate(t, "state-3"))

	stub.mu.Lock()
	creates, writes, files := stub.creates, stub.writes, len(stub.byID)
	stub.mu.Unlock()

	if creates != 1 || files != 1 {
		t.Fatalf("created %d files (%d live), want exactly 1 — a document owns one file, rewritten", creates, files)
	}
	if writes != 2 {
		t.Fatalf("rewrote %d times, want 2", writes)
	}
	got, err := store.LoadCheckpoint(context.Background(), "doc-1")
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if text := textOf(t, got.Update); text != "state-3" {
		t.Fatalf("loaded %q, want the most recent state", text)
	}
}

// TestSaveSurfacesDedupConflict pins the 409. It is content DEDUPLICATION, not a
// concurrency guard: writing bytes already stored under another file in the same
// bucket is refused. It must surface rather than be retried as transient — and it
// must never be mistaken for stale-owner protection, which this store does not
// provide (it reports Unfenced).
func TestSaveSurfacesDedupConflict(t *testing.T) {
	store, _ := newStoreForTest(t)
	save(t, store, "doc-a", realUpdate(t, "shared-content"))
	save(t, store, "doc-b", realUpdate(t, "distinct-content"))

	_, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc-b", Update: realUpdate(t, "shared-content"), StateVector: []byte("sv"),
	})
	if err == nil {
		t.Fatal("a dedup conflict must surface, not be silently accepted")
	}
	if !strings.Contains(err.Error(), "dedup conflict") {
		t.Fatalf("error = %v, want it to name the dedup conflict so it is not retried as transient", err)
	}
}

// TestSaveDoesNotForkOnServerError guards the create-fallback. Only a 404 (the
// file is gone) may fall back to creating. A transient 500 must surface: creating
// a second file would FORK the document, leaving the pointer on the old file
// while fresh state lands somewhere nothing will ever read.
func TestSaveDoesNotForkOnServerError(t *testing.T) {
	var creates int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			http.Error(w, "backend unavailable", http.StatusInternalServerError)
		case http.MethodPost:
			creates++
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "file-new"})
		default:
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	res := &mapResolver{pointers: map[backend.DocumentID]string{"doc-1": "file-existing"}, bucket: "b"}
	store, err := New(Config{BaseURL: srv.URL, FallbackBucketID: "b"}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc-1", Update: []byte("x"), StateVector: []byte("sv"),
	}); err == nil {
		t.Fatal("a server error on rewrite must surface, not fall back to creating a second file")
	}
	if creates != 0 {
		t.Fatalf("created %d files after a failed rewrite; that forks the document", creates)
	}
}

// TestSaveRecreatesWhenTheFileIsGone is the other half: a pointer whose file was
// removed out of band must not wedge saving forever. The rewrite 404s and the
// store creates a replacement, recording the new pointer.
func TestSaveRecreatesWhenTheFileIsGone(t *testing.T) {
	store, stub := newStoreForTest(t)
	save(t, store, "doc-1", realUpdate(t, "state-1"))

	stub.mu.Lock()
	for id := range stub.byID {
		delete(stub.byID, id)
	}
	stub.byHash = map[string]string{}
	stub.mu.Unlock()

	save(t, store, "doc-1", realUpdate(t, "state-2"))
	stub.mu.Lock()
	creates := stub.creates
	stub.mu.Unlock()
	if creates != 2 {
		t.Fatalf("created %d files, want 2 (initial + replacement after the file vanished)", creates)
	}
	got, err := store.LoadCheckpoint(context.Background(), "doc-1")
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if text := textOf(t, got.Update); text != "state-2" {
		t.Fatalf("loaded %q after recreate, want state-2", text)
	}
}

// TestSaveFailsWhenThePointerCannotBeRecorded covers the window where bytes are
// durable but unreachable. If recording the new pointer fails, nothing maps the
// document to the file just written, so a later load would report the document as
// never saved. Reporting success there would tell the caller its state is safe
// when it is not findable.
func TestSaveFailsWhenThePointerCannotBeRecorded(t *testing.T) {
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)
	store, err := New(Config{BaseURL: srv.URL, FallbackBucketID: "b"}, failingRecorder{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc-1", Update: []byte("x"), StateVector: []byte("sv"),
	}); err == nil {
		t.Fatal("a save whose pointer could not be recorded must fail: the bytes are durable but unreachable")
	}
}

type failingRecorder struct{}

func (failingRecorder) Pointer(context.Context, backend.DocumentID) (string, string, error) {
	return "", "b", ErrNoPointer
}
func (failingRecorder) Record(context.Context, backend.DocumentID, string) error {
	return errors.New("index unavailable")
}

// textOf applies an update to a fresh document and reads the text back, so a
// content assertion survives any legal re-encoding of the same state.
func textOf(t *testing.T, update []byte) string {
	t.Helper()
	doc := crdt.NewDoc("verify")
	if err := crdt.ApplyUpdate(doc, update, nil); err != nil {
		t.Fatalf("apply loaded update: %v", err)
	}
	return doc.GetText("t").ToString()
}

// TestLoadReportsCorruptWhenTheFileIsGone is the property that used to live in the
// service package as "a populated pointer whose blob is missing must fail
// materialization" (FR-018a).
//
// The index saying a document HAS state while the content is gone is NOT the same
// as a document that was never saved, and the difference is load-bearing: the room
// seeds create-time content on ErrNotFound, so collapsing the two would resurrect
// stale content over a document that had real content, and the next save would
// overwrite the last good state with it. ErrNotFound is reserved for a document
// with no pointer at all.
//
// Non-vacuity: return persistence.ErrNotFound from fetch's 404 branch instead, and
// this test fails.
func TestLoadReportsCorruptWhenTheFileIsGone(t *testing.T) {
	store, stub := newStoreForTest(t)
	save(t, store, "doc-1", realUpdate(t, "state-1"))

	// The pointer stays recorded; the file disappears out of band.
	stub.mu.Lock()
	for id := range stub.byID {
		delete(stub.byID, id)
	}
	stub.mu.Unlock()

	_, err := store.LoadCheckpoint(context.Background(), "doc-1")
	if errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("a pointer whose content is gone must NOT report ErrNotFound: the caller would seed over a document that had real content")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Fatalf("LoadCheckpoint with a missing file = %v, want ErrCorrupt", err)
	}
}

// TestLoadReportsNotFoundForADocumentWithNoPointer is the other side: a document
// that genuinely has no stored state reports ErrNotFound, which is what lets the
// room seed its create-time content.
func TestLoadReportsNotFoundForADocumentWithNoPointer(t *testing.T) {
	store, _ := newStoreForTest(t)
	if _, err := store.LoadCheckpoint(context.Background(), "never-saved"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("LoadCheckpoint on a never-saved document = %v, want ErrNotFound", err)
	}
}

// TestDeleteRemovesTheDocumentsFile covers the production erasure path, which had
// no test at all — Delete sat at 0% while the owner-delete cascade depended on it.
//
// The four properties the contract names, against a real HTTP round-trip rather
// than the in-process store that stands in for this one everywhere else.
func TestDeleteRemovesTheDocumentsFile(t *testing.T) {
	store, stub := newStoreForTest(t)
	ctx := context.Background()

	update := realUpdate(t, "delete-me")
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Update: update, StateVector: []byte("derived-on-read"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if stub.count() != 1 {
		t.Fatalf("precondition: expected one stored file, got %d", stub.count())
	}

	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if stub.count() != 0 {
		t.Fatalf("the file survived Delete: %d still stored", stub.count())
	}

	// IDEMPOTENT. The cascade retries, and the second attempt must not fail the
	// operation it is completing.
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc"}); err != nil {
		t.Fatalf("second Delete must succeed (the cascade retries): %v", err)
	}
}

// TestDeleteSucceedsForADocumentThatNeverHadAFile is the other idempotence case:
// the document has no pointer at all, so there is nothing to erase. A cascade
// over a document that was created and deleted without ever being saved must not
// fail on this step.
func TestDeleteSucceedsForADocumentThatNeverHadAFile(t *testing.T) {
	store, _ := newStoreForTest(t)
	if err := store.Delete(context.Background(), persistence.DeleteRequest{DocumentID: "never-saved"}); err != nil {
		t.Fatalf("Delete on a document with no file: %v", err)
	}
}

// TestDeleteRejectsAFenceWithoutTouchingTheNetwork pins the Unfenced contract at
// the erasure path.
//
// This store cannot hold an epoch, so it reports Unfenced and a non-zero fence is
// ErrUnexpectedFence. The assertion that matters is the SECOND one: the rejection
// must happen before the pointer is resolved and before any request is sent. A
// store that returned the right error after issuing the DELETE would satisfy an
// error-only check while having already erased the document — which is the whole
// failure mode "a rejected delete leaves the state intact" exists to prevent.
func TestDeleteRejectsAFenceWithoutTouchingTheNetwork(t *testing.T) {
	store, stub := newStoreForTest(t)
	ctx := context.Background()

	update := realUpdate(t, "fenced-delete")
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Update: update, StateVector: []byte("derived-on-read"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc", Fence: 9}); !errors.Is(err, persistence.ErrUnexpectedFence) {
		t.Fatalf("Delete with a fence against an Unfenced store = %v, want ErrUnexpectedFence", err)
	}
	if stub.count() != 1 {
		t.Fatal("a rejected delete erased the file anyway; the fence must be checked before the request is sent")
	}
}
