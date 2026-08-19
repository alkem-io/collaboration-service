package fileservice

import (
	"bytes"
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

// The core's CheckpointPersistenceDeletion suite is deliberately NOT run against
// this store, and the reason is now stated upstream rather than inferred here.
//
// Its load-after-delete clause requires ErrNotFound. This store returns ErrCorrupt
// while the index row survives, because it does not own the pointer — that lives
// in server's metadata row. persistence/store.go (v0.0.6) states the precondition:
// "a partial owner cannot satisfy Deleter alone ... a component store failing the
// suite on this rule has a shape mismatch, not a bug."
//
// The guarantee is real, it just belongs a layer up: the completed purge cascade
// drops the row, and a rowless document loads as ErrNotFound. The suite therefore
// belongs against purgeDurable, not against the blob store.
//
// I re-added the invocation once when v0.0.6 fixed the unrelated codec-fixture
// defect, and it failed on exactly this clause. Recorded so the next person does
// not repeat it. The four properties it would check are covered directly below
// and in TestLoadAfterDeleteReportsCorruptWhileTheIndexRowSurvives.

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
		DocumentID: backend.DocumentID(id), Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("sv"),
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
		DocumentID: "doc-b", Encoding: persistence.EncodingV2, Update: realUpdate(t, "shared-content"), StateVector: []byte("sv"),
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
		DocumentID: "doc-1", Encoding: persistence.EncodingV2, Update: []byte("x"), StateVector: []byte("sv"),
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
		DocumentID: "doc-1", Encoding: persistence.EncodingV2, Update: []byte("x"), StateVector: []byte("sv"),
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
// opens an EMPTY editable document on ErrNotFound, so collapsing the two would
// open a document that has real content as empty, and the next save would write
// that empty state over the last good one. ErrNotFound is reserved for a document
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
		t.Fatal("a pointer whose content is gone must NOT report ErrNotFound: the caller would open a document that has real content as EMPTY, and the next save would make that permanent")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Fatalf("LoadCheckpoint with a missing file = %v, want ErrCorrupt", err)
	}
}

// TestLoadReportsNotFoundForADocumentWithNoPointer is the other side: a document
// that genuinely has no stored state reports ErrNotFound, which is what lets the
// room open it empty and editable.
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
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("derived-on-read"),
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

// TestLoadAfterDeleteReportsCorruptWhileTheIndexRowSurvives pins what the
// persistence contract REQUIRES for a dangling pointer.
//
// This is what persistence.ErrNotFound's own doc requires: a store that resolves a
// document through a pointer, finds the pointer set and the target gone, has not
// found "no history". Reporting ErrNotFound there makes the caller treat a
// document that HAD content as new — here, open it EMPTY — and the next save
// overwrites the last good state. Silent data loss arriving through the error
// type. Use ErrCorrupt.
//
// That is exactly this store's dangling-row window. purgeDurable erases the blob
// FIRST and drops the index row second, so a broker failure between the two steps
// leaves the row outliving the blob. Reporting ErrNotFound there would send
// restoreInto down its open-empty path, and the next save would make that empty
// document durable — the deleted document's content replaced by nothing.
//
// What genuinely conflicts is Deleter's load-after-delete clause, which requires
// ErrNotFound in the same situation. Both rules cannot hold for a store that does
// not own the pointer — ours lives in server's metadata row. Raised upstream and
// resolved in favour of ErrNotFound's rule, since the data-loss argument is the
// stronger one; the Deleter clause now states its precondition that the
// implementation owns everything making the document findable.
//
// The consequence for this file: the blob store is not a conforming Deleter ON
// ITS OWN and should not be measured as one. The load-after-delete guarantee
// belongs to whatever owns the whole document — the purge cascade, which does
// provide it end to end, because the completed cascade drops the row and a
// rowless document loads as ErrNotFound.
func TestLoadAfterDeleteReportsCorruptWhileTheIndexRowSurvives(t *testing.T) {
	store, _ := newStoreForTest(t)
	ctx := context.Background()

	update := realUpdate(t, "delete-then-load")
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := store.LoadCheckpoint(ctx, "doc")
	if errors.Is(err, persistence.ErrNotFound) {
		t.Fatal("load reported ErrNotFound while the index row still carries a pointer; restoreInto treats that as 'never saved' and opens the document EMPTY, so the next save writes an empty document over one whose blob was just erased")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Fatalf("load after Delete with the row still present = %v, want ErrCorrupt", err)
	}
}

// TestDeleteHonoursACancelledContext pins the last deletion property: a cancelled
// caller must not have its erasure carried out anyway.
func TestDeleteHonoursACancelledContext(t *testing.T) {
	store, stub := newStoreForTest(t)

	update := realUpdate(t, "keep-me")
	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Delete(ctx, persistence.DeleteRequest{DocumentID: "doc"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete with a cancelled context = %v, want context.Canceled", err)
	}
	if stub.count() != 1 {
		t.Fatalf("a cancelled Delete erased the file anyway: %d stored", stub.count())
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
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: []byte("derived-on-read"),
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

// storeAgainst builds a store pointed at an arbitrary handler, for driving the
// HTTP failure branches a well-behaved stub never produces.
func storeAgainst(t *testing.T, h http.HandlerFunc, pointers map[backend.DocumentID]string) *Store {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	res := &mapResolver{pointers: pointers, bucket: "bucket-test"}
	store, err := New(Config{BaseURL: srv.URL, FallbackBucketID: "bucket-test"}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return store
}

// TestUnexpectedStatusesAreSurfacedNotSwallowed covers the default branches on
// all three verbs.
//
// The shared reason is that file-service is a separate service that can fail in
// ways this adapter has no mapping for — a 500, a 502 from a proxy, a 403 from a
// misconfigured gateway. Every one of those must surface. Swallowing them means a
// save silently not persisting, a load silently returning nothing, or a delete
// silently leaving content behind for a document the owner erased.
func TestUnexpectedStatusesAreSurfacedNotSwallowed(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		store := storeAgainst(t, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "gateway blew up", http.StatusBadGateway)
		}, map[backend.DocumentID]string{})

		_, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
			DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "x"), StateVector: []byte("v"),
		})
		if err == nil {
			t.Fatal("a 502 on create must surface; swallowing it means the save silently did not persist")
		}
		if !strings.Contains(err.Error(), "502") {
			t.Fatalf("error = %v, want it to carry the status", err)
		}
	})

	t.Run("fetch", func(t *testing.T) {
		store := storeAgainst(t, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "forbidden", http.StatusForbidden)
		}, map[backend.DocumentID]string{"doc": "file-1"})

		if _, err := store.LoadCheckpoint(context.Background(), "doc"); err == nil {
			t.Fatal("a 403 on fetch must surface; swallowing it would look like an empty document")
		}
	})

	t.Run("delete", func(t *testing.T) {
		store := storeAgainst(t, func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "nope", http.StatusInternalServerError)
		}, map[backend.DocumentID]string{"doc": "file-1"})

		if err := store.Delete(context.Background(), persistence.DeleteRequest{DocumentID: "doc"}); err == nil {
			t.Fatal("a 500 on delete must surface; the owner-delete cascade would otherwise report success while the content is still there")
		}
	})
}

// TestCreateRejectsAResponseWithNoID covers the guard on a syntactically valid
// but useless create response.
//
// An empty id is worse than an error: the bytes are stored, but nothing can ever
// address them again. Recording "" as the pointer would make every later load
// resolve to nothing and every later save create yet another orphan.
func TestCreateRejectsAResponseWithNoID(t *testing.T) {
	store := storeAgainst(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"","externalID":"abc"}`))
	}, map[backend.DocumentID]string{})

	_, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "x"), StateVector: []byte("v"),
	})
	if err == nil {
		t.Fatal("a create response with an empty id must fail; the bytes are stored but nothing could ever address them again")
	}
}

// TestCreateRejectsAnUndecodableResponse covers the JSON-decode branch.
func TestCreateRejectsAnUndecodableResponse(t *testing.T) {
	store := storeAgainst(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`not json`))
	}, map[backend.DocumentID]string{})

	_, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "x"), StateVector: []byte("v"),
	})
	if err == nil {
		t.Fatal("an undecodable create response must fail rather than yield a zero-value pointer")
	}
}

// TestSaveRejectsAnOversizeSnapshotBeforeUploading covers the MaxUploadSize guard.
//
// It runs BEFORE the request, which is the point: file-service would refuse the
// body anyway, but only after the whole snapshot crossed the network. On a
// document near the cap that is tens of megabytes uploaded per flush to be
// rejected every time.
func TestSaveRejectsAnOversizeSnapshotBeforeUploading(t *testing.T) {
	reached := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
	t.Cleanup(srv.Close)
	res := &mapResolver{pointers: map[backend.DocumentID]string{}, bucket: "bucket-test"}
	store, err := New(Config{BaseURL: srv.URL, FallbackBucketID: "bucket-test", MaxUploadSize: 8}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "well over eight bytes"), StateVector: []byte("v"),
	}); err == nil {
		t.Fatal("a snapshot over MaxUploadSize must be refused")
	}
	if reached {
		t.Fatal("the oversize snapshot was uploaded before being refused; the guard must run before the request")
	}
}

// TestLoadReturnsAStateVectorThatDescribesTheDocument is the regression for the
// silent-empty-vector defect, found by independent review.
//
// This store's blob is a bare Yjs update with no V1/V2 discriminator, so a load
// must choose a decoder. Choosing wrong is NOT an error:
// EncodeStateVectorFromUpdate on V2 bytes returns `err == nil` and the vector
// `00` — "this document has nothing from any client". Every checkpoint this
// service writes is V2 (Room.persist), so every load was returning that.
//
// It survived because the conformance suite's fixtures are V1-encoded, so the
// V1 decoder was correct for them: the suite asserted the vector describes the
// update, and it did — for bytes production never produces. Green over a live
// defect.
//
// The assertion here is against the document's OWN state vector, so it cannot
// pass by agreeing with whichever decoder the store happens to call.
//
// Non-vacuity: switch the load path back to the V1 decoder and this fails with
// an empty vector.
func TestLoadReturnsAStateVectorThatDescribesTheDocument(t *testing.T) {
	store, _ := newStoreForTest(t)
	ctx := context.Background()

	// Built exactly as production does: V2.
	doc := crdt.NewDoc("vector-fixture")
	doc.GetText("content").Insert(0, "state vector must not be empty", crdt.Object{})
	update, err := crdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatalf("encode V2 update: %v", err)
	}
	truth := crdt.EncodeStateVector(doc)

	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: update, StateVector: truth,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	cp, err := store.LoadCheckpoint(ctx, "doc")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// len <= 1 is the encoding of "no client state" — a single zero length prefix.
	if len(cp.StateVector) <= 1 {
		t.Fatalf("LoadCheckpoint returned an EMPTY state vector (%v) for a document with content; a caller diffing against it would conclude the server holds nothing and resend everything", cp.StateVector)
	}
	if !bytes.Equal(cp.StateVector, truth) {
		t.Fatalf("state vector = %v, want the document's own %v", cp.StateVector, truth)
	}
}

// TestSaveRefusesAV1UpdateRatherThanStoringIt pins the single-codec decision.
//
// The blob is a BARE Yjs update — no envelope, because other systems read these
// files directly — so there is nowhere to record which codec produced it. A store
// that cannot record the codec must accept exactly one and refuse the other
// LOUDLY; decoding whatever arrives is the defect the declared encoding exists to
// remove.
//
// V2 is that one codec, and this is not a new restriction: Room.persist writes
// EncodeStateAsUpdateV2 and restoreInto reads with ApplyUpdateV2, so the durable
// path has always been V2 end to end. A V1 blob could never have been restored.
// The refusal makes that explicit at the boundary instead of leaving it to fail
// later as corruption.
func TestSaveRefusesAV1UpdateRatherThanStoringIt(t *testing.T) {
	store, _ := newStoreForTest(t)

	doc := crdt.NewDoc("v1-fixture")
	doc.GetText("content").Insert(0, "written by a V1 writer", crdt.Object{})
	update, err := crdt.EncodeStateAsUpdate(doc, nil)
	if err != nil {
		t.Fatalf("encode V1 update: %v", err)
	}

	_, err = store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV1,
		Update: update, StateVector: crdt.EncodeStateVector(doc),
	})
	if !errors.Is(err, persistence.ErrUnsupportedEncoding) {
		t.Fatalf("saving a V1 update = %v, want ErrUnsupportedEncoding; storing it would leave a blob this store can only read as V2, and reading V1 bytes with the V2 decoder returns an EMPTY vector with no error", err)
	}
	// Refused before the network, so nothing was written.
	if _, err := store.LoadCheckpoint(context.Background(), "doc"); !errors.Is(err, persistence.ErrNotFound) {
		t.Fatalf("a refused save left state behind: load = %v, want ErrNotFound", err)
	}
}
