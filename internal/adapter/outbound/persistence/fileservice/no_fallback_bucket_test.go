package fileservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
)

// A document's snapshot belongs in the DOCUMENT'S bucket, and there is no second
// choice. These tests defend the absence of a fallback, which is the kind of thing
// a later reader restores as a "robustness" improvement — so each one states what
// the fallback would actually cost.

// countingTransport fails the test if the store issues ANY HTTP request. The
// refusal must land BEFORE the create: a blob written into a bucket that does not
// own the document is not undone by returning an error afterwards.
type countingTransport struct{ n atomic.Int64 }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.n.Add(1)
	return nil, errors.New("no request should have been issued")
}

// TestFirstSaveRefusesWhenDocumentHasNoBucket is the central RED.
//
// The document EXISTS — the resolver reached the index and answered — but its row
// names no owning bucket. Before this change the snapshot went to a configured
// deployment-wide bucket, producing a blob the document's delete cascade never
// reaches (the cascade walks the document's own bucket), so the content outlives
// the document as an orphan. Ownership and lifecycle only — a snapshot carries no
// authorizationId, so the destination bucket grants it nothing either way.
//
// Non-vacuity: restore `if bucket == "" { bucket = s.cfg.FallbackBucketID }` and
// this fails on the create count — the save succeeds and a file is written.
func TestFirstSaveRefusesWhenDocumentHasNoBucket(t *testing.T) {
	tr := &countingTransport{}
	res := &mapResolver{pointers: map[backend.DocumentID]string{}, bucket: ""}
	store, err := New(Config{
		BaseURL:    "http://file-service.invalid",
		HTTPClient: &http.Client{Transport: tr},
	}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID:  "doc-without-bucket",
		Encoding:    persistence.EncodingV2,
		Update:      realUpdate(t, "content"),
		StateVector: []byte("sv"),
	})
	if err == nil {
		t.Fatal("saved a snapshot for a document whose index row names no bucket; that blob has no owner and no delete cascade")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Errorf("error = %v, want ErrCorrupt — the index and the document's ownership data disagree, which is not transient", err)
	}
	if n := tr.n.Load(); n != 0 {
		t.Errorf("issued %d HTTP requests before refusing; the refusal must precede the create, or the orphan blob already exists", n)
	}
	if got := res.recordCount(); got != 0 {
		t.Errorf("recorded %d pointers for a refused save", got)
	}
}

// TestFirstSaveUsesTheDocumentsOwnBucket is the positive control for the test
// above: the same code path, with a bucket present, must still create — and in
// THAT bucket, not merely somewhere. Without this, a store that refused every
// first save would pass the refusal test.
func TestFirstSaveUsesTheDocumentsOwnBucket(t *testing.T) {
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)

	res := &mapResolver{pointers: map[backend.DocumentID]string{}, bucket: "bucket-of-this-document"}
	store, err := New(Config{BaseURL: srv.URL}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := store.SaveCheckpoint(context.Background(), persistence.SaveCheckpointRequest{
		DocumentID:  "doc-with-bucket",
		Encoding:    persistence.EncodingV2,
		Update:      realUpdate(t, "content"),
		StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("SaveCheckpoint with a per-document bucket: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.creates != 1 {
		t.Fatalf("created %d files, want 1", stub.creates)
	}
	for id, bucket := range stub.bucket {
		if bucket != "bucket-of-this-document" {
			t.Errorf("file %s landed in bucket %q, want the document's own bucket", id, bucket)
		}
	}
}

// TestRewriteIgnoresBucketEntirely pins the unchanged half: once a pointer exists
// the rewrite addresses the file by id, so an empty bucket is irrelevant and must
// NOT start failing saves for documents that are already stored.
func TestRewriteIgnoresBucketEntirely(t *testing.T) {
	stub := newStub()
	srv := httptest.NewServer(stub.handler())
	t.Cleanup(srv.Close)

	res := &mapResolver{pointers: map[backend.DocumentID]string{}, bucket: "b"}
	store, err := New(Config{BaseURL: srv.URL}, res)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "one"), StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed save: %v", err)
	}

	// The bucket disappears from the index AFTER the file exists.
	res.mu.Lock()
	res.bucket = ""
	res.mu.Unlock()

	if _, err := store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: "doc", Encoding: persistence.EncodingV2, Update: realUpdate(t, "two"), StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("rewrite of an already-stored document must not consult the bucket: %v", err)
	}
	if got := stub.createCount(); got != 1 {
		t.Errorf("created %d files; a rewrite must reuse the existing one", got)
	}
}
