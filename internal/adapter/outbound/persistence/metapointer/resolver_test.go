package metapointer

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/go-yjs/backend"

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	fsstore "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/fileservice"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestPointerReturnsTheDocumentsOwnBucket carries forward the property that used
// to live in the service package's persist_bucket_test: a document's snapshot goes
// into the document's OWN storage bucket, not a single configured fallback.
//
// The mechanism moved (FR-018a): the room used to thread the bucket into
// BlobStore.Put; now the store resolves it here, from the index row the server
// returns on collaboration-fetch. The assertion follows the property to its new
// home rather than being dropped with the mechanism.
func TestPointerReturnsTheDocumentsOwnBucket(t *testing.T) {
	const docID = "doc-with-own-bucket"
	const ownBucket = "abcd1234-0000-0000-0000-000000000001"

	meta := metainmem.New()
	ctx := context.Background()
	if err := meta.Save(ctx, model.Metadata{
		ID: docID, ContentType: model.ContentTypeMemo,
		ContentPointer: "file-1", StorageBucketID: ownBucket,
	}); err != nil {
		t.Fatalf("seed index: %v", err)
	}

	pointer, bucket, err := New(meta).Pointer(ctx, backend.DocumentID(docID))
	if err != nil {
		t.Fatalf("Pointer: %v", err)
	}
	if pointer != "file-1" {
		t.Fatalf("pointer = %q, want the row's ContentPointer", pointer)
	}
	if bucket != ownBucket {
		t.Fatalf("bucket = %q, want the document's own bucket %q", bucket, ownBucket)
	}
}

// TestPointerReportsNoPointerBeforeTheFirstSave distinguishes "this document has
// no file yet" from a lookup failure. A row exists (the server pre-registered the
// document) but carries no ContentPointer, so the store must CREATE rather than
// treat it as an error — and it still needs the bucket to create into.
func TestPointerReportsNoPointerBeforeTheFirstSave(t *testing.T) {
	const docID = "doc-not-saved-yet"
	const ownBucket = "abcd1234-0000-0000-0000-000000000002"

	meta := metainmem.New()
	ctx := context.Background()
	if err := meta.Save(ctx, model.Metadata{
		ID: docID, ContentType: model.ContentTypeMemo, StorageBucketID: ownBucket,
	}); err != nil {
		t.Fatalf("seed index: %v", err)
	}

	_, bucket, err := New(meta).Pointer(ctx, backend.DocumentID(docID))
	if !errors.Is(err, fsstore.ErrNoPointer) {
		t.Fatalf("Pointer on a row with no file = %v, want ErrNoPointer", err)
	}
	if bucket != ownBucket {
		t.Fatalf("bucket = %q, want the document's own bucket even with no pointer yet", bucket)
	}
}

// TestPointerReportsNoPointerForAnUnknownDocument treats a missing row the same
// way: nothing stored yet.
func TestPointerReportsNoPointerForAnUnknownDocument(t *testing.T) {
	_, _, err := New(metainmem.New()).Pointer(context.Background(), "nope")
	if !errors.Is(err, fsstore.ErrNoPointer) {
		t.Fatalf("Pointer on an unknown document = %v, want ErrNoPointer", err)
	}
}

// TestRecordPreservesTheRestOfTheRow guards the read-modify-write. The index row
// carries content type, authorization policy, owner and bucket alongside the
// pointer, and Save takes a whole row — so recording a pointer must not blank the
// fields it does not own.
func TestRecordPreservesTheRestOfTheRow(t *testing.T) {
	const docID = "doc-preserve"
	meta := metainmem.New()
	ctx := context.Background()
	original := model.Metadata{
		ID: docID, ContentType: model.ContentTypeWhiteboard,
		AuthorizationPolicyID: "policy-7", OwnerRef: "owner-3",
		StorageBucketID: "bucket-9", Version: 4,
	}
	if err := meta.Save(ctx, original); err != nil {
		t.Fatalf("seed index: %v", err)
	}

	if err := New(meta).Record(ctx, backend.DocumentID(docID), "file-new"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	got, err := meta.Load(ctx, docID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ContentPointer != "file-new" {
		t.Fatalf("ContentPointer = %q, want the recorded pointer", got.ContentPointer)
	}
	if got.ContentType != original.ContentType || got.AuthorizationPolicyID != original.AuthorizationPolicyID ||
		got.OwnerRef != original.OwnerRef || got.StorageBucketID != original.StorageBucketID {
		t.Fatalf("recording a pointer blanked other index fields: %+v", got)
	}
}
