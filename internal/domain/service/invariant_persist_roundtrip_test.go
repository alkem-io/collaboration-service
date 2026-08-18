package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvPersistRoundtrip — INV-PERSIST-ROUNDTRIP (spec 002 FR-016). The persistence
// layer round-trips byte-identically and loses no metadata field, independent of CRDT
// merge semantics (a CRDT-core-independent server property). Green now; pins behaviour the
// redesign must preserve.
func TestInvPersistRoundtrip(t *testing.T) {
	ctx := context.Background()
	deps := newTestDeps()

	// Blob: bytes in == bytes out (includes embedded NULs / high bytes — no text mangling).
	data := []byte{0x00, 0x01, 0xff, 0x42, 0x00, 0x99, 0x7f}
	ptr, err := deps.blob.Put(ctx, "doc-rt", "bucket-1", data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := deps.blob.Get(ctx, ptr)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("blob round-trip changed bytes: got %x want %x", got, data)
	}

	// Metastore: the index fields round-trip (version is store-managed, so not asserted).
	meta := model.Metadata{
		ID: "doc-rt", ContentType: model.ContentTypeWhiteboard, ContentPointer: ptr,
		BlobStore: "inline", OwnerRef: "owner-9", AuthorizationPolicyID: "policy-3", StorageBucketID: "bucket-1",
	}
	if err := deps.meta.Save(ctx, meta); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := deps.meta.Load(ctx, "doc-rt")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.ContentType != meta.ContentType || loaded.ContentPointer != meta.ContentPointer ||
		loaded.BlobStore != meta.BlobStore || loaded.OwnerRef != meta.OwnerRef ||
		loaded.AuthorizationPolicyID != meta.AuthorizationPolicyID || loaded.StorageBucketID != meta.StorageBucketID {
		t.Fatalf("metastore round-trip lost fields:\n got  %+v\n want %+v", loaded, meta)
	}
}
