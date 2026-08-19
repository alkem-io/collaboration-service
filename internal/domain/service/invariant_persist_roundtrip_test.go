package service

import (
	"bytes"
	"context"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvPersistRoundtrip — INV-PERSIST-ROUNDTRIP (spec 002 FR-016). The persistence
// layer round-trips byte-identically and loses no metadata field, independent of CRDT
// merge semantics (a CRDT-core-independent server property). Green now; pins behaviour the
// redesign must preserve.
func TestInvPersistRoundtrip(t *testing.T) {
	ctx := context.Background()
	deps := newTestDeps()

	// State: bytes in == bytes out, with no text mangling.
	//
	// Restructured for the checkpoint profile (FR-018a): the original wrote an
	// arbitrary byte string. A checkpoint store DERIVES the state vector by parsing
	// the stored update, so arbitrary bytes are correctly rejected as ErrCorrupt —
	// the fixture had to become a real update. The property under test is unchanged
	// and the fixture is if anything harsher: a v2 update carries embedded NULs and
	// high bytes of its own, and the multi-byte text below adds more.
	src := ycrdt.NewDoc("rt", ycrdt.WithClientID(7))
	src.GetText("t").Insert(0, "round\x00trip — ünïcøde ✓", ycrdt.Object{})
	data, err := ycrdt.EncodeStateAsUpdateV2(src, nil)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	if err := deps.putState(ctx, "doc-rt", data); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := deps.storedState(ctx, "doc-rt")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("state round-trip changed bytes: got %x want %x", got, data)
	}

	// Metadata store: the index fields round-trip (version is store-managed, so not asserted).
	meta := model.Metadata{
		ID: "doc-rt", ContentType: model.ContentTypeWhiteboard, ContentPointer: "file-rt", OwnerRef: "owner-9", AuthorizationPolicyID: "policy-3", StorageBucketID: "bucket-1",
	}
	if err := deps.meta.Save(ctx, meta); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := deps.meta.Load(ctx, "doc-rt")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.ContentType != meta.ContentType || loaded.ContentPointer != meta.ContentPointer ||
		loaded.OwnerRef != meta.OwnerRef ||
		loaded.AuthorizationPolicyID != meta.AuthorizationPolicyID || loaded.StorageBucketID != meta.StorageBucketID {
		t.Fatalf("metadata-store round-trip lost fields:\n got  %+v\n want %+v", loaded, meta)
	}
}
