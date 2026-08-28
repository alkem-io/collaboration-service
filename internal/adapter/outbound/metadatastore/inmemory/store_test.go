package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestSaveBumpsVersionOnUpsert(t *testing.T) {
	s := New()
	ctx := context.Background()
	meta := model.Metadata{ID: "d1", ContentType: model.ContentTypeMemo}

	if err := s.Save(ctx, meta); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	first, _ := s.Load(ctx, "d1")
	if !first.Migrated {
		t.Fatal("new in-memory document was marked as legacy migration pending")
	}
	if first.Version != 1 {
		t.Errorf("first version = %d, want 1", first.Version)
	}

	if err := s.Save(ctx, meta); err != nil {
		t.Fatalf("second Save: %v", err)
	}
	second, _ := s.Load(ctx, "d1")
	if second.Version != 2 {
		t.Errorf("second version = %d, want 2", second.Version)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("CreatedAt changed across upsert: %v -> %v", first.CreatedAt, second.CreatedAt)
	}
}

func TestLoadMissingIsNotFound(t *testing.T) {
	if _, err := New().Load(context.Background(), "absent"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Load(absent) error = %v, want ErrNotFound", err)
	}
}

// TestSnapshotSaveDoesNotWipeLifecycleMetadata asserts that a per-snapshot persist
// (Room.persist), which historically carried blank lifecycle fields, does NOT wipe
// the pre-registered owner_ref / authorization_policy_id the delete cascade keys
// off (FR-023). The in-memory store does a wholesale row replace, so without
// "blank = unchanged" preservation the first snapshot save would clobber them.
func TestSnapshotSaveDoesNotWipeLifecycleMetadata(t *testing.T) {
	s := New()
	ctx := context.Background()

	// document.created pre-register: lifecycle metadata, no snapshot yet.
	if err := s.Save(ctx, model.Metadata{
		ID:                    "doc",
		ContentType:           model.ContentTypeWhiteboard,
		OwnerRef:              "callout-7",
		AuthorizationPolicyID: "pol-7",
	}); err != nil {
		t.Fatalf("pre-register Save: %v", err)
	}

	// First snapshot persist: content_pointer set, lifecycle fields blank.
	if err := s.Save(ctx, model.Metadata{
		ID:             "doc",
		ContentType:    model.ContentTypeWhiteboard,
		ContentPointer: "blob-1",
	}); err != nil {
		t.Fatalf("snapshot Save: %v", err)
	}

	got, _ := s.Load(ctx, "doc")
	if got.OwnerRef != "callout-7" {
		t.Errorf("snapshot save wiped owner_ref: got %q, want preserved %q", got.OwnerRef, "callout-7")
	}
	if got.AuthorizationPolicyID != "pol-7" {
		t.Errorf("snapshot save wiped authorization_policy_id: got %q, want preserved %q", got.AuthorizationPolicyID, "pol-7")
	}
	if got.ContentPointer != "blob-1" {
		t.Errorf("snapshot save did not set content_pointer: got %q, want %q", got.ContentPointer, "blob-1")
	}
}

// TestRedeliveredPreRegisterDoesNotOrphanBlob asserts that a REDELIVERED
// document.created (a blind PreRegister with a blank content_pointer) against a
// document that already has a live snapshot does NOT clobber content_pointer back
// to "" (which would orphan the persisted blob) nor flip the stored fields away from the
// snapshot's backend — blank snapshot columns mean "unchanged".
func TestRedeliveredPreRegisterDoesNotOrphanBlob(t *testing.T) {
	s := New()
	ctx := context.Background()

	// Live snapshot state (a room persisted at least once).
	if err := s.Save(ctx, model.Metadata{
		ID:             "doc",
		ContentType:    model.ContentTypeMemo,
		ContentPointer: "blob-live",
		OwnerRef:       "callout-9",
	}); err != nil {
		t.Fatalf("snapshot Save: %v", err)
	}

	// Redelivered document.created: blank content_pointer.
	if err := s.Save(ctx, model.Metadata{
		ID:          "doc",
		ContentType: model.ContentTypeMemo,
		OwnerRef:    "callout-9",
	}); err != nil {
		t.Fatalf("redelivered pre-register Save: %v", err)
	}

	got, _ := s.Load(ctx, "doc")
	if got.ContentPointer != "blob-live" {
		t.Errorf("redelivered pre-register orphaned the blob: content_pointer = %q, want preserved %q", got.ContentPointer, "blob-live")
	}
}

// TestBlankContentTypePreservesTheStoredOne covers the same blank-means-unchanged
// rule as the content pointer, for the field whose loss is hardest to see.
//
// A redelivered document.created carrying a blank contentType must not overwrite
// the stored one. If it did, the row would say "" and the next open would resolve
// the type from the ?type= handshake instead — which defaults to memo when the
// client omits it. A whiteboard would then materialize a MEMO root: a durable
// wrong-type document that no client can render, produced by a duplicate event
// rather than by anything a user did.
func TestBlankContentTypePreservesTheStoredOne(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Save(ctx, model.Metadata{ID: "doc", ContentType: model.ContentTypeWhiteboard}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := s.Save(ctx, model.Metadata{ID: "doc", OwnerRef: "callout-1"}); err != nil {
		t.Fatalf("redelivered Save: %v", err)
	}
	got, _ := s.Load(ctx, "doc")
	if got.ContentType != model.ContentTypeWhiteboard {
		t.Fatalf("a blank contentType overwrote the stored one: got %q, want preserved %q; the next open would resolve the type from the handshake and could materialize a memo root for a whiteboard", got.ContentType, model.ContentTypeWhiteboard)
	}
}

// TestPartialSavePreservesMultiUserDecision pins nil as "producer omitted the
// additive field", not "erase the last explicit license decision".
func TestPartialSavePreservesMultiUserDecision(t *testing.T) {
	s := New()
	ctx := context.Background()
	singleUser := false
	if err := s.Save(ctx, model.Metadata{
		ID: "doc", ContentType: model.ContentTypeWhiteboard, IsMultiUser: &singleUser,
	}); err != nil {
		t.Fatalf("initial Save: %v", err)
	}
	if err := s.Save(ctx, model.Metadata{ID: "doc", OwnerRef: "callout-1"}); err != nil {
		t.Fatalf("partial Save: %v", err)
	}
	got, err := s.Load(ctx, "doc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.IsMultiUser == nil || *got.IsMultiUser {
		t.Fatalf("partial Save replaced explicit false with %v", got.IsMultiUser)
	}
}
