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

	// First snapshot persist: content_pointer/checkpoint_store set, lifecycle fields blank.
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
// to "" (which would orphan the persisted blob) nor flip checkpoint_store away from the
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

	// Redelivered document.created: blank content_pointer + checkpoint_store.
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
