//go:build integration

// Integration test for the Postgres metadata store against a real Postgres.
// Run with: go test -tags=integration ./...
//
// Required env (a local Postgres works):
//
//	POSTGRES_TEST_DSN=postgres://user:pass@localhost:5432/collab_test?sslmode=disable
package postgres

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestPostgresRoundTrip(t *testing.T) {
	dsn := os.Getenv("POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_TEST_DSN not set")
	}
	if err := Migrate(dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	ctx := context.Background()
	store, pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	const id = model.DocumentID("integration-doc")
	_ = store.Delete(ctx, id) // clean slate

	// Absent → ErrNotFound.
	if _, err := store.Load(ctx, id); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("Load(absent) = %v, want ErrNotFound", err)
	}

	// First save → version 1.
	meta := model.Metadata{
		ID:                    id,
		ContentType:           model.ContentTypeWhiteboard,
		ContentPointer:        "ptr-1",
		BlobStore:             model.BlobStoreFileService,
		AuthorizationPolicyID: "pol-1",
		OwnerRef:              "owner-1",
	}
	if err := store.Save(ctx, meta); err != nil {
		t.Fatalf("Save v1: %v", err)
	}
	got, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load v1: %v", err)
	}
	if got.Version != 1 || got.ContentPointer != "ptr-1" ||
		got.BlobStore != model.BlobStoreFileService || got.AuthorizationPolicyID != "pol-1" {
		t.Fatalf("v1 row = %+v", got)
	}

	// Second save → version bumps to 2, fields update.
	meta.ContentPointer = "ptr-2"
	if err := store.Save(ctx, meta); err != nil {
		t.Fatalf("Save v2: %v", err)
	}
	got, _ = store.Load(ctx, id)
	if got.Version != 2 || got.ContentPointer != "ptr-2" {
		t.Fatalf("v2 row = %+v", got)
	}

	// Snapshot-style save (Room.persist rebuilds Metadata from room state and
	// carries no OwnerRef): a blank owner_ref/authorization_policy_id must be
	// treated as "unchanged" and PRESERVE the pre-registered lifecycle metadata,
	// not wipe it — otherwise the delete cascade loses its owner_ref key (FR-023).
	snapshotSave := model.Metadata{
		ID:             id,
		ContentType:    model.ContentTypeWhiteboard,
		ContentPointer: "ptr-3",
		BlobStore:      model.BlobStoreFileService,
		// OwnerRef + AuthorizationPolicyID intentionally empty (snapshot path).
	}
	if err := store.Save(ctx, snapshotSave); err != nil {
		t.Fatalf("Save snapshot: %v", err)
	}
	got, _ = store.Load(ctx, id)
	if got.Version != 3 || got.ContentPointer != "ptr-3" {
		t.Fatalf("snapshot row = %+v", got)
	}
	if got.OwnerRef != "owner-1" {
		t.Fatalf("snapshot save wiped owner_ref: got %q, want preserved %q", got.OwnerRef, "owner-1")
	}
	if got.AuthorizationPolicyID != "pol-1" {
		t.Fatalf("snapshot save wiped authorization_policy_id: got %q, want preserved %q", got.AuthorizationPolicyID, "pol-1")
	}

	// Delete → idempotent.
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete (idempotent): %v", err)
	}
	if _, err := store.Load(ctx, id); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("Load after Delete = %v, want ErrNotFound", err)
	}
}
