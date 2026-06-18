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
