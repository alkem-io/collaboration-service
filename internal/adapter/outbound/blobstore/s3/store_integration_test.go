//go:build integration

// Integration test for the S3 blob store against a real S3-compatible endpoint
// (MinIO/localstack). Run with: go test -tags=integration ./...
//
// Required env (a local MinIO works):
//
//	S3_TEST_ENDPOINT=http://localhost:9000
//	S3_TEST_BUCKET=collab-snapshots   (must already exist)
//	S3_TEST_ACCESS_KEY=minioadmin
//	S3_TEST_SECRET_KEY=minioadmin
//	S3_TEST_REGION=us-east-1
package s3

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestS3RoundTripAgainstRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	bucket := os.Getenv("S3_TEST_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("S3_TEST_ENDPOINT / S3_TEST_BUCKET not set")
	}

	ctx := context.Background()
	store, err := New(ctx, Config{
		Bucket:          bucket,
		Region:          os.Getenv("S3_TEST_REGION"),
		Endpoint:        endpoint,
		AccessKeyID:     os.Getenv("S3_TEST_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("S3_TEST_SECRET_KEY"),
		UsePathStyle:    true,
		KeyPrefix:       "collab-test/",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const id = "integration-doc"
	want := []byte("real-s3-snapshot")
	pointer, err := store.Put(ctx, id, want)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, pointer)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %q, want %q", got, want)
	}
	if err := store.Delete(ctx, pointer); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, pointer); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}
