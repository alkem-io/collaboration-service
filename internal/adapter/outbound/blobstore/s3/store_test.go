package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeS3 is an in-memory stand-in for the S3 API subset the store uses. It lets
// the adapter's request shaping and error mapping be unit-tested without a live
// S3/minio. A build-tagged integration test (store_integration_test.go) covers
// the real client against minio/localstack.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	bucket  string
}

func newFakeS3(bucket string) *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, bucket: bucket}
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if *in.Bucket != f.bucket {
		return nil, errors.New("wrong bucket")
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[*in.Key] = body
	f.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	data, ok := f.objects[*in.Key]
	f.mu.Unlock()
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "not found"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	delete(f.objects, *in.Key)
	f.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

func newTestStore(t *testing.T) (*Store, *fakeS3) {
	t.Helper()
	fake := newFakeS3("snapshots")
	return newWithAPI(fake, "snapshots"), fake
}

func TestPutGetRoundTrip(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	want := []byte("v2-snapshot-bytes")
	pointer, err := store.Put(ctx, "doc-1", want)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if pointer != "doc-1" {
		t.Errorf("pointer = %q, want the object key doc-1", pointer)
	}
	got, err := store.Get(ctx, pointer)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("Get = %q, want %q", got, want)
	}
}

func TestPutOverwrites(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	if _, err := store.Put(ctx, "doc", []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := store.Put(ctx, "doc", []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	got, _ := store.Get(ctx, "doc")
	if string(got) != "v2" {
		t.Errorf("Get after overwrite = %q, want v2", got)
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	_, err := store.Get(context.Background(), "absent")
	if !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Get(absent) err = %v, want ErrNotFound", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	store, fake := newTestStore(t)
	ctx := context.Background()
	if _, err := store.Put(ctx, "doc", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, "doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Deleting again is a no-op (S3 DeleteObject is already idempotent).
	if err := store.Delete(ctx, "doc"); err != nil {
		t.Errorf("Delete(absent) = %v, want nil", err)
	}
	fake.mu.Lock()
	_, exists := fake.objects["doc"]
	fake.mu.Unlock()
	if exists {
		t.Error("object still present after Delete")
	}
}

func TestKeyPrefix(t *testing.T) {
	fake := newFakeS3("snapshots")
	store := newWithAPIAndPrefix(fake, "snapshots", "collab/")
	ctx := context.Background()
	pointer, err := store.Put(ctx, "doc-1", []byte("x"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// The pointer the metadata records is the doc-relative key; the stored
	// object lives under the prefix.
	if pointer != "doc-1" {
		t.Errorf("pointer = %q, want doc-1 (prefix is adapter-internal)", pointer)
	}
	fake.mu.Lock()
	_, prefixed := fake.objects["collab/doc-1"]
	fake.mu.Unlock()
	if !prefixed {
		t.Error("object not stored under the configured prefix")
	}
	got, err := store.Get(ctx, pointer)
	if err != nil || string(got) != "x" {
		t.Errorf("Get under prefix = %q, %v", got, err)
	}
}

// failingS3 returns an error from every operation, to drive the adapter's
// error-wrapping paths.
type failingS3 struct{ err error }

func (f failingS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, f.err
}

func (f failingS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, f.err
}

func (f failingS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, f.err
}

func TestPutErrorSurfaces(t *testing.T) {
	store := newWithAPI(failingS3{err: errors.New("access denied")}, "b")
	if _, err := store.Put(context.Background(), "doc", []byte("x")); err == nil {
		t.Error("expected Put to surface the S3 error")
	}
}

func TestGetGenericErrorSurfaces(t *testing.T) {
	store := newWithAPI(failingS3{err: errors.New("throttled")}, "b")
	_, err := store.Get(context.Background(), "doc")
	if err == nil || errors.Is(err, model.ErrNotFound) {
		t.Errorf("expected a non-NotFound error, got %v", err)
	}
}

func TestGetNoSuchKeyTypeIsNotFound(t *testing.T) {
	store := newWithAPI(failingS3{err: &s3types.NoSuchKey{}}, "b")
	if _, err := store.Get(context.Background(), "doc"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("NoSuchKey err = %v, want ErrNotFound", err)
	}
}

func TestDeleteErrorSurfaces(t *testing.T) {
	store := newWithAPI(failingS3{err: errors.New("boom")}, "b")
	if err := store.Delete(context.Background(), "doc"); err == nil {
		t.Error("expected Delete to surface the S3 error")
	}
}

func TestNewValidates(t *testing.T) {
	cases := []Config{
		{Region: "us-east-1"}, // missing bucket
		{Bucket: "b"},         // missing region (and no endpoint)
	}
	for i, c := range cases {
		if _, err := New(context.Background(), c); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}

func TestNewBuildsClientOffline(t *testing.T) {
	// New builds the aws-sdk client without any network I/O (LoadDefaultConfig +
	// NewFromConfig are local); a static-endpoint MinIO-style config exercises the
	// credential + endpoint + path-style options.
	store, err := New(context.Background(), Config{
		Bucket:          "snapshots",
		Region:          "us-east-1",
		Endpoint:        "http://localhost:9000",
		KeyPrefix:       "collab/",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if store.bucket != "snapshots" || store.prefix != "collab/" {
		t.Errorf("store = %+v", store)
	}
}
