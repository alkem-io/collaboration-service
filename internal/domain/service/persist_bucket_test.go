package service

import (
	"context"
	"sync"
	"testing"
	"time"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// bucketCapturingBlob wraps an inline BlobStore and records the bucketID handed
// to Put, so a test can assert the room threads the document's OWN storage
// bucket (from the metadata index) into the persist path.
type bucketCapturingBlob struct {
	inner   *blobinline.Store
	mu      sync.Mutex
	buckets []string
}

func (b *bucketCapturingBlob) Put(ctx context.Context, pointer, bucketID string, data []byte) (string, error) {
	b.mu.Lock()
	b.buckets = append(b.buckets, bucketID)
	b.mu.Unlock()
	return b.inner.Put(ctx, pointer, bucketID, data)
}
func (b *bucketCapturingBlob) Get(ctx context.Context, p string) ([]byte, error) {
	return b.inner.Get(ctx, p)
}
func (b *bucketCapturingBlob) Delete(ctx context.Context, p string) error {
	return b.inner.Delete(ctx, p)
}

func (b *bucketCapturingBlob) lastBucket() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buckets) == 0 {
		return ""
	}
	return b.buckets[len(b.buckets)-1]
}

// TestPersistUsesPerDocumentBucketFromMetadata asserts the core of the fix: a
// document whose metadata index carries its own StorageBucketID has every
// snapshot persisted into THAT bucket — the room loads the bucket in
// loadSnapshot and threads it through BlobStore.Put, rather than relying on the
// BlobStore's single configured fallback bucket.
func TestPersistUsesPerDocumentBucketFromMetadata(t *testing.T) {
	const docID = "doc-with-own-bucket"
	const ownBucket = "abcd1234-0000-0000-0000-000000000001"

	meta := metainmem.New()
	blob := &bucketCapturingBlob{inner: blobinline.New()}
	open := authopen.New()

	// Pre-seed the index row the server would return on collaboration-fetch:
	// the document carries its own storage bucket.
	if err := meta.Save(context.Background(), model.Metadata{
		ID:              docID,
		ContentType:     model.ContentTypeMemo,
		StorageBucketID: ownBucket,
		BlobStore:       model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Blob: blob, Auth: open, AuthZ: open}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("hello ")
	waitFor(t, "snapshot persisted", func() bool { return hasControlKind(a, model.ControlSaved) })

	if got := blob.lastBucket(); got != ownBucket {
		t.Fatalf("snapshot persisted into bucket %q, want the document's own %q", got, ownBucket)
	}

	// And the saved metadata row stays truthful about the bucket.
	saved, err := meta.Load(context.Background(), docID)
	if err != nil {
		t.Fatalf("reload metadata: %v", err)
	}
	if saved.StorageBucketID != ownBucket {
		t.Errorf("persisted metadata StorageBucketID = %q, want %q", saved.StorageBucketID, ownBucket)
	}
}
