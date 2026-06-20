package service

import (
	"context"
	"errors"
	"testing"
	"time"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// errInjectedBlobDelete is the sentinel a test blob store returns from Delete to
// drive the cascade error path.
var errInjectedBlobDelete = errors.New("blob delete failed")

// TestPurgeDisconnectsAndPurgesLiveRoom asserts the owner-delete cascade on a
// live room: connected clients get room-closed, the room is released, and the
// metadata row + snapshot blob are purged (FR-023, SC-010, T015).
func TestPurgeDisconnectsAndPurgesLiveRoom(t *testing.T) {
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second, // long: only the cascade releases it
		SendBuffer:   256,
	})

	a := newFakeClient(t)
	a.join(mgr, "purge-live", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("doomed ")

	// Wait for a snapshot so there is a blob to purge.
	waitFor(t, "snapshot persisted", func() bool {
		_, err := deps.blob.Get(context.Background(), "purge-live")
		return err == nil
	})

	if err := mgr.Purge(context.Background(), "purge-live"); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// The room is released and the connected client got a room-closed control.
	waitFor(t, "room released on purge", func() bool { return mgr.RoomCount() == 0 })
	if !hasControlKind(a, model.ControlRoomClosed) {
		t.Fatal("connected client did not receive room-closed on purge")
	}

	// Metadata + blob are gone (no orphan).
	if _, err := deps.meta.Load(context.Background(), "purge-live"); err == nil {
		t.Fatal("metadata row not purged")
	}
	if _, err := deps.blob.Get(context.Background(), "purge-live"); err == nil {
		t.Fatal("snapshot blob not purged")
	}
}

// TestPurgeAbsentDocumentIsNoOp asserts deleting a document with no live room and
// no durable rows is a no-op success (idempotency, SC-010).
func TestPurgeAbsentDocumentIsNoOp(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	if err := mgr.Purge(context.Background(), "never-existed"); err != nil {
		t.Fatalf("purge of absent doc should be a no-op: %v", err)
	}
}

// TestPurgeDurableOnlyDocument asserts the cascade purges the durable rows of a
// document that has no live room (persisted earlier, room since released).
func TestPurgeDurableOnlyDocument(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())

	// Seed a metadata row + blob directly (the "persisted, room released" state).
	if _, err := deps.blob.Put(context.Background(), "durable-only", "", []byte("snap")); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: "durable-only", ContentType: model.ContentTypeMemo,
		ContentPointer: "durable-only", BlobStore: model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	if err := mgr.Purge(context.Background(), "durable-only"); err != nil {
		t.Fatalf("purge durable-only: %v", err)
	}
	if _, err := deps.meta.Load(context.Background(), "durable-only"); err == nil {
		t.Fatal("durable metadata not purged")
	}
	if _, err := deps.blob.Get(context.Background(), "durable-only"); err == nil {
		t.Fatal("durable blob not purged")
	}
}

// TestPreRegisterWritesMetadata asserts document.created pre-registers a metadata
// row (the optional create path, T015).
func TestPreRegisterWritesMetadata(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	meta := model.Metadata{
		ID: "pre-reg", ContentType: model.ContentTypeWhiteboard, OwnerRef: "callout-1",
	}
	if err := mgr.PreRegister(context.Background(), meta); err != nil {
		t.Fatalf("pre-register: %v", err)
	}
	got, err := deps.meta.Load(context.Background(), "pre-reg")
	if err != nil {
		t.Fatalf("load pre-registered: %v", err)
	}
	if got.ContentType != model.ContentTypeWhiteboard || got.OwnerRef != "callout-1" {
		t.Fatalf("pre-registered row mismatch: %+v", got)
	}
}

// TestReEvaluateNoLiveRoomIsNoOp asserts re-evaluating a document with no live
// room does not panic or block (document.access_changed for an idle doc).
func TestReEvaluateNoLiveRoomIsNoOp(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	mgr.ReEvaluate("not-live") // must not panic
}

// TestPurgeLiveRoomSurfacesBlobError asserts a blob-delete failure during the
// cascade of a live room propagates out of Purge (so the bus/HTTP caller sees a
// genuine failure, not a false success).
func TestPurgeLiveRoomSurfacesBlobError(t *testing.T) {
	open := authopen.New()
	deps := Deps{
		Metadata: metainmem.New(),
		Blob:     deleteFailingBlob{inner: blobinline.New()},
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "purge-err", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("x ")
	waitFor(t, "snapshot persisted", func() bool { return hasControlKind(a, model.ControlSaved) })

	if err := mgr.Purge(context.Background(), "purge-err"); err == nil {
		t.Fatal("expected purge to surface the blob delete error")
	}
}

// TestPurgeDurableSurfacesMetadataLoadError asserts a metadata Load failure (non
// NotFound) on a durable-only purge propagates out (no silent success).
func TestPurgeDurableSurfacesMetadataLoadError(t *testing.T) {
	open := authopen.New()
	deps := Deps{
		Metadata: failingMetaLoad{metainmem.New()},
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), nil, nil)
	if err := mgr.Purge(context.Background(), "load-err"); err == nil {
		t.Fatal("expected purge to surface the metadata load error")
	}
}

// deleteFailingBlob is a BlobStore whose Delete errors (non-NotFound), to drive
// the cascade error path.
type deleteFailingBlob struct{ inner *blobinline.Store }

func (d deleteFailingBlob) Put(ctx context.Context, p, bucketID string, data []byte) (string, error) {
	return d.inner.Put(ctx, p, bucketID, data)
}
func (d deleteFailingBlob) Get(ctx context.Context, p string) ([]byte, error) {
	return d.inner.Get(ctx, p)
}
func (deleteFailingBlob) Delete(context.Context, string) error {
	return errInjectedBlobDelete
}

// errInjectedMetaDelete is the sentinel a test metadata store returns from Delete
// to drive the cascade metadata-delete error path.
var errInjectedMetaDelete = errors.New("metadata delete failed")

// failingMetaDelete is a MetadataStore whose Delete errors with a non-NotFound
// error, so the purge cascade must surface it (not swallow it as idempotent).
type failingMetaDelete struct{ port.MetadataStore }

func (failingMetaDelete) Delete(context.Context, model.DocumentID) error {
	return errInjectedMetaDelete
}

// TestPurgeLiveRoomSurfacesMetadataDeleteError asserts that a non-NotFound
// metadata delete failure during the live-room cascade propagates out rather
// than being treated as idempotent success (mirrors the blob-delete error path,
// T015; CR presence.go purge idempotency).
func TestPurgeLiveRoomSurfacesMetadataDeleteError(t *testing.T) {
	open := authopen.New()
	deps := Deps{
		Metadata: failingMetaDelete{metainmem.New()},
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "meta-del-err", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("x ")
	waitFor(t, "snapshot persisted", func() bool { return hasControlKind(a, model.ControlSaved) })

	if err := mgr.Purge(context.Background(), "meta-del-err"); err == nil {
		t.Fatal("expected purge to surface the metadata delete error")
	}
}

// TestPurgeDurableSurfacesMetadataDeleteError asserts the no-live-room durable
// purge also surfaces a non-NotFound metadata delete failure (purgeDurable),
// rather than swallowing it.
func TestPurgeDurableSurfacesMetadataDeleteError(t *testing.T) {
	open := authopen.New()
	inner := metainmem.New()
	// Seed a row so purgeDurable's Load succeeds and it proceeds to Delete.
	if err := inner.Save(context.Background(), model.Metadata{ID: "durable-del-err", ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	deps := Deps{
		Metadata: failingMetaDelete{inner},
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), nil, nil)
	if err := mgr.Purge(context.Background(), "durable-del-err"); err == nil {
		t.Fatal("expected durable purge to surface the metadata delete error")
	}
}
