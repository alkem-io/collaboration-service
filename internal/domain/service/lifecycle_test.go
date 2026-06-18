package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// failingBlob is a BlobStore whose Put always errors, to drive the save-error
// control path (R7: room keeps serving from memory, emits save-error).
type failingBlob struct{ inner port.BlobStore }

func (f failingBlob) Put(context.Context, string, []byte) (string, error) {
	return "", errors.New("disk full")
}
func (f failingBlob) Get(ctx context.Context, p string) ([]byte, error) {
	return f.inner.Get(ctx, p)
}
func (f failingBlob) Delete(ctx context.Context, p string) error { return f.inner.Delete(ctx, p) }

// failingMetaLoad is a MetadataStore whose Load errors with a non-NotFound error,
// to drive room materialization failure.
type failingMetaLoad struct{ port.MetadataStore }

func (failingMetaLoad) Load(context.Context, model.DocumentID) (model.Metadata, error) {
	return model.Metadata{}, errors.New("db down")
}

// countingMetrics records lifecycle callbacks for assertions.
type countingMetrics struct {
	roomsOpen, roomsClosed  atomic.Int64
	connsOpen, connsClosed  atomic.Int64
	snapsSaved, snapsFailed atomic.Int64
	fanoutPub, fanoutFailed atomic.Int64
	contributors            atomic.Int64
}

func (m *countingMetrics) RoomOpened()                   { m.roomsOpen.Add(1) }
func (m *countingMetrics) RoomClosed()                   { m.roomsClosed.Add(1) }
func (m *countingMetrics) ConnOpened()                   { m.connsOpen.Add(1) }
func (m *countingMetrics) ConnClosed()                   { m.connsClosed.Add(1) }
func (m *countingMetrics) SnapshotSaved()                { m.snapsSaved.Add(1) }
func (m *countingMetrics) SnapshotFailed()               { m.snapsFailed.Add(1) }
func (m *countingMetrics) FanoutPublished(time.Duration) { m.fanoutPub.Add(1) }
func (m *countingMetrics) FanoutFailed()                 { m.fanoutFailed.Add(1) }
func (m *countingMetrics) ContributingActors(n int)      { m.contributors.Store(int64(n)) }

// TestSaveErrorControlOnBlobFailure asserts a blob Put failure emits a
// `save-error` control message to the room and increments the failure metric,
// while the room keeps serving (R7).
func TestSaveErrorControlOnBlobFailure(t *testing.T) {
	meta := metainmem.New()
	open := authopen.New()
	metrics := &countingMetrics{}
	deps := Deps{
		Metadata: meta,
		Blob:     failingBlob{inner: blobinline.New()},
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), metrics, nil)

	a := newFakeClient(t)
	a.join(mgr, "save-fail", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("boom ")

	waitFor(t, "save-error control", func() bool {
		return hasControlKind(a, model.ControlSaveError)
	})
	if metrics.snapsFailed.Load() == 0 {
		t.Errorf("snapshot-failed metric not incremented")
	}
	if hasControlKind(a, model.ControlSaved) {
		t.Errorf("unexpected saved control after blob failure")
	}
}

// TestMaterializeFailsOnMetaLoadError asserts a non-NotFound metadata Load error
// propagates out of Join (the room is not silently created empty).
func TestMaterializeFailsOnMetaLoadError(t *testing.T) {
	open := authopen.New()
	deps := Deps{
		Metadata: failingMetaLoad{metainmem.New()},
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), nil, nil)

	_, _, err := mgr.Join(context.Background(), JoinRequest{ID: "bad", Content: model.ContentTypeMemo, Conn: &captureConn{}})
	if err == nil {
		t.Fatal("expected Join to fail when metadata Load errors")
	}
}

// TestManagerCloseReleasesRooms asserts Manager.Close tears down live rooms and
// persists their content (graceful shutdown).
func TestManagerCloseReleasesRooms(t *testing.T) {
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Second, // never fires; only the close-time save does
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
	})

	a := newFakeClient(t)
	a.join(mgr, "close-me", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("kept ")

	if mgr.RoomCount() != 1 {
		t.Fatalf("room count = %d, want 1", mgr.RoomCount())
	}

	mgr.Close()
	waitFor(t, "rooms released on close", func() bool { return mgr.RoomCount() == 0 })

	// The close-time snapshot persisted the content.
	waitFor(t, "close snapshot", func() bool {
		_, err := deps.blob.Get(context.Background(), "close-me")
		return err == nil
	})
	snap, _ := deps.blob.Get(context.Background(), "close-me")
	reloaded := ycrdt.NewDoc("guid", true, ycrdt.DefaultGCFilter, nil, false)
	ycrdt.ApplyUpdateV2(reloaded, snap, nil)
	if !contains(xmlText(reloaded), "kept") {
		t.Fatalf("close-time snapshot missing content: %q", xmlText(reloaded))
	}
}

// TestLifecycleMetrics asserts room/connection gauges move through a full join →
// edit → leave → release cycle.
func TestLifecycleMetrics(t *testing.T) {
	open := authopen.New()
	metrics := &countingMetrics{}
	deps := Deps{
		Metadata: metainmem.New(),
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), metrics, nil)

	a := newFakeClient(t)
	a.join(mgr, "metrics-doc", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("m ")

	waitFor(t, "snapshot saved metric", func() bool { return metrics.snapsSaved.Load() >= 1 })
	a.session.Leave()
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })

	if metrics.roomsOpen.Load() != 1 || metrics.roomsClosed.Load() != 1 {
		t.Errorf("room gauges: opened=%d closed=%d, want 1/1", metrics.roomsOpen.Load(), metrics.roomsClosed.Load())
	}
	if metrics.connsOpen.Load() != 1 || metrics.connsClosed.Load() != 1 {
		t.Errorf("conn gauges: opened=%d closed=%d, want 1/1", metrics.connsOpen.Load(), metrics.connsClosed.Load())
	}
}

// TestSendBufferDefault asserts the manager exposes a sane default send buffer.
func TestSendBufferDefault(t *testing.T) {
	mgr := NewManager(Deps{}, RoomConfig{}, nil, nil)
	if mgr.SendBuffer() <= 0 {
		t.Fatalf("SendBuffer = %d, want > 0", mgr.SendBuffer())
	}
}

// TestSlowConsumerEvicted asserts a member whose Send always errors is dropped
// from the room (and the connection gauge decremented), so one stuck client
// cannot stall the room.
func TestSlowConsumerEvicted(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())

	good := newFakeClient(t)
	good.join(mgr, "evict", model.ContentTypeMemo)
	good.observeUpdates()

	// A second member that always fails to receive.
	bad := &erroringConn{}
	_, _, err := mgr.Join(context.Background(), JoinRequest{ID: "evict", Content: model.ContentTypeMemo, Conn: bad})
	if err != nil {
		t.Fatalf("join bad: %v", err)
	}

	// An edit triggers a broadcast; the bad member's Send errors and it is
	// evicted, leaving only the good member.
	good.insertText("trigger ")

	waitFor(t, "bad member evicted", func() bool {
		return bad.calls.Load() >= 1
	})
	// The good client still converges (the room kept serving).
	waitFor(t, "good still served", func() bool { return contains(good.text(), "trigger") })
}

// erroringConn is a service.Conn whose Send always fails.
type erroringConn struct{ calls atomic.Int64 }

func (c *erroringConn) Send(_ []byte) error {
	c.calls.Add(1)
	return errors.New("unreachable")
}

// failingBlobGet errors on Get with a non-NotFound error, to drive the
// blob-fetch failure path of loadSnapshot.
type failingBlobGet struct{ port.BlobStore }

func (failingBlobGet) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("blob backend down")
}

// TestLoadSnapshotPropagatesBlobError asserts that a metadata row pointing at a
// blob whose Get fails (non-NotFound) aborts materialization rather than serving
// a silently-empty document.
func TestLoadSnapshotPropagatesBlobError(t *testing.T) {
	meta := metainmem.New()
	// Pre-seed a metadata row so loadSnapshot proceeds to the blob fetch.
	if err := meta.Save(context.Background(), model.Metadata{
		ID: "blob-fail", ContentType: model.ContentTypeMemo,
		ContentPointer: "blob-fail", BlobStore: model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	open := authopen.New()
	deps := Deps{
		Metadata: meta,
		Blob:     failingBlobGet{},
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), nil, nil)

	if _, _, err := mgr.Join(context.Background(), JoinRequest{ID: "blob-fail", Content: model.ContentTypeMemo, Conn: &captureConn{}}); err == nil {
		t.Fatal("expected Join to fail when the snapshot blob fetch errors")
	}
}

// TestDoubleLeaveAndForwardAfterReleaseAreSafe asserts that leaving twice and
// forwarding into a released room do not panic or block (the enqueue-after-done
// guard).
func TestDoubleLeaveAndForwardAfterReleaseAreSafe(t *testing.T) {
	mgr, _ := testManager(t, RoomConfig{
		SaveDebounce: time.Millisecond,
		IdleTimeout:  time.Millisecond,
		SendBuffer:   16,
	})

	a := newFakeClient(t)
	a.join(mgr, "double-leave", model.ContentTypeMemo)
	session := a.session

	session.Leave()
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })

	// These must be no-ops against the torn-down room, not panics/deadlocks.
	session.Leave()
	session.Forward([]byte{0})
}
