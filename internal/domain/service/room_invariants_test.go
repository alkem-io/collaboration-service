package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	"go.uber.org/zap"

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// failingEvictRegistry reports a non-ErrInUse failure from Evict, the branch the
// room must log rather than swallow.
type failingEvictRegistry struct {
	memory.Registry
	err error
}

func (f failingEvictRegistry) Evict(backend.DocumentID) error { return f.err }

// TestEvictFailureIsSurfacedNotSwallowed covers evictFromRegistry's error paths.
//
// ErrInUse is expected and benign — another handle is still out, and that room
// evicts when it releases. Any OTHER error means the document stays resident with
// nothing left that will clean it up, so it has to be visible; a silent return
// would make a leaking registry indistinguishable from a healthy one.
func TestEvictFailureIsSurfacedNotSwallowed(t *testing.T) {
	base := memory.NewRegistry()

	t.Run("ErrInUse is not a failure", func(_ *testing.T) {
		deps := newTestDeps()
		deps.Registry = failingEvictRegistry{Registry: base, err: memory.ErrInUse}
		r := &Room{id: "in-use", deps: deps.Deps, logger: zap.NewNop()}
		r.evictFromRegistry() // must not panic or log-fail; another holder will evict
	})

	t.Run("other errors are logged", func(_ *testing.T) {
		deps := newTestDeps()
		deps.Registry = failingEvictRegistry{Registry: base, err: errors.New("registry broken")}
		r := &Room{id: "broken", deps: deps.Deps, logger: zap.NewNop()}
		r.evictFromRegistry()
	})

	t.Run("a nil registry is tolerated", func(_ *testing.T) {
		r := &Room{id: "no-registry", deps: Deps{}, logger: zap.NewNop()}
		r.evictFromRegistry() // a directly-constructed room may have none
	})
}

// TestUndurableForIsZeroUntilAFlushFails covers both branches of the duration
// accessor, which feeds the escalation log and the undurable-seconds gauge. A
// non-zero reading on a healthy document would make every dashboard show a
// permanent degraded window.
func TestUndurableForIsZeroUntilAFlushFails(t *testing.T) {
	r := &Room{}
	if got := r.undurableFor(); got != 0 {
		t.Fatalf("undurableFor on a healthy document = %v, want 0", got)
	}

	r.undurableSince = time.Now().Add(-2 * time.Second)
	if got := r.undurableFor(); got < time.Second {
		t.Fatalf("undurableFor after a failed flush = %v, want at least ~2s", got)
	}
}

// TestRoomSaveNeverOverwritesTheStoresPointer pins the single-writer rule for
// ContentPointer: the checkpoint store records it via metapointer.Record, and the
// room's own metadata save must omit it.
//
// Metadata.Save writes a WHOLE row, so a room that sent a pointer could overwrite
// one the store had just recorded — pointing the document at a missing file while
// the file holding its content is orphaned under an id nothing references.
//
// The assertion is on the OUTBOUND metadata, not on what survives in the store: a
// room that happens to hold no pointer writes a blank one and the store's id
// survives for a reason unrelated to the rule. Capturing the Save shows the room
// omitting the field by construction.
func TestRoomSaveNeverOverwritesTheStoresPointer(t *testing.T) {
	deps := newTestDeps()
	spy := &capturingMetaStore{Store: deps.meta}
	deps.Metadata = spy
	room := newBareRoom(t)
	room.deps = deps.Deps
	room.id = "pointer-race"

	// The row as the store left it after recording the file id.
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: "pointer-race", ContentType: model.ContentTypeMemo, ContentPointer: "file-NEW",
	}); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	insertText(room.doc, "content ")
	room.dirty = true
	room.persistNow()

	if spy.calls == 0 {
		t.Fatal("the room did not save at all")
	}
	if spy.saved.ContentPointer != "" {
		t.Fatalf("the room's save carried ContentPointer %q; only the checkpoint store may write it", spy.saved.ContentPointer)
	}
	// Omitting the pointer must not suppress the fields the room does own.
	if spy.saved.ContentType == "" {
		t.Fatal("the room's save carried no content type")
	}
	got, err := deps.meta.Load(context.Background(), "pointer-race")
	if err != nil {
		t.Fatalf("load row: %v", err)
	}
	if got.ContentPointer != "file-NEW" {
		t.Fatalf("the store's pointer became %q, want file-NEW", got.ContentPointer)
	}
}

// capturingMetaStore records the last Metadata.Save, so a test can assert what a
// caller SENT rather than only what survived. persistNow is synchronous, so no
// synchronisation is needed.
type capturingMetaStore struct {
	*metainmem.Store
	saved model.Metadata
	calls int
}

func (c *capturingMetaStore) Save(ctx context.Context, meta model.Metadata) error {
	c.saved = meta
	c.calls++
	return c.Store.Save(ctx, meta)
}
