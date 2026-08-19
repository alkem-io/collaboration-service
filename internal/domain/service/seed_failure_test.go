package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	"go.uber.org/zap"

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

// TestRoomDoesNotClobberAPointerTheStoreJustRecorded is the regression for a
// document-destroying interleaving found by independent review.
//
// ContentPointer belongs to the checkpoint store; the rest of the row belongs to
// the room. Metadata.Save writes the WHOLE row, so a stale cached pointer in the
// room overwrites one the store just recorded.
//
// The reachable path: the file behind the pointer vanishes out of band, so
// SaveCheckpoint recreates it under a new id and records that id. If the room
// then writes its cached OLD id back, the document resolves to a file that no
// longer exists — which this adapter reports as ErrCorrupt, so the document will
// not open at all — while the file holding its content is orphaned under an id
// nothing references. Unrecoverable without manual repair.
//
// The room previously refreshed the pointer only on the first save of its
// lifetime, which is precisely the window this misses: recreation happens later.
//
// Non-vacuity: restore the pointerChecked one-shot and this fails with the room
// having written the stale pointer back.
func TestRoomDoesNotClobberAPointerTheStoreJustRecorded(t *testing.T) {
	deps := newTestDeps()
	room := newBareRoom(t)
	room.deps = deps.Deps
	room.id = "pointer-race"
	room.pointer = "file-OLD" // what the room cached earlier

	// The row as the STORE left it after recreating the file.
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: "pointer-race", ContentType: model.ContentTypeMemo, ContentPointer: "file-NEW",
	}); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	insertText(room.doc, "content ")
	room.dirty = true
	room.persistNow()

	got, err := deps.meta.Load(context.Background(), "pointer-race")
	if err != nil {
		t.Fatalf("load row: %v", err)
	}
	if got.ContentPointer != "file-NEW" {
		t.Fatalf("the room wrote pointer %q over the store's %q; the document now resolves to a file that does not exist and the one holding its content is orphaned", got.ContentPointer, "file-NEW")
	}
}
