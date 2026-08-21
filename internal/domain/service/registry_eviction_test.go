package service

import (
	"context"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// residentInRegistry reports whether the registry still holds a document.
//
// Expressed through Acquire rather than a lookup because that is the only
// observation the Registry interface offers, and it is the one that matters: a
// resident entry hands the cached document straight back, an evicted one runs the
// open function again.
func residentInRegistry(t *testing.T, reg memory.Registry, id model.DocumentID) bool {
	t.Helper()
	opened := false
	handle, err := reg.Acquire(context.Background(), backend.DocumentID(id), func(context.Context) (*ycrdt.Doc, error) {
		opened = true
		return newRoomDoc(string(id)), nil
	})
	if err != nil {
		t.Fatalf("probing the registry for %s: %v", id, err)
	}
	handle.Release()
	_ = reg.Evict(backend.DocumentID(id)) // leave the registry as we found it
	return !opened
}

// TestReleasedRoomReleasesItsRegistrySlot is the ordinary path: a room that goes
// idle must not leave its document resident.
func TestReleasedRoomReleasesItsRegistrySlot(t *testing.T) {
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, RoomConfig{
		SendBuffer:   64,
		SaveDebounce: 5 * time.Millisecond,
		IdleTimeout:  10 * time.Millisecond, // release promptly once empty
	}, nil, nil)
	// The Manager owns the registry and overwrites Deps.Registry with it, so the
	// entry has to be observed THERE. Inspecting a registry of our own would make
	// this vacuous: it would report a clean registry no room ever touched.
	reg := mgr.registry

	const doc model.DocumentID = "evict-on-idle"
	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("some content ")
	a.session.Leave()

	waitFor(t, "room released after its last member left", func() bool { return mgr.RoomCount() == 0 })

	if residentInRegistry(t, reg, doc) {
		t.Fatal("the document is still resident after its room released; identity must not outlive the room that owned it")
	}
}

// TestRoomTornDownDuringMaterializationReleasesItsRegistrySlot is the path the
// Manager's own bookkeeping cannot cover, and the reason eviction lives in the
// room's teardown rather than in Manager.remove.
//
// newRoom runs OFF the registry lock, so a room can be built and then refused
// before it is ever registered: a shutdown that began during materialization, or
// an owner-delete cascade raising the tombstone in the same window. Such a room
// never enters m.rooms, so remove's `removed` flag is false — an evict guarded by
// it would simply not run. But the room DID acquire a registry handle, so the
// document would outlive every room that ever owned it, with nothing left that
// would ever clean it up.
//
// Constructed directly rather than raced through Manager.acquire: the property is
// about what teardown does with the handle, and racing a cascade against
// materialization would test the scheduler instead.
//
// Non-vacuity: drop the evictFromRegistry call from teardown and this fails —
// the never-registered room's document stays resident forever.
func TestRoomTornDownDuringMaterializationReleasesItsRegistrySlot(t *testing.T) {
	deps := newTestDeps()
	reg := memory.NewRegistry()
	deps.Registry = reg

	const doc model.DocumentID = "aborted-materialization"
	room, err := newRoom(context.Background(), doc, model.ContentTypeMemo,
		deps.Deps, fastConfig(), NopMetrics{}, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	if !residentInRegistry(t, reg, doc) {
		t.Fatal("precondition: a materialized room must hold its document in the registry")
	}

	// The abort: torn down without ever being registered, exactly as acquire does
	// when a shutdown or an owner-delete wins the race.
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	if residentInRegistry(t, reg, doc) {
		t.Fatal("a room torn down before registration left its document resident; nothing else evicts it, so that document is retained for the life of the process")
	}
}
