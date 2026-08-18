package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestConcurrentPurgesOfOneDocumentShareTheTombstone covers the refcount branch
// in endPurge.
//
// The tombstone is a count, not a flag, and the count is what makes concurrent
// cascades safe: if the first Purge to finish deleted the entry outright, it
// would lift the tombstone out from under a second cascade still running, and a
// Join arriving in the remainder of that second cascade would be admitted — the
// resurrection the tombstone exists to prevent.
//
// Non-vacuity: change endPurge to delete unconditionally and the mid-cascade Join
// below is admitted.
func TestConcurrentPurgesOfOneDocumentShareTheTombstone(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const doc model.DocumentID = "double-purge"

	// Two overlapping cascades: raise twice, lower once, and the document must
	// still be refused.
	mgr.beginPurge(doc)
	mgr.beginPurge(doc)
	mgr.endPurge(doc)

	a := newFakeClient(t)
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: a.identity, Conn: a,
	}); err == nil {
		t.Fatal("a Join was admitted while a second cascade was still running; the first cascade to finish must not lift the tombstone for the other")
	}

	// The second cascade finishes and the document becomes joinable again.
	mgr.endPurge(doc)
	b := newFakeClient(t)
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: b.identity, Conn: b,
	}); err != nil {
		t.Fatalf("join after every cascade completed: %v", err)
	}
}

// TestPurgeDurableReportsAStoreThatCannotDelete covers purgeDurable's capability
// branch: a checkpoint store with no Delete cannot complete an owner-delete, and
// the cascade must report that rather than dropping the index row and calling it
// done — which would leave the content behind with nothing pointing at it.
func TestPurgeDurableReportsAStoreThatCannotDelete(t *testing.T) {
	deps := newTestDeps()
	deps.Checkpoint = nonDeletingStore{}
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	if err := mgr.Purge(context.Background(), "no-deleter"); err == nil {
		t.Fatal("a purge against a store that cannot delete must fail; silently dropping the index row would orphan the content")
	}
}

// TestAcquireRefusesOnceShutdownHasBegun covers the closed check on both the fast
// path and the singleflight re-check.
//
// The two are not redundant. Materialization runs OFF the registry lock, so a
// shutdown can begin in that window; without the second check a room would be
// registered after the drain snapshot was taken and would never be drained —
// its unsaved edits lost with no shutdown flush.
func TestAcquireRefusesOnceShutdownHasBegun(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	mgr.Close()

	// Fast path: closed is observed before any materialization.
	if _, err := mgr.acquire(context.Background(), "after-close", model.ContentTypeMemo); err == nil {
		t.Fatal("acquire must refuse once shutdown has begun")
	}

	// Concurrent acquires all refuse, exercising the singleflight arm too.
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = mgr.acquire(context.Background(), "after-close", model.ContentTypeMemo)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Fatalf("concurrent acquire %d was admitted after shutdown", i)
		}
	}
}

// TestAcquireReturnsTheLiveRoomToASecondCaller covers the registry-hit arm inside
// the singleflight, which is what makes concurrent first-connects share one room
// rather than materialize several.
func TestAcquireReturnsTheLiveRoomToASecondCaller(t *testing.T) {
	mgr, _ := testManager(t, RoomConfig{
		SendBuffer: 64, SaveDebounce: time.Hour, IdleTimeout: time.Hour,
	})

	first, err := mgr.acquire(context.Background(), "shared", model.ContentTypeMemo)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	t.Cleanup(first.finish)

	second, err := mgr.acquire(context.Background(), "shared", model.ContentTypeMemo)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second != first {
		t.Fatal("a second acquire materialized a DIFFERENT room for the same document; two rooms would hold two live copies of one document and diverge")
	}
}
