package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestDeleteTombstoneBridgesConfirmedPublishToOwnerRemoval pins the simplified
// lifecycle ordering: the event is confirmed before server starts deleting, so
// the metadata row can legitimately still exist when collaboration-service
// receives it. The temporary tombstone must refuse that otherwise-valid join;
// after expiry the authoritative metadata row decides again.
func TestDeleteTombstoneBridgesConfirmedPublishToOwnerRemoval(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const doc model.DocumentID = "delete-in-progress"
	if err := mgr.PreRegister(context.Background(), model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo,
	}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}
	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("pre-delete eviction: %v", err)
	}

	client := newFakeClient(t)
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: client.identity, Conn: client,
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("join during delete tombstone = %v, want ErrForbidden", err)
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("tombstoned join materialized %d room(s)", mgr.RoomCount())
	}

	// Drive expiry deterministically instead of waiting five minutes.
	mgr.mu.Lock()
	mgr.deleteTombstones[doc] = time.Now().Add(-time.Second)
	mgr.mu.Unlock()
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: client.identity, Conn: client,
	}); err != nil {
		t.Fatalf("join after tombstone expiry: %v", err)
	}
}

func TestDuplicateDeleteRenewsTheTombstone(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const doc model.DocumentID = "duplicate-delete"

	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	mgr.mu.Lock()
	first := mgr.deleteTombstones[doc]
	mgr.mu.Unlock()

	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("duplicate delete: %v", err)
	}
	mgr.mu.Lock()
	second := mgr.deleteTombstones[doc]
	mgr.mu.Unlock()

	if !second.After(first) {
		t.Fatalf("duplicate did not renew tombstone: first=%s second=%s", first, second)
	}
}

// TestAnUnrelatedDeleteCostsOneRetryNotAPermanentRefusal pins the deliberate
// trade the delete epoch makes.
//
// The epoch is Manager-wide, so deleting ANY document invalidates every admission
// already in flight — including for documents nobody touched. That complements
// the per-id tombstone, which cannot reach a join that crossed its check before
// the event arrived. What must never happen is the price becoming permanent: the
// client reconnects, captures the new epoch, and gets in.
//
// Non-vacuity: make the epoch monotonically compared (>= instead of !=) and the
// first Join below stops being refused, collapsing the guarantee this replaced.
func TestAnUnrelatedDeleteCostsOneRetryNotAPermanentRefusal(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const doc model.DocumentID = "innocent-bystander"

	// The document EXISTS, so the only thing that can refuse the first join is the
	// epoch. Without this it would be refused for not existing and the assertion
	// would pass vacuously.
	if err := mgr.PreRegister(context.Background(), model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	// A join that captured its epoch, then a delete of a DIFFERENT document lands
	// before it acquires. Driven by holding the captured epoch explicitly, because
	// the real interleaving is a nanosecond wide.
	mgr.mu.Lock()
	stale := mgr.deleteEpoch
	mgr.mu.Unlock()
	if err := mgr.CloseDeleted(context.Background(), "some-other-document"); err != nil {
		t.Fatalf("CloseDeleted of an unrelated document: %v", err)
	}
	if _, err := mgr.acquire(context.Background(), doc, model.ContentTypeMemo, stale); !errors.Is(err, errRoomUnavailable) {
		t.Fatalf("acquire on a stale epoch = %v, want errRoomUnavailable", err)
	}
	if mgr.RoomCount() != 0 {
		t.Fatalf("a refused acquire left %d room(s) registered", mgr.RoomCount())
	}

	// The retry: a fresh Join captures the current epoch and is admitted.
	a := newFakeClient(t)
	if _, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo, Identity: a.identity, Conn: a,
	}); err != nil {
		t.Fatalf("join after the unrelated delete: %v; the refusal must be transient, not permanent", err)
	}
}

// TestAStaleEpochIsRefusedEvenWhenAWarmRoomIsRegistered discriminates the
// PER-CALLER epoch check that runs after singleflight returns.
//
// A warm room is handed straight back from the registry, so this shape never
// materializes and never reaches the pre-insert check. The post-singleflight check
// is the only thing between an existing room and a stale admission.
//
// Non-vacuity: remove ONLY that check and this fails while the materialization
// test still passes.
func TestAStaleEpochIsRefusedEvenWhenAWarmRoomIsRegistered(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const doc model.DocumentID = "warm-room"

	// A live, registered room, so acquire returns it without materializing.
	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	if mgr.RoomCount() != 1 {
		t.Fatalf("precondition: %d rooms, want 1 registered so acquire returns it without materializing", mgr.RoomCount())
	}

	mgr.mu.Lock()
	stale := mgr.deleteEpoch
	mgr.mu.Unlock()

	// Some other document is deleted. The warm room is untouched and must stay
	// untouched — but an admission holding the pre-delete epoch is stale.
	if err := mgr.CloseDeleted(context.Background(), "an-unrelated-document"); err != nil {
		t.Fatalf("CloseDeleted: %v", err)
	}

	if _, err := mgr.acquire(context.Background(), doc, model.ContentTypeMemo, stale); !errors.Is(err, errRoomUnavailable) {
		t.Fatalf("acquire on a stale epoch with a warm room = %v, want errRoomUnavailable", err)
	}
	// The unrelated delete must not have closed the warm room.
	if mgr.RoomCount() != 1 {
		t.Fatalf("%d rooms after an unrelated delete, want the warm room still live; a global epoch must invalidate ADMISSIONS, never existing sessions", mgr.RoomCount())
	}
}

// TestAcquireRefusesOnceShutdownHasBegun covers the shutdown check inside acquire:
// once Close has taken its drain snapshot, no further room may be materialized, or
// it would never be drained and its edits would be lost.
//
// The shutdown-DURING-materialization branch is a different one and is pinned by
// TestAShutdownStartingDuringMaterializationLeavesNoLiveRoom.
func TestAcquireRefusesOnceShutdownHasBegun(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	mgr.Close()

	if _, err := mgr.acquire(context.Background(), "after-close", model.ContentTypeMemo, 0); err == nil {
		t.Fatal("acquire must refuse once shutdown has begun")
	}
}

// TestAcquireReturnsTheLiveRoomToASecondCaller covers the registry-hit arm inside
// the singleflight, which is what makes concurrent first-connects share one room
// rather than materialize several.
func TestAcquireReturnsTheLiveRoomToASecondCaller(t *testing.T) {
	mgr, _ := testManager(t, RoomConfig{
		SendBuffer: 64, SaveDebounce: time.Hour, IdleTimeout: time.Hour,
	})

	first, err := mgr.acquire(context.Background(), "shared", model.ContentTypeMemo, 0)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	t.Cleanup(releaseRoom(first))

	second, err := mgr.acquire(context.Background(), "shared", model.ContentTypeMemo, 0)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second != first {
		t.Fatal("a second acquire materialized a DIFFERENT room for the same document; two rooms would hold two live copies of one document and diverge")
	}
}
