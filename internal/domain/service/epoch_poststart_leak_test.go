package service

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAnAbandonedAcquisitionDoesNotLeakAMemberlessRoom drives the exact
// interleaving that leaked, deterministically rather than by timing.
//
// acquire has TWO epoch checks. The pre-insert one runs inside the singleflight
// function, before the room is registered. The per-caller one runs AFTER, because
// singleflight collapses concurrent acquisitions and a caller holding a stale
// epoch can be handed a room produced by one holding a fresh epoch.
//
// A delete landing BETWEEN them — anywhere in the window spanning
// metrics.RoomOpened() and startRoom() — leaves the room registered, Active,
// counted and RUNNING, while Join returns errRoomUnavailable and never enqueues
// cmdJoin. The idle timer is armed only by cmdJoin/cmdLeave/cmdMessage, so it was
// never armed at all: the goroutine, the Y.Doc, the registry handle and the hub
// subscription were held for the process lifetime, and the room went on applying
// peer updates and flushing for a document nobody was editing.
//
// The epoch is Manager-wide, so the producer is a delete of ANY document, not of
// this one — which is what makes the window ordinary rather than exotic.
//
// The existing tests do not reach this: manager_branches_test.go bumps the epoch
// BEFORE acquire, so it hits the pre-insert teardown and never registers a room.
//
// Non-vacuity: remove the enqueue(cmdLeave) from the stale branch in acquire and
// this fails — RoomCount stays 1 forever.
func TestAnAbandonedAcquisitionDoesNotLeakAMemberlessRoom(t *testing.T) {
	deps := newTestDeps()
	ctx := context.Background()
	const doc model.DocumentID = "abandoned-acquisition"

	hook := &roomOpenedHook{}
	mgr := NewManager(deps.Deps, fastConfig(), hook, zap.NewNop())
	t.Cleanup(mgr.Close)

	if err := mgr.PreRegister(ctx, model.Metadata{ID: doc, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	// Land the delete INSIDE the window, deterministically. RoomOpened() is called
	// after the room is registered and before startRoom, i.e. exactly between the
	// pre-insert epoch check and the per-caller one — so a metrics double is a
	// precise seam for the interleaving, with no sleeps and no timing assumptions.
	//
	// The delete is for a DIFFERENT document on purpose: deleteEpoch is
	// Manager-wide, so any document's deletion invalidates this caller's captured
	// epoch. That is what makes the window ordinary rather than exotic.
	hook.onRoomOpened = func() {
		if err := mgr.CloseDeleted(ctx, "some-other-document"); err != nil {
			t.Errorf("CloseDeleted: %v", err)
		}
	}

	client := newFakeClient(t)
	_, _, err := mgr.Join(ctx, JoinRequest{ID: doc, Content: model.ContentTypeMemo, Conn: client})
	if err == nil {
		t.Fatal("Join succeeded despite a delete landing mid-acquisition")
	}

	// The refusal is correct. What must not survive it is the room.
	waitFor(t, "the abandoned room to be released", func() bool { return mgr.RoomCount() == 0 })
	if got := mgr.RoomCount(); got != 0 {
		t.Fatalf("rooms after an abandoned acquisition = %d, want 0; a memberless room is holding its goroutine, Y.Doc, registry handle and hub subscription for the process lifetime", got)
	}
}

// roomOpenedHook is a Metrics double whose only purpose is a deterministic seam
// at RoomOpened — the one call site inside the leak window.
type roomOpenedHook struct {
	NopMetrics
	onRoomOpened func()
}

func (h *roomOpenedHook) RoomOpened() {
	if h.onRoomOpened != nil {
		h.onRoomOpened()
	}
}
