package service

import (
	"context"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestCloseDeletedEvictsALiveRoomAndDeletesNothing is RED #1 for the owner-delete
// slice, and the one that pins its whole point.
//
// `server` confirms document.deleted BEFORE it starts removing the entity,
// profile, storage bucket and checkpoint blob. This event can therefore arrive
// while the durable state still exists, but ownership remains with `server` and
// collab must not race its cascade with a second delete.
//
// What collab still owes is the LIVE part: tell the connected clients with the
// one stable typed code, and evict the room.
//
// It seeds a real snapshot first, deliberately. Asserting only "the room went
// away" would pass against a store that still deleted; the surviving snapshot is
// what discriminates.
//
// Non-vacuity: restore either durable delete in the teardown path and the two
// survival assertions fail.
func TestCloseDeletedEvictsALiveRoomAndDeletesNothing(t *testing.T) {
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second, // long: only the close releases the room
		SendBuffer:   256,
	})

	// TWO connected clients: the acceptance boundary is room-wide, and a teardown
	// that ended only the first would pass with one.
	a, b := newFakeClient(t), newFakeClient(t)
	a.join(mgr, "close-live", model.ContentTypeMemo)
	b.join(mgr, "close-live", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("doomed ")

	// A real snapshot, so "nothing was deleted" is a claim about something.
	waitFor(t, "snapshot persisted", func() bool {
		_, err := deps.storedState(context.Background(), "close-live")
		return err == nil
	})

	if err := mgr.CloseDeleted(context.Background(), "close-live"); err != nil {
		t.Fatalf("CloseDeleted: %v", err)
	}

	waitFor(t, "room released on the owner-delete close", func() bool { return mgr.RoomCount() == 0 })

	// EVERY member is told, with the one stable typed code, BEFORE its socket
	// closes. The ordering is the property clients depend on: a close without a
	// reason is indistinguishable from a network failure, and the client would
	// reconnect into a document that no longer exists.
	for name, c := range map[string]*fakeClient{"a": a, "b": b} {
		end, toldFirst := c.sessionEnd()
		if end == nil {
			t.Fatalf("client %s was never ended", name)
		}
		if end.Code != model.CodeDocumentDeleted {
			t.Errorf("client %s session end = %q, want %q", name, end.Code, model.CodeDocumentDeleted)
		}
		if !toldFirst {
			t.Errorf("client %s socket closed BEFORE its document-deleted control; a bare close reads as a network failure and the client retries", name)
		}
		if !hasControlCode(c, model.CodeDocumentDeleted) {
			t.Errorf("client %s did not receive the session-end/document-deleted control", name)
		}
	}

	// The discriminating half: collab touched neither.
	if _, err := deps.meta.Load(context.Background(), "close-live"); err != nil {
		t.Fatalf("metadata row was deleted (%v); the row belongs to server", err)
	}
	if _, err := deps.storedState(context.Background(), "close-live"); err != nil {
		t.Fatalf("snapshot was deleted (%v); the blob belongs to file-service and its bucket cascade belongs to server", err)
	}
}

// TestCloseDeletedDoesNotFlushOnTheWayOut is the other half of "deletes nothing",
// and it is a separate failure: not deleting is useless if the teardown persists.
//
// A room torn down for an owner-delete holds in-memory state the owner just
// deleted. Flushing it would write a checkpoint back for a document that no
// longer exists — resurrecting content through the save path rather than failing
// to remove it. The ordinary shutdown teardown (cmdClose) DOES flush, so this is
// a real branch, not a tautology.
//
// Non-vacuity: pass a flush hook to teardown on the cmdCloseDeleted branch and the
// revision below advances.
func TestCloseDeletedDoesNotFlushOnTheWayOut(t *testing.T) {
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: time.Hour, // no debounced save can fire on its own
		IdleTimeout:  time.Hour,
		SendBuffer:   256,
	})

	const doc model.DocumentID = "close-no-flush"
	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("never-persisted ")

	// Nothing has been written yet: the debounce is an hour away.
	if _, err := deps.storedState(context.Background(), string(doc)); err == nil {
		t.Fatal("precondition failed: a snapshot exists before the close, so a flush on teardown would be invisible")
	}

	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("CloseDeleted: %v", err)
	}
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })

	if _, err := deps.storedState(context.Background(), string(doc)); err == nil {
		t.Fatal("the owner-delete teardown flushed; it wrote content back for a document the owner deleted")
	}
}

// TestCloseDeletedIsIdempotentAcrossRedeliveries is RED #3, and it carries the
// cold-path property too. The broker delivers at-least-once and a requeued event is
// redelivered, so the same document.deleted arrives twice — once against a live
// room, and again once there is no room at all, which is exactly the cold case.
//
// Non-vacuity: it drives the second delivery through the same public entry point
// rather than asserting a counter, so a close that panicked or errored on an
// already-closed room fails here.
func TestCloseDeletedIsIdempotentAcrossRedeliveries(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())

	const doc model.DocumentID = "redelivered"
	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)

	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })

	// The redelivery lands on a document with no room left.
	if err := mgr.CloseDeleted(context.Background(), doc); err != nil {
		t.Fatalf("redelivery must be a no-op success, got %v", err)
	}
	if mgr.RoomCount() != 0 {
		t.Fatal("the redelivery materialized a room for a deleted document")
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
