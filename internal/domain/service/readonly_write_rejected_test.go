package service

import (
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestACleanReadOnlyJoinIsNotToldItsEditWasRejected is the guard against
// manufacturing an error out of the ordinary handshake.
//
// Every y-protocols client answers the server's SyncStep1 with a SyncStep2, and
// for a viewer that reply is normally empty. Treating it as a refused mutation
// meant EVERY supported read-only join was told its update had been rejected —
// and client-web reacts to update-rejected by discarding its generation and
// resyncing, so a plain viewer connecting would surface an error and churn.
//
// A viewer with nothing to send has nothing refused. It is told it is read-only,
// which is the frame that exists for exactly this, and nothing else.
func TestACleanReadOnlyJoinIsNotToldItsEditWasRejected(t *testing.T) {
	const doc model.DocumentID = "readonly-clean-join"
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	waitFor(t, "the read-only capability to be announced", func() bool {
		return hasControlKind(viewer, model.ControlReadOnlyState)
	})

	// DRAIN BEFORE ASSERTING ABSENCE. The handshake Step2 is queued behind the
	// frames above, so checking immediately would pass even if it DID produce a
	// rejection — the assertion would race the thing it is meant to catch. A second
	// join whose reply only returns once the loop has processed everything ahead of
	// it is the barrier this package already uses.
	mgr.mu.Lock()
	room := mgr.rooms[doc]
	mgr.mu.Unlock()
	if room == nil {
		t.Fatal("no live room")
	}
	room.enqueue(command{kind: cmdLeave})
	barrier := newFakeClient(t)
	barrier.join(mgr, doc, model.ContentTypeMemo)

	if hasControlKind(viewer, model.ControlUpdateRejected) {
		t.Fatal("a viewer was told its update was rejected merely for joining; the handshake SyncStep2 is not an edit, and client-web responds to this by resyncing and surfacing an error")
	}
}

// TestAReadOnlySessionIsTOLDItsRealEditWasRefused is the regression for a silent
// permanent divergence, on the case that actually has a producer.
//
// A SyncMessageUpdate from a member that may not write was dropped with NO
// reply. The sender had already applied that struct locally, so its next update
// sat at clock k+1 against a server that never received k — pending forever,
// behind a struct the server refused and never mentioned. The two documents
// could not reconverge and nothing said why.
//
// The concrete producer is an established collaborator downgraded mid-session by
// the inactivity sweep, with edits in flight. It reuses the existing
// update-rejected signal, which clients already handle by dropping their
// generation and resyncing.
//
// SCOPE: this asserts the client is TOLD. It deliberately does NOT assert any
// restoration of write access — reporting a capability and changing one are
// different contracts, and only the first is in scope.
//
// Non-vacuity: delete the rejectedNotWritable branch in handleSync and this fails
// on the wait; make it fire for Step2 as well and the sibling test above fails.
func TestAReadOnlySessionIsTOLDItsRealEditWasRefused(t *testing.T) {
	const doc model.DocumentID = "readonly-write-refused"
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	// A real edit, forwarded as a SyncMessageUpdate — what a browser sends after a
	// mid-session downgrade.
	viewer.insertText("a viewer writes")

	waitFor(t, "the refused edit to be answered", func() bool {
		return hasControlKind(viewer, model.ControlUpdateRejected)
	})
}

// TestARefusedReadOnlyEditNeverReachesAnotherMember pins the substantive half:
// refusing the write means it is not applied and therefore never fans out.
// Asserted on CONTENT, which is the invariant that matters.
func TestARefusedReadOnlyEditNeverReachesAnotherMember(t *testing.T) {
	const doc model.DocumentID = "readonly-write-not-broadcast"
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	bystander := newFakeClient(t)
	bystander.join(mgr, doc, model.ContentTypeMemo)
	bystander.observeUpdates()

	viewer.insertText("a viewer writes")

	waitFor(t, "the refused edit to be answered", func() bool {
		return hasControlKind(viewer, model.ControlUpdateRejected)
	})

	// The bystander is a viewer too, and its clean join must also be quiet.
	if hasControlKind(bystander, model.ControlUpdateRejected) {
		t.Fatal("a bystander that only joined was told an update of its own was rejected")
	}

	var seen string
	bystander.withDoc(func(d *ycrdt.Doc) {
		seen = d.GetXMLFragment(memoRoot).ToString()
	})
	if strings.Contains(seen, "a viewer writes") {
		t.Fatalf("a refused read-only edit was relayed to another member: %q", seen)
	}
}
