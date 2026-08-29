package service

import (
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestACleanReadOnlyJoinStaysConnected is the guard against manufacturing an
// error out of the ordinary handshake.
//
// Every y-protocols client answers the server's SyncStep1 with a SyncStep2, and
// for a viewer that reply is normally empty. Treating it as a refused mutation
// meant EVERY supported read-only join was told its update had been rejected —
// and client-web reacts to update-rejected by discarding its generation and
// resyncing, so a plain viewer connecting would surface an error and churn.
//
// A viewer with nothing to send has nothing refused. It is told it is read-only,
// which is the frame that exists for exactly this, and nothing else.
func TestACleanReadOnlyJoinStaysConnected(t *testing.T) {
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

	if _, ended := viewer.sessionEnd(); ended {
		t.Fatal("a viewer session ended merely for completing its read-only handshake")
	}
}

// TestAReadOnlyWriteEndsTheOffendingSession prevents silent divergence. Viewer
// capability is immutable for one admission; a real mutation is a protocol fault.
func TestAReadOnlyWriteEndsTheOffendingSession(t *testing.T) {
	const doc model.DocumentID = "readonly-write-refused"
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	// A real edit, forwarded as a SyncMessageUpdate.
	viewer.insertText("a viewer writes")

	waitFor(t, "the refused edit to be answered", func() bool {
		return hasControlCode(viewer, model.CodeForbidden)
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
		return hasControlCode(viewer, model.CodeForbidden)
	})

	if hasControlCode(bystander, model.CodeForbidden) {
		t.Fatal("a bystander was ended for another viewer's refused write")
	}

	var seen string
	bystander.withDoc(func(d *ycrdt.Doc) {
		seen = d.GetXMLFragment(memoRoot).ToString()
	})
	if strings.Contains(seen, "a viewer writes") {
		t.Fatalf("a refused read-only edit was relayed to another member: %q", seen)
	}
}
