package service

import (
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAReadOnlySessionIsTOLDItsWriteWasRefused is the regression for a silent
// permanent divergence.
//
// A mutating sync from a member that may not write was dropped with NO reply:
// dispatchSync returned {mutating:true, applied:false} and handleSync had no
// branch for it. The sender had already applied that struct to its own document,
// so its next update sat at clock k+1 against a server that never received k —
// pending forever, behind a struct the server refused and never mentioned. The
// two documents could not reconverge, and nothing anywhere said why.
//
// The schema-rejection branch two cases above solves the identical problem by
// sending update-rejected, which the client already handles by dropping its
// generation and resyncing. This uses that same existing signal rather than
// inventing a second recovery mechanism.
//
// SCOPE: this asserts the client is TOLD. It deliberately does NOT assert any
// automatic restoration of write access — reporting a capability and changing one
// are different contracts, and only the first is in scope here.
//
// Non-vacuity: delete the rejectedNotWritable branch in handleSync and this fails
// at the first assertion — the viewer is told nothing, which is the bug.
func TestAReadOnlySessionIsTOLDItsWriteWasRefused(t *testing.T) {
	const doc model.DocumentID = "readonly-write-refused"
	// read=true, write=false: an established, legitimate viewer session.
	authz := &scriptedAuthZ{decide: decideBy(true, false)}
	mgr, _ := admissionManager(t, authz, doc)

	viewer := newFakeClient(t)
	viewer.join(mgr, doc, model.ContentTypeMemo)
	viewer.observeUpdates()

	// The viewer edits its own document and forwards the update, exactly as a
	// browser would after a mid-session downgrade.
	viewer.insertText("a viewer writes")

	waitFor(t, "the refused write to be answered", func() bool {
		return hasControlKind(viewer, model.ControlUpdateRejected)
	})

	if !hasControlKind(viewer, model.ControlUpdateRejected) {
		t.Fatal("a read-only session's write was dropped silently; the sender cannot know to resync and its document diverges permanently")
	}
}

// TestARefusedReadOnlyWriteNeverReachesAnotherMember pins the substantive half:
// refusing the write means the update is not applied and therefore never fans out.
//
// The assertion is on CONTENT, not on control frames. A second read-only client
// legitimately receives its own update-rejected during its handshake (its
// SyncStep2 is itself a refused write), so "did a bystander see an
// update-rejected" cannot distinguish whose rejection it was. Whether the
// bystander's DOCUMENT contains the viewer's text can.
func TestARefusedReadOnlyWriteNeverReachesAnotherMember(t *testing.T) {
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

	waitFor(t, "the refused write to be answered", func() bool {
		return hasControlKind(viewer, model.ControlUpdateRejected)
	})

	var seen string
	bystander.withDoc(func(d *ycrdt.Doc) {
		seen = d.GetXMLFragment(memoRoot).ToString()
	})
	if strings.Contains(seen, "a viewer writes") {
		t.Fatalf("a refused read-only write was relayed to another member: %q", seen)
	}
}
