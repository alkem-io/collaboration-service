package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestRoomWithNoStoredStateOpensEmptyAndEditable guards the empty-creation edge
// (FR-010): a document with no checkpoint opens empty and editable, with no
// error. file-service holds a document's only content, reached through its
// contentPointer, so a document with no pointer has no content by definition —
// materialization must not fabricate any.
func TestRoomWithNoStoredStateOpensEmptyAndEditable(t *testing.T) {
	t.Parallel()
	const docID = "empty-fresh"

	meta := metainmem.New()
	blob := persistinprocess.New()
	open := authopen.New()

	if err := meta.Save(context.Background(), model.Metadata{
		ID:          docID,
		ContentType: model.ContentTypeMemo,
		// no ContentPointer: nothing stored.
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Checkpoint: blob, Auth: open, AuthZ: open}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	c := newFakeClient(t)
	c.join(mgr, docID, model.ContentTypeMemo)
	c.observeUpdates()

	if got := c.text(); got != "" {
		t.Fatalf("empty fresh document opened with content %q, want empty", got)
	}

	// And it is editable: an edit persists and the room is healthy (no save-error).
	c.insertText("typed ")
	waitFor(t, "edit persisted", func() bool { return hasControlKind(c, model.ControlSaved) })
	if hasControlKind(c, model.ControlSaveError) {
		t.Fatal("fresh empty document emitted a save-error")
	}
}

// TestConventionUsesPersistedTypeOverStaleHandshake is the regression for the
// stale-handshake convention bug: a WHITEBOARD pre-registered (document.created /
// HTTP-create) with NO snapshot and NO seed, opened by a client that omits ?type=
// (which the WS adapter defaults to MEMO), must still materialize the WHITEBOARD
// convention roots — because loadSnapshot corrects r.content to the persisted
// meta.ContentType (the persisted type wins per the documented contract) and
// applyConvention now seeds off r.content, not the stale handshake parameter.
//
// Fail-before/pass-after: with the previous applyConvention(doc, content) the room
// seeded the MEMO root (a spurious Y.XmlFragment "default") instead of the
// whiteboard roots (elements/files/appState) — a durable wrong-type root that
// defeats applyConvention's anti-race guarantee. We inspect doc.Share directly
// (NOT GetMap/GetXmlFragment, which are get-or-create and would mask the bug) so
// the assertion sees only the roots the convention actually materialized.
func TestConventionUsesPersistedTypeOverStaleHandshake(t *testing.T) {
	t.Parallel()
	const docID = "wb-no-type"

	deps := newTestDeps()
	// Pre-register the document as a WHITEBOARD with no ContentPointer and no seed —
	// the "created, never opened in a session" state.
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID:          docID,
		ContentType: model.ContentTypeWhiteboard,
	}); err != nil {
		t.Fatalf("pre-register whiteboard: %v", err)
	}

	// Build the room with the STALE handshake type the WS adapter yields for an
	// omitted ?type= (memo). loadSnapshot must correct r.content to whiteboard, and
	// applyConvention must seed the whiteboard roots off the corrected type.
	room, err := newRoom(context.Background(), docID, model.ContentTypeMemo, deps.Deps, DefaultRoomConfig(), nil, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	t.Cleanup(room.finish)

	if room.content != model.ContentTypeWhiteboard {
		t.Fatalf("room.content = %q, want whiteboard (persisted type must win over the stale handshake)", room.content)
	}
	// The whiteboard convention roots exist; the memo "default" fragment does NOT.
	for _, root := range []string{"elements", "files", "appState"} {
		if !room.doc.ToJson().Has(root) {
			t.Fatalf("whiteboard root %q was not materialized; roots are %v", root, shareKeys(room.doc))
		}
	}
	if room.doc.ToJson().Has("default") {
		t.Fatalf("the stale memo convention root \"default\" was materialized for a whiteboard doc; roots are %v", shareKeys(room.doc))
	}
}

// shareKeys lists the root share names materialized on a doc, for assertion
// messages.
// Doc.Share is unexported in go-yjs; ToJson ranges over EVERY share root
// (including ones materialized but still empty), so its keys are an exact
// exported equivalent of the old Share key set — the assertion is preserved,
// not weakened (FR-018a).
func shareKeys(doc *ycrdt.Doc) []string {
	return doc.ToJson().Keys()
}

// corruptLoadStore reports ErrCorrupt: the index says this document HAS state and
// the state could not be produced.
type corruptLoadStore struct{ *persistinprocess.Store }

func (corruptLoadStore) LoadCheckpoint(context.Context, backend.DocumentID) (persistence.Checkpoint, error) {
	return persistence.Checkpoint{}, fmt.Errorf("%w: file behind the pointer is missing", persistence.ErrCorrupt)
}

// TestCorruptStoredStateFailsOpenAndNeverOpensEmpty is the data-loss guard that
// matters most now that the first-open seed is gone.
//
// With the seed removed, ErrNotFound legitimately opens an EMPTY editable
// document. ErrCorrupt must NOT: it means the index says state exists and it
// could not be read. If the two were conflated, a document whose blob is
// temporarily unreachable would open empty, the room would look normal to the
// first client, and the next save would write that empty document over the last
// good state — destroying content through an error type rather than a bug.
//
// So materialization must FAIL and the room must not be handed out. A refused
// join is recoverable; a silently emptied document is not.
func TestCorruptStoredStateFailsOpenAndNeverOpensEmpty(t *testing.T) {
	deps := newTestDeps()
	deps.Checkpoint = corruptLoadStore{Store: persistinprocess.New()}
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: "corrupt-doc", ContentType: model.ContentTypeMemo, ContentPointer: "ptr",
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	room, err := newRoom(context.Background(), "corrupt-doc", model.ContentTypeMemo,
		deps.Deps, fastConfig(), NopMetrics{}, zap.NewNop())
	if err == nil {
		room.finish()
		t.Fatal("materialization succeeded against unreadable stored state; the room would serve an EMPTY document and the next save would overwrite the last good state")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Fatalf("materialization must fail with ErrCorrupt, got %v", err)
	}
}
