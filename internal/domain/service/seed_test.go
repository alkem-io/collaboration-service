package service

import (
	"context"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// memoSeedV2 builds the V2-encoded state of a memo Y.Doc carrying text — the
// shape the server stores at create time and delivers on collaboration-fetch for
// the first-open seed (memo content is already a Yjs-V2 snapshot, applied
// directly).
func memoSeedV2(t *testing.T, text string) []byte {
	t.Helper()
	doc := newRoomDoc("seed")
	insertText(doc, text)
	snap, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatalf("encode memo seed: %v", err)
	}
	return snap
}

// whiteboardSeedV2 builds the V2-encoded state of a whiteboard Y.Doc carrying one
// element — the shape the server produces from the initial scene (binding
// populateYDoc, server-side) and delivers on collaboration-fetch for the seed.
func whiteboardSeedV2(t *testing.T, id string, props map[string]interface{}) []byte {
	t.Helper()
	doc := newRoomDoc("seed")
	addElement(doc, id, props)
	snap, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if err != nil {
		t.Fatalf("encode whiteboard seed: %v", err)
	}
	return snap
}

// TestRoomSeedsFromStoredContentOnFirstOpen is the US1 regression: a freshly
// created document has a metadata row but NO live snapshot yet (no
// ContentPointer / no blob). collaboration-fetch delivers the document's stored
// content (Metadata.SeedContent); the room MUST materialize from it so the first
// opener (and any teammate) sees the creation content rather than an empty editor
// (FR-003, SC-002). Covers both document types: memo seeds apply the V2 update;
// whiteboard seeds apply the V2 snapshot. Each row also asserts the seed is
// PROMOTED to a real per-document snapshot on the first save (ContentPointer set),
// so subsequent opens load the blob and the one-time seed is not re-applied.
//
// Fail-before/pass-after: before the seed wiring, loadSnapshot returns a blank
// doc for a pointer-less metadata row, so the joined client converges to empty
// content and these assertions fail.
func TestRoomSeedsFromStoredContentOnFirstOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		content    model.ContentType
		seed       []byte
		assertSeen func(t *testing.T, c *fakeClient)
		assertBlob func(t *testing.T, doc *ycrdt.Doc)
	}{
		{
			name:    "memo applies the V2 update",
			content: model.ContentTypeMemo,
			seed:    memoSeedV2(t, "created text "),
			assertSeen: func(t *testing.T, c *fakeClient) {
				t.Helper()
				waitFor(t, "memo seed visible to opener", func() bool {
					return contains(c.text(), "created text")
				})
			},
			assertBlob: func(t *testing.T, doc *ycrdt.Doc) {
				t.Helper()
				if !contains(xmlText(doc), "created text") {
					t.Fatalf("promoted snapshot missing seeded memo text: %q", xmlText(doc))
				}
			},
		},
		{
			name:    "whiteboard applies the V2 snapshot",
			content: model.ContentTypeWhiteboard,
			seed:    whiteboardSeedV2(t, "elem-1", map[string]interface{}{"type": "rectangle"}),
			assertSeen: func(t *testing.T, c *fakeClient) {
				t.Helper()
				waitFor(t, "whiteboard seed visible to opener", func() bool {
					return c.hasElement("elem-1")
				})
			},
			assertBlob: func(t *testing.T, doc *ycrdt.Doc) {
				t.Helper()
				if !hasElement(doc, "elem-1") {
					t.Fatalf("promoted snapshot missing seeded whiteboard element: %v", elementKeys(doc))
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const docID = "seed-doc"

			meta := metainmem.New()
			blob := blobinline.New()
			open := authopen.New()

			// The server pre-registers the document on create with its stored
			// content but NO ContentPointer yet — the snapshot blob is written on
			// the first collaboration save. This is the "created, never opened in a
			// session" state (US1 acceptance #3).
			if err := meta.Save(context.Background(), model.Metadata{
				ID:          docID,
				ContentType: tc.content,
				BlobStore:   model.BlobStoreInline,
				SeedContent: tc.seed,
			}); err != nil {
				t.Fatalf("seed metadata: %v", err)
			}

			mgr := NewManager(Deps{Metadata: meta, Blob: blob, Auth: open, AuthZ: open}, RoomConfig{
				SaveDebounce: 10 * time.Millisecond,
				IdleTimeout:  10 * time.Second,
				SendBuffer:   64,
			}, nil, nil)

			// Open the freshly-created document for the first time.
			c := newFakeClient(t)
			c.join(mgr, docID, tc.content)
			c.observeUpdates()

			// The creation content is present and the document is non-empty on open.
			tc.assertSeen(t, c)

			// First save promotes the seed to a real per-document snapshot: a
			// ContentPointer is written so subsequent opens load the blob.
			waitFor(t, "seed promoted to a persisted snapshot", func() bool {
				m, err := meta.Load(context.Background(), docID)
				return err == nil && m.ContentPointer != ""
			})

			saved, err := meta.Load(context.Background(), docID)
			if err != nil {
				t.Fatalf("reload metadata: %v", err)
			}
			if saved.ContentPointer == "" {
				t.Fatal("first save did not set a ContentPointer (seed not promoted)")
			}

			// The promoted blob carries the seeded content (the seed became the
			// document's first real snapshot, not a transient in-memory state).
			snap, err := blob.Get(context.Background(), saved.ContentPointer)
			if err != nil {
				t.Fatalf("get promoted snapshot: %v", err)
			}
			reloaded := newRoomDoc("verify")
			ycrdt.ApplyUpdateV2(reloaded, snap, nil)
			tc.assertBlob(t, reloaded)
		})
	}
}

// TestRoomDoesNotSeedWhenLiveSnapshotExists guards the precedence rule: when a
// document already has a persisted snapshot (a ContentPointer + blob), the live
// snapshot wins and any stale SeedContent on the metadata row is IGNORED. The seed
// is strictly a first-open bootstrap for a document that has never been saved —
// re-applying it over an evolved snapshot would resurrect deleted/old content.
func TestRoomDoesNotSeedWhenLiveSnapshotExists(t *testing.T) {
	t.Parallel()
	const docID = "already-saved"

	meta := metainmem.New()
	blob := blobinline.New()
	open := authopen.New()

	// The live snapshot says "live content"; the (stale) seed says "stale seed".
	liveDoc := newRoomDoc("live")
	insertText(liveDoc, "live content ")
	liveSnap, err := ycrdt.EncodeStateAsUpdateV2(liveDoc, nil)
	if err != nil {
		t.Fatalf("encode live snapshot: %v", err)
	}
	pointer, err := blob.Put(context.Background(), docID, "", liveSnap)
	if err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	if err := meta.Save(context.Background(), model.Metadata{
		ID:             docID,
		ContentType:    model.ContentTypeMemo,
		ContentPointer: pointer,
		BlobStore:      model.BlobStoreInline,
		Version:        1,
		SeedContent:    memoSeedV2(t, "stale seed "),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Blob: blob, Auth: open, AuthZ: open}, RoomConfig{
		SaveDebounce: 10 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   64,
	}, nil, nil)

	c := newFakeClient(t)
	c.join(mgr, docID, model.ContentTypeMemo)

	waitFor(t, "live snapshot loaded", func() bool { return contains(c.text(), "live content") })
	if contains(c.text(), "stale seed") {
		t.Fatalf("stale seed leaked over the live snapshot: %q", c.text())
	}
}

// TestRoomFreshWithoutSeedStaysEmpty guards the empty-creation edge (FR-010): a
// document with neither a snapshot nor SeedContent opens empty and editable, with
// no error — the seed branch must not fabricate content.
func TestRoomFreshWithoutSeedStaysEmpty(t *testing.T) {
	t.Parallel()
	const docID = "empty-fresh"

	meta := metainmem.New()
	blob := blobinline.New()
	open := authopen.New()

	if err := meta.Save(context.Background(), model.Metadata{
		ID:          docID,
		ContentType: model.ContentTypeMemo,
		BlobStore:   model.BlobStoreInline,
		// no SeedContent, no ContentPointer.
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{Metadata: meta, Blob: blob, Auth: open, AuthZ: open}, RoomConfig{
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
		BlobStore:   model.BlobStoreInline,
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
		if _, ok := room.doc.Share[root]; !ok {
			t.Fatalf("whiteboard root %q was not materialized; doc.Share has %v", root, shareKeys(room.doc))
		}
	}
	if _, ok := room.doc.Share["default"]; ok {
		t.Fatalf("the stale memo convention root \"default\" was materialized for a whiteboard doc; doc.Share has %v", shareKeys(room.doc))
	}
}

// shareKeys lists the root share names materialized on a doc, for assertion
// messages.
func shareKeys(doc *ycrdt.Doc) []string {
	keys := make([]string, 0, len(doc.Share))
	for k := range doc.Share {
		keys = append(keys, k)
	}
	return keys
}
