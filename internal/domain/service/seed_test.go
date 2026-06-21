package service

import (
	"context"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"

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
	return ycrdt.EncodeStateAsUpdateV2(doc, nil)
}

// whiteboardSeedV2 builds the V2-encoded state of a whiteboard Y.Doc carrying one
// element — the shape the server produces from the initial scene (binding
// populateYDoc, server-side) and delivers on collaboration-fetch for the seed.
func whiteboardSeedV2(t *testing.T, id string, props map[string]interface{}) []byte {
	t.Helper()
	doc := newRoomDoc("seed")
	addElement(doc, id, props)
	return ycrdt.EncodeStateAsUpdateV2(doc, nil)
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
	liveSnap := ycrdt.EncodeStateAsUpdateV2(liveDoc, nil)
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
