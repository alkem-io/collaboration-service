package service

import (
	"context"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAPoisonedMemoCheckpointFailsMaterialization is the memo counterpart of
// TestAPoisonedCheckpointFailsMaterialization, and it closes a gap that had a
// concrete producer.
//
// Cold-load validation was whiteboard-only. A memo whose STORED state carries an
// inline data: image src — written by the client generation that still had the
// dataURL paste fallback, or migrated from collaborative-document-service —
// therefore loaded cleanly. initShadow then cloned that poison into the shadow,
// and every subsequent client update was rejected against pre-existing poison the
// sender never wrote. client-web responds to update-rejected by discarding its
// generation and reloading server state, which reloads the same poison: the
// document becomes permanently unwritable, and the client loops.
//
// That is verbatim the discard-and-reseed loop the whiteboard branch was written
// to prevent, so the fix is to apply the existing memo validator here, not to
// invent a new rule.
//
// Non-vacuity: restore the `if r.content == model.ContentTypeWhiteboard` guard
// around the cold-load check and this fails — the poisoned memo materializes.
func TestAPoisonedMemoCheckpointFailsMaterialization(t *testing.T) {
	deps := newTestDeps()
	ctx := context.Background()
	const doc model.DocumentID = "poisoned-memo-checkpoint"

	// A stored MEMO carrying an inline image src, as the legacy client produced.
	seed := newRoomDoc(string(doc))
	applyConvention(seed, model.ContentTypeMemo)
	setMemoImage(t, seed, "data:image/png;base64,iVBORw0KGgo=")
	snapshot, err := ycrdt.EncodeStateAsUpdateV2(seed, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := deps.store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(doc), Encoding: persistence.EncodingV2,
		Update: snapshot, StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := deps.meta.Save(ctx, model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(deps.Deps, fastConfig(), nil, zap.NewNop())
	t.Cleanup(mgr.Close)

	client := newFakeClient(t)
	_, _, joinErr := mgr.Join(ctx, JoinRequest{ID: doc, Content: model.ContentTypeMemo, Conn: client})
	if joinErr == nil {
		t.Fatal("a poisoned memo checkpoint materialized a live room; every later update is rejected against poison the client never wrote, and it reloads that poison forever")
	}
}

// TestACleanMemoCheckpointStillMaterializes is the other half, and it is what
// stops the fix above from being implemented as "memos never load".
//
// It also pins the original whiteboard-only justification as no longer
// applicable: validating a memo must NOT materialize a files root on a document
// that should not have one.
func TestACleanMemoCheckpointStillMaterializes(t *testing.T) {
	deps := newTestDeps()
	ctx := context.Background()
	const doc model.DocumentID = "clean-memo-checkpoint"

	seed := newRoomDoc(string(doc))
	applyConvention(seed, model.ContentTypeMemo)
	setMemoImage(t, seed, "01a02f98-ceb1-7cb3-b032-1a9e6ce5e2f9")
	snapshot, err := ycrdt.EncodeStateAsUpdateV2(seed, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := deps.store.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(doc), Encoding: persistence.EncodingV2,
		Update: snapshot, StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	if err := deps.meta.Save(ctx, model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(deps.Deps, fastConfig(), nil, zap.NewNop())
	t.Cleanup(mgr.Close)

	client := newFakeClient(t)
	if _, _, joinErr := mgr.Join(ctx, JoinRequest{ID: doc, Content: model.ContentTypeMemo, Conn: client}); joinErr != nil {
		t.Fatalf("a CLEAN memo checkpoint was refused: %v", joinErr)
	}

	// The validator must not have grown the document a whiteboard-shaped root.
	mgr.mu.Lock()
	room := mgr.rooms[doc]
	mgr.mu.Unlock()
	if room == nil {
		t.Fatal("no live room after a successful join")
	}
	if got := room.doc.GetMap(assetsRoot).GetSize(); got != 0 {
		t.Fatalf("memo grew a %q root with %d entries; validating a memo must not materialize a whiteboard root", assetsRoot, got)
	}
}
