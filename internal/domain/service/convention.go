package service

import (
	"errors"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// newRoomDoc constructs the authoritative, garbage-collected Y.Doc that backs a
// room. The collaboration service holds plaintext docs (FR-021); GC is enabled
// (the configurable GC policy is FR-025, refined in the y-crdt fork).
func newRoomDoc(guid string) *ycrdt.Doc {
	return ycrdt.NewDoc(guid, true, ycrdt.DefaultGCFilter, nil, false)
}

// applyConvention materializes the root shared type for a document's content
// type (data-model.md "CRDT document conventions"). The service is otherwise
// content-agnostic — clients own the inner shape — but accessing the root by the
// conventional name and the correct type here makes the root exist with the
// right kind even on a brand-new (never-persisted) doc, so the first client to
// bind sees the expected structure rather than having to create it racily.
//
//   - memo:       root Y.XmlFragment named "default" (ProseMirror/TipTap binding)
//   - whiteboard: root id-keyed Y.Map "elements", plus "files" and "appState"
//     maps (Excalidraw scene)
func applyConvention(doc *ycrdt.Doc, content model.ContentType) {
	switch content {
	case model.ContentTypeMemo:
		// y-prosemirror binds the fragment named "default".
		_ = doc.GetXmlFragment("default")

	case model.ContentTypeWhiteboard:
		// Excalidraw scene roots: elements (id-keyed), files, appState.
		_ = doc.GetMap("elements")
		_ = doc.GetMap("files")
		_ = doc.GetMap("appState")
	}
}

// isNotFound reports whether err is the port's not-found sentinel. Centralized
// so the room never imports errors directly for this single branch.
func isNotFound(err error) bool {
	return errors.Is(err, model.ErrNotFound)
}

// NewMigrationDoc constructs an authoritative GC'd Y.Doc identical to the one a
// live room uses (newRoomDoc), exported for the one-time migration tool
// (internal/migrate) so a migrated snapshot is byte-shape-identical to a
// room-persisted one. Migration runs off the run loop entirely, so there is no
// concurrency to guard here.
func NewMigrationDoc(guid string) *ycrdt.Doc {
	return newRoomDoc(guid)
}

// ApplyMemoConvention materializes the memo root convention (Y.XmlFragment
// "default") on doc — the migration-tool entry point to applyConvention for the
// memo case, so a migrated memo rehydrates with the same root a fresh live room
// would create.
func ApplyMemoConvention(doc *ycrdt.Doc) {
	applyConvention(doc, model.ContentTypeMemo)
}

// ApplyWhiteboardConvention materializes the whiteboard root convention
// (id-keyed "elements" + "files" + "appState" maps) on doc, the migration-tool
// entry point to applyConvention for the whiteboard case.
func ApplyWhiteboardConvention(doc *ycrdt.Doc) {
	applyConvention(doc, model.ContentTypeWhiteboard)
}
