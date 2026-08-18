package service

import (
	"errors"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// newRoomDoc constructs the authoritative, garbage-collected Y.Doc that backs a
// room. The collaboration service holds plaintext docs (FR-021); GC is enabled
// (the configurable GC policy is FR-025; go-yjs enables GC by default, matching the yjs Doc constructor).
func newRoomDoc(guid string) *ycrdt.Doc {
	return ycrdt.NewDoc(guid)
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
