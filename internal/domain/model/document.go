// Package model holds the domain types of the collaboration service: the
// document metadata/index, the document content type and its conventions, the
// participant's collaborator mode, and the server→client control messages.
// These are plain named structs with zero infrastructure imports — the
// inward-pointing core of the hexagon that the ports (internal/domain/port) and
// adapters are defined against.
//
// The live room and its awareness state are NOT here: they are service.Room and
// the core's ycrdt.Awareness, which own behaviour rather than shape. Durable
// document bytes are persistence.Checkpoint, owned by the core's contract.
//
// Shapes follow specs/003-unify-collab-yjs/data-model.md.
package model

import "time"

// ContentType selects the CRDT document convention (and therefore the client
// binding) for a document. It is metadata, never baked into the document id —
// a single id namespace spans both memo and whiteboard documents.
type ContentType string

const (
	// ContentTypeMemo is rich text: the Y.Doc root is a Y.XmlFragment named
	// "default", bound to ProseMirror/TipTap on the client.
	ContentTypeMemo ContentType = "memo"
	// ContentTypeWhiteboard is an Excalidraw scene: the Y.Doc root holds an
	// id-keyed Y.Map of per-element Y.Maps, giving per-property concurrent
	// merge.
	ContentTypeWhiteboard ContentType = "whiteboard"
)

// DocumentID is the single id namespace shared by memos and whiteboards.
type DocumentID string

// Metadata is the small, queryable index row for a collaboration document,
// owned by the Alkemio server (or the in-process index in tests/local). It records
// where the blob lives and who owns the document's lifecycle — never the blob
// bytes themselves (those live in the checkpoint store behind ContentPointer).
type Metadata struct {
	ID DocumentID
	// ContentType drives the document convention and client binding.
	ContentType ContentType
	// Version is bumped on every persisted snapshot; reserved for a future
	// version timeline (FR-025).
	Version int
	// ContentPointer locates the snapshot inside the blob store (inline row
	// key / file-service object id).
	ContentPointer string
	// AuthorizationPolicyID is the Alkemio authorization policy this document
	// is evaluated against (OPEN-1). The authzeval AuthZ adapter passes it to
	// the authorization-evaluation-service; empty in open mode.
	AuthorizationPolicyID string
	// StorageBucketID is the document's OWN storage bucket (its
	// profile.storageBucket.id, carried on collaboration-fetch). The
	// file-service checkpoint store persists each snapshot into this per-document
	// bucket so blobs co-locate with the document's other media rather than
	// piling into one flat platform bucket. If it is empty that store REFUSES the
	// first save — there is no configured fallback bucket to divert to.
	StorageBucketID string
	// OwnerRef is the parent Alkemio entity that owns this document's
	// lifecycle; the delete cascade keys off it (FR-023).
	OwnerRef  string
	CreatedAt time.Time
	UpdatedAt time.Time
}
