// Package model holds the domain types of the collaboration service: the
// document metadata/index, the persisted snapshot, the live in-memory room,
// and the ephemeral awareness/presence state. These are plain named structs
// with zero infrastructure imports — the inward-pointing core of the hexagon
// that the ports (internal/domain/port) and adapters are defined against.
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

// BlobStoreKind identifies which BlobStore adapter holds a document's encoded
// snapshot. It is persisted in the metadata row so a document can be rehydrated
// from the right backend regardless of the running configuration.
type BlobStoreKind string

const (
	// BlobStoreInline keeps the blob in the main DB; the content pointer is
	// the metadata row key (today's behavior).
	BlobStoreInline BlobStoreKind = "inline"
	// BlobStoreFileService offloads the blob to the existing file-service;
	// the content pointer is a file-service object id.
	BlobStoreFileService BlobStoreKind = "file-service"

	// There is deliberately no s3 or local kind. Both existed to serve the
	// standalone deployment that constitution v3.0.0 withdrew, their adapters were
	// removed with the BlobStore port, and no code can read a blob from either.
	// Keeping them as accepted wire values would mean taking a metadata row that
	// says "this document's content is in S3", accepting it, and then reading from
	// whichever store this process happens to be configured with — the wrong
	// backend, silently. Rejecting the row is the truthful answer.
)

// DocumentID is the single id namespace shared by memos and whiteboards.
type DocumentID string

// Metadata is the small, queryable index row for a collaboration document,
// owned by the Alkemio server (or the standalone metadata store). It records
// where the blob lives and who owns the document's lifecycle — never the blob
// bytes themselves (those live in the BlobStore behind ContentPointer).
type Metadata struct {
	ID DocumentID
	// ContentType drives the document convention and client binding.
	ContentType ContentType
	// Version is bumped on every persisted snapshot; reserved for a future
	// version timeline (FR-025).
	Version int
	// ContentPointer locates the snapshot inside the blob store (inline row
	// key / file-service object id / S3 key).
	ContentPointer string
	// BlobStore names the adapter that holds the blob for ContentPointer.
	BlobStore BlobStoreKind
	// AuthorizationPolicyID is the Alkemio authorization policy this document
	// is evaluated against (OPEN-1). The authzeval AuthZ adapter passes it to
	// the authorization-evaluation-service; empty in open/standalone mode.
	AuthorizationPolicyID string
	// StorageBucketID is the document's OWN storage bucket (its
	// profile.storageBucket.id, carried on collaboration-fetch). The
	// file-service BlobStore persists each snapshot into this per-document
	// bucket so blobs co-locate with the document's other media rather than
	// piling into one flat platform bucket. Empty in standalone / no-metadata
	// mode, where the BlobStore falls back to its configured bucket.
	StorageBucketID string
	// SeedContent is the document's stored content delivered on
	// collaboration-fetch for the FIRST-OPEN SEED (R4): a freshly-created
	// document has its content persisted by the server but no collaboration
	// snapshot yet (no ContentPointer). When the room materializes such a
	// document, it seeds the Y.Doc from these bytes so the first opener — or a
	// teammate — sees the creation content instead of an empty editor (FR-003).
	// It is a full Yjs-V2 state for both document types (memo: the rich-text
	// snapshot; whiteboard: the scene snapshot the binding produced server-side),
	// applied via ApplyUpdateV2. It is consulted ONLY when there is no live
	// snapshot — once the room persists, the snapshot blob (ContentPointer) is
	// the source of truth and the seed is ignored. Empty for documents that
	// already have a snapshot, are empty-on-create, or in standalone/no-metadata
	// mode.
	SeedContent []byte
	// OwnerRef is the parent Alkemio entity that owns this document's
	// lifecycle; the delete cascade keys off it (FR-023).
	OwnerRef  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Snapshot is the encoded full Y.Doc state for a document. Live edits travel
// the wire as y-protocols v1; the durable snapshot is encoded v2 (v1 remains
// readable). Written debounced/throttled per room (R7); latest-only today.
type Snapshot struct {
	ID DocumentID
	// Version matches the Metadata.Version the bytes were persisted at.
	Version int
	// Data is the encoded Y.Doc state (v2) handed to BlobStore.Put.
	Data []byte
}
