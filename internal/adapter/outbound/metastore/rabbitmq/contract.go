// Package rabbitmq is the Alkemio-default MetadataStore (port.MetadataStore): it
// rides the Alkemio server's RabbitMQ bus, persisting the small document index
// (NOT the blob) over a request/reply RPC. It speaks the NEW UNIFIED
// collaboration contract that replaces the two legacy dialects
// (collaborative-document-service's collaboration-document-save/-fetch and
// whiteboard-collaboration-service's save/fetch) — OPEN-3.
//
// CROSS-REPO CONTRACT (hand-off to the `server` owner): the consumer side of
// these patterns lives in the Alkemio `server` repo and DOES NOT EXIST YET. This
// adapter publishes to the contract below; `server` must implement the matching
// @MessagePattern / @EventPattern handlers. The wire shape is the NestJS RMQ
// request/reply envelope { pattern, data, id } with AMQP correlationId + replyTo,
// so a NestJS @MessagePattern handler consumes it natively.
//
//	REQUEST/REPLY (publish to the server queue, await a reply on a reply queue):
//	  pattern "collaboration-save"   data SaveData    → reply { success: true } | { error }
//	  pattern "collaboration-fetch"  data FetchData   → reply FetchReply
//	  pattern "collaboration-info"   data InfoData    → reply InfoReply
//	FIRE-AND-FORGET (publish, no reply):
//	  pattern "collaboration-contribution"  data ContributionData
//
// The metadata/blob split (persistence-ports.md) is honoured: SaveData carries
// only the index — contentPointer + blobStore locate the blob in the BlobStore;
// the snapshot bytes NEVER cross this bus.
package rabbitmq

// SaveData is the collaboration-save request payload: the index row only.
// (carries forward nothing of the legacy binaryStateInBase64 / content fields —
// the blob lives in the BlobStore, located by ContentPointer + BlobStore.)
type SaveData struct {
	ID                    string `json:"id"`
	ContentType           string `json:"contentType"`
	Version               int    `json:"version"`
	ContentPointer        string `json:"contentPointer"`
	BlobStore             string `json:"blobStore"`
	AuthorizationPolicyID string `json:"authorizationPolicyId,omitempty"`
	OwnerRef              string `json:"ownerRef,omitempty"`
}

// SaveReply is the collaboration-save reply: success xor an error string
// (mirrors the legacy SaveOutputData { success } | { error } union).
type SaveReply struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// FetchData is the collaboration-fetch request payload: the document id.
type FetchData struct {
	ID string `json:"id"`
}

// FetchReply is the collaboration-fetch reply: the index row (Found=false means
// no such document — the unified, typed replacement for the legacy
// contentBase64|undefined and the memo not_found code).
type FetchReply struct {
	Found                 bool   `json:"found"`
	ContentType           string `json:"contentType,omitempty"`
	Version               int    `json:"version,omitempty"`
	ContentPointer        string `json:"contentPointer,omitempty"`
	BlobStore             string `json:"blobStore,omitempty"`
	AuthorizationPolicyID string `json:"authorizationPolicyId,omitempty"`
	// StorageBucketID is the document's own profile.storageBucket.id (mirrors
	// the server FetchOutputData.storageBucketId). The file-service BlobStore
	// uploads each snapshot into THIS bucket so blobs co-locate with the
	// document rather than a single flat platform bucket. Absent for documents
	// the server cannot resolve a bucket for; the BlobStore then falls back to
	// its configured bucket.
	StorageBucketID string `json:"storageBucketId,omitempty"`
	// Content is the document's stored content for the FIRST-OPEN SEED (R4):
	// when a freshly-created document has no collaboration snapshot yet (no
	// ContentPointer), the server delivers its persisted content here so the room
	// materializes from it on first open instead of opening empty (FR-003). It is
	// a full Yjs-V2 state for both document types (memo: the rich-text snapshot;
	// whiteboard: the scene snapshot the server produced from the initial scene
	// via the binding) — the room applies it via ApplyUpdateV2. Go marshals
	// []byte as base64, so the NestJS server sends/receives this field as a
	// base64 string. Absent once the document has a live snapshot (the blob is
	// then authoritative) and for empty-on-create documents.
	Content  []byte `json:"content,omitempty"`
	OwnerRef string `json:"ownerRef,omitempty"`
	Error    string `json:"error,omitempty"`
}

// DeleteData is the collaboration-delete request payload (the owner-delete
// cascade purges the index row; the blob is purged separately via the BlobStore).
type DeleteData struct {
	ID string `json:"id"`
}

// DeleteReply is the collaboration-delete reply.
type DeleteReply struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// InfoData is the collaboration-info request payload: who is asking about which
// document (carried forward from the legacy info pattern, unified field names —
// actorId, never userId, per constitution §III).
type InfoData struct {
	ActorID string `json:"actorId"`
	ID      string `json:"id"`
}

// InfoReply carries the collaborator-mode inputs forward from BOTH legacy
// dialects: read + update + maxCollaborators (whiteboard's 3 fields), plus the
// optional isMultiUser memos added. maxCollaborators is a pointer so "unset"
// (whiteboard's number|undefined) is distinguishable from zero.
type InfoReply struct {
	Read             bool  `json:"read"`
	Update           bool  `json:"update"`
	MaxCollaborators *int  `json:"maxCollaborators,omitempty"`
	IsMultiUser      *bool `json:"isMultiUser,omitempty"`
}

// ContributionData is the collaboration-contribution event payload: the per-
// window set of contributing actors (carried forward from the legacy
// collaboration-memo-contribution { memoId, users } and contribution
// { whiteboardId, users } — unified under a single id field). Fire-and-forget.
type ContributionData struct {
	ID    string `json:"id"`
	Users []User `json:"users"`
}

// User is a contributing actor id (legacy UserInfo { id }).
type User struct {
	ID string `json:"id"`
}

// Pattern names (NestJS @MessagePattern / @EventPattern strings).
const (
	PatternSave         = "collaboration-save"
	PatternFetch        = "collaboration-fetch"
	PatternDelete       = "collaboration-delete"
	PatternInfo         = "collaboration-info"
	PatternContribution = "collaboration-contribution"
)
