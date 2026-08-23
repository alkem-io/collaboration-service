// Package rabbitmq is the Alkemio-default MetadataStore (port.MetadataStore): it
// rides the Alkemio server's RabbitMQ bus, persisting the small document index
// (NOT the blob) over a request/reply RPC. It speaks the NEW UNIFIED
// collaboration contract that replaces the two legacy dialects
// (collaborative-document-service's collaboration-document-save/-fetch and
// whiteboard-collaboration-service's save/fetch) — OPEN-3.
//
// CROSS-REPO CONTRACT: the consumer side lives in the Alkemio `server` repo and
// IS IMPLEMENTED as of the 006 work — see server's
// src/services/collaboration-integration/collaboration-integration.controller.ts
// (@MessagePattern SAVE/FETCH/DELETE/INFO over Transport.RMQ). It reached
// `server` on feat/006-collab-content-unification, so a check against `develop`
// alone will suggest it is missing; it is not. The wire shape is the NestJS RMQ
// request/reply envelope { pattern, data, id } with AMQP correlationId +
// replyTo, so a NestJS @MessagePattern handler consumes it natively.
//
// Changing any payload or pattern below is therefore a BREAKING cross-repo
// change requiring a matching `server` change, not a local edit.
//
//	REQUEST/REPLY (publish to the server queue, await a reply on a reply queue):
//	  pattern "collaboration-save"   data SaveData    → reply { success: true } | { error }
//	  pattern "collaboration-fetch"  data FetchData   → reply FetchReply
//	FIRE-AND-FORGET (publish, no reply):
//	  pattern "collaboration-contribution"  data ContributionData
//
// The metadata/blob split (persistence-ports.md) is honoured: SaveData carries
// only the index — contentPointer locates the blob in file-service;
// the snapshot bytes NEVER cross this bus.
package rabbitmq

// SaveData is the collaboration-save request payload: the index row only.
// (carries forward nothing of the legacy binaryStateInBase64 / content fields —
// the blob lives in file-service, located by ContentPointer.)
type SaveData struct {
	ID                    string `json:"id"`
	ContentType           string `json:"contentType"`
	Version               int    `json:"version"`
	ContentPointer        string `json:"contentPointer"`
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
	AuthorizationPolicyID string `json:"authorizationPolicyId,omitempty"`
	// StorageBucketID is the document's own profile.storageBucket.id (mirrors
	// the server FetchOutputData.storageBucketId). The file-service checkpoint store
	// uploads each snapshot into THIS bucket so blobs co-locate with the
	// document rather than a single flat platform bucket. Absent for documents
	// the server cannot resolve a bucket for; the checkpoint store then falls back to
	// its configured bucket.
	StorageBucketID string `json:"storageBucketId,omitempty"`
	OwnerRef        string `json:"ownerRef,omitempty"`
	Error           string `json:"error,omitempty"`
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
	PatternContribution = "collaboration-contribution"
)
