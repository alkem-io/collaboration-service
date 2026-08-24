// Package rabbitmq is the Alkemio-default MetadataStore (port.MetadataStore): it
// rides the Alkemio server's RabbitMQ bus, persisting the small document index
// (NOT the blob) over a request/reply RPC. It speaks the NEW UNIFIED
// collaboration contract that replaces the two legacy dialects
// (collaborative-document-service's collaboration-document-save/-fetch and
// whiteboard-collaboration-service's save/fetch) — OPEN-3.
//
// CROSS-REPO CONTRACT: the consumer side lives in the Alkemio `server` repo —
// src/services/collaboration-integration/collaboration-integration.controller.ts.
// WHICH `server` BRANCHES CARRY IT IS NOT ASSERTED HERE, and a reader who does not
// find it on the branch they happen to have checked out has learned nothing about
// this contract: branch placement moves, merges, and is renamed, so any sentence
// naming one is stale the moment either side advances. What is durable is the wire
// shape — the NestJS RMQ request/reply envelope { pattern, data, id } with AMQP
// correlationId + replyTo, so a NestJS @MessagePattern handler consumes it
// natively.
//
// WHAT THIS SIDE SPEAKS is exactly the three patterns listed below: save, fetch,
// and the fire-and-forget contribution event. Two further patterns exist in the
// contract's history — DELETE and INFO — and this service issues NEITHER. Whether
// a handler for them survives on any given `server` branch is not asserted here:
// it is `server`'s to retire on its own schedule, and naming a branch state would
// be stale as soon as either side moved. They are simply not part of what this
// package speaks:
//
//	DELETE  retired: `server` owns the index row and removes it, along with the
//	        profile, bucket and blob, BEFORE publishing document.deleted — so
//	        there is nothing left for this service to purge on arrival.
//	INFO    retired under KISS-018: the capability it carried is already owned by
//	        the authorization-evaluation-service, which decides per actor and per
//	        document. Wiring it would install a second authorization-shaped
//	        decision point beside that one.
//
// Naming them here rather than omitting them is deliberate: a reader who finds
// those handlers in `server`, or these pattern names anywhere in its history, should
// learn from THIS file that the absence is a decision and not an oversight.
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
	// document rather than a single flat platform bucket. Absent for a document
	// `server` cannot resolve a bucket for — and that is a REFUSED save, not a
	// diverted one: the checkpoint store has no fallback bucket.
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
