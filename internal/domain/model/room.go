package model

// Room is a live, in-memory document session: the authoritative plaintext
// Y.Doc (FR-021), the set of connected clients, the current awareness state,
// and the limits counters. A room is lazily materialized on first authenticated
// connect (loading the latest Snapshot) and released on idle/empty or delete.
// It is never persisted — only its debounced Snapshot is.
//
// This skeleton models identity and bookkeeping only; the authoritative Y.Doc,
// the connection registry, and the snapshot-debounce machinery land with the
// room-lifecycle task (T007).
type Room struct {
	ID DocumentID
	// ContentType is copied from the document Metadata at materialization so
	// the room can validate/route updates without re-reading metadata.
	ContentType ContentType
	// Connections is the count of clients currently joined to the room. A
	// room with zero connections is eligible for idle release.
	Connections int
}

// Awareness is the ephemeral, per-participant presence state broadcast over the
// y-protocols awareness channel (ws-protocol type 1) and the custom ephemeral
// channel (type 2): cursor/pointer, online/idle, and viewer-vs-collaborator
// mode. It is TTL'd and fanned out on awareness:{id}; it is NEVER persisted.
type Awareness struct {
	// ClientID is the y-protocols awareness client id (per connection).
	ClientID uint64
	// ActorID is the authenticated Alkemio identity behind the connection,
	// or empty in 'open' standalone mode.
	ActorID string
	// Mode is the participant's collaboration mode (viewer or collaborator),
	// subject to inactivity downgrade (FR-014).
	Mode CollaboratorMode
}

// CollaboratorMode is a participant's per-document interaction mode, granted by
// AuthZ at the handshake and re-evaluated on document.access_changed. An
// inactive collaborator may be downgraded to viewer (FR-014).
type CollaboratorMode string

const (
	// ModeViewer may read the document but not mutate it.
	ModeViewer CollaboratorMode = "viewer"
	// ModeCollaborator may mutate the document.
	ModeCollaborator CollaboratorMode = "collaborator"
)
