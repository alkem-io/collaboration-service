package model

// CollaboratorMode is a participant's per-document interaction mode, granted by
// AuthZ once at the handshake. It is the SESSION's capability and holds for
// the life of that WebSocket; a reconnect is evaluated again. An
// inactive collaborator may be downgraded to viewer (FR-014).
type CollaboratorMode string

const (
	// ModeViewer may read the document but not mutate it.
	ModeViewer CollaboratorMode = "viewer"
	// ModeCollaborator may mutate the document.
	ModeCollaborator CollaboratorMode = "collaborator"
)
