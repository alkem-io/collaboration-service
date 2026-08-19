package model

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
