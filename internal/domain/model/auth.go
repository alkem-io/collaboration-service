package model

// Identity is the authenticated principal resolved from the WebSocket handshake
// (Alkemio token/cookie via Oathkeeper/Kratos). In 'open' standalone mode the
// Auth adapter returns an anonymous identity with an empty ActorID.
type Identity struct {
	// ActorID is the Alkemio actor id (never "userId" — fleet convention).
	// Empty means anonymous/open mode.
	ActorID string
}

// Privilege is the action a participant attempts against a document; AuthZ maps
// it to a viewer-vs-collaborator grant (or denial).
type Privilege string

const (
	// PrivilegeRead grants read (viewer) access to a document.
	PrivilegeRead Privilege = "read"
	// PrivilegeUpdateContent grants mutate (collaborator) access.
	PrivilegeUpdateContent Privilege = "update-content"
)

// AuthDecision is the result of an AuthZ evaluation. A non-nil error from the
// port means the question could not be answered (transport failure, open
// breaker, degraded auth service) and callers MUST fail closed — never treat an
// error as a healthy denial. A clean denial is AuthDecision{Allowed: false}.
type AuthDecision struct {
	Allowed bool
	// Reason is an optional human-readable explanation for observability; it
	// MUST NOT carry secrets.
	Reason string
}
