package model

import "github.com/google/uuid"

// Identity is the authenticated principal resolved from the WebSocket handshake
// (Alkemio token/cookie via Oathkeeper/Kratos). In 'open' standalone mode the
// Auth adapter returns an identity with a nil ActorID.
type Identity struct {
	// ActorID is the Alkemio actor id (never "userId" — fleet convention).
	// Nil means open mode, where AuthZ is bypassed. A pointer to uuid.Nil is the
	// real anonymous principal used by authzeval and must not be confused with
	// open mode.
	ActorID *uuid.UUID
}

// IdentityFromActorID validates an actor id at an authentication boundary and
// constructs the typed domain identity. Presented actor ids are UUIDs; an empty
// string is not a valid presented identity.
func IdentityFromActorID(raw string) (Identity, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return Identity{}, err
	}
	return Identity{ActorID: &id}, nil
}

// ActorIDString formats the typed actor id at an outbound transport boundary.
// Open mode has no actor id and formats as the empty string.
func (i Identity) ActorIDString() string {
	if i.ActorID == nil {
		return ""
	}
	return i.ActorID.String()
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
