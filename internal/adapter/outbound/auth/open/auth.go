// Package open is the in-process Auth + AuthZ adapter for development and
// tests: it authenticates every connection as an anonymous identity and grants
// every privilege, so the service runs as a single zero-dependency binary.
//
// The Alkemio composition splits the two roles across sibling packages: the
// `header` adapter is handshake AuthN (it trusts the gateway-stamped actor id),
// and `authzeval` is per-document AuthZ against the
// authorization-evaluation-service. Neither role is served by this package.
package open

import (
	"context"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Auth is the open authenticator/authorizer: anonymous identity, all privileges.
type Auth struct{}

// New constructs the open auth adapter.
func New() *Auth { return &Auth{} }

// Authenticate accepts any (including no) credential as an anonymous identity
// with a nil ActorID — open mode bypasses AuthZ entirely.
func (a *Auth) Authenticate(_ context.Context, _ string) (model.Identity, error) {
	return model.Identity{}, nil
}

// Evaluate grants every privilege on every document.
func (a *Auth) Evaluate(_ context.Context, _ model.Identity, _ model.DocumentID, _ model.Privilege) (model.AuthDecision, error) {
	return model.AuthDecision{Allowed: true, Reason: "open mode"}, nil
}

// compile-time assertions that Auth satisfies both auth ports.
var (
	_ port.Auth  = (*Auth)(nil)
	_ port.AuthZ = (*Auth)(nil)
)
