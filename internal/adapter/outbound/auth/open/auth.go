// Package open is the standalone Auth + AuthZ adapter: it authenticates every
// connection as an anonymous identity and grants every privilege. It is the
// zero-dependency default that lets the service run outside Alkemio (single
// binary, no auth-evaluation-service). The authzeval adapter (sibling package,
// task T006) provides the Alkemio token handshake + authorization-evaluation-
// service implementation.
package open

import (
	"context"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Auth is the open authenticator/authorizer for standalone mode.
type Auth struct{}

// New constructs the open auth adapter.
func New() *Auth { return &Auth{} }

// Authenticate accepts any (including no) credential as an anonymous identity
// with a nil ActorID — open mode bypasses AuthZ entirely.
func (a *Auth) Authenticate(_ context.Context, _ model.HandshakeCredentials) (model.Identity, error) {
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
