// Package header is the gateway-terminated handshake-AuthN adapter (option (a),
// the Alkemio prod default). It TRUSTS the actor id stamped in the gateway header
// (AUTH_TOKEN_HEADER, e.g. X-Alkemio-Actor-Id) — the gateway (Traefik forwardAuth
// → server /rest/internal/forward-auth) already authenticated the request and
// resolved the actor id, exactly as file-service's ActorHeaderExtractor does.
//
// It is a separate adapter from authzeval so handshake-AuthN is selected
// independently of per-document AuthZ (SC-014):
// a non-empty header is the upstream-authenticated actor id; a missing/empty header
// means the gateway did not run and is rejected (401) — never downgraded to
// anonymous (constitution §V).
package header

import (
	"context"
	"fmt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Auth is the gateway-terminated authenticator. It holds no state — the gateway
// is the source of truth for the actor id.
type Auth struct{}

// New constructs the header AuthN adapter.
func New() *Auth { return &Auth{} }

// Authenticate trusts the gateway-stamped actor id carried in the actor-id
// header. An empty header means the gateway did not run; the handshake is
// rejected (401), never anonymous-downgraded (§V; FR-021).
func (a *Auth) Authenticate(_ context.Context, actorIDCredential string) (model.Identity, error) {
	if actorIDCredential == "" {
		return model.Identity{}, fmt.Errorf("missing gateway actor-id header")
	}
	identity, err := model.IdentityFromActorID(actorIDCredential)
	if err != nil {
		return model.Identity{}, fmt.Errorf("gateway actor-id header: %w", err)
	}
	return identity, nil
}

// compile-time assertion that Auth satisfies the handshake-AuthN port.
var _ port.Auth = (*Auth)(nil)
