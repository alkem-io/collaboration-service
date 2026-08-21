package oidc

// The oidc adapter's resolution entry point (T018.6): try the handshake
// credentials in the SAME priority order the server's forward-auth.controller.ts
// uses — cookie → bearer → guest → anonymous sentinel — and apply the §V failure
// semantics: a MISSING credential resolves to the anonymous sentinel (never 401),
// a PRESENTED-but-invalid credential is a hard error (401), and a dependency
// failure on a credential-bearing handshake rejects (503-equivalent), never a
// silent anonymous downgrade.
//
// Each path is INERT when its dependency is unconfigured (nil Session ⇒ cookie
// path off; nil Bearer ⇒ bearer path off), so the adapter degrades to cookie-only
// or bearer-only.

import (
	"context"
	"errors"
	"fmt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Config carries the constructed credential-path dependencies. Either may be nil
// to disable that path (the wiring leaves it nil when its config is absent).
type Config struct {
	// Session validates the BFF cookie session (nil ⇒ cookie path off).
	Session *sessionStore
	// Bearer validates the Hydra RS256 access token (nil ⇒ bearer path off).
	Bearer *bearerValidator
}

// Adapter is the direct-validation handshake-AuthN adapter (AUTH_MODE=oidc).
type Adapter struct {
	session *sessionStore
	bearer  *bearerValidator
}

// New constructs the oidc adapter from its credential-path dependencies.
func New(cfg Config) *Adapter {
	return &Adapter{session: cfg.Session, bearer: cfg.Bearer}
}

// Authenticate resolves the handshake identity by validating the presented
// credentials in priority order (cookie → bearer → guest → anonymous sentinel).
//
//   - Cookie: a valid session → its actor id; a not-found session id (logged
//     out / reaped) falls through (state (a), §V: missing ≠ failed); a
//     tombstoned/expired/corrupt session, or an unreachable store, is a hard
//     error (401/503).
//   - Bearer: a valid token → its actor id; any presented-but-invalid token is a
//     hard error (401). Collab is STRICTER than forward-auth here, which swallows
//     bearer errors — §V/FR-023 require the 401.
//   - Guest: ?guestName= → named anonymous (the sentinel principal; the display
//     name is presence-only, OPEN-6).
//   - None: the anonymous sentinel (FR-023) so a public-read doc stays reachable.
func (a *Adapter) Authenticate(ctx context.Context, creds model.HandshakeCredentials) (model.Identity, error) {
	// 1. BFF cookie session (browsers/SPA).
	if a.session != nil && creds.CookieSID != "" {
		actorID, err := a.session.Resolve(ctx, creds.CookieSID)
		switch {
		case err == nil:
			identity, parseErr := model.IdentityFromActorID(actorID)
			if parseErr != nil {
				return model.Identity{}, fmt.Errorf("oidc cookie actor id: %w", parseErr)
			}
			return identity, nil
		case errors.Is(err, errSessionNotFound):
			// Cookie present but no live session — not a failure; fall through.
		default:
			// Tombstoned / expired / corrupt / store-unreachable → reject (§V).
			return model.Identity{}, err
		}
	}

	// 2. Hydra RS256 bearer (API/M2M).
	if a.bearer != nil && creds.BearerToken != "" {
		actorID, err := a.bearer.Validate(ctx, creds.BearerToken)
		if err != nil {
			// Presented-but-invalid bearer → 401 (FR-023; stricter than forward-auth).
			return model.Identity{}, err
		}
		identity, parseErr := model.IdentityFromActorID(actorID)
		if parseErr != nil {
			return model.Identity{}, fmt.Errorf("oidc bearer actor id: %w", parseErr)
		}
		return identity, nil
	}

	// 3. Guest (?guestName=) → named anonymous: the principal is the anonymous
	//    sentinel; the display name rides presence only (OPEN-6). No distinct
	//    guest principal is minted off-gateway.
	// 4. No credential → anonymous sentinel (never 401, §V/FR-023).
	//
	// Both cases resolve to the sentinel principal; the guest display name is
	// surfaced to presence by the WS/awareness layer from creds.GuestName, not
	// from the Identity.
	return model.AnonymousIdentity(), nil
}

// compile-time assertion that Adapter satisfies the handshake-AuthN port.
var _ port.Auth = (*Adapter)(nil)
