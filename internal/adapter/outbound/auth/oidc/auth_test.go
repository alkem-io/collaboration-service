package oidc

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// jwksHarness wraps a signing key + static provider for the bearer path; it reuses
// the helpers in hydra_jwks_test.go.
type jwksHarness struct {
	signKey  jwk.Key
	provider keySetProvider
}

func newJWKSHarness(t *testing.T) *jwksHarness {
	t.Helper()
	key, provider := testJWKS(t)
	return &jwksHarness{signKey: key, provider: provider}
}

// bearerToken mints a valid-issuer/audience bearer carrying the given actor id,
// expiring an hour out.
func (h *jwksHarness) bearerToken(t *testing.T, actorID string) string {
	return "Bearer " + signToken(t, h.signKey, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", actorID)
	})
}

// newBothPathsAdapter builds an oidc adapter with BOTH the cookie session path
// (miniredis) and the bearer path (static JWKS) enabled, returning the adapter,
// the miniredis handle for seeding sessions, and the JWKS harness.
func newBothPathsAdapter(t *testing.T) (*Adapter, *miniredis.Miniredis, *jwksHarness) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	h := newJWKSHarness(t)
	a := New(Config{
		Session: newSessionStore(client),
		Bearer: newBearerValidator(h.provider, bearerConfig{
			Issuer:       "https://hydra/",
			Audiences:    []string{"alkemio-web"},
			ClockSkew:    30 * time.Second,
			ActorIDClaim: defaultActorIDClaim,
		}),
	})
	return a, mr, h
}

// TestCookiePathResolvesActorID asserts a valid cookie session resolves to its
// actor id (cookie has priority over bearer/guest).
func TestCookiePathResolvesActorID(t *testing.T) {
	a, mr, _ := newBothPathsAdapter(t)
	actor := "cookie-actor"
	seedSession(t, mr, "sid-ok", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: futureUnix(),
	})
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{CookieSID: "sid-ok"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != actor {
		t.Errorf("ActorID = %q, want %q", id.ActorID, actor)
	}
}

// TestCookieNotFoundFallsThroughToAnonymous asserts a cookie whose Redis key is
// gone (logged out / reaped) is NOT a failure — it falls through to the anonymous
// sentinel (state (a), §V: missing ≠ failed).
func TestCookieNotFoundFallsThroughToAnonymous(t *testing.T) {
	a, _, _ := newBothPathsAdapter(t)
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{CookieSID: "ghost-sid"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != model.ANONYMOUS_ACTOR_ID {
		t.Errorf("ActorID = %q, want anonymous sentinel", id.ActorID)
	}
}

// TestCookieTombstonedIs401 asserts a PRESENTED-but-invalid cookie session
// (tombstoned) is a hard error (401) — NOT an anonymous fall-through (§V,
// FR-023).
func TestCookieTombstonedIs401(t *testing.T) {
	a, mr, _ := newBothPathsAdapter(t)
	now := time.Now().Unix()
	actor := "x"
	seedSession(t, mr, "sid-tomb", alkemioSessionPayload{
		AlkemioActorID: &actor, ExpiresAt: futureUnix(), AbsoluteExpiresAt: futureUnix(), TerminatedAt: &now,
	})
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{CookieSID: "sid-tomb"}); err == nil {
		t.Fatal("tombstoned cookie session should be a 401, got nil error")
	}
}

// TestBearerPathResolvesActorID asserts a valid bearer (no cookie) resolves to
// its claim.
func TestBearerPathResolvesActorID(t *testing.T) {
	a, _, h := newBothPathsAdapter(t)
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{BearerToken: h.bearerToken(t, "bearer-actor")})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != "bearer-actor" {
		t.Errorf("ActorID = %q, want bearer-actor", id.ActorID)
	}
}

// TestBearerInvalidIs401 asserts a PRESENTED-but-invalid bearer (expired) is a
// hard 401 — collab is STRICTER than forward-auth here, which swallows bearer
// errors (FR-023, SC-013).
func TestBearerInvalidIs401(t *testing.T) {
	a, _, h := newBothPathsAdapter(t)
	expired := "Bearer " + signToken(t, h.signKey, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(-time.Hour)).Claim("alkemio_actor_id", "x")
	})
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{BearerToken: expired}); err == nil {
		t.Fatal("expired bearer should be a 401, got nil error")
	}
}

// TestCookieHasPriorityOverBearer asserts that when BOTH a valid cookie and a
// valid bearer are presented, the cookie wins (forward-auth priority order).
func TestCookieHasPriorityOverBearer(t *testing.T) {
	a, mr, h := newBothPathsAdapter(t)
	cookieActor := "cookie-wins"
	seedSession(t, mr, "sid-pri", alkemioSessionPayload{
		AlkemioActorID: &cookieActor, ExpiresAt: futureUnix(), AbsoluteExpiresAt: futureUnix(),
	})
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{
		CookieSID: "sid-pri", BearerToken: h.bearerToken(t, "bearer-loses"),
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != cookieActor {
		t.Errorf("ActorID = %q, want the cookie actor (priority)", id.ActorID)
	}
}

// TestGuestNameIsNamedAnonymous asserts ?guestName= resolves to the anonymous
// SENTINEL as the principal, with the display name carried for presence only
// (OPEN-6) — no distinct guest principal.
func TestGuestNameIsNamedAnonymous(t *testing.T) {
	a, _, _ := newBothPathsAdapter(t)
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{GuestName: "Ada Lovelace"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != model.ANONYMOUS_ACTOR_ID {
		t.Errorf("guest principal = %q, want anonymous sentinel", id.ActorID)
	}
}

// TestNoCredentialIsAnonymousSentinel asserts a handshake with NO credential
// resolves to the anonymous sentinel (never 401) so a public-read doc is
// reachable (§V; FR-023).
func TestNoCredentialIsAnonymousSentinel(t *testing.T) {
	a, _, _ := newBothPathsAdapter(t)
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != model.ANONYMOUS_ACTOR_ID {
		t.Errorf("no-credential ActorID = %q, want anonymous sentinel", id.ActorID)
	}
}

// TestBearerOnlyDegradeIgnoresCookie asserts a bearer-only adapter (no session
// path) ignores a presented cookie and validates the bearer.
func TestBearerOnlyDegradeIgnoresCookie(t *testing.T) {
	h := newJWKSHarness(t)
	a := New(Config{
		Bearer: newBearerValidator(h.provider, bearerConfig{
			Issuer: "https://hydra/", Audiences: []string{"alkemio-web"},
			ClockSkew: 30 * time.Second, ActorIDClaim: defaultActorIDClaim,
		}),
	})
	// A cookie is presented but the cookie path is disabled — it must be ignored,
	// not error, and the bearer resolves.
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{
		CookieSID: "sid-ignored", BearerToken: h.bearerToken(t, "be-actor"),
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != "be-actor" {
		t.Errorf("ActorID = %q, want be-actor", id.ActorID)
	}
}

// TestCookieOnlyDegradeIgnoresBearer asserts a cookie-only adapter (no bearer
// path) ignores a presented bearer and resolves the cookie / falls through.
func TestCookieOnlyDegradeIgnoresBearer(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	a := New(Config{Session: newSessionStore(client)})

	// Bearer presented but bearer path disabled → ignored → no cookie → anonymous.
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{BearerToken: "Bearer whatever"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.ActorID != model.ANONYMOUS_ACTOR_ID {
		t.Errorf("ActorID = %q, want anonymous sentinel (bearer ignored)", id.ActorID)
	}
}
