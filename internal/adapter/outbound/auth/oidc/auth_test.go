package oidc

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
	goredis "github.com/redis/go-redis/v9"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

const (
	cookieActorID = "11111111-1111-1111-1111-111111111111"
	bearerActorID = "22222222-2222-2222-2222-222222222222"
)

func assertActorID(t *testing.T, id model.Identity, want string) {
	t.Helper()
	if id.ActorID == nil || id.ActorID.String() != want {
		t.Errorf("ActorID = %v, want %s", id.ActorID, want)
	}
}

func assertAnonymousActorID(t *testing.T, id model.Identity) {
	t.Helper()
	if id.ActorID == nil || *id.ActorID != uuid.Nil {
		t.Errorf("ActorID = %v, want anonymous sentinel", id.ActorID)
	}
}

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
	actor := cookieActorID
	seedSession(t, mr, "sid-ok", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: futureUnix(),
	})
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{CookieSID: "sid-ok"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	assertActorID(t, id, actor)
}

func TestCookieMalformedActorIDIsRejected(t *testing.T) {
	a, mr, _ := newBothPathsAdapter(t)
	actor := "not-a-uuid"
	seedSession(t, mr, "sid-malformed", alkemioSessionPayload{
		AlkemioActorID: &actor, ExpiresAt: futureUnix(), AbsoluteExpiresAt: futureUnix(),
	})
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{CookieSID: "sid-malformed"}); err == nil {
		t.Fatal("cookie carrying a malformed actor id should be rejected")
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
	assertAnonymousActorID(t, id)
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
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{BearerToken: h.bearerToken(t, bearerActorID)})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	assertActorID(t, id, bearerActorID)
}

func TestBearerMalformedActorIDIsRejected(t *testing.T) {
	a, _, h := newBothPathsAdapter(t)
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{BearerToken: h.bearerToken(t, "not-a-uuid")}); err == nil {
		t.Fatal("bearer carrying a malformed actor id should be rejected")
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
	cookieActor := "33333333-3333-3333-3333-333333333333"
	seedSession(t, mr, "sid-pri", alkemioSessionPayload{
		AlkemioActorID: &cookieActor, ExpiresAt: futureUnix(), AbsoluteExpiresAt: futureUnix(),
	})
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{
		CookieSID: "sid-pri", BearerToken: h.bearerToken(t, "44444444-4444-4444-4444-444444444444"),
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	assertActorID(t, id, cookieActor)
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
	assertAnonymousActorID(t, id)
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
	assertAnonymousActorID(t, id)
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
		CookieSID: "sid-ignored", BearerToken: h.bearerToken(t, "55555555-5555-5555-5555-555555555555"),
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	assertActorID(t, id, "55555555-5555-5555-5555-555555555555")
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
	assertAnonymousActorID(t, id)
}
