package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// serveJWKS stands up an httptest endpoint serving the public set for signKey, so
// the production keySetProvider (a background-refreshed jwk.Cache) is exercised
// over real HTTP. Returns the JWKS URL.
func serveJWKS(t *testing.T, signKey jwk.Key) string {
	t.Helper()
	pub, err := jwk.PublicKeyOf(signKey)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		t.Fatalf("add key: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestNewBearerValidatorOverLiveJWKS exercises the production wiring:
// NewBearerValidator builds a jwk.Cache over a real JWKS URL (newJWKSCache +
// jwksCache.keySet), and a valid token verifies through it.
func TestNewBearerValidatorOverLiveJWKS(t *testing.T) {
	key, _ := testJWKS(t)
	jwksURL := serveJWKS(t, key)

	v, err := NewBearerValidator(context.Background(), BearerConfig{
		JWKSURL:   jwksURL,
		Issuer:    "https://hydra/",
		Audiences: []string{"alkemio-web"},
		// ClockSkew left zero to exercise the 30s default branch.
	})
	if err != nil {
		t.Fatalf("NewBearerValidator: %v", err)
	}
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "live-actor")
	})
	id, err := v.Validate(context.Background(), "Bearer "+tok)
	if err != nil {
		t.Fatalf("Validate over live JWKS: %v", err)
	}
	if id != "live-actor" {
		t.Errorf("actor id = %q, want live-actor", id)
	}
}

// TestNewBearerValidatorBadJWKSURL exercises the newJWKSCache error path — a
// malformed JWKS URL fails registration, so NewBearerValidator errors.
func TestNewBearerValidatorBadJWKSURL(t *testing.T) {
	if _, err := NewBearerValidator(context.Background(), BearerConfig{JWKSURL: "://not a url"}); err == nil {
		t.Fatal("NewBearerValidator with a malformed JWKS URL: expected error, got nil")
	}
}

// TestNewBearerValidatorDefaultsClockSkew asserts a zero ClockSkew defaults to 30s
// (the server's jose clockTolerance) — a token within 30s of expiry still verifies.
func TestNewBearerValidatorDefaultsClockSkew(t *testing.T) {
	key, _ := testJWKS(t)
	jwksURL := serveJWKS(t, key)
	v, err := NewBearerValidator(context.Background(), BearerConfig{JWKSURL: jwksURL, Issuer: "https://hydra/"})
	if err != nil {
		t.Fatalf("NewBearerValidator: %v", err)
	}
	// Expired 10s ago — inside the defaulted 30s skew, so it must still verify.
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Expiration(time.Now().Add(-10*time.Second)).
			Claim("alkemio_actor_id", "skewed")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err != nil {
		t.Fatalf("token within default 30s skew should verify: %v", err)
	}
}

// TestNewBearerValidatorDefaultsActorClaim asserts newBearerValidator defaults the
// actor claim to alkemio_actor_id when the config leaves it empty.
func TestNewBearerValidatorDefaultsActorClaim(t *testing.T) {
	key, provider := testJWKS(t)
	v := newBearerValidator(provider, bearerConfig{Issuer: "https://hydra/", ClockSkew: 30 * time.Second}) // no ActorIDClaim
	if v.cfg.ActorIDClaim != defaultActorIDClaim {
		t.Fatalf("ActorIDClaim default = %q, want %q", v.cfg.ActorIDClaim, defaultActorIDClaim)
	}
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Expiration(time.Now().Add(time.Hour)).
			Claim("alkemio_actor_id", "claimed")
	})
	id, err := v.Validate(context.Background(), "Bearer "+tok)
	if err != nil || id != "claimed" {
		t.Fatalf("Validate with defaulted claim: id=%q err=%v", id, err)
	}
}

// TestBearerNoAudienceWhenAllowListSet asserts a token with NO aud is rejected
// when an allow-list is configured (the checkAudience no-audience branch).
func TestBearerNoAudienceWhenAllowListSet(t *testing.T) {
	key, provider := testJWKS(t)
	v := newBearerValidator(provider, bearerConfig{
		Issuer: "https://hydra/", Audiences: []string{"alkemio-web"},
		ClockSkew: 30 * time.Second, ActorIDClaim: defaultActorIDClaim,
	})
	// No Audience() on the token.
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Expiration(time.Now().Add(time.Hour)).
			Claim("alkemio_actor_id", "x")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("token without aud (allow-list set): expected error, got nil")
	}
	_ = jwa.RS256 // keep the jwa import meaningful in this file
}
