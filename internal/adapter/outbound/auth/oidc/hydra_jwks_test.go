package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// testJWKS builds a signing key + the corresponding public JWK Set, and a static
// keySetProvider over the public set (no network — the JWKS-cache fetch is the
// production keySetProvider, swapped out here).
func testJWKS(t *testing.T) (signKey jwk.Key, provider keySetProvider) {
	t.Helper()
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	signKey, err = jwk.Import(raw)
	if err != nil {
		t.Fatalf("import jwk: %v", err)
	}
	_ = signKey.Set(jwk.KeyIDKey, "test-kid")
	_ = signKey.Set(jwk.AlgorithmKey, jwa.RS256())
	pub, err := jwk.PublicKeyOf(signKey)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		t.Fatalf("add key: %v", err)
	}
	return signKey, staticKeySet{set}
}

// staticKeySet is a keySetProvider returning a fixed JWK Set (no fetch).
type staticKeySet struct{ set jwk.Set }

func (s staticKeySet) keySet(_ context.Context) (jwk.Set, error) { return s.set, nil }

// signToken mints an RS256 JWT with the given claims via the signing key.
func signToken(t *testing.T, key jwk.Key, build func(b *jwt.Builder) *jwt.Builder) string {
	t.Helper()
	tok, err := build(jwt.NewBuilder()).Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), key))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return string(signed)
}

func newTestValidator(provider keySetProvider) *bearerValidator {
	return newBearerValidator(provider, bearerConfig{
		Issuer:       "https://hydra/",
		Audiences:    []string{"alkemio-web", "synapse-client"},
		ClockSkew:    30 * time.Second,
		ActorIDClaim: defaultActorIDClaim,
	})
}

// TestBearerValidResolvesActorID asserts a well-formed RS256 token with the right
// issuer/audience/claim resolves to alkemio_actor_id.
func TestBearerValidResolvesActorID(t *testing.T) {
	key, provider := testJWKS(t)
	v := newTestValidator(provider)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})

	id, err := v.Validate(context.Background(), "Bearer "+tok)
	if err != nil {
		t.Fatalf("Validate: unexpected error %v", err)
	}
	if id != "actor-77" {
		t.Errorf("actor id = %q, want actor-77", id)
	}
}

// TestBearerMalformedHeaderRejected asserts a non-"Bearer <token>" header is
// rejected.
func TestBearerMalformedHeaderRejected(t *testing.T) {
	_, provider := testJWKS(t)
	v := newTestValidator(provider)
	for _, h := range []string{"", "Basic abc", "Bearer", "bearerx token"} {
		if _, err := v.Validate(context.Background(), h); err == nil {
			t.Errorf("Validate(%q): expected error, got nil", h)
		}
	}
}

// TestBearerExpiredRejected asserts an expired token (beyond clock skew) is
// rejected (401), never anonymous.
func TestBearerExpiredRejected(t *testing.T) {
	key, provider := testJWKS(t)
	v := newTestValidator(provider)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(-time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("expired token: expected error, got nil")
	}
}

// TestBearerWrongIssuerRejected asserts a token from an unexpected issuer is
// rejected.
func TestBearerWrongIssuerRejected(t *testing.T) {
	key, provider := testJWKS(t)
	v := newTestValidator(provider)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://evil/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("wrong issuer: expected error, got nil")
	}
}

// TestBearerWrongAudienceRejected asserts a token whose aud is outside the
// allow-list is rejected.
func TestBearerWrongAudienceRejected(t *testing.T) {
	key, provider := testJWKS(t)
	v := newTestValidator(provider)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"other-client"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("wrong audience: expected error, got nil")
	}
}

// TestBearerMissingActorClaimRejected asserts a valid-signature token WITHOUT the
// alkemio_actor_id claim is rejected (mirrors the server's missing-claim 401).
func TestBearerMissingActorClaimRejected(t *testing.T) {
	key, provider := testJWKS(t)
	v := newTestValidator(provider)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour))
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("missing actor claim: expected error, got nil")
	}
}

// TestBearerForgedSignatureRejected asserts a token signed by a DIFFERENT key
// (not in the JWKS) fails signature verification.
func TestBearerForgedSignatureRejected(t *testing.T) {
	_, provider := testJWKS(t)
	v := newTestValidator(provider)

	// Mint a token with an unrelated key.
	forgedRaw, _ := rsa.GenerateKey(rand.Reader, 2048)
	forged, _ := jwk.Import(forgedRaw)
	_ = forged.Set(jwk.KeyIDKey, "test-kid") // same kid, wrong key
	_ = forged.Set(jwk.AlgorithmKey, jwa.RS256())
	tok := signToken(t, forged, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"alkemio-web"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("forged signature: expected error, got nil")
	}
}

// TestBearerJWKSFetchErrorRejects asserts a JWKS-fetch failure (key source
// unreachable) rejects rather than silently downgrading to anonymous (§V).
func TestBearerJWKSFetchErrorRejects(t *testing.T) {
	v := newBearerValidator(errKeySet{}, bearerConfig{
		Issuer:       "https://hydra/",
		ClockSkew:    30 * time.Second,
		ActorIDClaim: defaultActorIDClaim,
	})
	key, _ := testJWKS(t)
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "a")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err == nil {
		t.Fatal("JWKS fetch error: expected error, got nil")
	}
}

// errKeySet is a keySetProvider whose fetch always fails.
type errKeySet struct{}

func (errKeySet) keySet(_ context.Context) (jwk.Set, error) {
	return nil, errors.New("jwks unreachable")
}

// TestBearerAnyAudienceWhenAllowListEmpty asserts an empty audience allow-list
// accepts any audience (the allow-list is optional).
func TestBearerAnyAudienceWhenAllowListEmpty(t *testing.T) {
	key, provider := testJWKS(t)
	v := newBearerValidator(provider, bearerConfig{
		Issuer:       "https://hydra/",
		ClockSkew:    30 * time.Second,
		ActorIDClaim: defaultActorIDClaim,
	})
	tok := signToken(t, key, func(b *jwt.Builder) *jwt.Builder {
		return b.Issuer("https://hydra/").Audience([]string{"anything"}).
			Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", "actor-77")
	})
	if _, err := v.Validate(context.Background(), "Bearer "+tok); err != nil {
		t.Fatalf("empty allow-list should accept any audience: %v", err)
	}
}
