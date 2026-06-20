package oidc

// Hydra RS256 bearer path (T018.5): validate the Authorization: Bearer <jwt>
// access token against the Hydra JWKS — RS256 signature + issuer + audience
// allow-list + alkemio_actor_id claim + clock tolerance — and return the
// alkemio_actor_id. Mirrors the server's hydra-bearer.validator.ts (jose
// jwtVerify) 1:1; a presented-but-invalid token is a 401-class error, never an
// anonymous downgrade (constitution §V).

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// defaultActorIDClaim is the JWT claim Hydra stamps the Alkemio actor id in
// (mirrors the server's payload.alkemio_actor_id).
const defaultActorIDClaim = "alkemio_actor_id"

// bearerRE matches "Bearer <token>" (case-insensitive scheme), mirroring the
// server's BEARER_RE.
var bearerRE = regexp.MustCompile(`(?i)^Bearer\s+(\S+)$`)

// keySetProvider yields the current Hydra JWK Set used to verify a bearer's
// RS256 signature. The production implementation is a background-refreshed
// jwk.Cache; tests inject a static set, so verification logic is exercised
// without a network.
type keySetProvider interface {
	keySet(ctx context.Context) (jwk.Set, error)
}

// bearerConfig carries the bearer-validation parameters (mirroring the server's
// OIDC config — issuer / audience allow-list / clock tolerance).
type bearerConfig struct {
	// Issuer is the expected `iss`; enforced when non-empty.
	Issuer string
	// Audiences is the `aud` allow-list; any audience is accepted when empty.
	Audiences []string
	// ClockSkew is the acceptable clock tolerance for exp/nbf/iat (server: 30s).
	ClockSkew time.Duration
	// ActorIDClaim is the claim the actor id is read from (default
	// alkemio_actor_id).
	ActorIDClaim string
}

// bearerValidator verifies Hydra RS256 access tokens against the JWKS.
type bearerValidator struct {
	keys keySetProvider
	cfg  bearerConfig
}

// newBearerValidator constructs a bearer validator over a key-set provider.
func newBearerValidator(keys keySetProvider, cfg bearerConfig) *bearerValidator {
	if cfg.ActorIDClaim == "" {
		cfg.ActorIDClaim = defaultActorIDClaim
	}
	return &bearerValidator{keys: keys, cfg: cfg}
}

// BearerConfig is the exported bearer-path configuration the composition root
// passes to NewBearerValidator. It mirrors the server's OIDC config: the JWKS
// URL, the expected issuer, the audience allow-list, and the clock tolerance.
type BearerConfig struct {
	// JWKSURL is the Hydra JWKS endpoint (HYDRA_JWKS_URL); a background-refreshed
	// cache is built over it.
	JWKSURL string
	// Issuer is the expected `iss` (HYDRA_ISSUER_URL); enforced when non-empty.
	Issuer string
	// Audiences is the `aud` allow-list (BEARER_AUD_ALLOW_LIST); any audience is
	// accepted when empty.
	Audiences []string
	// ClockSkew is the acceptable clock tolerance (default 30s when zero).
	ClockSkew time.Duration
}

// NewBearerValidator builds the Hydra RS256 bearer validator with a
// background-refreshed JWKS cache over cfg.JWKSURL. The ctx governs the cache's
// background-refresh goroutine lifetime (process-scoped); per-request lookups
// honour the Authenticate ctx. Returned for wiring into oidc.Config.Bearer.
func NewBearerValidator(ctx context.Context, cfg BearerConfig) (*bearerValidator, error) { //nolint:revive // returns an unexported type by design; constructed only via this func.
	provider, err := newJWKSCache(ctx, cfg.JWKSURL)
	if err != nil {
		return nil, err
	}
	skew := cfg.ClockSkew
	if skew <= 0 {
		skew = 30 * time.Second
	}
	return newBearerValidator(provider, bearerConfig{
		Issuer:       cfg.Issuer,
		Audiences:    cfg.Audiences,
		ClockSkew:    skew,
		ActorIDClaim: defaultActorIDClaim,
	}), nil
}

// Validate verifies the Authorization header value and returns the token's
// alkemio_actor_id. Every failure (malformed header, fetch error, bad signature,
// expired, wrong issuer/audience, missing claim) is an error → 401/reject; it
// never returns a nil error with an empty actor id.
func (v *bearerValidator) Validate(ctx context.Context, authorization string) (string, error) {
	m := bearerRE.FindStringSubmatch(authorization)
	if m == nil {
		return "", fmt.Errorf("oidc: malformed bearer header")
	}
	token := m[1]

	set, err := v.keys.keySet(ctx)
	if err != nil {
		// JWKS source unreachable → reject, never anonymous (§V).
		return "", fmt.Errorf("oidc: fetch JWKS: %w", err)
	}

	opts := []jwt.ParseOption{
		// Verify against the JWKS, matching the signing key by the token's `kid`.
		// Each JWK in Hydra's set carries `alg: RS256`, so jwx binds the
		// verification algorithm to the key (no attacker-controlled alg from the
		// token header) — the alg-confusion defense.
		jwt.WithKeySet(set),
		jwt.WithValidate(true),
		jwt.WithAcceptableSkew(v.cfg.ClockSkew),
	}
	if v.cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(v.cfg.Issuer))
	}
	// Audience allow-list: jwt.WithAudience requires the token's aud to CONTAIN
	// the given value, so one acceptable match satisfies it. jwx has no built-in
	// "any of" audience option, so validate the audience ourselves below when an
	// allow-list is configured.

	parsed, err := jwt.Parse([]byte(token), opts...)
	if err != nil {
		return "", fmt.Errorf("oidc: verify bearer: %w", err)
	}

	if err := v.checkAudience(parsed); err != nil {
		return "", err
	}

	var actorID string
	if err := parsed.Get(v.cfg.ActorIDClaim, &actorID); err != nil || actorID == "" {
		return "", fmt.Errorf("oidc: bearer missing %s claim", v.cfg.ActorIDClaim)
	}
	return actorID, nil
}

// checkAudience enforces the audience allow-list (any-of semantics): the token's
// aud must intersect the configured allow-list. An empty allow-list accepts any
// audience.
func (v *bearerValidator) checkAudience(tok jwt.Token) error {
	if len(v.cfg.Audiences) == 0 {
		return nil
	}
	aud, ok := tok.Audience()
	if !ok || len(aud) == 0 {
		return fmt.Errorf("oidc: bearer has no audience")
	}
	allow := make(map[string]struct{}, len(v.cfg.Audiences))
	for _, a := range v.cfg.Audiences {
		allow[a] = struct{}{}
	}
	for _, a := range aud {
		if _, ok := allow[a]; ok {
			return nil
		}
	}
	return fmt.Errorf("oidc: bearer audience not in allow-list")
}

// newJWKSCache builds a background-refreshed JWKS cache over the Hydra JWKS URL.
// It satisfies keySetProvider. The cache fetches the JWKS lazily on first Lookup
// and refreshes it periodically, returning the last good set if a refresh fails.
func newJWKSCache(ctx context.Context, jwksURL string) (keySetProvider, error) {
	cache, err := jwk.NewCache(ctx, httprc.NewClient())
	if err != nil {
		return nil, fmt.Errorf("oidc: new JWKS cache: %w", err)
	}
	if err := cache.Register(ctx, jwksURL, jwk.WithMinInterval(15*time.Minute)); err != nil {
		return nil, fmt.Errorf("oidc: register JWKS url: %w", err)
	}
	return &jwksCache{cache: cache, url: jwksURL}, nil
}

// jwksCache adapts a *jwk.Cache to keySetProvider.
type jwksCache struct {
	cache *jwk.Cache
	url   string
}

func (c *jwksCache) keySet(ctx context.Context) (jwk.Set, error) {
	return c.cache.Lookup(ctx, c.url)
}

// ensure jwa is referenced (RS256 alg restriction is applied implicitly via the
// key's alg in the JWK Set; keep the import meaningful for a future explicit
// algorithm allow-list).
var _ = jwa.RS256
