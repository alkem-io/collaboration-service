//go:build e2e

package e2e

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// Actor id fixtures. Every production producer of an actor id supplies a UUID —
// the gateway stamps `ctx.actorID` or the nil-UUID sentinel
// (server: core/auth/oidc/forward-auth.controller.ts), and the cookie/bearer
// paths resolve entity ids, which are `@PrimaryGeneratedColumn('uuid')`. The
// handshake now validates that, so these fixtures are UUIDs rather than names.
//
// It matters most for the NEGATIVE tests: with a non-UUID id, a tombstoned or
// expired session would 401 because the id was malformed, not because the
// session was rejected — passing for the wrong reason.
const (
	actorEditor = "11111111-1111-1111-1111-111111111111"
	actorViewer = "22222222-2222-2222-2222-222222222222"
	actorAlice  = "33333333-3333-3333-3333-333333333333"
	actorBob    = "44444444-4444-4444-4444-444444444444"
	actorEd     = "55555555-5555-5555-5555-555555555555"
	actorGhost  = "66666666-6666-6666-6666-666666666666"
	actorX      = "77777777-7777-7777-7777-777777777777"
	actorY      = "88888888-8888-8888-8888-888888888888"
	actorCarol  = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	actorDave   = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	actorForged = "99999999-9999-9999-9999-999999999999"
)

// --- oidc e2e harness: stub BFF-Redis (miniredis) + stub Hydra JWKS ---

// oidcEnv is the per-test oidc fixture: a miniredis BFF session store, an httptest
// JWKS endpoint, and the signing key used to mint bearer tokens.
type oidcEnv struct {
	mr      *miniredis.Miniredis
	jwksURL string
	signKey jwk.Key
}

// startOIDCEnv stands up the cookie-session Redis (miniredis) and the Hydra JWKS
// endpoint (httptest serving the public key), returning the fixture.
func startOIDCEnv(t *testing.T) *oidcEnv {
	t.Helper()
	mr := miniredis.RunT(t)

	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	signKey, err := jwk.Import(raw)
	if err != nil {
		t.Fatalf("import jwk: %v", err)
	}
	_ = signKey.Set(jwk.KeyIDKey, "e2e-kid")
	_ = signKey.Set(jwk.AlgorithmKey, jwa.RS256())
	pub, err := jwk.PublicKeyOf(signKey)
	if err != nil {
		t.Fatalf("public jwk: %v", err)
	}
	set := jwk.NewSet()
	_ = set.AddKey(pub)

	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(jwks.Close)

	return &oidcEnv{mr: mr, jwksURL: jwks.URL, signKey: signKey}
}

// config returns the oidc-mode config wired to this fixture (open AuthZ, so the
// focus is purely on AuthN: a resolved actor / the anonymous sentinel may read +
// edit). The session-Redis URL points at miniredis; the JWKS URL at the stub.
func (e *oidcEnv) config() *config.Config {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeOIDC
	cfg.AuthZMode = config.AuthZModeOpen
	cfg.OIDC = config.OIDCConfig{
		SessionRedisURL:    "redis://" + e.mr.Addr(),
		SessionCookieName:  "alkemio_session",
		JWKSURL:            e.jwksURL,
		IssuerURL:          "https://hydra.e2e/",
		BearerAudAllowList: []string{"alkemio-web"},
		ClockSkewSeconds:   30,
	}
	return cfg
}

// seedSession writes a BFF session payload under alkemio:sid:<sid>.
func (e *oidcEnv) seedSession(t *testing.T, sid, actorID string, mutate func(map[string]any)) {
	t.Helper()
	now := time.Now().Unix()
	payload := map[string]any{
		"alkemio_actor_id":    actorID,
		"expires_at":          now + 3600,
		"absolute_expires_at": now + 7200,
		"sub":                 "sub-" + actorID,
	}
	if mutate != nil {
		mutate(payload)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}
	if err := e.mr.Set("alkemio:sid:"+sid, string(raw)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

// bearer mints an RS256 access token carrying actorID for the configured
// issuer/audience.
func (e *oidcEnv) bearer(t *testing.T, actorID string, mutate func(b *jwt.Builder) *jwt.Builder) string {
	t.Helper()
	b := jwt.NewBuilder().
		Issuer("https://hydra.e2e/").
		Audience([]string{"alkemio-web"}).
		Expiration(time.Now().Add(time.Hour)).
		Claim("alkemio_actor_id", actorID)
	if mutate != nil {
		b = mutate(b)
	}
	tok, err := b.Build()
	if err != nil {
		t.Fatalf("build token: %v", err)
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), e.signKey))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return string(signed)
}

// handshakeStatus performs a plain HTTP GET on /collab/{id} with the given
// headers and returns the status code, so a test can assert 401 vs an upgrade
// attempt (101/426/etc.) before any WebSocket framing.
func handshakeStatus(t *testing.T, httpBase, documentID string, headers map[string]string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, httpBase+"/collab/"+documentID, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// dialMemoWithHeaders dials a memo room with arbitrary handshake headers (cookie
// / bearer), returning a converging wsClient. Used to prove a credential
// authenticates end to end over a real WS upgrade.
func dialMemoWithHeaders(t *testing.T, base, documentID string, headers map[string]string) *wsClient {
	t.Helper()
	hdr := http.Header{}
	for k, v := range headers {
		hdr.Set(k, v)
	}
	return dialWithDialOptions(t, base, documentID, "memo", &websocket.DialOptions{HTTPHeader: hdr})
}

// --- the oidc e2e proofs (SC-013/SC-014) ---

// TestOIDCCookieAuthenticatesEndToEnd boots the service in oidc mode and proves a
// valid BFF cookie session authenticates over the WS handshake — two clients with
// distinct cookie-resolved actors converge on a shared memo (SC-013).
func TestOIDCCookieAuthenticatesEndToEnd(t *testing.T) {
	env := startOIDCEnv(t)
	env.seedSession(t, "sid-alice", actorAlice, nil)
	env.seedSession(t, "sid-bob", actorBob, nil)

	httpBase := testAppHTTP(t, env.config())
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	const docID = "e2e-oidc-cookie"
	alice := dialMemoWithHeaders(t, wsBase, docID, map[string]string{"Cookie": "alkemio_session=sid-alice"})
	bob := dialMemoWithHeaders(t, wsBase, docID, map[string]string{"Cookie": "alkemio_session=sid-bob"})
	time.Sleep(150 * time.Millisecond)

	alice.insertMemo("from-alice ")
	if !eventually(func() bool {
		return contains(bob.memoText(), "from-alice")
	}) {
		t.Fatalf("cookie-authenticated edit did not converge: bob=%q", bob.memoText())
	}
}

// TestOIDCBearerAuthenticatesEndToEnd proves a valid Hydra RS256 bearer
// authenticates over the WS handshake and converges (SC-013).
func TestOIDCBearerAuthenticatesEndToEnd(t *testing.T) {
	env := startOIDCEnv(t)
	httpBase := testAppHTTP(t, env.config())
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	const docID = "e2e-oidc-bearer"
	a := dialMemoWithHeaders(t, wsBase, docID, map[string]string{"Authorization": "Bearer " + env.bearer(t, actorCarol, nil)})
	b := dialMemoWithHeaders(t, wsBase, docID, map[string]string{"Authorization": "Bearer " + env.bearer(t, actorDave, nil)})
	time.Sleep(150 * time.Millisecond)

	a.insertMemo("from-carol ")
	if !eventually(func() bool { return contains(b.memoText(), "from-carol") }) {
		t.Fatalf("bearer-authenticated edit did not converge: b=%q", b.memoText())
	}
}

// TestOIDCMissingCredentialResolvesToSentinel proves a no-credential handshake is
// NOT 401 — it resolves to the anonymous sentinel and a public-read doc is
// reachable (open AuthZ lets it edit), per §V (missing ≠ failed) / FR-023.
func TestOIDCMissingCredentialResolvesToSentinel(t *testing.T) {
	env := startOIDCEnv(t)
	httpBase := testAppHTTP(t, env.config())
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	const docID = "e2e-oidc-anon"
	// A no-credential client converges with a cookie-authenticated one — the
	// anonymous sentinel is a resolvable principal, not a 401.
	anon := dial(t, wsBase, docID, "memo")
	env.seedSession(t, "sid-ed", actorEd, nil)
	ed := dialMemoWithHeaders(t, wsBase, docID, map[string]string{"Cookie": "alkemio_session=sid-ed"})
	time.Sleep(150 * time.Millisecond)

	ed.insertMemo("hello-anon ")
	if !eventually(func() bool { return contains(anon.memoText(), "hello-anon") }) {
		t.Fatalf("anonymous client could not read a public doc: anon=%q", anon.memoText())
	}
}

// TestOIDCGuestNameIsNamedAnonymous proves ?guestName= resolves (no 401) — the
// principal is the anonymous sentinel (OPEN-6); the handshake succeeds.
func TestOIDCGuestNameIsNamedAnonymous(t *testing.T) {
	env := startOIDCEnv(t)
	httpBase := testAppHTTP(t, env.config())

	// guestName but no cookie/bearer: handshake resolves (sentinel), so it is NOT
	// 401 — the GET attempts a WS upgrade rather than rejecting.
	if status := handshakeStatus(t, httpBase, "e2e-oidc-guest?guestName=Ada", nil); status == http.StatusUnauthorized {
		t.Fatalf("guestName handshake was 401 — should resolve to named-anonymous")
	}
}

// TestOIDCTombstonedSessionIs401 proves a PRESENTED-but-invalid cookie (tombstoned
// session) is rejected at the handshake with 401 (§V; FR-023; SC-013).
func TestOIDCTombstonedSessionIs401(t *testing.T) {
	env := startOIDCEnv(t)
	env.seedSession(t, "sid-dead", actorGhost, func(p map[string]any) {
		p["terminated_at"] = time.Now().Unix() // tombstone
	})
	httpBase := testAppHTTP(t, env.config())

	if status := handshakeStatus(t, httpBase, "e2e-oidc-tomb", map[string]string{"Cookie": "alkemio_session=sid-dead"}); status != http.StatusUnauthorized {
		t.Fatalf("tombstoned-session handshake status = %d, want 401", status)
	}
}

// TestOIDCExpiredSessionIs401 proves a PRESENTED-but-expired cookie session is a
// 401 at the handshake.
func TestOIDCExpiredSessionIs401(t *testing.T) {
	env := startOIDCEnv(t)
	env.seedSession(t, "sid-old", actorGhost, func(p map[string]any) {
		p["absolute_expires_at"] = time.Now().Add(-time.Hour).Unix()
	})
	httpBase := testAppHTTP(t, env.config())

	if status := handshakeStatus(t, httpBase, "e2e-oidc-exp", map[string]string{"Cookie": "alkemio_session=sid-old"}); status != http.StatusUnauthorized {
		t.Fatalf("expired-session handshake status = %d, want 401", status)
	}
}

// TestOIDCForgedBearerIs401 proves a PRESENTED-but-invalid bearer (signed by an
// unknown key) is a 401 — collab is stricter than the gateway's bearer-swallow
// (FR-023; SC-013).
func TestOIDCForgedBearerIs401(t *testing.T) {
	env := startOIDCEnv(t)
	httpBase := testAppHTTP(t, env.config())

	// Mint a token with a foreign key not in the JWKS.
	forgedRaw, _ := rsa.GenerateKey(rand.Reader, 2048)
	forged, _ := jwk.Import(forgedRaw)
	_ = forged.Set(jwk.KeyIDKey, "e2e-kid")
	_ = forged.Set(jwk.AlgorithmKey, jwa.RS256())
	tok, _ := jwt.NewBuilder().Issuer("https://hydra.e2e/").Audience([]string{"alkemio-web"}).
		Expiration(time.Now().Add(time.Hour)).Claim("alkemio_actor_id", actorForged).Build()
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), forged))
	if err != nil {
		t.Fatalf("sign forged: %v", err)
	}

	if status := handshakeStatus(t, httpBase, "e2e-oidc-forged", map[string]string{"Authorization": "Bearer " + string(signed)}); status != http.StatusUnauthorized {
		t.Fatalf("forged-bearer handshake status = %d, want 401", status)
	}
}

// TestHeaderModeTrustsOnlyAStampedActorID asserts `header` AuthN end to end
// (SC-014): a no-header handshake is 401, because header mode trusts the stamp and
// an absent stamp means the gateway did not run; a stamped handshake authenticates
// and converges.
func TestHeaderModeTrustsOnlyAStampedActorID(t *testing.T) {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeHeader
	cfg.AuthZMode = config.AuthZModeOpen // isolate AuthN from authZ
	httpBase := testAppHTTP(t, cfg)
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	// No actor-id header → 401 (gateway didn't run).
	if status := handshakeStatus(t, httpBase, "e2e-header-401", nil); status != http.StatusUnauthorized {
		t.Fatalf("header-mode no-header handshake status = %d, want 401", status)
	}

	// With the gateway-stamped header (defaults to Authorization), it authenticates
	// and converges — exactly the prior behaviour.
	const docID = "e2e-header-ok"
	a := dialWithToken(t, wsBase, docID, "memo", actorX)
	b := dialWithToken(t, wsBase, docID, "memo", actorY)
	time.Sleep(150 * time.Millisecond)
	a.insertMemo("header-text ")
	if !eventually(func() bool { return contains(b.memoText(), "header-text") }) {
		t.Fatalf("header-mode authenticated edit did not converge: b=%q", b.memoText())
	}
}
