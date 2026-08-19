// Non-tagged unit tests for the composition root (app.New / buildDeps / buildBlob
// / blobKindFor / policyResolver). These cover the standalone happy path and the
// adapter-selection ERROR branches that need no live backend — the integration
// test (build tag `integration`) covers the durable happy paths that do.
package app

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// standaloneConfig is the zero-dependency selection (inmemory / inline / open)
// with the epic R9 limit defaults, the base every error case mutates one field of.
func standaloneConfig() *config.Config {
	return &config.Config{
		Port:          0,
		Fanout:        config.FanoutInMemory,
		MetadataStore: config.MetadataStoreInMemory,
		BlobStore:     config.BlobStoreInline,
		AuthMode:      config.AuthModeOpen,
		AuthZMode:     config.AuthZModeOpen,
		Limits: config.LimitsConfig{
			MaxDocBytes: 32 << 20, MaxConnsPerRoom: 50,
			UpdateRatePerSec: 50, UpdateBurst: 50,
			CollaboratorInactivitySeconds: 120, ContributionWindowSeconds: 60,
		},
	}
}

// TestNewStandaloneWiresAndCloses asserts app.New assembles the full standalone
// hexagon (inmemory metadata, inline blob, open auth, in-memory fan-out) without a
// single external dependency, exposes a router + started manager, and tears down
// cleanly on Close — the SC-012 zero-dependency-binary guarantee, through the REAL
// composition root the production entrypoint uses.
func TestNewStandaloneWiresAndCloses(t *testing.T) {
	application, err := New(standaloneConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("app.New (standalone): %v", err)
	}
	// Register teardown immediately so a failing assertion below cannot leak the
	// started manager/backends. Close must be safe to call and idempotent.
	t.Cleanup(application.Close)
	if application.Handler == nil {
		t.Fatal("standalone app has no HTTP handler")
	}
	if application.Manager == nil {
		t.Fatal("standalone app has no room manager")
	}

	// The router serves operational endpoints — prove it is mounted and live.
	srv := httptest.NewServer(application.Handler)
	t.Cleanup(srv.Close)
	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/healthz status = %d, want 200", resp.StatusCode)
	}
}

// TestNewFailsOnHubError asserts a redis fan-out selection with a
// malformed REDIS_URL fails fast in buildHub, so New returns an error and
// (via buildDeps' cleanup) leaves nothing started — no half-configured run.
func TestNewFailsOnHubError(t *testing.T) {
	cfg := standaloneConfig()
	cfg.Fanout = config.FanoutRedis
	cfg.Redis = config.RedisConfig{URL: "://not-a-redis-url"}

	if _, err := New(cfg, zap.NewNop()); err == nil {
		t.Fatal("New must fail when the redis broadcaster URL is invalid")
	}
}

// TestBuildCheckpointErrorsOnIncompleteConfig asserts the file-service branch
// fails fast when its base URL is missing, rather than constructing a store that
// cannot reach anything.
//
// Restructured from the old buildBlob tests (FR-018a): the removed branches
// are gone. They existed to serve the standalone deployment that constitution
// v3.0.0 §III withdrew, and there is no persistence implementation for them —
// a selection config.Load has already accepted resolves to a usable store.
func TestBuildCheckpointErrorsOnIncompleteConfig(t *testing.T) {
	cfg := &config.Config{BlobStore: config.BlobStoreFileService}
	if _, err := buildCheckpoint(cfg, metainmem.New()); err == nil {
		t.Fatal("buildCheckpoint must error when file-service has no base URL")
	}
}

// TestBuildCheckpointFallsBackToInProcess asserts `inline` resolves to the
// in-process store, which backs the test suite, the local development loop and
// the zero-dependency smoke test (§III).
//
// There are exactly two persistence paths: file-service for production, and the
// in-process store for the test suite and local development. This test used to
// sweep several removed selector values as well, on the reasoning that
// "everything that is not file-service resolves to the in-process store" — which
// described the behaviour accurately and pinned it as if it were desirable. It was
// not: that same fallback is what let an unrecognised selector come up healthy on
// a store that loses everything at restart. The gate now lives in config.Load
// (TestUnsupportedBlobStoreValuesFailStartup), so nothing else reaches this
// function, and asserting that it lands somewhere plausible would only
// re-legitimise the hole.
func TestBuildCheckpointFallsBackToInProcess(t *testing.T) {
	store, err := buildCheckpoint(&config.Config{BlobStore: config.BlobStoreInline}, metainmem.New())
	if err != nil {
		t.Fatalf("buildCheckpoint(inline): %v", err)
	}
	if store == nil {
		t.Fatal("buildCheckpoint(inline) returned no store")
	}
}

// TestBlobKindForMapsEverySelection asserts blobKindFor maps each configured blob
// backend to the kind persisted in the metadata row — the value a document
// rehydrates from regardless of the running config (T005.6). A wrong mapping would
// rehydrate from the wrong backend, so this is a real persistence invariant.
func TestBlobKindForMapsEverySelection(t *testing.T) {
	cases := []struct {
		mode config.BlobStoreMode
		want model.BlobStoreKind
	}{
		{config.BlobStoreFileService, model.BlobStoreFileService},
		{config.BlobStoreInline, model.BlobStoreInline},
		{config.BlobStoreMode("unknown"), model.BlobStoreInline}, // default → inline
	}
	for _, c := range cases {
		if got := blobKindFor(c.mode); got != c.want {
			t.Errorf("blobKindFor(%q) = %q, want %q", c.mode, got, c.want)
		}
	}
}

// erroringMeta is a MetadataStore whose Load errors, to drive policyResolver's
// load-error branch.
type erroringMeta struct{ port.MetadataStore }

func (erroringMeta) Load(context.Context, model.DocumentID) (model.Metadata, error) {
	return model.Metadata{}, errors.New("metadata backend down")
}

// TestPolicyResolverPropagatesLoadError asserts policyResolver.PolicyID surfaces a
// metadata Load error rather than returning an empty policy id — evaluating
// against an empty policy would silently change the access decision, so the error
// must propagate (fail closed).
func TestPolicyResolverPropagatesLoadError(t *testing.T) {
	pr := policyResolver{meta: erroringMeta{}}
	if _, err := pr.PolicyID(context.Background(), "doc"); err == nil {
		t.Fatal("PolicyID must propagate a metadata Load error")
	}
}

// TestPolicyResolverReturnsPolicyID asserts policyResolver.PolicyID returns the
// stored authorization policy id from the metadata row (the happy path).
func TestPolicyResolverReturnsPolicyID(t *testing.T) {
	meta := metainmem.New()
	ctx := context.Background()
	if err := meta.Save(ctx, model.Metadata{
		ID: "doc", ContentType: model.ContentTypeMemo, AuthorizationPolicyID: "policy-7",
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	pr := policyResolver{meta: meta}
	got, err := pr.PolicyID(ctx, "doc")
	if err != nil {
		t.Fatalf("PolicyID: %v", err)
	}
	if got != "policy-7" {
		t.Fatalf("PolicyID = %q, want policy-7", got)
	}
}

// TestBuildDepsStandaloneSucceeds asserts buildDeps assembles a complete dependency
// set for the standalone selection and returns a usable cleanup (idempotent, no
// backends to close). The returned Deps must have every port populated, since the
// domain consumes interfaces and a nil port would panic at first use.
func TestBuildDepsStandaloneSucceeds(t *testing.T) {
	deps, cleanup, err := buildDeps(standaloneConfig(), zap.NewNop())
	if err != nil {
		t.Fatalf("buildDeps (standalone): %v", err)
	}
	if deps.Hub == nil || deps.Metadata == nil || deps.Checkpoint == nil ||
		deps.Auth == nil || deps.AuthZ == nil {
		t.Fatal("buildDeps left an outbound port nil")
	}
	cleanup() // no-op for the standalone backends; must not panic
}

// TestLifecycleQueueIsDistinctFromMetastoreQueue asserts the lifecycle consumer
// binds its OWN queue — never the metastore RPC queue. A shared queue would let
// RabbitMQ round-robin-steal metastore fetch/save RPCs to the lifecycle consumer
// (memo joins then time out), so this separation is the core of the RMQ topology
// fix.
func TestLifecycleQueueIsDistinctFromMetastoreQueue(t *testing.T) {
	cfg := &config.Config{
		MetadataStore: config.MetadataStoreRabbitMQ,
		RabbitMQ: config.RabbitMQConfig{
			Queue:          "alkemio-collaboration",
			LifecycleQueue: "alkemio-collaboration-lifecycle",
		},
	}
	got := lifecycleQueue(cfg)
	if got == cfg.RabbitMQ.Queue {
		t.Fatalf("lifecycle consumer queue %q must differ from the metastore RPC queue %q", got, cfg.RabbitMQ.Queue)
	}
	if got != "alkemio-collaboration-lifecycle" {
		t.Fatalf("lifecycleQueue = %q, want the configured LifecycleQueue", got)
	}

	// Belt-and-suspenders: an empty LifecycleQueue falls back to the package
	// default, still distinct from the metastore queue.
	cfg.RabbitMQ.LifecycleQueue = ""
	if fallback := lifecycleQueue(cfg); fallback != config.DefaultLifecycleQueue || fallback == cfg.RabbitMQ.Queue {
		t.Fatalf("empty LifecycleQueue fallback = %q, want default %q (distinct from %q)", fallback, config.DefaultLifecycleQueue, cfg.RabbitMQ.Queue)
	}
}

// spyAuth records the credentials the handshake reads, so a test can assert
// buildRouter threads the configured header through to the ws handler. It rejects
// (no upgrade) so the request resolves to a plain 401 over httptest.
type spyAuth struct{ creds model.HandshakeCredentials }

func (s *spyAuth) Authenticate(_ context.Context, creds model.HandshakeCredentials) (model.Identity, error) {
	s.creds = creds
	return model.Identity{}, errors.New("denied")
}

// TestBuildRouterThreadsAuthTokenHeader asserts buildRouter wires
// cfg.Auth.TokenHeader into the ws handshake, so the Alkemio deployment can point
// the handshake at the gateway's resolved actor-id header
// (AUTH_TOKEN_HEADER=X-Alkemio-Actor-Id) while standalone keeps Authorization.
// Driven through the real router + handler with a spy Auth recording what header
// the token came from.
func TestBuildRouterThreadsAuthTokenHeader(t *testing.T) {
	cfg := standaloneConfig()
	cfg.Auth.TokenHeader = "X-Alkemio-Actor-Id"

	spy := &spyAuth{}
	deps, cleanup, err := buildDeps(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	defer cleanup()
	deps.Auth = spy // override the open auth with the recording spy

	router, mgr := buildRouter(cfg, deps, zap.NewNop())
	defer mgr.Close()

	req := httptest.NewRequest("GET", "/collab/doc-route", nil)
	req.Header.Set("Authorization", "auth-tok")
	req.Header.Set("X-Alkemio-Actor-Id", "actor-tok")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if spy.creds.ActorIDHeader != "actor-tok" {
		t.Fatalf("handshake read actor-id header %q, want %q (TokenHeader not threaded through buildRouter)", spy.creds.ActorIDHeader, "actor-tok")
	}
}

// TestNewAuthZEvalModeWires asserts header AuthN + authzeval AuthZ selects the
// authzeval AuthZ adapter (buildAuthZ's non-open branch) and New still assembles —
// the breaker is lazy, so no live auth-evaluation service is needed to wire it.
func TestNewAuthZEvalModeWires(t *testing.T) {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeHeader
	cfg.AuthZMode = config.AuthZModeEval
	cfg.AuthZEval = config.AuthZEvalConfig{
		ServiceURL:              "http://localhost:1/eval",
		BreakerFailureThreshold: 5,
		BreakerTimeoutSeconds:   30,
		BreakerHalfOpenMaxReqs:  1,
	}
	application, err := New(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("New (authzeval) must wire without a live eval service: %v", err)
	}
	t.Cleanup(application.Close)
	if application.Manager == nil {
		t.Fatal("authzeval-mode app has no manager")
	}
}

// oidcConfig returns a standalone config switched to oidc AuthN with the given
// session-Redis and JWKS URLs (open AuthZ), for buildAuthN wiring tests.
func oidcConfig(sessionRedisURL, jwksURL string) *config.Config {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeOIDC
	cfg.AuthZMode = config.AuthZModeOpen
	cfg.OIDC = config.OIDCConfig{
		SessionRedisURL:   sessionRedisURL,
		SessionCookieName: "alkemio_session",
		JWKSURL:           jwksURL,
		IssuerURL:         "https://hydra/",
		ClockSkewSeconds:  30,
	}
	return cfg
}

// TestBuildAuthNOIDCCookieOnlyWires asserts buildAuthN constructs the oidc adapter
// in cookie-only mode (session Redis set, no JWKS) and registers the Redis closer.
func TestBuildAuthNOIDCCookieOnlyWires(t *testing.T) {
	var closers []func()
	auth, err := buildAuthN(oidcConfig("redis://localhost:6379", ""), &closers)
	if err != nil {
		t.Fatalf("buildAuthN (oidc cookie-only): %v", err)
	}
	if auth == nil {
		t.Fatal("nil oidc auth adapter")
	}
	if len(closers) != 1 {
		t.Fatalf("expected the session-Redis closer to be registered, got %d closers", len(closers))
	}
	for _, c := range closers {
		c()
	}
}

// TestBuildAuthNOIDCBadSessionRedisURL asserts a malformed SESSION_REDIS_URL fails
// the wiring (buildOIDCAuth parse-error branch).
func TestBuildAuthNOIDCBadSessionRedisURL(t *testing.T) {
	var closers []func()
	if _, err := buildAuthN(oidcConfig("://bad redis url", ""), &closers); err == nil {
		t.Fatal("buildAuthN with a malformed SESSION_REDIS_URL: expected error, got nil")
	}
}

// TestBuildAuthNOIDCBadJWKSURL asserts a malformed HYDRA_JWKS_URL fails the wiring
// (buildOIDCAuth bearer-validator error branch).
func TestBuildAuthNOIDCBadJWKSURL(t *testing.T) {
	var closers []func()
	if _, err := buildAuthN(oidcConfig("", "://bad jwks url"), &closers); err == nil {
		t.Fatal("buildAuthN with a malformed HYDRA_JWKS_URL: expected error, got nil")
	}
}

// TestBuildAuthNHeaderWires asserts buildAuthN selects the header adapter for
// header mode (no dependencies).
func TestBuildAuthNHeaderWires(t *testing.T) {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeHeader
	var closers []func()
	auth, err := buildAuthN(cfg, &closers)
	if err != nil || auth == nil {
		t.Fatalf("buildAuthN (header): auth=%v err=%v", auth, err)
	}
	if len(closers) != 0 {
		t.Fatalf("header mode should register no closers, got %d", len(closers))
	}
}
