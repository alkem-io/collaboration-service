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

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// standaloneConfig is the zero-dependency selection (inmemory / inline / open)
// with the epic R9 limit defaults, the base every error case mutates one field of.
func standaloneConfig() *config.Config {
	return &config.Config{
		Port:      0,
		Fanout:    config.FanoutInMemory,
		MetaStore: config.MetaStoreInMemory,
		BlobStore: config.BlobStoreInline,
		AuthMode:  config.AuthModeOpen,
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

// TestNewFailsOnBroadcasterError asserts a redis fan-out selection with a
// malformed REDIS_URL fails fast in buildBroadcaster, so New returns an error and
// (via buildDeps' cleanup) leaves nothing started — no half-configured run.
func TestNewFailsOnBroadcasterError(t *testing.T) {
	cfg := standaloneConfig()
	cfg.Fanout = config.FanoutRedis
	cfg.Redis = config.RedisConfig{URL: "://not-a-redis-url"}

	if _, err := New(cfg, zap.NewNop()); err == nil {
		t.Fatal("New must fail when the redis broadcaster URL is invalid")
	}
}

// TestNewFailsOnLocalBlobMissingRoot asserts BlobStore=local with no LocalBlobRoot
// fails in buildBlob (the local store rejects an empty root), so New errors rather
// than booting with an unwritable blob backend.
func TestNewFailsOnLocalBlobMissingRoot(t *testing.T) {
	cfg := standaloneConfig()
	cfg.BlobStore = config.BlobStoreLocal
	cfg.LocalBlobRoot = "" // invalid: the local store requires a root

	if _, err := New(cfg, zap.NewNop()); err == nil {
		t.Fatal("New must fail when BLOB_STORE=local has no root configured")
	}
}

// TestBuildBlobErrorsOnIncompleteConfig asserts buildBlob fails fast for each
// durable blob backend whose required config is missing (file-service without a
// BaseURL, s3 without a bucket, local without a root). These are the
// fail-fast-on-incomplete-config branches (constitution §XV).
func TestBuildBlobErrorsOnIncompleteConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.Config
	}{
		{"file-service without base url", &config.Config{BlobStore: config.BlobStoreFileService}},
		{"s3 without bucket", &config.Config{BlobStore: config.BlobStoreS3}},
		{"local without root", &config.Config{BlobStore: config.BlobStoreLocal}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildBlob(tc.cfg); err == nil {
				t.Fatalf("buildBlob(%s) must error on incomplete config", tc.name)
			}
		})
	}
}

// TestBuildBlobLocalSucceedsWithRoot asserts the local blob branch builds a usable
// store when a root is supplied (the happy local branch, app.go ~145-147).
func TestBuildBlobLocalSucceedsWithRoot(t *testing.T) {
	cfg := &config.Config{BlobStore: config.BlobStoreLocal, LocalBlobRoot: t.TempDir()}
	blob, err := buildBlob(cfg)
	if err != nil {
		t.Fatalf("buildBlob(local): %v", err)
	}
	if blob == nil {
		t.Fatal("buildBlob(local) returned a nil store")
	}
}

// TestBuildBlobInlineDefault asserts an unrecognized/default blob selection wires
// the inline store (the zero-dependency default).
func TestBuildBlobInlineDefault(t *testing.T) {
	blob, err := buildBlob(&config.Config{BlobStore: config.BlobStoreInline})
	if err != nil {
		t.Fatalf("buildBlob(inline): %v", err)
	}
	if blob == nil {
		t.Fatal("buildBlob(inline) returned a nil store")
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
		{config.BlobStoreS3, model.BlobStoreS3},
		{config.BlobStoreLocal, model.BlobStoreLocal},
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
	if deps.Broadcaster == nil || deps.Metadata == nil || deps.Blob == nil ||
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
		MetaStore: config.MetaStoreRabbitMQ,
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

// spyAuth records the token string the handshake reads, so a test can assert
// buildRouter threads the configured header through to the ws handler. It rejects
// (no upgrade) so the request resolves to a plain 401 over httptest.
type spyAuth struct{ token string }

func (s *spyAuth) Authenticate(_ context.Context, token string) (model.Identity, error) {
	s.token = token
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

	if spy.token != "actor-tok" {
		t.Fatalf("handshake read token %q, want %q (TokenHeader not threaded through buildRouter)", spy.token, "actor-tok")
	}
}

// TestNewAuthZEvalModeWires asserts AUTH_MODE=authzeval selects the authzeval
// adapter pair (buildAuth's non-open branch) and New still assembles — the breaker
// is lazy, so no live auth-evaluation service is needed to wire it.
func TestNewAuthZEvalModeWires(t *testing.T) {
	cfg := standaloneConfig()
	cfg.AuthMode = config.AuthModeAuthZEval
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
