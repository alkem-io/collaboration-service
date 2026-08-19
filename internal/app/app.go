// Package app assembles the collaboration-service hexagon: it selects a concrete
// adapter per outbound port from configuration, wires the domain service to the
// inbound HTTP/WS surface, and starts the optional RabbitMQ lifecycle consumer.
//
// It is the single composition root shared by the production entrypoint
// (cmd/server) and the end-to-end test suite (test/e2e), so both boot the
// service through exactly the same wiring — the e2e suite exercises the real
// adapter-selection logic, not a hand-assembled copy of it (constitution
// anti-pattern 3: no duplicate logic).
package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/antst/go-yjs/backend/hub"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/antst/go-yjs/backend/persistence"

	httpAdapter "github.com/alkem-io/collaboration-service/internal/adapter/inbound/http"
	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/lifecycle"
	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/ws"
	"github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/authzeval"
	authheader "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/header"
	authoidc "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/oidc"
	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	hubredis "github.com/alkem-io/collaboration-service/internal/adapter/outbound/hub/redis"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	metapostgres "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/postgres"
	metarabbitmq "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/rabbitmq"
	persistfileservice "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/fileservice"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/metapointer"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// App is the assembled, ready-to-serve collaboration service: the HTTP handler
// (operational endpoints + the collaboration WebSocket + the standalone REST
// API) bound to a started room Manager, plus a Close that releases every live
// room and tears down the backends that hold connections.
type App struct {
	// Handler is the chi router serving /healthz, /metrics, /collab/{id} (WS),
	// and the standalone create/delete REST API.
	Handler http.Handler
	// Manager is the started room registry; the lifecycle consumer and the REST
	// API drive it. Exposed for the e2e suite to assert room state directly.
	Manager *service.Manager

	closers []func()
}

// New assembles the service from configuration: it selects each adapter, builds
// the router and room Manager, and starts the RabbitMQ lifecycle consumer in
// Alkemio mode. The returned App's Close must be called on shutdown to release
// live rooms (persisting a final snapshot each) and close the durable backends.
// A wiring failure (bad backend config, unreachable bus) returns an error and
// leaves nothing started.
func New(cfg *config.Config, logger *zap.Logger) (*App, error) {
	warnUnsupportedTopology(cfg, logger)

	deps, depsCleanup, err := buildDeps(cfg, logger)
	if err != nil {
		return nil, err
	}

	handler, manager := buildRouter(cfg, deps, logger)

	a := &App{Handler: handler, Manager: manager}
	// Order matters on Close: stop the lifecycle consumer, then release rooms
	// (Manager.Close), then close the durable backends. We append in that reverse
	// order below and Close drains last-in-first-out.
	a.closers = append(a.closers, depsCleanup)
	a.closers = append(a.closers, manager.Close)

	if err := startLifecycle(cfg, manager, logger, &a.closers); err != nil {
		a.Close()
		return nil, err
	}
	return a, nil
}

// Close releases every live room (final snapshot + teardown) and closes the
// durable backends, in reverse wiring order. Idempotent enough for a deferred
// call after a failed New.
func (a *App) Close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
	a.closers = nil
}

// buildDeps selects a concrete adapter per outbound port from configuration and
// assembles the domain dependency set (T004.4/T005.6/T006.4). It returns a
// cleanup that closes the backends that hold connections (redis/rabbitmq/postgres)
// on shutdown. The standalone defaults (inmemory / inline / open) keep the
// service a single zero-dependency binary (SC-012); any other selection wires the
// matching durable adapter, failing fast if its config is incomplete.
// warnUnsupportedTopology reports the one configuration combination this service
// does not support (FR-022b): multi-pod fan-out with a DURABLE store.
//
// Why it is unsupported. Every pod serving a document holds its own authoritative
// copy and flushes the WHOLE document on its own schedule. The hub carries edits
// between them, but nothing decides which pod's flush wins — so two pods that
// briefly diverge (a dropped pub/sub message, a partition, a restart) will each
// write a complete state over the other's, and the later writer silently discards
// whatever the earlier one had that it never received. There is no ownership
// mechanism, no fence, and the checkpoint store replaces rather than merges.
//
// It is a WARNING rather than a startup failure on purpose: the combination is
// legitimate for a read-heavy or short-lived deployment where an operator
// understands the tradeoff, and refusing to boot would strand them. But it must
// be said out loud, before serving, naming both keys — the failure mode is silent
// data loss that appears only under conditions nobody reproduces on purpose.
func warnUnsupportedTopology(cfg *config.Config, logger *zap.Logger) {
	if cfg.Fanout != config.FanoutRedis || cfg.BlobStore != config.BlobStoreFileService {
		return
	}
	logger.Warn("UNSUPPORTED CONFIGURATION: multi-pod fan-out with a durable store and no document ownership mechanism",
		zap.String("FANOUT_MODE", string(cfg.Fanout)),
		zap.String("BLOB_STORE", string(cfg.BlobStore)),
		zap.String("consequence", "every pod flushes the whole document on its own schedule with nothing deciding which write wins; two pods that diverge will overwrite each other and the later writer silently discards edits it never received"),
		zap.String("supported", "run a single pod (FANOUT_MODE=inmemory) with BLOB_STORE=file-service"),
	)
}

func buildDeps(cfg *config.Config, logger *zap.Logger) (service.Deps, func(), error) {
	var closers []func()
	cleanup := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}

	fanout, err := buildHub(cfg, logger, &closers)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	metadata, contributor, err := buildMetadata(cfg, &closers)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	checkpoint, err := buildCheckpoint(cfg, metadata)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	auth, err := buildAuthN(cfg, &closers)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}
	authz := buildAuthZ(cfg, metadata)

	return service.Deps{
		Hub:         fanout,
		Metadata:    metadata,
		Checkpoint:  checkpoint,
		Auth:        auth,
		AuthZ:       authz,
		Contributor: contributor,
	}, cleanup, nil
}

func buildHub(cfg *config.Config, logger *zap.Logger, closers *[]func()) (hub.Hub, error) {
	if cfg.Fanout != config.FanoutRedis {
		// The core's shipped single-process hub. Not a no-op: the room publishes and
		// subscribes on this path too, so single-pod exercises the same code multi-pod
		// does rather than a stub.
		return hub.NewInProcess(), nil
	}
	// The instance id distinguishes THIS process on the wire so its own publishes,
	// which Redis loops straight back, are not delivered a second time on top of
	// the synchronous local delivery. Uniqueness is what matters, not stability
	// across restarts.
	h, err := hubredis.New(cfg.Redis.URL, uuid.NewString())
	if err != nil {
		return nil, fmt.Errorf("redis fan-out: %w", err)
	}
	*closers = append(*closers, func() { _ = h.Close() })
	logger.Info("redis fan-out enabled")
	return h, nil
}

// buildMetadata selects the MetadataStore and, for the Alkemio (rabbitmq) bus,
// the Contributor that publishes the north-star contribution event over the same
// bus (T013). A nil Contributor (standalone) defaults to a domain no-op.
func buildMetadata(cfg *config.Config, closers *[]func()) (port.MetadataStore, port.Contributor, error) {
	switch cfg.MetadataStore {
	case config.MetadataStoreRabbitMQ:
		client, store, err := metarabbitmq.Connect(metarabbitmq.Config{
			URL: cfg.RabbitMQ.URL, Queue: cfg.RabbitMQ.Queue,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("rabbitmq metadata store: %w", err)
		}
		*closers = append(*closers, func() { _ = client.Close() })
		// The rabbitmq store also satisfies port.Contributor (collaboration-
		// contribution event), so analytics ride the same bus.
		return store, store, nil
	case config.MetadataStorePostgres:
		if err := metapostgres.Migrate(cfg.Postgres.DSN); err != nil {
			return nil, nil, fmt.Errorf("postgres migrate: %w", err)
		}
		store, pool, err := metapostgres.Connect(context.Background(), cfg.Postgres.DSN)
		if err != nil {
			return nil, nil, fmt.Errorf("postgres metadata store: %w", err)
		}
		*closers = append(*closers, pool.Close)
		return store, nil, nil
	default:
		// Standalone in-process store: the zero-dep path (SC-012).
		return metainmem.New(), nil, nil
	}
}

// startLifecycle starts the RabbitMQ lifecycle consumer (document.deleted cascade,
// optional created/access_changed) on the Alkemio bus, registering its closer.
// It is a no-op in standalone mode — the create/delete HTTP API replaces the bus
// events there (T015/T016). A failure to connect is fatal in Alkemio mode (the
// cascade is a correctness requirement: no orphan documents).
//
// The consumer binds the DEDICATED lifecycle queue (cfg.RabbitMQ.LifecycleQueue),
// NOT the metadata-store RPC queue. RabbitMQ round-robins a queue across its consumers,
// so binding the metadata-store queue here would let the lifecycle consumer steal a
// fraction of collaboration-fetch/-save RPCs and drop them — memo joins then time
// out. The two consumers must never share a queue.
func startLifecycle(cfg *config.Config, manager *service.Manager, logger *zap.Logger, closers *[]func()) error {
	if cfg.MetadataStore != config.MetadataStoreRabbitMQ {
		return nil
	}
	queue := lifecycleQueue(cfg)
	consumer, err := lifecycle.Connect(lifecycle.Config{
		URL: cfg.RabbitMQ.URL, Queue: queue,
	}, manager, logger.Named("lifecycle"))
	if err != nil {
		return fmt.Errorf("lifecycle consumer: %w", err)
	}
	*closers = append(*closers, func() { _ = consumer.Close() })
	logger.Info("lifecycle consumer enabled", zap.String("queue", queue))
	return nil
}

// lifecycleQueue is the queue the lifecycle consumer binds: the dedicated
// LifecycleQueue, NEVER the metadata-store RPC queue. RabbitMQ round-robins a queue
// across its consumers, so binding the metadata-store queue would let the lifecycle
// consumer steal metadata-store fetch/save RPCs. config.Load already defaults and
// validates LifecycleQueue (distinct from Queue); this falls back to the package
// default only as a belt-and-suspenders guard for a hand-built Config.
func lifecycleQueue(cfg *config.Config) string {
	if cfg.RabbitMQ.LifecycleQueue != "" {
		return cfg.RabbitMQ.LifecycleQueue
	}
	return config.DefaultLifecycleQueue
}

// buildCheckpoint selects the persistence adapter.
//
// The return type is persistence.DeletingCheckpointStore, not CheckpointStore:
// deletion is OPTIONAL in the contract (some media are forbidden to delete — WORM
// storage, object locks, regulated archival tiers), so a caller that needs
// erasure must type-assert and fail loudly rather than trust that Delete exists.
// Asserting it HERE means a store that cannot delete fails startup, instead of
// surfacing the first time an owner deletes a document and the cascade cannot
// complete.
//
// Only two shapes exist. file-service is the deployed one; `inline` resolves to
// the in-process store, which backs the test suite, the local development loop
// and the zero-dependency smoke test (§III). Any other value is rejected by
// config.Load and cannot reach here.
func buildCheckpoint(cfg *config.Config, metadata port.MetadataStore) (persistence.DeletingCheckpointStore, error) {
	if cfg.BlobStore == config.BlobStoreFileService {
		return persistfileservice.New(persistfileservice.Config{
			BaseURL:          cfg.FileService.BaseURL,
			FallbackBucketID: cfg.FileService.StorageBucketID,
			MaxUploadSize:    cfg.FileService.MaxUploadSize,
		}, metapointer.New(metadata))
	}
	return persistinprocess.New(), nil
}

// buildAuthN selects the handshake-AuthN adapter from cfg.AuthMode, independently
// of AuthZ (Wave 5, T018.7). The oidc adapter constructs its credential-path
// dependencies (a session-Redis client + a JWKS cache), registering the Redis
// client's closer; each path is left inert when its config is absent.
func buildAuthN(cfg *config.Config, closers *[]func()) (port.Auth, error) {
	switch cfg.AuthMode {
	case config.AuthModeHeader:
		return authheader.New(), nil
	case config.AuthModeOIDC:
		return buildOIDCAuth(cfg, closers)
	default: // config.AuthModeOpen
		return authopen.New(), nil
	}
}

// buildOIDCAuth constructs the direct-validation oidc adapter: the BFF
// cookie-session path (a Redis client over SESSION_REDIS_URL) and the Hydra
// RS256 bearer path (a background-refreshed JWKS cache over HYDRA_JWKS_URL). Each
// path is left nil — and therefore inert — when its config is absent (config.Load
// has already guaranteed at least one is set).
func buildOIDCAuth(cfg *config.Config, closers *[]func()) (port.Auth, error) {
	oc := authoidc.Config{}

	if cfg.OIDC.SessionRedisURL != "" {
		opts, err := goredis.ParseURL(cfg.OIDC.SessionRedisURL)
		if err != nil {
			return nil, fmt.Errorf("oidc session redis: parse SESSION_REDIS_URL: %w", err)
		}
		client := goredis.NewClient(opts)
		*closers = append(*closers, func() { _ = client.Close() })
		oc.Session = authoidc.NewSessionStore(client)
	}

	if cfg.OIDC.JWKSURL != "" {
		// The cache refreshes the JWKS in the background; bind that goroutine to a
		// cancelable context owned by App so Close tears it down (this composition
		// root is reused by tests and any in-process boot/shutdown, so an
		// uncancelled context.Background would leak a refresher per New). Lookups
		// still honour the per-request ctx in Authenticate.
		jwksCtx, cancel := context.WithCancel(context.Background())
		validator, err := authoidc.NewBearerValidator(jwksCtx, authoidc.BearerConfig{
			JWKSURL:   cfg.OIDC.JWKSURL,
			Issuer:    cfg.OIDC.IssuerURL,
			Audiences: cfg.OIDC.BearerAudAllowList,
			ClockSkew: time.Duration(cfg.OIDC.ClockSkewSeconds) * time.Second,
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("oidc bearer validator: %w", err)
		}
		*closers = append(*closers, cancel)
		oc.Bearer = validator
	}

	return authoidc.New(oc), nil
}

// buildAuthZ selects the per-document-AuthZ adapter from cfg.AuthZMode,
// independently of AuthN (Wave 5, T018.7).
func buildAuthZ(cfg *config.Config, metadata port.MetadataStore) port.AuthZ {
	if cfg.AuthZMode != config.AuthZModeEval {
		return authopen.New()
	}
	return authzeval.New(authzeval.Config{
		ServiceURL:              cfg.AuthZEval.ServiceURL,
		BreakerFailureThreshold: cfg.AuthZEval.BreakerFailureThreshold,
		BreakerTimeout:          time.Duration(cfg.AuthZEval.BreakerTimeoutSeconds) * time.Second,
		BreakerHalfOpenMaxReqs:  cfg.AuthZEval.BreakerHalfOpenMaxReqs,
	}, policyResolver{metadata})
}

// policyResolver adapts a MetadataStore to authzeval.PolicyResolver: it resolves
// a document's authorizationPolicyId from its metadata row (OPEN-1).
type policyResolver struct{ meta port.MetadataStore }

// PolicyID resolves the document's Alkemio authorization policy id from its
// metadata row, so the authzeval adapter can evaluate access against it (OPEN-1).
func (p policyResolver) PolicyID(ctx context.Context, id model.DocumentID) (string, error) {
	m, err := p.meta.Load(ctx, id)
	if err != nil {
		return "", err
	}
	return m.AuthorizationPolicyID, nil
}

// blobKindFor maps the configured blob store to the kind persisted in each saved
// metadata row, so a document rehydrates from the right backend (T005.6).
func blobKindFor(mode config.BlobStoreMode) model.BlobStoreKind {
	switch mode {
	case config.BlobStoreFileService:
		return model.BlobStoreFileService
	default:
		return model.BlobStoreInline
	}
}

func buildRouter(cfg *config.Config, deps service.Deps, logger *zap.Logger) (http.Handler, *service.Manager) {
	httpAdapter.InitMetrics()

	roomCfg := service.DefaultRoomConfig()
	roomCfg.BlobKind = blobKindFor(cfg.BlobStore)
	roomCfg.Limits = service.Limits{
		MaxDocBytes:      cfg.Limits.MaxDocBytes,
		MaxConnsPerRoom:  cfg.Limits.MaxConnsPerRoom,
		UpdateRatePerSec: cfg.Limits.UpdateRatePerSec,
		UpdateBurst:      cfg.Limits.UpdateBurst,
		// 0 means "use the core default" rather than "escalate on the first failed
		// flush", which a literal 0 threshold would mean — a single transient blip
		// would then tear down a healthy room and discard its edits.
		FlushFailureThreshold: cfg.Limits.FlushFailureThreshold,
	}
	roomCfg.CollaboratorInactivity = time.Duration(cfg.Limits.CollaboratorInactivitySeconds) * time.Second
	roomCfg.ContributionWindow = time.Duration(cfg.Limits.ContributionWindowSeconds) * time.Second
	roomCfg.IdleTimeout = time.Duration(cfg.Limits.IdleReleaseSeconds) * time.Second
	// Apply unconditionally: 0 is a meaningful value (disables the periodic save
	// debounce — persist only on idle-release/close), so guarding on >0 would make
	// SAVE_DEBOUNCE_MILLIS=0 silently fall back to the 500ms default.
	roomCfg.SaveDebounce = time.Duration(cfg.Limits.SaveDebounceMillis) * time.Millisecond
	manager := service.NewManager(deps, roomCfg, httpAdapter.PrometheusMetrics{}, logger.Named("rooms"))

	collab := &ws.Handler{
		Auth:    deps.Auth,
		Manager: manager,
		Logger:  logger.Named("ws"),
		// The `header` AuthN adapter reads the gateway-stamped actor id from this
		// header. The Alkemio deployment terminates auth at the gateway and forwards
		// the resolved actor id (AUTH_TOKEN_HEADER=X-Alkemio-Actor-Id); standalone/
		// open mode keeps the bearer-style Authorization default. The `oidc` adapter
		// ignores it and reads the cookie/bearer/guest credentials instead.
		TokenHeader: cfg.Auth.TokenHeader,
		// The `oidc` adapter reads the bare BFF session id from this cookie
		// (OIDC_SESSION_COOKIE_NAME, default alkemio_session); header/open ignore it.
		CookieName: cfg.OIDC.SessionCookieName,
		// A single inbound WS message must accommodate a full-doc SyncStep2 (the v2
		// snapshot, up to MaxDocBytes) plus framing; the 32 KiB coder/websocket
		// default would close the socket on any real document and loop the client.
		ReadLimitBytes: ws.ReadLimitFor(cfg.Limits.MaxDocBytes),
	}

	routerDeps := httpAdapter.Deps{
		CollabHandler: collab,
		Logger:        logger,
	}
	// The standalone create/delete REST API is the no-bus lifecycle equivalent
	// (T016). In Alkemio/rabbitmq mode the server owns document lifecycle over the
	// bus (the lifecycle consumer), so these unauthenticated endpoints must NOT be
	// exposed — leaving CollabAPI nil omits the REST surface entirely.
	//
	// When AuthZ is authzeval (AUTHZ_MODE=authzeval, independent of the AuthN
	// mode), a create MUST carry an authorizationPolicyId — an empty one registers
	// a document that fails every later authorization evaluation (the authzeval
	// adapter fails closed on an empty policy). Require it at the handler so such a
	// document is never persisted; in open AuthZ everything is granted, so the
	// policy id is optional.
	if cfg.MetadataStore != config.MetadataStoreRabbitMQ {
		routerDeps.CollabAPI = &httpAdapter.CollabAPIHandler{
			Lifecycle:                  manager,
			RequireAuthorizationPolicy: cfg.AuthZMode == config.AuthZModeEval,
		}
	}

	router := httpAdapter.NewRouter(routerDeps)
	return router, manager
}
