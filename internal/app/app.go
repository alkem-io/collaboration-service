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

	"github.com/google/uuid"
	"go.uber.org/zap"

	httpAdapter "github.com/alkem-io/collaboration-service/internal/adapter/inbound/http"
	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/lifecycle"
	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/ws"
	"github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/authzeval"
	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobfileservice "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/fileservice"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	bloblocal "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/local"
	blobs3 "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/s3"
	fanoutinmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/fanout/inmemory"
	fanoutredis "github.com/alkem-io/collaboration-service/internal/adapter/outbound/fanout/redis"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	metapostgres "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/postgres"
	metarabbitmq "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/rabbitmq"
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
func buildDeps(cfg *config.Config, logger *zap.Logger) (service.Deps, func(), error) {
	var closers []func()
	cleanup := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}

	broadcaster, err := buildBroadcaster(cfg, logger, &closers)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	metadata, contributor, err := buildMetadata(cfg, &closers)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	blob, err := buildBlob(cfg)
	if err != nil {
		cleanup()
		return service.Deps{}, nil, err
	}

	auth, authz := buildAuth(cfg, metadata)

	return service.Deps{
		Broadcaster: broadcaster,
		Metadata:    metadata,
		Blob:        blob,
		Auth:        auth,
		AuthZ:       authz,
		Contributor: contributor,
	}, cleanup, nil
}

func buildBroadcaster(cfg *config.Config, logger *zap.Logger, closers *[]func()) (port.ClusterBroadcaster, error) {
	if cfg.Fanout != config.FanoutRedis {
		return fanoutinmem.New(), nil
	}
	// Each pod needs a unique source id for echo suppression; a generated UUID
	// is sufficient (uniqueness, not stability across restarts, is what matters).
	b, err := fanoutredis.New(cfg.Redis.URL, uuid.NewString())
	if err != nil {
		return nil, fmt.Errorf("redis fan-out: %w", err)
	}
	*closers = append(*closers, func() { _ = b.Close() })
	logger.Info("redis fan-out enabled")
	return b, nil
}

// buildMetadata selects the MetadataStore and, for the Alkemio (rabbitmq) bus,
// the Contributor that publishes the north-star contribution event over the same
// bus (T013). A nil Contributor (standalone) defaults to a domain no-op.
func buildMetadata(cfg *config.Config, closers *[]func()) (port.MetadataStore, port.Contributor, error) {
	switch cfg.MetaStore {
	case config.MetaStoreRabbitMQ:
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
	case config.MetaStorePostgres:
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
// NOT the metastore RPC queue. RabbitMQ round-robins a queue across its consumers,
// so binding the metastore queue here would let the lifecycle consumer steal a
// fraction of collaboration-fetch/-save RPCs and drop them — memo joins then time
// out. The two consumers must never share a queue.
func startLifecycle(cfg *config.Config, manager *service.Manager, logger *zap.Logger, closers *[]func()) error {
	if cfg.MetaStore != config.MetaStoreRabbitMQ {
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
// LifecycleQueue, NEVER the metastore RPC queue. RabbitMQ round-robins a queue
// across its consumers, so binding the metastore queue would let the lifecycle
// consumer steal metastore fetch/save RPCs. config.Load already defaults and
// validates LifecycleQueue (distinct from Queue); this falls back to the package
// default only as a belt-and-suspenders guard for a hand-built Config.
func lifecycleQueue(cfg *config.Config) string {
	if cfg.RabbitMQ.LifecycleQueue != "" {
		return cfg.RabbitMQ.LifecycleQueue
	}
	return config.DefaultLifecycleQueue
}

func buildBlob(cfg *config.Config) (port.BlobStore, error) {
	switch cfg.BlobStore {
	case config.BlobStoreFileService:
		return blobfileservice.New(blobfileservice.Config{
			BaseURL:         cfg.FileService.BaseURL,
			StorageBucketID: cfg.FileService.StorageBucketID,
			AuthorizationID: cfg.FileService.AuthorizationID,
			MaxUploadSize:   cfg.FileService.MaxUploadSize,
		})
	case config.BlobStoreS3:
		return blobs3.New(context.Background(), blobs3.Config{
			Bucket: cfg.S3.Bucket, Region: cfg.S3.Region, Endpoint: cfg.S3.Endpoint,
			KeyPrefix: cfg.S3.KeyPrefix, AccessKeyID: cfg.S3.AccessKeyID,
			SecretAccessKey: cfg.S3.SecretAccessKey, UsePathStyle: cfg.S3.UsePathStyle,
		})
	case config.BlobStoreLocal:
		return bloblocal.New(cfg.LocalBlobRoot)
	default:
		return blobinline.New(), nil
	}
}

func buildAuth(cfg *config.Config, metadata port.MetadataStore) (port.Auth, port.AuthZ) {
	if cfg.AuthMode != config.AuthModeAuthZEval {
		open := authopen.New()
		return open, open
	}
	adapter := authzeval.New(authzeval.Config{
		ServiceURL:              cfg.AuthZEval.ServiceURL,
		BreakerFailureThreshold: cfg.AuthZEval.BreakerFailureThreshold,
		BreakerTimeout:          time.Duration(cfg.AuthZEval.BreakerTimeoutSeconds) * time.Second,
		BreakerHalfOpenMaxReqs:  cfg.AuthZEval.BreakerHalfOpenMaxReqs,
	}, policyResolver{metadata})
	return adapter, adapter
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
	case config.BlobStoreS3:
		return model.BlobStoreS3
	case config.BlobStoreLocal:
		return model.BlobStoreLocal
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
		// The handshake reads the identity token from this header. The Alkemio
		// deployment terminates auth at the gateway and forwards the resolved actor
		// id in a header (AUTH_TOKEN_HEADER=X-Alkemio-Actor-Id); standalone/open mode
		// keeps the bearer-style Authorization default.
		TokenHeader: cfg.Auth.TokenHeader,
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
	// When auth is authzeval, a create MUST carry an authorizationPolicyId — an
	// empty one registers a document that fails every later authorization
	// evaluation (the authzeval adapter fails closed on an empty policy). Require it
	// at the handler so such a document is never persisted; in open mode authZ
	// grants everything, so the policy id is optional.
	if cfg.MetaStore != config.MetaStoreRabbitMQ {
		routerDeps.CollabAPI = &httpAdapter.CollabAPIHandler{
			Lifecycle:                  manager,
			RequireAuthorizationPolicy: cfg.AuthMode == config.AuthModeAuthZEval,
		}
	}

	router := httpAdapter.NewRouter(routerDeps)
	return router, manager
}
