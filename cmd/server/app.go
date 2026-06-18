package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	httpAdapter "github.com/alkem-io/collaboration-service/internal/adapter/inbound/http"
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

	metadata, err := buildMetadata(cfg, &closers)
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

func buildMetadata(cfg *config.Config, closers *[]func()) (port.MetadataStore, error) {
	switch cfg.MetaStore {
	case config.MetaStoreRabbitMQ:
		client, store, err := metarabbitmq.Connect(metarabbitmq.Config{
			URL: cfg.RabbitMQ.URL, Queue: cfg.RabbitMQ.Queue,
		})
		if err != nil {
			return nil, fmt.Errorf("rabbitmq metadata store: %w", err)
		}
		*closers = append(*closers, func() { _ = client.Close() })
		return store, nil
	case config.MetaStorePostgres:
		if err := metapostgres.Migrate(cfg.Postgres.DSN); err != nil {
			return nil, fmt.Errorf("postgres migrate: %w", err)
		}
		store, pool, err := metapostgres.Connect(context.Background(), cfg.Postgres.DSN)
		if err != nil {
			return nil, fmt.Errorf("postgres metadata store: %w", err)
		}
		*closers = append(*closers, pool.Close)
		return store, nil
	default:
		// Standalone in-process store (no METADATA_STORE selection maps here in
		// practice, but keeps the zero-dep path explicit).
		return metainmem.New(), nil
	}
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
	manager := service.NewManager(deps, roomCfg, httpAdapter.PrometheusMetrics{}, logger.Named("rooms"))

	collab := &ws.Handler{
		Auth:    deps.Auth,
		Manager: manager,
		Logger:  logger.Named("ws"),
	}

	router := httpAdapter.NewRouter(httpAdapter.Deps{
		CollabHandler: collab,
		Logger:        logger,
	})
	return router, manager
}

func newHTTPServer(port int, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// No write/idle timeout: a collaboration WebSocket connection is
		// long-lived and the y-protocols layer owns its own keepalive (R7).
		MaxHeaderBytes: 1 << 20, // 1 MiB
	}
}

func shutdownServer(srv *http.Server, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", zap.Error(err))
	}
}
