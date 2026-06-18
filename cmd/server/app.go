package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	httpAdapter "github.com/alkem-io/collaboration-service/internal/adapter/inbound/http"
	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/ws"
	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	fanoutinmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/fanout/inmemory"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// buildDeps selects a concrete adapter per outbound port from configuration and
// assembles the domain dependency set. Phase 1 wires the standalone-default
// adapters for every selection; the Alkemio adapters (redis fan-out, rabbitmq
// metadata, file-service/s3 blob, authzeval) are mounted here as they land
// (tasks T004–T006).
func buildDeps(cfg *config.Config, logger *zap.Logger) service.Deps {
	// Phase 1 wires only the standalone-default adapter for every port. Any
	// selection that asks for a not-yet-implemented backend falls back with a
	// warning so the requested topology is visible in the logs (the durable
	// metadata store has no Phase-1 backend, so it always warns).
	if cfg.Fanout == config.FanoutRedis {
		logger.Warn("redis fan-out not yet implemented; using single-pod in-memory broadcaster")
	}
	logger.Warn("durable metadata store not yet implemented; using in-process store",
		zap.String("requested", string(cfg.MetaStore)))
	if cfg.BlobStore != config.BlobStoreInline {
		logger.Warn("offload blob store not yet implemented; using inline store",
			zap.String("requested", string(cfg.BlobStore)))
	}
	if cfg.AuthMode == config.AuthModeAuthZEval {
		logger.Warn("authzeval adapter not yet implemented; using open auth")
	}

	open := authopen.New()
	return service.Deps{
		Broadcaster: fanoutinmem.New(),
		Metadata:    metainmem.New(),
		Blob:        blobinline.New(),
		Auth:        open,
		AuthZ:       open,
	}
}

func buildRouter(_ *config.Config, deps service.Deps, logger *zap.Logger) (http.Handler, *service.Manager) {
	httpAdapter.InitMetrics()

	manager := service.NewManager(deps, service.DefaultRoomConfig(), httpAdapter.PrometheusMetrics{}, logger.Named("rooms"))

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
