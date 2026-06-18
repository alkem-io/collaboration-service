// Package main boots the Alkemio collaboration-service: it wires the hexagonal
// core (domain service + ports) to its selected adapters — cluster fan-out,
// metadata store, blob store, and auth — and serves the operational HTTP
// surface (/healthz, /metrics) plus the collaboration WebSocket endpoint
// (/collab/{documentId}) until SIGINT/SIGTERM triggers a graceful shutdown.
//
// This is the Phase-1 (provisioning) wiring: the standalone-default adapters
// (single-pod fan-out, in-process metadata/blob, open auth) are selected and
// the server runs; the y-protocols room machinery behind the WS endpoint lands
// with tasks T007–T016 of specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
)

func run() int {
	logger, err := config.NewLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		return 1
	}
	defer func() { _ = logger.Sync() }()

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", zap.Error(err))
		return 1
	}

	deps, cleanup, err := buildDeps(cfg, logger)
	if err != nil {
		logger.Error("failed to wire adapters", zap.Error(err))
		return 1
	}
	defer cleanup()
	logger.Info("collaboration core wired",
		zap.String("fanout", string(cfg.Fanout)),
		zap.String("metadata_store", string(cfg.MetaStore)),
		zap.String("blob_store", string(cfg.BlobStore)),
		zap.String("auth_mode", string(cfg.AuthMode)),
	)

	router, manager := buildRouter(cfg, deps, logger)

	// In Alkemio mode, start the RabbitMQ lifecycle consumer (document.deleted
	// cascade + optional created/access_changed). Standalone uses the HTTP API.
	var lifecycleClosers []func()
	if err := buildLifecycle(cfg, manager, logger, &lifecycleClosers); err != nil {
		logger.Error("failed to start lifecycle consumer", zap.Error(err))
		return 1
	}
	defer func() {
		for _, c := range lifecycleClosers {
			c()
		}
	}()

	srv := newHTTPServer(cfg.Port, router)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("server starting", zap.Int("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	exitCode := 0
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("server failed", zap.Error(err))
		exitCode = 1
	}

	// Stop accepting new connections, then release every live room (persisting a
	// final snapshot per room) so in-flight edits survive the shutdown.
	shutdownServer(srv, logger)
	manager.Close()
	logger.Info("server stopped")
	return exitCode
}

func main() {
	os.Exit(run())
}
