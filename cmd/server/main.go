// Package main boots the Alkemio collaboration-service: it loads configuration,
// assembles the hexagon via internal/app (domain service + selected adapters,
// the operational HTTP surface, the collaboration WebSocket endpoint, and the
// standalone REST API / RabbitMQ lifecycle consumer), and serves until
// SIGINT/SIGTERM triggers a graceful shutdown that releases every live room
// (persisting a final snapshot each).
//
// The standalone-default adapters (single-pod fan-out, in-process metadata/blob,
// open auth) keep this a single zero-dependency binary; any durable backend is
// selected purely by configuration (SC-012). The composition root lives in
// internal/app so cmd/server and the e2e suite boot through identical wiring.
package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
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

	application, err := app.New(cfg, logger)
	if err != nil {
		logger.Error("failed to assemble service", zap.Error(err))
		return 1
	}
	defer application.Close()
	logger.Info("collaboration core wired",
		zap.String("fanout", string(cfg.Fanout)),
		zap.String("metadata_store", string(cfg.MetadataStore)),
		zap.String("blob_store", string(cfg.BlobStore)),
		zap.String("auth_mode", string(cfg.AuthMode)),
		zap.String("authz_mode", string(cfg.AuthZMode)),
	)

	srv := newHTTPServer(cfg.Port, application.Handler)

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
	// final snapshot per room) via application.Close so in-flight edits survive.
	shutdownServer(srv, logger)
	logger.Info("server stopped")
	return exitCode
}

func main() {
	os.Exit(run())
}
