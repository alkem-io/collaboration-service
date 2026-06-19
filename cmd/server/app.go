package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// newHTTPServer builds the HTTP server for the collaboration surface. A
// collaboration WebSocket is long-lived and the y-protocols layer owns its own
// keepalive (R7), so there is deliberately no write/idle timeout.
func newHTTPServer(port int, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
	}
}

func shutdownServer(srv *http.Server, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", zap.Error(err))
	}
}
