package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// Deps are the inbound HTTP/WS handler dependencies the router mounts.
type Deps struct {
	// CollabHandler upgrades and serves /collab/{documentId} WebSocket
	// connections (the inbound ws adapter).
	CollabHandler http.Handler
	// Logger is the structured request logger.
	Logger *zap.Logger
}

// NewRouter builds the chi v5 router: operational endpoints (liveness/readiness
// at /healthz, Prometheus at /metrics) plus the collaboration WebSocket route.
// InitMetrics must have been called before NewRouter so /metrics has a
// populated registry.
func NewRouter(deps Deps) *chi.Mux {
	r := chi.NewRouter()

	r.Use(chimw.Recoverer)
	r.Use(RequestID)
	r.Use(RequestLogger(deps.Logger))

	// Readiness/liveness probe (process-alive in Phase 1).
	r.Get("/healthz", ServeHealthz)

	// Prometheus scrape endpoint.
	r.Handle("/metrics", MetricsHandler())

	// One document per connection (y-websocket model): wss://<host>/collab/<id>.
	r.Method(http.MethodGet, "/collab/{documentId}", deps.CollabHandler)

	return r
}
