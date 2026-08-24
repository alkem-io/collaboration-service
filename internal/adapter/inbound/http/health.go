package http

import (
	"encoding/json"
	"net/http"
)

// HealthResponse is the body returned by the process-alive endpoint (/healthz).
type HealthResponse struct {
	Status string `json:"status"`
}

// Render writes the response as JSON with the given status code.
func (r HealthResponse) Render(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(r)
}

// ServeHealthz reports PROCESS-ALIVE ONLY. It returns 200 as long as the HTTP
// server is serving, and proves nothing about the configured backends: a green
// /healthz does NOT mean Redis, RabbitMQ, file-service or the
// authorization-evaluation-service are reachable.
//
// Stated flatly because the previous wording promised dependency probes "added
// alongside their adapters" — every one of those adapters now exists and no probe
// was added, so the comment read as a plan while describing the finished state.
func ServeHealthz(w http.ResponseWriter, _ *http.Request) {
	HealthResponse{Status: "ok"}.Render(w, http.StatusOK)
}
