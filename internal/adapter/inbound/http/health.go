package http

import (
	"encoding/json"
	"net/http"
)

// HealthResponse is the body returned by the liveness/readiness endpoints.
type HealthResponse struct {
	Status string `json:"status"`
}

// Render writes the response as JSON with the given status code.
func (r HealthResponse) Render(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(r)
}

// ServeHealthz is the readiness/liveness handler. With persistence and fan-out
// being optional pluggable ports (single-binary standalone default), the
// Phase-1 skeleton reports process-alive only; dependency probes (DB/Redis/bus,
// when configured) are added alongside their adapters (tasks T004–T006).
func ServeHealthz(w http.ResponseWriter, _ *http.Request) {
	HealthResponse{Status: "ok"}.Render(w, http.StatusOK)
}
