package http

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type contextKey string

const ctxKeyRequestID contextKey = "requestID"

// RequestID generates a unique request ID, stores it on the context, and
// echoes it back in the X-Request-ID response header.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New().String()
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID extracts the request ID from the context, or "" if unset.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return id
	}
	return ""
}

// RequestLogger logs each request's method, path, status, and duration.
// Probe endpoints (/healthz) are excluded: Kubernetes hits them every few
// seconds and they would dominate the log stream.
func RequestLogger(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(ww, r)

			if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
				return
			}

			logger.Info("request",
				zap.String("requestID", GetRequestID(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.status),
				zap.Duration("duration", time.Since(start)),
			)
		})
	}
}

// statusWriter wraps http.ResponseWriter to capture the status code for the
// request log. Defaults to 200 because handlers that only Write never call
// WriteHeader explicitly.
type statusWriter struct {
	http.ResponseWriter
	status int
}

// WriteHeader records the status code before delegating to the wrapped writer.
func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the wrapped http.ResponseWriter so that interfaces the wrapper
// does not itself implement — notably http.Hijacker, which coder/websocket needs
// to take over the connection for a WebSocket upgrade — are reachable through it.
// This is the Go 1.20+ http.ResponseController convention; without it the
// /collab/{documentId} upgrade fails with 501 ("does not implement
// http.Hijacker") because statusWriter shadows the underlying Hijacker.
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
