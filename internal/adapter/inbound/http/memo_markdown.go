package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// MemoReadTimeout bounds the memo route independently of the public listener.
const MemoReadTimeout = 5 * time.Second

// NewMemoMarkdownRouter is mounted exclusively on the private listener. It
// receives only a saved reader; it has no room or user authorization dependency.
func NewMemoMarkdownRouter(read func(context.Context, model.DocumentID) (string, error)) http.Handler {
	router := chi.NewRouter()
	slots := make(chan struct{}, 2)
	router.Get("/internal/memos/{memoId}/markdown", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		id := chi.URLParam(r, "memoId")
		memoID, err := uuid.Parse(id)
		if len(id) != 36 || err != nil || memoID == uuid.Nil {
			ErrorResponse{Error: "invalid memo ID"}.Render(w, http.StatusBadRequest)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			ErrorResponse{Error: "memo Markdown temporarily unavailable"}.Render(w, http.StatusServiceUnavailable)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), MemoReadTimeout)
		defer cancel()
		body, err := read(ctx, model.DocumentID(memoID.String()))
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		switch {
		case errors.Is(err, model.ErrNotFound):
			ErrorResponse{Error: "memo not found"}.Render(w, http.StatusNotFound)
		case errors.Is(err, service.ErrSavedContent):
			ErrorResponse{Error: "saved memo content is unavailable"}.Render(w, http.StatusBadGateway)
		case err != nil:
			ErrorResponse{Error: "memo Markdown temporarily unavailable"}.Render(w, http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
			_, _ = io.WriteString(w, body)
		}
	})
	return router
}
