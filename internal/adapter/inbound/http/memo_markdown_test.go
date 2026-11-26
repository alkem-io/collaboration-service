package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

const memoFixtureID = "bd9c8903-ab3e-49ae-8f47-bb42f2883d37"
const memoFixturePath = "/internal/memos/" + memoFixtureID + "/markdown"

func TestMemoMarkdownHTTPContract(t *testing.T) {
	for _, tt := range []struct {
		name, id, markdown string
		err                error
		status, calls      int
	}{
		{"saved", memoFixtureID, "# Saved\n", nil, 200, 1},
		{"empty", memoFixtureID, "", nil, 200, 1},
		{"bad ID", "bad", "", nil, 400, 0},
		{"nil ID", "00000000-0000-0000-0000-000000000000", "", nil, 400, 0},
		{"not a memo", memoFixtureID, "", model.ErrNotFound, 404, 1},
		{"bad checkpoint", memoFixtureID, "secret text", fmt.Errorf("secret pointer: %w", service.ErrSavedContent), 502, 1},
		{"unreachable", memoFixtureID, "secret text", errors.New("secret upstream"), 503, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			router := NewMemoMarkdownRouter(func(ctx context.Context, id model.DocumentID) (string, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok || id != memoFixtureID {
					t.Fatalf("reader: id=%s or missing deadline", id)
				}
				return tt.markdown, tt.err
			})
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/internal/memos/"+tt.id+"/markdown", nil))
			if resp.Code != tt.status || calls != tt.calls || resp.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d reader calls=%d Cache-Control=%q; want %d, %d, no-store", resp.Code, calls, resp.Header().Get("Cache-Control"), tt.status, tt.calls)
			}
			if tt.status == 200 {
				if resp.Body.String() != tt.markdown || resp.Header().Get("Content-Type") != "text/markdown; charset=utf-8" {
					t.Fatalf("unexpected markdown response %v %q", resp.Header(), resp.Body.String())
				}
			} else if strings.Contains(resp.Body.String(), "secret") {
				t.Fatalf("private content or error detail leaked: %q", resp.Body.String())
			}
		})
	}
}

func TestMemoMarkdownPublicRouterNeverReachesPrivateReader(t *testing.T) {
	public := newTestRouter(t)
	resp := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, memoFixturePath, nil)
	request.Header.Set("X-Alkemio-Actor-Id", memoFixtureID)
	public.ServeHTTP(resp, request)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("memo path on the public mux returned %d", resp.Code)
	}
}

func TestMemoMarkdownSaturationAndCancellation(t *testing.T) {
	started := make(chan struct{}, 3)
	router := NewMemoMarkdownRouter(func(ctx context.Context, _ model.DocumentID) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, memoFixturePath, nil))
			done <- struct{}{}
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two admitted memo readers did not start")
		}
	}
	saturated := httptest.NewRecorder()
	router.ServeHTTP(saturated, httptest.NewRequest(http.MethodGet, memoFixturePath, nil))
	if saturated.Code != http.StatusServiceUnavailable || len(started) != 0 {
		t.Fatalf("third request got %d or reached the reader", saturated.Code)
	}
	cancel()
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("client cancellation did not reach the memo reader")
		}
	}
}
