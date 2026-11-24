package app

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

const testSavedMemo = "76beed5a-307a-4baf-8f23-b350efc6ea94"
const testMemoRoute = "/internal/memos/" + testSavedMemo + "/markdown"

func TestMemoReaderCapsBlobBeforeDecoding(t *testing.T) {
	released := make(chan struct{})
	defer close(released)
	deps, cfg := memoFixtureDeps(t, func(w http.ResponseWriter, r *http.Request) {
		// A chunked stream that never ends would stall any reader that tries
		// to buffer the blob before checking its size.
		_, _ = w.Write(make([]byte, service.PreviewSnapshotLimit+1))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
			return
		}
		select {
		case <-r.Context().Done():
		case <-released:
		}
	})
	handler := buildMemoHandler(deps)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequestWithContext(ctx, http.MethodGet, testMemoRoute, nil))
	if resp.Code != http.StatusBadGateway || bytes.Contains(resp.Body.Bytes(), []byte("test-file")) {
		t.Fatalf("large checkpoint did not get a generic 502: %d %q", resp.Code, resp.Body)
	}
	if cfg.FileService.MaxUploadSize != 30<<20 {
		t.Fatal("memo reader changed the room checkpoint configuration")
	}
}

func BenchmarkMemoHTTPCollaborationService(b *testing.B) {
	fixture := os.Getenv("COLLAB_MEMO_BENCHMARK_SNAPSHOT")
	if fixture == "" {
		b.Skip("set COLLAB_MEMO_BENCHMARK_SNAPSHOT to a representative saved memo")
	}
	snapshot, err := os.ReadFile(fixture) // #nosec G304 G703 -- local benchmark operator selects the input fixture.
	if err != nil {
		b.Fatal(err)
	}
	deps, _ := memoFixtureDeps(b, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(snapshot) })
	handler := buildMemoHandler(deps)
	srv := httptest.NewServer(handler)
	b.Cleanup(srv.Close)
	client := srv.Client()
	measure := func() int64 {
		resp, err := client.Get(srv.URL + testMemoRoute)
		if err != nil {
			b.Error(err)
			return 0
		}
		defer func() { _ = resp.Body.Close() }()
		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			b.Errorf("memo read: status=%d error=%v", resp.StatusCode, err)
		}
		return n
	}
	b.ReportAllocs()
	b.SetBytes(2 * int64(len(snapshot)))
	for b.Loop() {
		result := make(chan int64, 1)
		go func() { result <- measure() }()
		n := measure() + <-result
		b.ReportMetric(float64(n)/2, "markdown-B/read")
	}
}

func memoFixtureDeps(t testing.TB, reply http.HandlerFunc) (service.Deps, *config.Config) {
	t.Helper()
	fs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/file/test-file/content" {
			t.Errorf("unexpected file-service request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reply(w, r)
	}))
	t.Cleanup(fs.Close)
	cfg := standaloneConfig()
	cfg.CheckpointStore = config.CheckpointStoreFileService
	cfg.FileService = config.FileServiceConfig{BaseURL: fs.URL, MaxUploadSize: 30 << 20}
	deps, cleanup, err := buildDeps(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	if err := deps.Metadata.Save(t.Context(), model.Metadata{
		ID: testSavedMemo, ContentType: model.ContentTypeMemo, ContentPointer: "test-file",
	}); err != nil {
		t.Fatal(err)
	}
	return deps, cfg
}
