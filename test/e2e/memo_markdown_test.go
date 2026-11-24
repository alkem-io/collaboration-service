//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestMemoMarkdownReadsSavedColdContentOnPrivateListenerOnly(t *testing.T) {
	cfg := standaloneConfig()
	memo := loadMemoOracle(t, "marks-and-text")
	empty := loadMemoOracle(t, "cleared")
	var savedReads atomic.Int64
	blobs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		savedReads.Add(1)
		switch r.Method + " " + r.URL.Path {
		case "GET /internal/file/" + memo.id + "/content":
			_, _ = w.Write(memo.snapshot)
		case "GET /internal/file/" + empty.id + "/content":
			_, _ = w.Write(empty.snapshot)
		default:
			t.Errorf("unexpected file-service request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(blobs.Close)
	cfg.CheckpointStore = config.CheckpointStoreFileService
	cfg.FileService = config.FileServiceConfig{BaseURL: blobs.URL, MaxUploadSize: 30 << 20}
	a, err := app.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Collab-Test-Upstream", "public")
		a.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(public.Close)
	var privateCalls atomic.Int64
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		privateCalls.Add(1)
		a.MemoHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(private.Close)

	for _, fixture := range []memoOracleFixture{memo, empty} {
		err := a.Manager.PreRegister(t.Context(), model.Metadata{
			ID: model.DocumentID(fixture.id), ContentType: model.ContentTypeMemo,
			ContentPointer: fixture.id, Migrated: true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	path := "/internal/memos/" + memo.id + "/markdown"
	requireMemoResponse(t, public.URL+path, 404, "")
	if traefik := os.Getenv("COLLAB_TEST_TRAEFIK"); traefik != "" {
		gateway := startMemoTestGateway(t, traefik, public.URL)
		requireMemoResponse(t, gateway+path, 404, "")
		if savedReads.Load() != 0 {
			t.Fatal("a public memo request reached the saved blob")
		}
		// Same process/route also performs a real collaboration WS handshake.
		e2eCreated.Store(gateway+"|"+memo.id, struct{}{})
		ws := dial(t, "ws"+strings.TrimPrefix(gateway, "http"), memo.id, "memo")
		if !eventually(func() bool { return strings.Contains(ws.memoText(), "café") }) {
			t.Fatal("gateway WebSocket did not return the saved memo")
		}
		ws.close()
	}
	if privateCalls.Load() != 0 {
		t.Fatal("a public request reached the private listener")
	}
	if !eventually(func() bool { return a.Manager.RoomCount() == 0 }) {
		t.Fatal("gateway positive control retained an active room")
	}
	requireMemoResponse(t, private.URL+path, 200, memo.markdown)
	requireMemoResponse(t, private.URL+"/internal/memos/"+empty.id+"/markdown", 200, "")
	for _, tc := range []struct {
		kind model.ContentType
		code int
	}{{model.ContentTypeWhiteboard, 404}, {model.ContentTypeMemo, 502}} {
		id := uuid.NewString()
		if err := a.Manager.PreRegister(t.Context(), model.Metadata{ID: model.DocumentID(id), ContentType: tc.kind}); err != nil {
			t.Fatal(err)
		}
		requireMemoResponse(t, private.URL+"/internal/memos/"+id+"/markdown", tc.code, "")
	}
	if a.Manager.RoomCount() != 0 {
		t.Fatal("an internal read opened a collaboration room")
	}
}

type memoOracleFixture struct {
	id       string
	markdown string
	snapshot []byte
}

func loadMemoOracle(t *testing.T, name string) memoOracleFixture {
	t.Helper()
	data, err := os.ReadFile("../../internal/domain/service/testdata/memo-markdown/oracle.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name, Markdown string
		Snapshot       []byte
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, f := range cases {
		if f.Name == name {
			return memoOracleFixture{uuid.NewString(), f.Markdown, f.Snapshot}
		}
	}
	t.Fatalf("missing memo oracle fixture %q", name)
	return memoOracleFixture{}
}

func requireMemoResponse(t *testing.T, url string, status int, markdown string) {
	t.Helper()
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != status {
		t.Fatalf("%s got status %d with body %q and err=%v, want %d", url, resp.StatusCode, body, err, status)
	}
	if status == http.StatusOK && (resp.Header.Get("Content-Type") != "text/markdown; charset=utf-8" ||
		resp.Header.Get("Cache-Control") != "no-store" || string(body) != markdown) {
		t.Fatalf("incorrect raw memo response: %v %q", resp.Header, body)
	}
}

func startMemoTestGateway(t *testing.T, binary, upstream string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	// The pinned server quickstart collaboration rule, port and upstream
	// substitution only. Identity middleware is not part of this route test.
	cfg := fmt.Sprintf("http:\n  routers:\n    collaboration:\n      rule: 'PathPrefix(`/collab`)'\n      entryPoints: [web]\n      priority: 150\n      service: collaboration\n  services:\n    collaboration:\n      loadBalancer:\n        servers:\n          - url: '%s'\n", upstream)
	path := filepath.Join(t.TempDir(), "routes.yml")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), binary, "--providers.file.filename="+path, // #nosec G204 -- the isolated test operator supplies the pinned binary.
		"--entrypoints.web.address="+addr, "--global.checknewversion=false", "--global.sendanonymoususage=false")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	client := http.Client{Timeout: 200 * time.Millisecond}
	if !eventually(func() bool {
		resp, err := client.Get("http://" + addr + "/collab/probe")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.Header.Get("X-Collab-Test-Upstream") == "public"
	}) {
		t.Fatal("Traefik did not start")
	}
	t.Log("real Traefik public path checked")
	return "http://" + addr
}
