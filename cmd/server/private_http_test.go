package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestRunReportsListenerBindFailure(t *testing.T) {
	setStandaloneEnv(t)
	private := bindFixturePort(t)
	public := bindFixturePort(t)
	t.Setenv("PORT", strconv.Itoa(public.Addr().(*net.TCPAddr).Port))
	t.Setenv("MEMO_MARKDOWN_ADDR", private.Addr().String())
	if err := public.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if got := run(ctx); got != 1 {
		t.Fatalf("occupied private port did not fail startup: %d", got)
	}
}

func TestRunRefusesInvalidPrivateConfigurationOrUnusableMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"private bind would expose raw memo reads", map[string]string{"MEMO_MARKDOWN_ADDR": "0.0.0.0:4007"}},
		{"metadata broker cannot be configured", map[string]string{
			"METADATA_STORE": "rabbitmq", "RABBITMQ_QUEUE": "test-meta",
			"LIFECYCLE_QUEUE": "test-lifecycle", "RABBITMQ_URL": "amqp://%zz-invalid",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setStandaloneEnv(t)
			public, private := bindFixturePort(t), bindFixturePort(t)
			t.Setenv("PORT", strconv.Itoa(public.Addr().(*net.TCPAddr).Port))
			t.Setenv("MEMO_MARKDOWN_ADDR", private.Addr().String())
			for _, l := range []net.Listener{public, private} {
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// If startup does not refuse the invalid configuration, this
			// supplies a bounded, otherwise normal shutdown.
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			if got := run(ctx); got != 1 {
				t.Fatalf("unsafe configuration looked like a successful run: exit=%d", got)
			}
		})
	}
}

func TestRunStopsPublicAndPrivateListeners(t *testing.T) {
	setStandaloneEnv(t)
	private := bindFixturePort(t)
	public := bindFixturePort(t)
	publicAddr, privateAddr := public.Addr().String(), private.Addr().String()
	t.Setenv("PORT", strconv.Itoa(public.Addr().(*net.TCPAddr).Port))
	t.Setenv("MEMO_MARKDOWN_ADDR", privateAddr)
	for _, listener := range []net.Listener{public, private} {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan int, 1)
	go func() { done <- run(ctx) }()
	const memoRoute = "/internal/memos/not-a-uuid/markdown"
	waitsForHTTPStatus(t, publicAddr, "/healthz", http.StatusOK)
	waitsForHTTPStatus(t, publicAddr, memoRoute, http.StatusNotFound)
	waitsForHTTPStatus(t, privateAddr, memoRoute, http.StatusBadRequest)
	waitsForHTTPStatus(t, privateAddr, "/healthz", http.StatusNotFound)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shutdown returned %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not release listeners")
	}
	for _, addr := range []string{publicAddr, privateAddr} {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("%s still listening after shutdown", addr)
		}
	}
}

func setStandaloneEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HUB_MODE", "inmemory")
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("CHECKPOINT_STORE", "inline")
	t.Setenv("AUTH_MODE", "open")
	t.Setenv("AUTHZ_MODE", "open")
}

func bindFixturePort(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func waitsForHTTPStatus(t *testing.T, addr, path string, status int) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + path)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == status {
				return
			}
			t.Fatalf("%s returned %d, want %d", addr, resp.StatusCode, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s failed to start", addr)
}

func TestPrivateMemoUsesSeparateListener(t *testing.T) {
	private := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "private-only") })
	public := newHTTPServer(0, http.NotFoundHandler())
	internal := newPrivateHTTPServer("127.0.0.1:0", private)
	if internal == nil || internal.Addr != "127.0.0.1:0" || internal.WriteTimeout == 0 {
		t.Fatal("private service did not get its own bounded listener")
	}
	for _, tc := range []struct {
		srv  *http.Server
		code int
	}{
		{public, http.StatusNotFound}, {internal, http.StatusOK},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- tc.srv.Serve(listener) }()
		t.Cleanup(func() {
			_ = tc.srv.Close()
			if err := <-done; !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		})
		client := http.Client{Timeout: time.Second}
		resp, err := client.Get("http://" + listener.Addr().String() + "/internal/memos/example/markdown")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != tc.code || (string(body) == "private-only") != (tc.code == http.StatusOK) {
			t.Fatalf("wanted status %d, got %d and body %q, err=%v", tc.code, resp.StatusCode, body, err)
		}
	}
}
