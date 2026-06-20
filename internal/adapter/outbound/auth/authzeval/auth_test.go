package authzeval

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// startH2CServer starts an httptest server speaking h2c (HTTP/2 cleartext),
// matching the auth-evaluation-service transport and the file-service client
// test harness this adapter reuses.
func startH2CServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// staticPolicies resolves a fixed documentId→policyId map (the MetadataStore's
// role in production, narrowed to the PolicyResolver the adapter needs).
type staticPolicies struct {
	policies map[model.DocumentID]string
	err      error
}

func (s staticPolicies) PolicyID(_ context.Context, id model.DocumentID) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	p, ok := s.policies[id]
	if !ok {
		return "", model.ErrNotFound
	}
	return p, nil
}

type evalRequest struct {
	ActorID               string `json:"actorId"`
	Privilege             string `json:"privilege"`
	AuthorizationPolicyID string `json:"authorizationPolicyId"`
}

func TestEvaluateGrantedPrivilege(t *testing.T) {
	var gotReq evalRequest
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		writeEval(w, true, "granted")
	}))

	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"doc-1": "pol-7"}})

	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "actor-1"}, "doc-1", model.PrivilegeUpdateContent)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !dec.Allowed {
		t.Error("expected Allowed=true")
	}
	// OPEN-1: the adapter resolves the document's policy id and sends the right
	// privilege string.
	if gotReq.ActorID != "actor-1" || gotReq.Privilege != "update-content" || gotReq.AuthorizationPolicyID != "pol-7" {
		t.Errorf("eval request = %+v", gotReq)
	}
}

func TestEvaluateTrimsTrailingSlashOnServiceURL(t *testing.T) {
	// A ServiceURL configured with a trailing slash must not produce a
	// "//internal/auth/evaluate" request path (which breaks routing on some
	// gateways).
	var gotPath string
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		writeEval(w, true, "")
	}))
	adapter := New(Config{ServiceURL: srv.URL + "/"}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if gotPath != "/internal/auth/evaluate" {
		t.Errorf("request path = %q, want /internal/auth/evaluate", gotPath)
	}
}

func TestEvaluateCleanDenial(t *testing.T) {
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeEval(w, false, "no grant")
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"doc-1": "pol-1"}})

	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "doc-1", model.PrivilegeRead)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if dec.Allowed {
		t.Error("expected a clean denial (Allowed=false), not an error")
	}
}

func TestEvaluateReadPrivilegeString(t *testing.T) {
	var gotReq evalRequest
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		writeEval(w, true, "")
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if gotReq.Privilege != "read" {
		t.Errorf("privilege = %q, want read", gotReq.Privilege)
	}
}

func TestEvaluateTransportFailureFailsClosed(t *testing.T) {
	// A transport failure (server returns 500) must be an ERROR, not a clean
	// denial — the caller fails closed (constitution §V, anti-pattern 13).
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})

	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead)
	if err == nil {
		t.Fatal("expected an error on transport failure (fail closed)")
	}
	if dec.Allowed {
		t.Error("a transport failure must never yield Allowed=true")
	}
}

func TestEvaluatePolicyResolveFailureFailsClosed(t *testing.T) {
	// If the document's policy id cannot be resolved, the adapter must error
	// (fail closed) rather than evaluate against an empty policy.
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeEval(w, true, "")
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{err: errors.New("metadata store down")})

	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err == nil {
		t.Error("expected an error when the policy id cannot be resolved")
	}
}

func TestEvaluateUnknownDocumentFailsClosed(t *testing.T) {
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeEval(w, true, "")
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{}})
	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "absent", model.PrivilegeRead); err == nil {
		t.Error("expected an error for a document with no resolvable policy")
	}
}

func TestEvaluateOpenBreakerFailsClosed(t *testing.T) {
	// After enough consecutive failures the breaker opens and short-circuits;
	// the adapter must keep returning errors (never a stale allow).
	var calls atomic.Int64
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	adapter := New(Config{
		ServiceURL:              srv.URL,
		BreakerFailureThreshold: 2,
		BreakerHalfOpenMaxReqs:  1,
		BreakerTimeout:          time.Minute,
	}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})

	ctx := context.Background()
	id := model.Identity{ActorID: "a"}
	for i := 0; i < 5; i++ {
		if _, err := adapter.Evaluate(ctx, id, "d", model.PrivilegeRead); err == nil {
			t.Fatalf("call %d: expected error", i)
		}
	}
	// Once open, the breaker short-circuits — far fewer than 5 backend calls.
	if n := calls.Load(); n >= 5 {
		t.Errorf("breaker did not open: %d backend calls for 5 evaluates", n)
	}
}

// Handshake AuthN moved out of authzeval into the `header` adapter (Wave 5,
// T018.2); its resolve/reject behaviour is proven in
// internal/adapter/outbound/auth/header/auth_test.go.

func TestEvaluateServiceDegraded503FailsClosed(t *testing.T) {
	// A 503 with a structured error body (auth service degraded) must be an
	// error, never a clean decision.
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allowed": false,
			"error":   map[string]any{"code": "DEPENDENCY_DOWN", "retryAfterMs": 500},
		})
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead)
	if err == nil {
		t.Fatal("expected an error on a 503 degraded response")
	}
	if dec.Allowed {
		t.Error("degraded response must never yield Allowed=true")
	}
}

func TestEvaluateBadResponseBodyFailsClosed(t *testing.T) {
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err == nil {
		t.Error("expected an error on a malformed auth response")
	}
}

func TestEvaluateTransportDialErrorFailsClosed(t *testing.T) {
	// An unreachable auth service: the request fails at the transport layer.
	adapter := New(Config{ServiceURL: "http://127.0.0.1:0"}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err == nil {
		t.Error("expected a transport error to an unreachable auth service")
	}
}

// TestEvaluateEmptyPolicyIDFailsClosed defends Evaluate's empty-policy branch
// (auth.go:146): a document whose resolver yields an empty (but non-error)
// policy id must be rejected with an error — evaluating against an empty policy
// would ask the auth service an unanswerable question, so the adapter fails
// closed instead of forwarding it.
func TestEvaluateEmptyPolicyIDFailsClosed(t *testing.T) {
	// The backend would grant if reached; it must NOT be reached.
	var reached atomic.Bool
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		writeEval(w, true, "")
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": ""}})

	if _, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead); err == nil {
		t.Error("expected an error for a document whose policy id resolves to empty")
	}
	if reached.Load() {
		t.Error("the auth service must not be asked when the policy id is empty")
	}
}

// TestEvaluateBadServiceURLFailsClosed defends doEvaluate's request-build branch
// (auth.go:169): a ServiceURL carrying an illegal control character makes
// http.NewRequestWithContext fail. New does not validate URL syntax, so this is
// reachable, and the failure must surface as an error (fail closed), never a
// clean allow.
func TestEvaluateBadServiceURLFailsClosed(t *testing.T) {
	adapter := New(Config{ServiceURL: "http://bad\x7fhost:1234"}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead)
	if err == nil {
		t.Error("expected an error building a request to a malformed ServiceURL")
	}
	if dec.Allowed {
		t.Error("a request-build failure must never yield Allowed=true")
	}
}

// TestEvaluate503WithoutStructuredBodyFailsClosed defends doEvaluate's
// generic-unavailable branch (auth.go:191): a 503 with NO structured error body
// still must be an error (service unavailable), not decoded as a decision —
// distinct from the 503-with-error-body case already covered.
func TestEvaluate503WithoutStructuredBodyFailsClosed(t *testing.T) {
	srv := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // empty body
	}))
	adapter := New(Config{ServiceURL: srv.URL}, staticPolicies{policies: map[model.DocumentID]string{"d": "p"}})
	dec, err := adapter.Evaluate(context.Background(), model.Identity{ActorID: "a"}, "d", model.PrivilegeRead)
	if err == nil {
		t.Error("expected an error on a 503 with no structured error body")
	}
	if dec.Allowed {
		t.Error("a 503 must never yield Allowed=true")
	}
}

func writeEval(w http.ResponseWriter, allowed bool, reason string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"allowed": allowed, "reason": reason})
}
