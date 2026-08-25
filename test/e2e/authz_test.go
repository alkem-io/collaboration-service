//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// Actor id fixtures. Every production producer of an actor id supplies a UUID —
// the gateway stamps ctx.actorID or the nil-UUID sentinel (server:
// core/auth/oidc/forward-auth.controller.ts) — and the handshake validates that,
// so these are UUIDs rather than names.
//
// They lived alongside the direct-validation e2e suite until that adapter was
// removed; only the two the authz suite actually uses were kept.
const (
	actorEditor = "11111111-1111-1111-1111-111111111111"
	actorViewer = "22222222-2222-2222-2222-222222222222"
)

// evalRequest mirrors the auth-evaluation-service request body the authzeval
// adapter sends: {actorId, privilege, authorizationPolicyId}.
type evalRequest struct {
	ActorID               string `json:"actorId"`
	Privilege             string `json:"privilege"`
	AuthorizationPolicyID string `json:"authorizationPolicyId"`
}

// evalResponse is the auth-evaluation-service response: {allowed, reason}.
type evalResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// startAuthEvalStub starts an h2c (HTTP/2 cleartext) auth-evaluation-service
// stand-in matching the adapter's transport. The decide func maps an eval
// request to an allow/deny — the e2e wires it to grant read to everyone and
// update-content only to the "editor" actor.
func startAuthEvalStub(t *testing.T, decide func(evalRequest) bool) string {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/internal/auth/evaluate") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var req evalRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(evalResponse{Allowed: decide(req), Reason: "stub"})
	})
	srv := httptest.NewUnstartedServer(h)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL
}

// authzevalConfig is the standalone config switched to authzeval auth against
// authURL, keeping the in-process metadata store (so PreRegister seeds a policy
// row the PolicyResolver can load).
func authzevalConfig(authURL string) *config.Config {
	cfg := standaloneConfig()
	// header AuthN (the gateway-stamped actor id arrives in the dedicated
	// e2eActorIDHeader) + authzeval AuthZ — the Wave-5 split of the former single
	// AUTH_MODE that bundled both (since removed).
	//
	// ActorIDHeader must be named explicitly: `header` mode has no default, and
	// standaloneConfig leaves it empty, so an unnamed header would authenticate
	// nobody and every dial would 401.
	cfg.AuthMode = config.AuthModeHeader
	cfg.Auth.ActorIDHeader = e2eActorIDHeader
	cfg.AuthZMode = config.AuthZModeEval
	cfg.AuthZEval = config.AuthZEvalConfig{
		ServiceURL:              authURL,
		BreakerFailureThreshold: 3,
		BreakerTimeoutSeconds:   15,
		BreakerHalfOpenMaxReqs:  2,
	}
	return cfg
}

// preRegister seeds a document's metadata row (with an authorization policy id)
// via the standalone REST API so the authzeval PolicyResolver can Load it — an
// unregistered doc, or one with no policy id, fails closed in authzeval mode.
func preRegister(t *testing.T, httpBase, documentID, contentType string) {
	t.Helper()
	// Claim the document so the dial path does not re-create it without the
	// authorization policy id this registration carries.
	e2eCreated.Store(httpBase+"|"+documentID, struct{}{})
	body, _ := json.Marshal(map[string]string{
		"contentType":           contentType,
		"authorizationPolicyId": "policy-" + documentID,
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		httpBase+"/collab/"+documentID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("pre-register POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("pre-register status = %d, want 201", resp.StatusCode)
	}
}

// TestAuthzEvalViewerCannotMutateCollaboratorCan boots the service in authzeval
// mode against a stub auth-evaluation-service that grants read to everyone but
// update-content only to the "editor" actor. The editor's edits are applied and
// converge; the viewer's edits are dropped at the server (the read-only gate),
// so the editor never sees the viewer's text — proving per-document authZ end to
// end (SC-008).
func TestAuthzEvalViewerCannotMutateCollaboratorCan(t *testing.T) {
	authURL := startAuthEvalStub(t, func(req evalRequest) bool {
		if req.Privilege == "read" {
			return true // everyone may read
		}
		return req.ActorID == actorEditor // only the editor may update-content
	})

	httpBase := testAppHTTP(t, authzevalConfig(authURL))
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	const docID = "e2e-authz-memo"
	preRegister(t, httpBase, docID, "memo")

	editor := dialAsActor(t, wsBase, docID, "memo", actorEditor)
	viewer := dialAsActor(t, wsBase, docID, "memo", actorViewer)
	time.Sleep(150 * time.Millisecond)

	// The editor (collaborator) writes — both clients converge on it.
	editor.insertMemo("editor-text ")
	if !eventually(func() bool {
		return contains(editor.memoText(), "editor-text") && contains(viewer.memoText(), "editor-text")
	}) {
		t.Fatalf("editor's edit did not converge:\n  editor=%q\n  viewer=%q", editor.memoText(), viewer.memoText())
	}

	// The viewer attempts to write — the server drops it (read-only gate), so the
	// editor never sees the viewer's text even after settling.
	viewer.insertMemo("viewer-text ")
	time.Sleep(500 * time.Millisecond) // give a (forbidden) edit time to NOT propagate
	if contains(editor.memoText(), "viewer-text") {
		t.Fatalf("viewer's edit reached the editor — read-only gate failed: %q", editor.memoText())
	}
}

// TestAuthzEvalUnauthenticatedHandshakeIs401 proves that in authzeval mode a
// handshake with no token is rejected at the HTTP layer with 401, before any
// WebSocket upgrade (SC-008). (Open mode, by contrast, authenticates everyone —
// covered by TestSinglePodOpenModeNoAuthRequired.)
func TestAuthzEvalUnauthenticatedHandshakeIs401(t *testing.T) {
	authURL := startAuthEvalStub(t, func(evalRequest) bool { return true })
	httpBase := testAppHTTP(t, authzevalConfig(authURL))

	resp, err := http.Get(httpBase + "/collab/e2e-authz-401")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("handshake status = %d, want 401", resp.StatusCode)
	}
}
