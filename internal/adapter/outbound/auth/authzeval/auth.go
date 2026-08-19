// Package authzeval is the Alkemio per-document AuthZ adapter (port.AuthZ):
//
//   - AuthZ — delegates per-document read/update-content decisions to the
//     authorization-evaluation-service over h2c HTTP/2
//     (POST /internal/auth/evaluate), guarded by a sony/gobreaker circuit breaker
//     and FAILING CLOSED (anti-pattern 13). It resolves the document's
//     authorizationPolicyId via the MetadataStore (OPEN-1) and evaluates
//     evaluate(actorId, "read" | "update-content", policyId).
//
// AuthN is NOT this package's concern: header-trusting handshake AuthN lives in
// the sibling `header` adapter, so handshake AuthN is selected independently of
// AuthZ (AUTH_MODE vs AUTHZ_MODE). This adapter is selected by
// AUTHZ_MODE=authzeval.
//
// The h2c + gobreaker client below reuses the file-service/wopi pattern verbatim
// (research.md OPEN-1); the only collab-specific addition is the
// documentId→policyId resolution.
package authzeval

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	gobreaker "github.com/sony/gobreaker/v2"
	"golang.org/x/net/http2"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// PolicyResolver resolves a document's authorization policy id (OPEN-1). The
// MetadataStore implements it (Load returns Metadata.AuthorizationPolicyID); it
// is narrowed here so the adapter depends only on what it needs.
type PolicyResolver interface {
	// PolicyID returns the document's authorization policy id (or an error the
	// caller treats as fail-closed).
	PolicyID(ctx context.Context, id model.DocumentID) (string, error)
}

// Config carries the authzeval settings (env: AUTH_SERVICE_URL + breaker
// tunables).
type Config struct {
	// ServiceURL is the authorization-evaluation-service base URL (h2c).
	ServiceURL string
	// BreakerFailureThreshold trips the breaker after this many consecutive
	// failures (default 3).
	BreakerFailureThreshold int
	// BreakerTimeout is how long the breaker stays open before half-open
	// (default 15s).
	BreakerTimeout time.Duration
	// BreakerHalfOpenMaxReqs is the probe count allowed in half-open (default 2).
	BreakerHalfOpenMaxReqs int
	// HTTPClient overrides the h2c client (tests); nil builds the default.
	HTTPClient *http.Client
}

// Adapter implements port.Auth and port.AuthZ for Alkemio deployments.
type Adapter struct {
	baseURL  string
	client   *http.Client
	breaker  *gobreaker.CircuitBreaker[model.AuthDecision]
	policies PolicyResolver
}

type evaluateRequest struct {
	ActorID               string `json:"actorId"`
	Privilege             string `json:"privilege"`
	AuthorizationPolicyID string `json:"authorizationPolicyId"`
}

type evaluateResponse struct {
	Allowed bool          `json:"allowed"`
	Reason  string        `json:"reason"`
	Error   *errorDetails `json:"error,omitempty"`
}

type errorDetails struct {
	Code         string `json:"code"`
	Dependency   string `json:"dependency,omitempty"`
	RetryAfterMs int    `json:"retryAfterMs,omitempty"`
}

// New constructs the authzeval adapter over the auth-evaluation-service and a
// policy resolver (the MetadataStore).
func New(cfg Config, policies PolicyResolver) *Adapter {
	failures := cfg.BreakerFailureThreshold
	if failures <= 0 {
		failures = 3
	}
	timeout := cfg.BreakerTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	halfOpen := cfg.BreakerHalfOpenMaxReqs
	if halfOpen <= 0 {
		halfOpen = 2
	}
	client := cfg.HTTPClient
	if client == nil {
		client = newH2CClient()
	}
	breaker := gobreaker.NewCircuitBreaker[model.AuthDecision](gobreaker.Settings{
		Name:        "authzeval",
		MaxRequests: uint32(halfOpen), //nolint:gosec // bounded positive above.
		Timeout:     timeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return int(c.ConsecutiveFailures) >= failures
		},
	})
	return &Adapter{
		// Trim a trailing slash so the request path is "/internal/auth/evaluate"
		// and never "//internal/auth/evaluate" when ServiceURL is configured with
		// a trailing slash (which can break routing on some gateways).
		baseURL:  strings.TrimRight(cfg.ServiceURL, "/"),
		client:   client,
		breaker:  breaker,
		policies: policies,
	}
}

// Evaluate decides whether identity holds privilege on the document. It resolves
// the document's authorizationPolicyId (OPEN-1) and asks the
// authorization-evaluation-service, guarded by the breaker. Any error (resolve
// failure, transport failure, open breaker, degraded service) is returned as an
// error so the caller fails closed — never a clean Allowed result.
func (a *Adapter) Evaluate(ctx context.Context, identity model.Identity, id model.DocumentID, privilege model.Privilege) (model.AuthDecision, error) {
	// Fail CLOSED on a missing resolver. The adapter cannot resolve a document's
	// policy id without it, so calling through would nil-panic in the auth path
	// (worse than a deny). Return an error instead so the caller denies access — an
	// authzeval adapter with no policy resolver is a misconfiguration, never an
	// allow.
	if a.policies == nil {
		return model.AuthDecision{}, fmt.Errorf("authzeval: no policy resolver configured")
	}
	policyID, err := a.policies.PolicyID(ctx, id)
	if err != nil {
		return model.AuthDecision{}, fmt.Errorf("resolve authorization policy for %s: %w", id, err)
	}
	if policyID == "" {
		return model.AuthDecision{}, fmt.Errorf("document %s has no authorization policy", id)
	}

	return a.breaker.Execute(func() (model.AuthDecision, error) {
		return a.doEvaluate(ctx, identity.ActorID, string(privilege), policyID)
	})
}

// doEvaluate performs the h2c POST to /internal/auth/evaluate. Only an HTTP 200
// with a decoded body yields a decision; everything else is an error (fail
// closed).
func (a *Adapter) doEvaluate(ctx context.Context, actorID, privilege, policyID string) (model.AuthDecision, error) {
	payload, err := json.Marshal(evaluateRequest{
		ActorID:               actorID,
		Privilege:             privilege,
		AuthorizationPolicyID: policyID,
	})
	if err != nil {
		return model.AuthDecision{}, fmt.Errorf("marshal auth request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/internal/auth/evaluate", bytes.NewReader(payload))
	if err != nil {
		return model.AuthDecision{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return model.AuthDecision{}, fmt.Errorf("h2c auth request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	const maxBody = 64 << 10 // auth responses are small JSON
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return model.AuthDecision{}, fmt.Errorf("read auth response: %w", err)
	}

	if resp.StatusCode == http.StatusServiceUnavailable {
		var er evaluateResponse
		if jsonErr := json.Unmarshal(body, &er); jsonErr == nil && er.Error != nil {
			return model.AuthDecision{}, fmt.Errorf("auth service degraded: %s", er.Error.Code)
		}
		return model.AuthDecision{}, fmt.Errorf("auth service unavailable (HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return model.AuthDecision{}, fmt.Errorf("auth service error (HTTP %d)", resp.StatusCode)
	}

	var er evaluateResponse
	if err := json.Unmarshal(body, &er); err != nil {
		return model.AuthDecision{}, fmt.Errorf("unmarshal auth response: %w", err)
	}
	return model.AuthDecision{Allowed: er.Allowed, Reason: er.Reason}, nil
}

// newH2CClient builds an HTTP/2 cleartext client with persistent connection
// multiplexing (the file-service/wopi pattern). http2.Transport re-establishes
// the TCP connection automatically if it drops.
func newH2CClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
}

// compile-time assertion that Adapter satisfies the per-document AuthZ port.
// (Handshake AuthN now lives in the sibling `header`/`oidc`/`open` adapters.)
var _ port.AuthZ = (*Adapter)(nil)
