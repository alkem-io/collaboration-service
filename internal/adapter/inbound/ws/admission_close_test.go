package ws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// fixedAuthZ answers every evaluation the same way, or fails.
type fixedAuthZ struct {
	allowed bool
	err     error
}

func (f fixedAuthZ) Evaluate(context.Context, model.Identity, model.DocumentID, model.Privilege) (model.AuthDecision, error) {
	if f.err != nil {
		return model.AuthDecision{}, f.err
	}
	return model.AuthDecision{Allowed: f.allowed}, nil
}

func serverWithAuthZ(t *testing.T, authz fixedAuthZ) string {
	t.Helper()
	mgr := service.NewManager(service.Deps{
		Metadata:   metainmem.New(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authz,
	}, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  40 * time.Millisecond,
		SendBuffer:   256,
	}, nil, zap.NewNop())
	t.Cleanup(mgr.Close)
	return newTestServerWithManager(t, mgr)
}

// closeStatusOf dials, reads until the server closes, and returns the status.
func closeStatusOf(t *testing.T, base, doc string) websocket.StatusCode {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, base+"/collab/"+doc+"?type=memo", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	_, _, readErr := conn.Read(ctx)
	if readErr == nil {
		t.Fatal("the server admitted a connection it should have refused")
	}
	return websocket.CloseStatus(readErr)
}

// TestADeniedReadClosesWithPolicyViolation asserts the client can tell the
// difference between "you may not have this" and "something broke".
//
// 1008 is a verdict: the client must not reconnect in a loop, because the answer
// will be the same. The reason carries no signal about whether the document
// exists — a denied caller learns only that it was denied.
func TestADeniedReadClosesWithPolicyViolation(t *testing.T) {
	base := serverWithAuthZ(t, fixedAuthZ{allowed: false})
	if got := closeStatusOf(t, base, "denied-doc"); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status = %d, want StatusPolicyViolation (%d)", got, websocket.StatusPolicyViolation)
	}
}

// TestAnAuthZOutageClosesWithATransientStatus asserts an unreachable
// authorization backend does NOT close as a policy violation.
//
// The distinction is the whole point of failing closed on the error rather than
// coercing it to a denial: a client that reads 1008 stops trying, so a five-minute
// authz outage would look to every user like a permanent loss of access to their
// own documents. 1011 keeps the reconnect loop alive.
func TestAnAuthZOutageClosesWithATransientStatus(t *testing.T) {
	base := serverWithAuthZ(t, fixedAuthZ{err: errors.New("authz unreachable")})
	got := closeStatusOf(t, base, "outage-doc")
	if got == websocket.StatusPolicyViolation {
		t.Fatal("an authorization outage closed as a policy violation; clients would treat a transient failure as a permanent denial and stop retrying")
	}
	if got != websocket.StatusInternalError {
		t.Fatalf("close status = %d, want StatusInternalError (%d)", got, websocket.StatusInternalError)
	}
}

// TestEachConnectionIsAuthorizedAfresh asserts a reconnect is a NEW session.
//
// This is the half of the session-lifetime contract that makes it defensible: an
// established socket keeps its capability until it closes, so the only thing that
// makes a revocation take effect is the next connection. If a reconnect reused
// anything from a previous one, revocation would never take effect at all.
func TestEachConnectionIsAuthorizedAfresh(t *testing.T) {
	authz := &togglingAuthZ{allowed: true}
	mgr := service.NewManager(service.Deps{
		Metadata:   metainmem.New(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authz,
	}, service.RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  40 * time.Millisecond,
		SendBuffer:   256,
	}, nil, zap.NewNop())
	t.Cleanup(mgr.Close)
	base := newTestServerWithManager(t, mgr)

	// First connection: admitted. Reading the server's SyncStep1 proves it, since
	// a refused join closes instead of sending anything.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	first := dialClient(t, base, "reconnect-doc", model.ContentTypeMemo)
	if !first.pump(ctx) {
		t.Fatal("the first connection was not admitted")
	}
	cancel()

	// Access is revoked while nobody is connected.
	authz.set(false)

	if got := closeStatusOf(t, base, "reconnect-doc"); got != websocket.StatusPolicyViolation {
		t.Fatalf("reconnect close status = %d, want StatusPolicyViolation (%d); a new socket must be evaluated again, or a revocation never takes effect",
			got, websocket.StatusPolicyViolation)
	}
}

type togglingAuthZ struct{ allowed bool }

func (t *togglingAuthZ) set(v bool) { t.allowed = v }

func (t *togglingAuthZ) Evaluate(context.Context, model.Identity, model.DocumentID, model.Privilege) (model.AuthDecision, error) {
	return model.AuthDecision{Allowed: t.allowed}, nil
}
