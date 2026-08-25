package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fixedAuthZ is an AuthZ stub with per-privilege fixed outcomes, to drive the
// viewer/collaborator gate (T014). A nil err with Allowed=false is a clean deny;
// a non-nil err is a fail-closed transport failure.
type fixedAuthZ struct {
	read   model.AuthDecision
	update model.AuthDecision
	err    error
}

func (a fixedAuthZ) Evaluate(_ context.Context, _ model.Identity, _ model.DocumentID, p model.Privilege) (model.AuthDecision, error) {
	if a.err != nil {
		return model.AuthDecision{}, a.err
	}
	if p == model.PrivilegeRead {
		return a.read, nil
	}
	return a.update, nil
}

// authZDeps wires the in-memory metadata-store/blob with a custom AuthZ + open authN.
func authZDeps(t *testing.T, authz fixedAuthZ) testDeps {
	t.Helper()
	d := newTestDeps()
	d.AuthZ = authz
	return d
}

// allow is a clean grant decision.
var allow = model.AuthDecision{Allowed: true}

// deny is a clean denial decision.
var deny = model.AuthDecision{Allowed: false}

// TestViewerUpdateNotApplied asserts a viewer (read granted, update denied) gets
// a read-only-state control and its sync updates are NOT applied, while a
// collaborator's edit converges (SC-008, T014).
func TestViewerUpdateNotApplied(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{read: allow, update: deny})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	viewer := newFakeClient(t)
	viewer.join(mgr, "viewer-doc", model.ContentTypeMemo)
	viewer.observeUpdates()

	// The viewer is told it is read-only on join.
	if !hasReadOnly(viewer, true) {
		t.Fatal("viewer did not receive a read-only-state control on join")
	}

	// The viewer attempts an edit; it must not reach the authoritative doc.
	viewer.insertText("viewer-edit ")

	// Give the room time to (not) apply it, then assert it never persisted to the
	// server doc by reconnecting a collaborator and checking the content.
	time.Sleep(60 * time.Millisecond)

	deps2 := deps // collaborator on the same doc/store
	deps2.AuthZ = fixedAuthZ{read: allow, update: allow}
	mgr2 := NewManager(deps2.Deps, fastConfig(), nil, nil)
	collab := newFakeClient(t)
	collab.join(mgr2, "viewer-doc", model.ContentTypeMemo)
	if contains(collab.text(), "viewer-edit") {
		t.Fatal("a viewer's update reached the document")
	}
}

// TestCollaboratorUpdateApplied asserts a collaborator (read + update granted)
// can mutate the document (the positive half of SC-008).
func TestCollaboratorUpdateApplied(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{read: allow, update: allow})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "collab-doc", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("collab-edit ")

	b := newFakeClient(t)
	b.join(mgr, "collab-doc", model.ContentTypeMemo)
	waitFor(t, "collaborator edit converges", func() bool {
		return contains(b.text(), "collab-edit")
	})
}

// TestReadDeniedRefusesJoin asserts a clean read denial refuses the join with
// ErrForbidden (the connection is not admitted).
func TestReadDeniedRefusesJoin(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{read: deny, update: deny})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	_, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: "forbidden", Content: model.ContentTypeMemo, Conn: &captureConn{},
	})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("join err = %v, want ErrForbidden", err)
	}
}

// TestAuthZErrorFailsClosed asserts an AuthZ transport error refuses the join
// (fail closed) — never silently admits as collaborator or viewer (§V).
func TestAuthZErrorFailsClosed(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{err: errors.New("auth service down")})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	_, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: "authz-down", Content: model.ContentTypeMemo, Conn: &captureConn{},
	})
	if err == nil {
		t.Fatal("expected join to fail closed on an AuthZ error")
	}
	if errors.Is(err, ErrForbidden) {
		t.Fatal("a fail-closed authZ error must be distinct from a clean deny")
	}
}

// TestMaxConnsPerRoomCap asserts a connection beyond the room cap is refused with
// ErrRoomFull and existing members are unaffected (FR-024, SC-009).
func TestMaxConnsPerRoomCap(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.MaxConnsPerRoom = 1
	cfg.Limits.UpdateRatePerSec = 0 // isolate the conn cap
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "capped", model.ContentTypeMemo)
	a.observeUpdates()

	// The second join exceeds the cap.
	_, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: "capped", Content: model.ContentTypeMemo, Conn: &captureConn{},
	})
	if !errors.Is(err, ErrRoomFull) {
		t.Fatalf("second join err = %v, want ErrRoomFull", err)
	}

	// The first member is unaffected — it still converges its own edits.
	a.insertText("still-here ")
	waitFor(t, "first member unaffected", func() bool { return contains(a.text(), "still-here") })
}

// TestUpdateRateLimitDisconnects asserts a connection that breaches the per-conn
// update rate is disconnected with a session-end control, and other collaborators
// keep working (FR-024, SC-009).
func TestUpdateRateLimitDisconnects(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.UpdateRatePerSec = 5
	cfg.Limits.UpdateBurst = 2
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "rate", model.ContentTypeMemo)
	a.observeUpdates()

	b := newFakeClient(t)
	b.join(mgr, "rate", model.ContentTypeMemo)
	b.observeUpdates()

	// Flood from A: many rapid edits exceed the 2-token burst at 5/s.
	for i := 0; i < 30; i++ {
		a.insertText("x")
	}

	waitFor(t, "rate-limited client disconnected", func() bool {
		for _, m := range controlMessages(a) {
			if m.Kind == model.ControlSessionEnd && m.Code == model.CodeUpdateRateExceeded {
				return true
			}
		}
		return false
	})

	// B is unaffected — it can still edit and converge.
	b.insertText("b-ok ")
	waitFor(t, "other collaborator unaffected", func() bool { return contains(b.text(), "b-ok") })
}

// TestMaxDocSizeDisconnects asserts an update that pushes the encoded doc past
// MaxDocBytes disconnects the offending connection (FR-024, SC-009).
func TestMaxDocSizeDisconnects(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.MaxDocBytes = 256 // tiny, so a modest edit breaches it
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "big-doc", model.ContentTypeMemo)
	a.observeUpdates()

	// A large insert pushes the encoded snapshot past the 256-byte cap.
	big := ""
	for i := 0; i < 200; i++ {
		big += "lorem "
	}
	a.insertText(big)

	waitFor(t, "oversized doc disconnects", func() bool {
		for _, m := range controlMessages(a) {
			if m.Kind == model.ControlSessionEnd && m.Code == model.CodeDocumentSizeLimitExceeded {
				return true
			}
		}
		return false
	})
}

// TestMaxDocSizeRejectsBeforeCommit asserts that an oversized update is rejected
// PRE-COMMIT: it never mutates or broadcasts the authoritative doc. A second
// collaborator must never receive the oversized payload, and a small follow-up
// edit from that collaborator must converge against the pre-rejection state (the
// rejected bytes are absent). This is the "enforce before committing" contract —
// the offender is disconnected, but no live mutation/broadcast leaked first.
func TestMaxDocSizeRejectsBeforeCommit(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.MaxDocBytes = 512 // tiny, so a modest edit breaches it
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "big-doc-2", model.ContentTypeMemo)
	a.observeUpdates()

	b := newFakeClient(t)
	b.join(mgr, "big-doc-2", model.ContentTypeMemo)
	b.observeUpdates()

	// A sends an oversized insert: applying it would push the encoded snapshot past
	// the 512-byte cap, so it must be rejected before any commit/broadcast.
	oversized := ""
	for i := 0; i < 300; i++ {
		oversized += "OVERSIZE "
	}
	a.insertText(oversized)

	// A is disconnected for the breach.
	waitFor(t, "oversized sender disconnected", func() bool {
		for _, m := range controlMessages(a) {
			if m.Kind == model.ControlSessionEnd && m.Code == model.CodeDocumentSizeLimitExceeded {
				return true
			}
		}
		return false
	})

	// B does a small, in-bounds edit and converges. The rejected oversized bytes
	// must be absent from B's converged doc — proving the live doc was never
	// mutated/broadcast by the rejected write.
	b.insertText("small-ok ")
	waitFor(t, "peer edit converges", func() bool { return contains(b.text(), "small-ok") })
	if contains(b.text(), "OVERSIZE") {
		t.Fatal("oversized update leaked to a peer: it must be rejected before commit/broadcast")
	}
}

// TestMaxDocSizeDisabledAllowsAnySize asserts that with MaxDocBytes disabled (0),
// the pre-commit size gate is a no-op: a large update applies and broadcasts
// normally (the limit check short-circuits without scratch-applying).
func TestMaxDocSizeDisabledAllowsAnySize(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.MaxDocBytes = 0 // disabled
	cfg.Limits.UpdateRatePerSec = 0
	deps := newTestDeps()
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClient(t)
	a.join(mgr, "no-limit", model.ContentTypeMemo)
	a.observeUpdates()

	big := ""
	for i := 0; i < 500; i++ {
		big += "lorem "
	}
	a.insertText(big)
	waitFor(t, "large edit applies when limit disabled", func() bool { return contains(a.text(), "lorem") })

	// No disconnect: the size limit is off.
	for _, m := range controlMessages(a) {
		if m.Kind == model.ControlSessionEnd {
			t.Fatalf("unexpected disconnect with MaxDocBytes disabled: %q", m.Code)
		}
	}
}

// controlMessages returns a copy of every control message a client received.
func controlMessages(c *fakeClient) []model.ControlMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.ControlMessage, len(c.control))
	copy(out, c.control)
	return out
}
