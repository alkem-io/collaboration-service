package service

import (
	"context"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// controlOf returns the first control message of the given kind the client
// received, or false when none was seen.
func controlOf(c *fakeClient, kind model.ControlKind) (model.ControlMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.control {
		if m.Kind == kind {
			return m, true
		}
	}
	return model.ControlMessage{}, false
}

// readOnlyReason returns the Reason of the first read-only-state{readOnly:true}
// control the client received, or ("", false) when none was seen.
func readOnlyReason(c *fakeClient) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.control {
		if m.Kind == model.ControlReadOnlyState && m.IsReadOnly() {
			return m.Reason, true
		}
	}
	return "", false
}

// TestJoinViewerReadOnlyReasonNoUpdateAccess asserts an authenticated actor that
// is granted read but denied update-content joins read-only with the
// `no-update-access` reason (OPEN-1), preserving today's read-only UX.
func TestJoinViewerReadOnlyReasonNoUpdateAccess(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{read: allow, update: deny})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	viewer := newFakeClientWithIdentity(t, "77777777-7777-7777-7777-777777777777")
	viewer.join(mgr, "ro-no-update", model.ContentTypeMemo)

	waitFor(t, "read-only-state on join", func() bool {
		_, ok := readOnlyReason(viewer)
		return ok
	})
	reason, _ := readOnlyReason(viewer)
	if reason != model.ReasonNoUpdateAccess {
		t.Fatalf("read-only reason = %q, want %q", reason, model.ReasonNoUpdateAccess)
	}
}

// TestJoinViewerReadOnlyReasonNotAuthenticated asserts an anonymous connection
// (nil ActorID) granted read but denied update-content joins read-only with the
// `not-authenticated` reason (OPEN-1).
func TestJoinViewerReadOnlyReasonNotAuthenticated(t *testing.T) {
	deps := authZDeps(t, fixedAuthZ{read: allow, update: deny})
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	viewer := newFakeClient(t) // open mode: nil ActorID
	viewer.join(mgr, "ro-anon", model.ContentTypeMemo)

	waitFor(t, "read-only-state on join", func() bool {
		_, ok := readOnlyReason(viewer)
		return ok
	})
	reason, _ := readOnlyReason(viewer)
	if reason != model.ReasonNotAuthenticated {
		t.Fatalf("read-only reason = %q, want %q", reason, model.ReasonNotAuthenticated)
	}
}

// TestSingleUserDocumentDowngradesOnlyTheSecondWriter pins the license gate at
// the room's serialized join boundary: the first update-authorized member keeps
// write access, while the next one joins as a viewer with the existing typed
// multi-user reason on both control channels.
func TestSingleUserDocumentDowngradesOnlyTheSecondWriter(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	isMultiUser := false
	if err := mgr.PreRegister(context.Background(), model.Metadata{
		ID: "single-user-license", ContentType: model.ContentTypeWhiteboard, IsMultiUser: &isMultiUser,
	}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	first := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	second := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
	first.joinExisting(mgr, "single-user-license", model.ContentTypeWhiteboard)
	second.joinExisting(mgr, "single-user-license", model.ContentTypeWhiteboard)

	if _, ok := readOnlyReason(first); ok {
		t.Fatal("first writer was downgraded")
	}
	reason, ok := readOnlyReason(second)
	if !ok || reason != model.ReasonMultiUserNotAllowed {
		t.Fatalf("second writer read-only reason = %q, %v; want %q", reason, ok, model.ReasonMultiUserNotAllowed)
	}
	mode, ok := controlOf(second, model.ControlCollaboratorMode)
	if !ok || mode.Mode != model.ModeViewer || mode.Reason != model.ReasonMultiUserNotAllowed {
		t.Fatalf("second writer collaborator-mode = %+v, %v", mode, ok)
	}
}

func TestMultiUserGateIsAdditiveAndRollingSafe(t *testing.T) {
	licensed := true
	for _, tc := range []struct {
		name        string
		isMultiUser *bool
	}{
		{name: "licensed", isMultiUser: &licensed},
		{name: "field absent", isMultiUser: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, _ := testManager(t, fastConfig())
			id := model.DocumentID("multi-user-" + tc.name)
			if err := mgr.PreRegister(context.Background(), model.Metadata{
				ID: id, ContentType: model.ContentTypeWhiteboard, IsMultiUser: tc.isMultiUser,
			}); err != nil {
				t.Fatalf("pre-register: %v", err)
			}

			first := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
			second := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
			first.joinExisting(mgr, id, model.ContentTypeWhiteboard)
			second.joinExisting(mgr, id, model.ContentTypeWhiteboard)
			if reason, downgraded := readOnlyReason(second); downgraded {
				t.Fatalf("second writer was downgraded with reason %q", reason)
			}
		})
	}
}

// TestInactivityDowngradeReason asserts an idle collaborator downgraded to viewer
// receives both a read-only-state{reason:inactivity} and a collaborator-mode
// {mode:viewer, reason:inactivity} control (OPEN-1).
func TestInactivityDowngradeReason(t *testing.T) {
	cfg := fastConfig()
	cfg.CollaboratorInactivity = 30 * time.Millisecond
	mgr, _ := testManager(t, cfg)

	a := newFakeClientWithIdentity(t, "88888888-8888-8888-8888-888888888888")
	a.join(mgr, "downgrade-reason", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("active ") // one mutation, then go idle

	waitFor(t, "collaborator-mode downgrade control", func() bool {
		_, ok := controlOf(a, model.ControlCollaboratorMode)
		return ok
	})

	ro, ok := readOnlyReason(a)
	if !ok {
		t.Fatal("no read-only-state control on inactivity downgrade")
	}
	if ro != model.ReasonInactivity {
		t.Fatalf("read-only reason = %q, want %q", ro, model.ReasonInactivity)
	}

	cm, _ := controlOf(a, model.ControlCollaboratorMode)
	if cm.Mode != model.ModeViewer {
		t.Fatalf("collaborator-mode mode = %q, want viewer", cm.Mode)
	}
	if cm.Reason != model.ReasonInactivity {
		t.Fatalf("collaborator-mode reason = %q, want %q", cm.Reason, model.ReasonInactivity)
	}
}

// TestReadOnlyReasonForIdentity is the focused unit test for the identity→reason
// mapping (OPEN-1): anonymous → not-authenticated, authenticated → no-update-access.
func TestReadOnlyReasonForIdentity(t *testing.T) {
	if got := readOnlyReasonForIdentity(model.Identity{}); got != model.ReasonNotAuthenticated {
		t.Errorf("anonymous reason = %q, want %q", got, model.ReasonNotAuthenticated)
	}
	if got := readOnlyReasonForIdentity(testIdentity("x")); got != model.ReasonNoUpdateAccess {
		t.Errorf("authenticated reason = %q, want %q", got, model.ReasonNoUpdateAccess)
	}
}
