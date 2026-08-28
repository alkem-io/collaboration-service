package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

type omitMultiUserMetadata struct{ port.MetadataStore }

func (s omitMultiUserMetadata) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	meta, err := s.MetadataStore.Load(ctx, id)
	meta.IsMultiUser = nil
	return meta, err
}

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

// TestMultiUserGateIsAdditiveAndRollingSafe preserves two-writer behavior for
// licensed documents and for rolling-deploy replies that omit the new field.
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

// TestLiveRoomRefreshesMultiUserDecisionAtJoin pins two ownership invariants:
// the room never writes its stale admission cache back during a flush, and each
// new session refreshes that cache from authoritative metadata.
func TestLiveRoomRefreshesMultiUserDecisionAtJoin(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initial        bool
		latest         bool
		secondReadOnly bool
	}{
		{name: "license removed", initial: true, latest: false, secondReadOnly: true},
		{name: "license granted", initial: false, latest: true, secondReadOnly: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr, deps := testManager(t, fastConfig())
			id := model.DocumentID("live-license-" + tc.name)
			if err := mgr.PreRegister(t.Context(), model.Metadata{
				ID: id, ContentType: model.ContentTypeWhiteboard, IsMultiUser: &tc.initial,
			}); err != nil {
				t.Fatalf("pre-register: %v", err)
			}

			first := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
			first.joinExisting(mgr, id, model.ContentTypeWhiteboard)
			first.observeUpdates()
			if err := deps.meta.Save(t.Context(), model.Metadata{ID: id, IsMultiUser: &tc.latest}); err != nil {
				t.Fatalf("update license decision: %v", err)
			}

			// Force the already-materialized room to persist after the external
			// decision changed. Its cached initial value must not overwrite the
			// server-owned value in the metadata store.
			first.insertText("force stale room flush ")
			waitFor(t, "stale room flush", func() bool {
				return hasControlKind(first, model.ControlSaved)
			})
			stored, err := deps.meta.Load(t.Context(), id)
			if err != nil {
				t.Fatalf("load metadata after room flush: %v", err)
			}
			if stored.IsMultiUser == nil {
				t.Fatalf("stored license decision = nil, want %v", tc.latest)
			}
			if *stored.IsMultiUser != tc.latest {
				t.Fatalf("stored license decision = %v, want %v", *stored.IsMultiUser, tc.latest)
			}

			second := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
			second.joinExisting(mgr, id, model.ContentTypeWhiteboard)
			_, readOnly := readOnlyReason(second)
			if readOnly != tc.secondReadOnly {
				t.Fatalf("second writer readOnly = %v, want %v", readOnly, tc.secondReadOnly)
			}
		})
	}
}

// TestLiveRoomKeepsExplicitDecisionWhenLatestReplyOmitsIt preserves rolling
// compatibility without allowing an old producer to erase a known denial.
func TestLiveRoomKeepsExplicitDecisionWhenLatestReplyOmitsIt(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	id := model.DocumentID("live-license-omitted")
	singleUser := false
	if err := mgr.PreRegister(t.Context(), model.Metadata{
		ID: id, ContentType: model.ContentTypeWhiteboard, IsMultiUser: &singleUser,
	}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	first := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	first.joinExisting(mgr, id, model.ContentTypeWhiteboard)
	mgr.deps.Metadata = omitMultiUserMetadata{MetadataStore: deps.meta}

	second := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
	second.joinExisting(mgr, id, model.ContentTypeWhiteboard)
	reason, readOnly := readOnlyReason(second)
	if !readOnly || reason != model.ReasonMultiUserNotAllowed {
		t.Fatalf("omitted decision changed denial: readOnly=%v reason=%q", readOnly, reason)
	}
}

// TestFullRoomStillRefreshesMultiUserDecision pins that capacity rejection does
// not discard an explicit server-owned decision. A later rolling-deploy reply
// may omit the field, so the room must cache the decision before returning
// ErrRoomFull or it can resurrect a stale entitlement when capacity frees.
func TestFullRoomStillRefreshesMultiUserDecision(t *testing.T) {
	cfg := fastConfig()
	cfg.Limits.MaxConnsPerRoom = 2
	mgr, deps := testManager(t, cfg)
	id := model.DocumentID("full-room-license-refresh")
	multiUser := true
	if err := mgr.PreRegister(t.Context(), model.Metadata{
		ID: id, ContentType: model.ContentTypeWhiteboard, IsMultiUser: &multiUser,
	}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}

	first := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	second := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
	first.joinExisting(mgr, id, model.ContentTypeWhiteboard)
	second.joinExisting(mgr, id, model.ContentTypeWhiteboard)

	singleUser := false
	if err := deps.meta.Save(t.Context(), model.Metadata{ID: id, IsMultiUser: &singleUser}); err != nil {
		t.Fatalf("remove license: %v", err)
	}
	blocked := newFakeClientWithIdentity(t, "33333333-3333-3333-3333-333333333333")
	_, _, err := mgr.Join(t.Context(), JoinRequest{
		ID:       id,
		Content:  model.ContentTypeWhiteboard,
		Identity: blocked.identity,
		Conn:     blocked,
	})
	if !errors.Is(err, ErrRoomFull) {
		t.Fatalf("join at capacity err = %v, want %v", err, ErrRoomFull)
	}

	// Model the next server in a rolling deployment omitting the new field.
	// The leave and subsequent join are serialized by the same room loop.
	mgr.deps.Metadata = omitMultiUserMetadata{MetadataStore: deps.meta}
	first.session.Leave()
	next := newFakeClientWithIdentity(t, "44444444-4444-4444-4444-444444444444")
	next.joinExisting(mgr, id, model.ContentTypeWhiteboard)
	reason, readOnly := readOnlyReason(next)
	if !readOnly || reason != model.ReasonMultiUserNotAllowed {
		t.Fatalf("omitted decision after full-room refresh: readOnly=%v reason=%q", readOnly, reason)
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
