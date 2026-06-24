package service

import (
	"context"
	"errors"
	"testing"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestResolveModeUpdateErrorFailsClosed asserts resolveMode returns an error
// (fails closed) when read is granted but the update-content evaluation errors —
// the connection must not be silently admitted as a viewer.
func TestResolveModeUpdateErrorFailsClosed(t *testing.T) {
	room := newBareRoom(t)
	room.deps.AuthZ = privAuthZ{
		read:   func() (model.AuthDecision, error) { return model.AuthDecision{Allowed: true}, nil },
		update: func() (model.AuthDecision, error) { return model.AuthDecision{}, errors.New("eval failed") },
	}
	if _, err := room.resolveMode(context.Background(), model.Identity{ActorID: "a"}); err == nil {
		t.Fatal("expected resolveMode to fail closed on an update-content error")
	}
}

// TestResolveModeReadErrorFailsClosed asserts resolveMode fails closed when the
// read evaluation itself errors.
func TestResolveModeReadErrorFailsClosed(t *testing.T) {
	room := newBareRoom(t)
	room.deps.AuthZ = privAuthZ{
		read: func() (model.AuthDecision, error) { return model.AuthDecision{}, errors.New("eval failed") },
	}
	if _, err := room.resolveMode(context.Background(), model.Identity{ActorID: "a"}); err == nil {
		t.Fatal("expected resolveMode to fail closed on a read error")
	}
}

// TestTrackAwarenessIDIgnoresMalformed asserts a malformed awareness payload does
// not corrupt the member's awareness mapping (it stays unset).
func TestTrackAwarenessIDIgnoresMalformed(t *testing.T) {
	room := newBareRoom(t)
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}
	// An empty awareness payload (zero client entries) is malformed for tracking.
	room.trackAwarenessID(1, []byte{0x00})
	if room.members[1].hasAwareness {
		t.Fatal("malformed awareness payload should not set the awareness id")
	}
}

// TestEvictAwarenessNoopWithoutMapping asserts dropMember does not panic and emits
// no eviction frame for a member that never announced an awareness id.
func TestEvictAwarenessNoopWithoutMapping(t *testing.T) {
	room := newBareRoom(t)
	peer := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}
	room.members[2] = roomMember{id: 2, conn: peer}
	// Member 1 has no awareness mapping → no eviction frame fanned to peer 2.
	if !room.dropMember(1) {
		t.Fatal("dropMember should report the member was present")
	}
	if peer.count() != 0 {
		t.Fatalf("unexpected eviction frame fanned to peer: %d", peer.count())
	}
}

// TestApplyPeerEphemeralAwareness asserts a peer-pod awareness frame is merged
// into the room awareness and fanned to local members (the multi-pod presence
// path), while a malformed peer frame is dropped.
func TestApplyPeerEphemeralAwareness(t *testing.T) {
	room := newBareRoom(t)
	local := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: local}

	// Build a real awareness frame from a separate awareness instance.
	other := ycrdt.NewAwareness(ycrdt.NewDoc("peer", true, ycrdt.DefaultGCFilter, nil, false))
	other.SetLocalState(ycrdt.MakeObject("user", "remote"))
	update := ycrdt.EncodeAwarenessUpdate(other, []ycrdt.Number{other.ClientID}, nil)
	frame := encodeAwarenessFrame(update)

	room.applyPeerEphemeral(frame)
	if local.count() == 0 {
		t.Fatal("peer awareness frame not fanned to the local member")
	}

	// A malformed peer frame is dropped without panic.
	room.applyPeerEphemeral([]byte{0xff})
}

// TestRecordActivityUnknownSrcIsNoOp asserts recording activity for an unknown
// connection is a no-op (it was already evicted).
func TestRecordActivityUnknownSrcIsNoOp(t *testing.T) {
	room := newBareRoom(t)
	room.recordActivity(99) // no member 99 → no panic, no contribution
	if len(room.contributors) != 0 {
		t.Fatal("recording activity for an unknown src added a contributor")
	}
}

// TestDisconnectUnknownIsNoOp asserts disconnecting an absent connection is a
// no-op (it was already evicted), not a panic.
func TestDisconnectUnknownIsNoOp(t *testing.T) {
	room := newBareRoom(t)
	room.disconnect(42, "gone") // no member 42
}

// TestSweepInactiveDisabled asserts the inactivity sweep is a no-op when the
// downgrade window is zero (the feature is opt-in).
func TestSweepInactiveDisabled(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.CollaboratorInactivity = 0
	c := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: c, mode: model.ModeCollaborator}
	room.sweepInactive()
	if c.count() != 0 {
		t.Fatal("a disabled inactivity sweep emitted a control message")
	}
}

// TestFlushContributionEmptyWindow asserts an empty contribution window sets the
// gauge to zero and emits no bus event.
func TestFlushContributionEmptyWindow(t *testing.T) {
	room := newBareRoom(t)
	metrics := &countingMetrics{}
	room.metrics = metrics
	contrib := &captureContributor{}
	room.deps.Contributor = contrib

	room.flushContribution(context.Background())
	if metrics.contributors.Load() != 0 {
		t.Fatalf("empty window gauge = %d, want 0", metrics.contributors.Load())
	}
	if contrib.lastActorCount() != 0 {
		t.Fatal("empty window emitted a contribution event")
	}
}

// TestFlushContributionToleratesEmitError asserts a Contributor emit failure is
// swallowed (logged, not propagated) — a missed analytics event must not break
// live collaboration (FR-014).
func TestFlushContributionToleratesEmitError(t *testing.T) {
	room := newBareRoom(t)
	room.deps.Contributor = erroringContributor{}
	room.contributors["actor-1"] = struct{}{}
	room.flushContribution(context.Background()) // must not panic/propagate
	if len(room.contributors) != 0 {
		t.Fatal("contribution window was not reset after a flush")
	}
}

// TestTrackAwarenessIDOnlyFirstFrame asserts the awareness id is learned once
// (from the first awareness frame) and not overwritten by a later frame.
func TestTrackAwarenessIDOnlyFirstFrame(t *testing.T) {
	room := newBareRoom(t)
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}

	first := ycrdt.NewAwareness(ycrdt.NewDoc("c1", true, ycrdt.DefaultGCFilter, nil, false))
	first.SetLocalState(ycrdt.MakeObject("u", "1"))
	room.trackAwarenessID(1, ycrdt.EncodeAwarenessUpdate(first, []ycrdt.Number{first.ClientID}, nil))
	got := room.members[1].awarenessID

	second := ycrdt.NewAwareness(ycrdt.NewDoc("c2", true, ycrdt.DefaultGCFilter, nil, false))
	second.SetLocalState(ycrdt.MakeObject("u", "2"))
	room.trackAwarenessID(1, ycrdt.EncodeAwarenessUpdate(second, []ycrdt.Number{second.ClientID}, nil))
	if room.members[1].awarenessID != got {
		t.Fatal("awareness id was overwritten by a later frame")
	}
}

// erroringContributor fails every Contribution emit.
type erroringContributor struct{}

func (erroringContributor) Contribution(context.Context, model.DocumentID, []string) error {
	return errors.New("bus down")
}

// TestApplyPeerEphemeralCustomChannel asserts a peer-pod custom ephemeral (type
// 2) frame is fanned to local members verbatim.
func TestApplyPeerEphemeralCustomChannel(t *testing.T) {
	room := newBareRoom(t)
	local := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: local}
	room.applyPeerEphemeral(encodeEphemeral([]byte("cursor")))
	if local.count() == 0 {
		t.Fatal("peer ephemeral frame not fanned to the local member")
	}
}

// TestReEvaluateNoChangeKeepsMode asserts a re-evaluation that yields the same
// grant does not emit a redundant read-only-state control.
func TestReEvaluateNoChangeKeepsMode(t *testing.T) {
	room := newBareRoom(t) // open AuthZ → everyone is a collaborator
	c := &captureConn{}
	room.members[1] = roomMember{id: 1, conn: c, mode: model.ModeCollaborator}
	room.reEvaluateMembers(context.Background())
	if c.count() != 0 {
		t.Fatal("a no-op re-evaluation emitted a control message")
	}
}

// privAuthZ is an AuthZ stub whose per-privilege outcomes are supplied as
// closures, to drive resolveMode's read/update branches.
type privAuthZ struct {
	read   func() (model.AuthDecision, error)
	update func() (model.AuthDecision, error)
}

func (a privAuthZ) Evaluate(_ context.Context, _ model.Identity, _ model.DocumentID, p model.Privilege) (model.AuthDecision, error) {
	if p == model.PrivilegeRead {
		return a.read()
	}
	if a.update != nil {
		return a.update()
	}
	return model.AuthDecision{Allowed: true}, nil
}
