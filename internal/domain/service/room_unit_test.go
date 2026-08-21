package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

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
	other := ycrdt.NewAwareness(ycrdt.NewDoc("peer"))
	_ = other.SetLocalState(ycrdt.MakeObject("user", "remote"))
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
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}
	room.flushContribution(context.Background()) // must not panic/propagate
	if _, retained := room.contributors[actor]; !retained {
		t.Fatal("failed contribution emit did not retain the actor for retry")
	}
}

// TestTrackAwarenessIDOnlyFirstFrame asserts the awareness id is learned once
// (from the first awareness frame) and not overwritten by a later frame.
func TestTrackAwarenessIDOnlyFirstFrame(t *testing.T) {
	room := newBareRoom(t)
	room.members[1] = roomMember{id: 1, conn: &captureConn{}}

	first := ycrdt.NewAwareness(ycrdt.NewDoc("c1"))
	_ = first.SetLocalState(ycrdt.MakeObject("u", "1"))
	room.trackAwarenessID(1, ycrdt.EncodeAwarenessUpdate(first, []ycrdt.Number{first.ClientID}, nil))
	got := room.members[1].awarenessID

	second := ycrdt.NewAwareness(ycrdt.NewDoc("c2"))
	_ = second.SetLocalState(ycrdt.MakeObject("u", "2"))
	room.trackAwarenessID(1, ycrdt.EncodeAwarenessUpdate(second, []ycrdt.Number{second.ClientID}, nil))
	if room.members[1].awarenessID != got {
		t.Fatal("awareness id was overwritten by a later frame")
	}
}

// erroringContributor fails every Contribution emit.
type erroringContributor struct{}

func (erroringContributor) Contribution(context.Context, model.DocumentID, []uuid.UUID) error {
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
