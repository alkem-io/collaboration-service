package service

import (
	"fmt"
	"sync/atomic"
)

// WHAT THIS IS, AND WHY THE REGISTRY DID NOT ABSORB IT (003 D3, T010).
//
// The port moved document identity, coalesced acquisition, eviction, invalidation
// and handle lifetime into memory.Registry, and D3 called for retiring 002's
// state machine "wherever it duplicates registry semantics". On inspection it
// duplicates none of them, because the two govern DIFFERENT OBJECTS:
//
//	registry  → the DOCUMENT. One live Y.Doc per id, who holds it, when it is
//	            destroyed. Shared across every room that ever serves that id.
//	roomState → the ROOM. Whether THIS serving entity may accept a command, and
//	            which caller owns its teardown. Private to one room.
//
// A document can be perfectly healthy while the room serving it is draining, and
// a room can be Active while its document is being invalidated underneath it —
// which is exactly the case handle.Done() exists to signal. Collapsing the two
// vocabularies would not simplify anything; it would make "is this document
// alive" and "may this room take work" the same question, which they are not.
//
// What 002 DID carry that the registry has now absorbed is gone: the `released`
// bool and the hand-built handle lifetime around it. What remains is the list D3
// explicitly reserved to this service — admission control, teardown idempotency,
// and the idle-release policy that drives Evict, since InProcessRegistry starts
// no goroutines and therefore has no eviction policy of its own.
//
// roomState is the explicit lifecycle state of a Room (spec 002 redesign, FR-012).
// It is the single source of truth for "what may happen now". Transitions are
// centrally enforced by lifecycle.transition, so a teardown or edge can never be
// mis-sequenced per call site — the seam-defect class this redesign eliminates.
// Illegal transitions are unrepresentable: transition refuses to move along an
// edge not in legalTransition's table.
type roomState int32

const (
	// stateMaterializing: the room is loading its snapshot and wiring its
	// subscription. It is NOT yet joinable and its run loop is not yet serving.
	stateMaterializing roomState = iota
	// stateActive: the run loop is serving; the room accepts commands and peer
	// updates. The ONLY state in which enqueue admits new work.
	stateActive
	// stateDraining: a teardown was triggered (shutdown | owner-delete | idle-empty). No new
	// work is accepted; the single teardown sequence (flush → aux-goroutine
	// teardown → close(done) → release) runs. Entered exactly once.
	stateDraining
	// stateClosed: released, done closed, removed from the registry. Terminal.
	stateClosed
)

func (s roomState) String() string {
	switch s {
	case stateMaterializing:
		return "Materializing"
	case stateActive:
		return "Active"
	case stateDraining:
		return "Draining"
	case stateClosed:
		return "Closed"
	default:
		return fmt.Sprintf("roomState(%d)", int32(s))
	}
}

// legalTransition reports whether from→to is a permitted lifecycle edge. This is
// the COMPLETE edge set — every transition not listed here is illegal:
//
//	Materializing → Active     (materialization succeeded; the room starts serving)
//	Materializing → Draining   (torn down before it served — abort, or close while
//	                            still materializing in the singleflight path)
//	Active        → Draining   (shutdown | owner-delete | idle-empty triggered teardown)
//	Draining      → Closed     (the teardown sequence finished)
//
// Closed is terminal. Active→Closed is illegal: an active room MUST pass through
// Draining so the teardown sequence (final snapshot flush, subscription teardown)
// always runs. Both live states (Materializing, Active) lead to teardown via
// Draining — there is no live state from which a room escapes release.
func legalTransition(from, to roomState) bool {
	switch from {
	case stateMaterializing:
		return to == stateActive || to == stateDraining
	case stateActive:
		return to == stateDraining
	case stateDraining:
		return to == stateClosed
	default: // stateClosed (terminal) and any unknown state
		return false
	}
}

// lifecycle holds a Room's current state behind an atomic, with centrally enforced
// transitions. Embed it in Room; the zero value is stateMaterializing, so a
// freshly-constructed room starts in the correct state with no initialization.
type lifecycle struct {
	state atomic.Int32
}

func (l *lifecycle) get() roomState      { return roomState(l.state.Load()) }
func (l *lifecycle) is(s roomState) bool { return l.get() == s }

// transition atomically moves from→to. It returns true iff the room was in `from`
// AND from→to is a legal edge (a CAS). A non-legal edge, or losing the CAS because
// another goroutine already advanced the state, returns false WITHOUT mutating —
// callers treat false as "someone else advanced the state first", which is exactly
// the idempotency the teardown relies on. It never moves along an illegal edge.
func (l *lifecycle) transition(from, to roomState) bool {
	if !legalTransition(from, to) {
		return false
	}
	return l.state.CompareAndSwap(int32(from), int32(to))
}

// activate marks a materialized room ready to serve (Materializing→Active).
func (l *lifecycle) activate() bool { return l.transition(stateMaterializing, stateActive) }

// beginTeardown claims the room's teardown for exactly ONE caller, moving it from
// whatever live state it is in (Materializing — torn down before it served — or
// Active) into Draining. It returns false if the room is already Draining or Closed
// (teardown already claimed), which is the idempotency the single teardown sequence
// relies on — replacing the scattered `released` bool. It loops so it still claims
// teardown if the state advances concurrently (e.g. Materializing→Active racing a
// close): there is no live state from which a room escapes release.
func (l *lifecycle) beginTeardown() bool {
	for {
		s := l.get()
		if s == stateDraining || s == stateClosed {
			return false
		}
		if l.state.CompareAndSwap(int32(s), int32(stateDraining)) {
			return true
		}
	}
}

// finishDraining finalizes teardown (Draining→Closed) after the teardown sequence
// has run.
func (l *lifecycle) finishDraining() bool { return l.transition(stateDraining, stateClosed) }
