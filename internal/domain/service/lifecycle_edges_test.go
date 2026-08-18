package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend/memory"
	"go.uber.org/zap"
)

// TestIllegalTransitionsAreRefusedWithoutMutating covers the edge table's reject
// branch, which is the property the whole teardown sequence rests on.
//
// transition is a CAS that ALSO validates the edge. Returning false must mean the
// state is unchanged — callers read false as "someone else advanced it first" and
// carry on, so a transition that refused an edge but moved the state anyway would
// leave a room in a state nobody believes it is in, with two callers both thinking
// the other owns the teardown.
//
// Closed is terminal in particular: a room that could leave Closed would be
// re-served after its document was destroyed.
func TestIllegalTransitionsAreRefusedWithoutMutating(t *testing.T) {
	for _, tc := range []struct {
		name string
		from roomState
		to   roomState
	}{
		{"materializing cannot jump to closed", stateMaterializing, stateClosed},
		{"active cannot go back to materializing", stateActive, stateMaterializing},
		{"active cannot jump to closed", stateActive, stateClosed},
		{"draining cannot go back to active", stateDraining, stateActive},
		{"closed is terminal", stateClosed, stateActive},
		{"closed cannot re-drain", stateClosed, stateDraining},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lc lifecycle
			lc.state.Store(int32(tc.from))

			if lc.transition(tc.from, tc.to) {
				t.Fatalf("transition %v→%v was allowed; it is not a legal edge", tc.from, tc.to)
			}
			if got := lc.get(); got != tc.from {
				t.Fatalf("state moved to %v on a REFUSED transition (was %v); callers read false as 'someone else advanced it' and would both proceed", got, tc.from)
			}
		})
	}
}

// TestLegalTransitionsAdvanceTheState is the positive half, so the test above
// cannot pass against a transition that always refuses.
func TestLegalTransitionsAdvanceTheState(t *testing.T) {
	for _, tc := range []struct {
		from roomState
		to   roomState
	}{
		{stateMaterializing, stateActive},
		{stateMaterializing, stateDraining},
		{stateActive, stateDraining},
		{stateDraining, stateClosed},
	} {
		var lc lifecycle
		lc.state.Store(int32(tc.from))
		if !lc.transition(tc.from, tc.to) {
			t.Fatalf("legal transition %v→%v was refused", tc.from, tc.to)
		}
		if got := lc.get(); got != tc.to {
			t.Fatalf("state = %v after %v→%v, want %v", got, tc.from, tc.to, tc.to)
		}
	}
}

// TestTransitionLosesTheCASWhenAnotherCallerAdvancedFirst covers the third
// outcome: a legal edge whose `from` no longer holds. This is the idempotency the
// teardown relies on — exactly one caller claims it.
func TestTransitionLosesTheCASWhenAnotherCallerAdvancedFirst(t *testing.T) {
	var lc lifecycle
	lc.state.Store(int32(stateDraining)) // someone already began teardown

	if lc.transition(stateActive, stateDraining) {
		t.Fatal("transition succeeded from a state the room had already left; two callers would each believe they own the teardown")
	}
	if got := lc.get(); got != stateDraining {
		t.Fatalf("state = %v, want it untouched at Draining", got)
	}
}

// TestEnqueueGivesUpWhenTheProducerContextExpires covers enqueueCtx's bounded
// slow path.
//
// The command buffer being full means the run loop is behind or wedged. A
// producer that blocked there indefinitely — the lifecycle consumer, a Leave, a
// re-evaluation — would be wedged behind it, and for the single-threaded
// lifecycle consumer that means every later event stalls too. Giving up at the
// caller's deadline is what keeps one stuck room from freezing the bus.
func TestEnqueueGivesUpWhenTheProducerContextExpires(t *testing.T) {
	r := newBareRoom(t)
	r.lc.state.Store(int32(stateActive))
	// A full buffer with no run loop draining it: every send must take the slow path.
	r.commands = make(chan command, 1)
	r.commands <- command{kind: cmdPersist}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	if r.enqueueCtx(ctx, command{kind: cmdPersist}) {
		t.Fatal("enqueue succeeded against a full buffer with no consumer")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("enqueue blocked for %v; it must give up at the caller's deadline, or one wedged room freezes the single-threaded lifecycle consumer", elapsed)
	}
}

// TestEnqueueRefusesOnceTheRoomLeavesActive covers the state guard, which refuses
// producers BEFORE done is closed so Join/Purge retry into a fresh room rather
// than queueing work onto a room that is going away.
func TestEnqueueRefusesOnceTheRoomLeavesActive(t *testing.T) {
	r := newBareRoom(t)
	r.commands = make(chan command, 4)

	for _, state := range []roomState{stateMaterializing, stateDraining, stateClosed} {
		r.lc.state.Store(int32(state))
		if r.enqueue(command{kind: cmdPersist}) {
			t.Fatalf("enqueue accepted work while the room was %v; only an Active room may take new commands", state)
		}
	}
	if len(r.commands) != 0 {
		t.Fatalf("%d commands were queued onto a non-Active room", len(r.commands))
	}
}

// failingCloseRegistry reports an error from Close, the branch closeRegistry logs.
type failingCloseRegistry struct {
	memory.Registry
	err error
}

func (f failingCloseRegistry) Close() error { return f.err }

// TestCloseRegistryLogsAFailureRatherThanPanicking covers closeRegistry's error
// path. It runs during shutdown, after the drain, so an unhandled error here
// would turn a clean shutdown into a crash on the way out — losing the exit
// status that tells an orchestrator the drain succeeded.
func TestCloseRegistryLogsAFailureRatherThanPanicking(_ *testing.T) {
	m := NewManager(newTestDeps().Deps, fastConfig(), nil, zap.NewNop())
	m.registry = failingCloseRegistry{Registry: memory.NewRegistry(), err: errors.New("registry busy")}

	m.closeRegistry() // must return normally

	// And the success path, so the test cannot pass against a no-op.
	m2 := NewManager(newTestDeps().Deps, fastConfig(), nil, zap.NewNop())
	m2.closeRegistry()
}
