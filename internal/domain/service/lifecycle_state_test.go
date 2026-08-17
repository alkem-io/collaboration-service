package service

import "testing"

// TestLegalTransitionEdgeTable pins the COMPLETE lifecycle edge set (spec 002
// FR-012). It is the executable form of the state machine: every (from,to) pair is
// asserted legal or illegal, so adding/removing an edge in legalTransition without
// updating this table fails. Non-vacuous by construction — it checks all 16 pairs.
func TestLegalTransitionEdgeTable(t *testing.T) {
	all := []roomState{stateMaterializing, stateActive, stateDraining, stateClosed}
	legal := map[[2]roomState]bool{
		{stateMaterializing, stateActive}:   true,
		{stateMaterializing, stateDraining}: true,
		{stateActive, stateDraining}:        true,
		{stateDraining, stateClosed}:        true,
	}
	for _, from := range all {
		for _, to := range all {
			want := legal[[2]roomState{from, to}]
			if got := legalTransition(from, to); got != want {
				t.Errorf("legalTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	// Pin the transitions whose ILLEGALITY carries the invariant: Active→Closed must
	// route through Draining (so the final-snapshot flush always runs), and a room
	// never closes without passing through Draining (the teardown sequence).
	if legalTransition(stateActive, stateClosed) {
		t.Error("Active→Closed must be illegal: an active room must drain (flush) before closing")
	}
	if legalTransition(stateMaterializing, stateClosed) {
		t.Error("Materializing→Closed must be illegal: teardown always passes through Draining")
	}
}

// TestLifecycleHappyPath walks the full legal path and asserts the state at each step.
func TestLifecycleHappyPath(t *testing.T) {
	var l lifecycle
	if !l.is(stateMaterializing) {
		t.Fatalf("zero value = %s, want Materializing", l.get())
	}
	if !l.activate() || !l.is(stateActive) {
		t.Fatalf("activate failed; state = %s", l.get())
	}
	if !l.beginTeardown() || !l.is(stateDraining) {
		t.Fatalf("beginTeardown failed; state = %s", l.get())
	}
	if !l.finishDraining() || !l.is(stateClosed) {
		t.Fatalf("finishDraining failed; state = %s", l.get())
	}
}

// TestBeginTeardownIdempotentFromActive is the teardown-entry guarantee: exactly one
// caller wins; the rest get false and do nothing. This replaces the scattered
// `released` bool and is what makes the single teardown sequence run once.
func TestBeginTeardownIdempotentFromActive(t *testing.T) {
	var l lifecycle
	l.activate()
	if !l.beginTeardown() {
		t.Fatal("first beginTeardown should win")
	}
	if l.beginTeardown() {
		t.Fatal("second beginTeardown must lose (teardown already started)")
	}
	if !l.is(stateDraining) {
		t.Fatalf("state = %s, want Draining", l.get())
	}
}

// TestBeginTeardownFromMaterializing covers tearing down a room that never served
// (abort / close-while-materializing): it still claims teardown and reaches Closed.
func TestBeginTeardownFromMaterializing(t *testing.T) {
	var l lifecycle // zero value = Materializing, never activated
	if !l.beginTeardown() || !l.is(stateDraining) {
		t.Fatalf("beginTeardown from Materializing failed; state = %s", l.get())
	}
	if l.beginTeardown() {
		t.Fatal("second beginTeardown must lose")
	}
	if !l.finishDraining() || !l.is(stateClosed) {
		t.Fatalf("finishDraining failed; state = %s", l.get())
	}
}

// TestIllegalTransitionsRefused asserts transition never moves along an illegal
// edge: an Active room cannot jump straight to Closed, and Closed is terminal.
func TestIllegalTransitionsRefused(t *testing.T) {
	var l lifecycle
	l.activate()
	if l.finishDraining() { // Active→Closed is not legal; finishDraining is Draining→Closed
		t.Fatal("finishDraining from Active must be refused")
	}
	if !l.is(stateActive) {
		t.Fatalf("state mutated on a refused transition: %s", l.get())
	}
	// Drive to Closed, then assert terminal.
	l.beginTeardown()
	l.finishDraining()
	if l.activate() || l.beginTeardown() || l.finishDraining() {
		t.Fatal("Closed must be terminal: no transition out of it")
	}
}

func TestRoomStateString(t *testing.T) {
	cases := map[roomState]string{
		stateMaterializing: "Materializing", stateActive: "Active",
		stateDraining: "Draining", stateClosed: "Closed",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", s, got, want)
		}
	}
}
