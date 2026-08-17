package model

import (
	"encoding/json"
	"testing"
)

// TestControlMessageReasonOmittedWhenEmpty asserts the OPEN-1 additions stay
// backward-compatible on the wire: a control message with no reason/mode (e.g. a
// plain saved/read-only frame) marshals without the new keys, so existing clients
// that ignore `reason`/`mode` see exactly today's shape.
func TestControlMessageReasonOmittedWhenEmpty(t *testing.T) {
	b, err := json.Marshal(ControlMessage{Kind: ControlReadOnlyState, ReadOnly: ReadOnlyState(true)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)
	if want := `{"kind":"read-only-state","readOnly":true}`; got != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}
}

// TestControlMessageReadOnlyFalseSurvivesOnWire is the regression for the
// access-regain bug: a read-only-state frame REGAINING edit access (readOnly =
// false) MUST keep the `readOnly` key on the wire. With the previous
// `bool,omitempty` field the false value was dropped, so the frame marshalled to
// `{"kind":"read-only-state"}` — shape-identical to a frame that never set the
// field — and a JS/TS client (no Go zero-value) never re-enabled editing after a
// grant, staying locked read-only until a full reconnect. The *bool keeps false
// explicit.
func TestControlMessageReadOnlyFalseSurvivesOnWire(t *testing.T) {
	b, err := json.Marshal(ControlMessage{Kind: ControlReadOnlyState, ReadOnly: ReadOnlyState(false)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"kind":"read-only-state","readOnly":false}`; string(b) != want {
		t.Fatalf("regain frame marshal = %s, want %s (readOnly:false must not be dropped)", string(b), want)
	}
}

// TestControlMessageReadOnlyOmittedOnNonReadOnlyFrame asserts the *bool stays
// backward-compatible for every OTHER control kind: a frame that carries no
// read-only state (e.g. saved) leaves ReadOnly nil, so the `readOnly` key is
// omitted entirely — it does not leak a spurious `readOnly:false` onto saved /
// room-user-change / collaborator-mode frames (which would mislead a client that
// reacts to the key's presence on any control frame).
func TestControlMessageReadOnlyOmittedOnNonReadOnlyFrame(t *testing.T) {
	b, err := json.Marshal(ControlMessage{Kind: ControlSaved, Version: 7})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"kind":"saved","version":7}`; string(b) != want {
		t.Fatalf("saved frame marshal = %s, want %s (no readOnly key)", string(b), want)
	}
}

// TestControlMessageReadOnlyReason asserts a read-only-state frame surfaces the
// granular reason code (OPEN-1) under the additive `reason` key.
func TestControlMessageReadOnlyReason(t *testing.T) {
	b, err := json.Marshal(ControlMessage{
		Kind:     ControlReadOnlyState,
		ReadOnly: ReadOnlyState(true),
		Reason:   ReasonNoUpdateAccess,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round ControlMessage
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if round.Reason != ReasonNoUpdateAccess {
		t.Fatalf("reason = %q, want %q", round.Reason, ReasonNoUpdateAccess)
	}
}

// TestControlMessageCollaboratorMode asserts the new collaborator-mode frame
// carries {mode, reason} (OPEN-1), the shape the WS-D client uses to preserve its
// collaborator-mode UX.
func TestControlMessageCollaboratorMode(t *testing.T) {
	b, err := json.Marshal(ControlMessage{
		Kind:   ControlCollaboratorMode,
		Mode:   ModeViewer,
		Reason: ReasonInactivity,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"kind":"collaborator-mode","reason":"inactivity","mode":"viewer"}`; string(b) != want {
		t.Fatalf("marshal = %s, want %s", string(b), want)
	}
}
