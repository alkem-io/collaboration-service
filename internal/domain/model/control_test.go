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
	b, err := json.Marshal(ControlMessage{Kind: ControlReadOnlyState, ReadOnly: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)
	if want := `{"kind":"read-only-state","readOnly":true}`; got != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}
}

// TestControlMessageReadOnlyReason asserts a read-only-state frame surfaces the
// granular reason code (OPEN-1) under the additive `reason` key.
func TestControlMessageReadOnlyReason(t *testing.T) {
	b, err := json.Marshal(ControlMessage{
		Kind:     ControlReadOnlyState,
		ReadOnly: true,
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
