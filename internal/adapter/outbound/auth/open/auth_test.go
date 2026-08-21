package open

import (
	"context"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAuthenticateIsAnonymousForAnyCredential asserts open mode resolves every
// handshake — with or without a gateway header — to an anonymous identity whose
// ActorID is NIL, distinct from the gateway-stamped uuid.Nil sentinel. AuthZ is
// bypassed entirely in open mode.
func TestAuthenticateIsAnonymousForAnyCredential(t *testing.T) {
	a := New()
	for _, credential := range []string{"", "actor", "00000000-0000-0000-0000-000000000000"} {
		id, err := a.Authenticate(context.Background(), credential)
		if err != nil {
			t.Fatalf("Authenticate(%q): unexpected error %v", credential, err)
		}
		if id.ActorID != nil {
			t.Errorf("Authenticate(%q).ActorID = %v, want nil (open mode)", credential, id.ActorID)
		}
	}
}

// TestEvaluateGrantsEverything asserts open mode grants every privilege on every
// document.
func TestEvaluateGrantsEverything(t *testing.T) {
	a := New()
	for _, p := range []model.Privilege{model.PrivilegeRead, model.PrivilegeUpdateContent} {
		dec, err := a.Evaluate(context.Background(), model.Identity{}, "doc-1", p)
		if err != nil {
			t.Fatalf("Evaluate(%s): unexpected error %v", p, err)
		}
		if !dec.Allowed {
			t.Errorf("Evaluate(%s).Allowed = false, want true (open grants all)", p)
		}
	}
}
