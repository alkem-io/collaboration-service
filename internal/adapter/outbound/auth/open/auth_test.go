package open

import (
	"context"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAuthenticateIsAnonymousForAnyCredential asserts open mode resolves every
// handshake — credentialed or not — to the anonymous identity with an empty
// ActorID (AuthZ is bypassed entirely in open mode).
func TestAuthenticateIsAnonymousForAnyCredential(t *testing.T) {
	a := New()
	for _, creds := range []model.HandshakeCredentials{
		{},
		{ActorIDHeader: "actor"},
		{BearerToken: "Bearer x", CookieSID: "sid", GuestName: "Ada"},
	} {
		id, err := a.Authenticate(context.Background(), creds)
		if err != nil {
			t.Fatalf("Authenticate(%#v): unexpected error %v", creds, err)
		}
		if id.ActorID != "" {
			t.Errorf("Authenticate(%#v).ActorID = %q, want empty (anonymous)", creds, id.ActorID)
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
