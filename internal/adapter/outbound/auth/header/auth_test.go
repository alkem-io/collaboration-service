package header

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// TestNonEmptyHeaderResolvesActorID asserts the gateway-stamped actor id in the
// header is trusted verbatim (the gateway already validated the credential).
func TestNonEmptyHeaderResolvesActorID(t *testing.T) {
	a := New()
	want := "11111111-1111-1111-1111-111111111111"
	id, err := a.Authenticate(context.Background(), want)
	if err != nil {
		t.Fatalf("Authenticate: unexpected error %v", err)
	}
	if id.ActorID == nil || id.ActorID.String() != want {
		t.Errorf("ActorID = %v, want %s", id.ActorID, want)
	}
}

// TestMalformedHeaderIsRejected proves a presented actor id cannot cross the
// authentication boundary as an unvalidated string.
func TestMalformedHeaderIsRejected(t *testing.T) {
	a := New()
	if _, err := a.Authenticate(context.Background(), "actor-123"); err == nil {
		t.Fatal("malformed gateway actor-id header should be rejected")
	}
}

// TestEmptyHeaderIsRejected asserts a missing/empty actor-id header is rejected:
// header mode trusts the stamp, so an absent stamp means the gateway did not run
// and there is no actor to trust (SC-014).
func TestEmptyHeaderIsRejected(t *testing.T) {
	a := New()
	if _, err := a.Authenticate(context.Background(), ""); err == nil {
		t.Fatal("empty header should be rejected, got nil error")
	}
}

// TestGatewayStampedAnonymousResolvesToTheNilUUIDSentinel is the anonymous path
// that survives the removal of direct credential validation.
//
// The gateway ALWAYS stamps the actor header — server's forward-auth controller
// sends ctx.actorID or, for an un-credentialed caller, ANONYMOUS_ACTOR_ID (the nil
// UUID), because downstream services 401 on a missing header by design. So
// anonymous production traffic arrives here as a well-formed nil UUID and must
// resolve to a NON-nil pointer to uuid.Nil — distinct from open mode's nil
// ActorID, and the distinction readOnlyReasonForIdentity depends on.
func TestGatewayStampedAnonymousResolvesToTheNilUUIDSentinel(t *testing.T) {
	id, err := New().Authenticate(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("the gateway's anonymous sentinel was rejected: %v", err)
	}
	if id.ActorID == nil {
		t.Fatal("ActorID is nil; the sentinel must be a NON-nil pointer, or it is indistinguishable from open mode")
	}
	if *id.ActorID != uuid.Nil {
		t.Errorf("ActorID = %v, want the nil-UUID sentinel", *id.ActorID)
	}
}
