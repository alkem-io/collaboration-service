package header

import (
	"context"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestNonEmptyHeaderResolvesActorID asserts the gateway-stamped actor id in the
// header is trusted verbatim (the gateway already validated the credential).
func TestNonEmptyHeaderResolvesActorID(t *testing.T) {
	a := New()
	want := "11111111-1111-1111-1111-111111111111"
	id, err := a.Authenticate(context.Background(), model.HandshakeCredentials{ActorIDHeader: want})
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
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{ActorIDHeader: "actor-123"}); err == nil {
		t.Fatal("malformed gateway actor-id header should be rejected")
	}
}

// TestEmptyHeaderIsRejected asserts a missing/empty actor-id header is rejected:
// header mode trusts the stamp, so an absent stamp means the gateway did not run
// and there is no actor to trust (SC-014).
func TestEmptyHeaderIsRejected(t *testing.T) {
	a := New()
	if _, err := a.Authenticate(context.Background(), model.HandshakeCredentials{}); err == nil {
		t.Fatal("empty header should be rejected, got nil error")
	}
}

// TestHeaderIgnoresOtherCredentials asserts the header adapter reads ONLY the
// actor-id header — a cookie/bearer/guest present without the header is still a
// rejection (header mode trusts the gateway stamp, nothing else).
func TestHeaderIgnoresOtherCredentials(t *testing.T) {
	a := New()
	creds := model.HandshakeCredentials{
		CookieSID:   "sid",
		BearerToken: "Bearer jwt",
		GuestName:   "Ada",
	}
	if _, err := a.Authenticate(context.Background(), creds); err == nil {
		t.Fatal("header mode without the actor-id header should reject, got nil error")
	}
}
