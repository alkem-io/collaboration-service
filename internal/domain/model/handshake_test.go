package model

import "testing"

func TestHandshakeCredentialsEmpty(t *testing.T) {
	if !(HandshakeCredentials{}).Empty() {
		t.Error("zero-value HandshakeCredentials should be Empty")
	}
	cases := []HandshakeCredentials{
		{ActorIDHeader: "actor-1"},
		{CookieSID: "sid-1"},
		{BearerToken: "Bearer x"},
		{GuestName: "Ada"},
	}
	for _, c := range cases {
		if c.Empty() {
			t.Errorf("%#v should not be Empty", c)
		}
	}
}

func TestAnonymousSentinelIsNilUUID(t *testing.T) {
	if ANONYMOUS_ACTOR_ID != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("ANONYMOUS_ACTOR_ID = %q, want nil UUID", ANONYMOUS_ACTOR_ID)
	}
	if got := AnonymousIdentity(); got.ActorID != ANONYMOUS_ACTOR_ID {
		t.Errorf("AnonymousIdentity().ActorID = %q, want sentinel", got.ActorID)
	}
}
