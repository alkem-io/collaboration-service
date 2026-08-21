package model

import (
	"testing"

	"github.com/google/uuid"
)

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
	if got := AnonymousIdentity(); got.ActorID == nil || *got.ActorID != uuid.Nil {
		t.Errorf("AnonymousIdentity().ActorID = %v, want sentinel", got.ActorID)
	}
}
