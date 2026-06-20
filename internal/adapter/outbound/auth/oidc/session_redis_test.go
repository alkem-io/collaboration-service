package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// newTestSessionStore spins up an in-process miniredis and returns a sessionStore
// reading the alkemio:sid:<sid> keys, plus the miniredis handle for seeding.
func newTestSessionStore(t *testing.T) (*sessionStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return newSessionStore(client), mr
}

// seedSession writes a session payload under the alkemio:sid:<sid> key.
func seedSession(t *testing.T, mr *miniredis.Miniredis, sid string, p alkemioSessionPayload) {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}
	if err := mr.Set(sessionKeyPrefix+sid, string(raw)); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func futureUnix() int64 { return time.Now().Add(time.Hour).Unix() }
func pastUnix() int64   { return time.Now().Add(-time.Hour).Unix() }

// TestSessionValidResolvesActorID asserts a live session resolves to its
// alkemio_actor_id.
func TestSessionValidResolvesActorID(t *testing.T) {
	store, mr := newTestSessionStore(t)
	actor := "actor-from-session"
	seedSession(t, mr, "sid-1", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: futureUnix(),
	})

	id, err := store.Resolve(context.Background(), "sid-1")
	if err != nil {
		t.Fatalf("Resolve: unexpected error %v", err)
	}
	if id != actor {
		t.Errorf("actor id = %q, want %q", id, actor)
	}
}

// TestSessionMissingKeyIsNotFound asserts a cookie whose Redis key does not exist
// (never-existed / reaped / logged-out) returns errSessionNotFound — NOT a
// validation failure (state (a): cookie present but no key → anonymous, §V).
func TestSessionMissingKeyIsNotFound(t *testing.T) {
	store, _ := newTestSessionStore(t)
	if _, err := store.Resolve(context.Background(), "no-such-sid"); !errors.Is(err, errSessionNotFound) {
		t.Fatalf("Resolve missing sid: err = %v, want errSessionNotFound", err)
	}
}

// TestSessionTombstonedIsInvalid asserts a tombstoned session (terminated_at set)
// is a 401-class invalid credential (state (b): had-a-session-now-invalid), NOT
// anonymous.
func TestSessionTombstonedIsInvalid(t *testing.T) {
	store, mr := newTestSessionStore(t)
	now := time.Now().Unix()
	actor := "actor-x"
	seedSession(t, mr, "sid-tomb", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: futureUnix(),
		TerminatedAt:      &now,
	})

	_, err := store.Resolve(context.Background(), "sid-tomb")
	if err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("tombstoned session: err = %v, want a validation error (not nil, not notFound)", err)
	}
}

// TestSessionAbsoluteExpiredIsInvalid asserts a session past its absolute ceiling
// is invalid (401), even if the Redis key still exists.
func TestSessionAbsoluteExpiredIsInvalid(t *testing.T) {
	store, mr := newTestSessionStore(t)
	actor := "actor-x"
	seedSession(t, mr, "sid-abs", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: pastUnix(),
	})
	if _, err := store.Resolve(context.Background(), "sid-abs"); err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("absolute-expired session: err = %v, want a validation error", err)
	}
}

// TestSessionIdleExpiredIsInvalid asserts a session past its sliding idle window
// (expires_at) is invalid (401).
func TestSessionIdleExpiredIsInvalid(t *testing.T) {
	store, mr := newTestSessionStore(t)
	actor := "actor-x"
	seedSession(t, mr, "sid-idle", alkemioSessionPayload{
		AlkemioActorID:    &actor,
		ExpiresAt:         pastUnix(),
		AbsoluteExpiresAt: futureUnix(),
	})
	if _, err := store.Resolve(context.Background(), "sid-idle"); err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("idle-expired session: err = %v, want a validation error", err)
	}
}

// TestSessionMissingActorClaimIsInvalid asserts a live session WITHOUT an
// alkemio_actor_id (anonymous BFF session) is treated as a missing-claim
// validation failure rather than resolving to an empty actor id.
func TestSessionMissingActorClaimIsInvalid(t *testing.T) {
	store, mr := newTestSessionStore(t)
	seedSession(t, mr, "sid-noactor", alkemioSessionPayload{
		ExpiresAt:         futureUnix(),
		AbsoluteExpiresAt: futureUnix(),
	})
	if _, err := store.Resolve(context.Background(), "sid-noactor"); err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("session without actor claim: err = %v, want a validation error", err)
	}
}

// TestSessionStoreUnreachableErrors asserts a Redis transport failure surfaces a
// non-notFound error so the caller rejects (503-equivalent) rather than silently
// downgrading to anonymous (§V).
func TestSessionStoreUnreachableErrors(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	store := newSessionStore(client)
	mr.Close() // tear the backend down so the GET fails at transport
	_ = client

	_, err := store.Resolve(context.Background(), "sid-any")
	if err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("store unreachable: err = %v, want a transport error (not nil, not notFound)", err)
	}
}

// TestSessionMalformedPayloadErrors asserts a non-JSON payload under the key is a
// validation error (corrupt session), not a silent anonymous downgrade.
func TestSessionMalformedPayloadErrors(t *testing.T) {
	store, mr := newTestSessionStore(t)
	if err := mr.Set(sessionKeyPrefix+"sid-bad", "{not-json"); err != nil {
		t.Fatalf("seed bad session: %v", err)
	}
	if _, err := store.Resolve(context.Background(), "sid-bad"); err == nil || errors.Is(err, errSessionNotFound) {
		t.Fatalf("malformed payload: err = %v, want a decode error", err)
	}
}
