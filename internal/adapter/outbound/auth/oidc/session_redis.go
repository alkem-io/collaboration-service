// Package oidc is the direct-validation handshake-AuthN adapter (option (b),
// AUTH_MODE=oidc). It validates the WS-handshake credential ITSELF — mirroring
// the server's forward-auth.controller.ts — instead of trusting a gateway-stamped
// header, so collab can run off-gateway and as defense-in-depth behind it.
//
// This file holds the BFF cookie-session path (T018.4): the bare session id from
// the alkemio_session cookie is looked up in Redis (alkemio:sid:<sid>), its
// AlkemioSessionPayload decoded, and its alkemio_actor_id returned — rejecting
// tombstoned (terminated_at) and expired (expires_at / absolute_expires_at)
// sessions. It mirrors the server's session-store.redis.ts +
// cookie-session.strategy.ts validation 1:1.
package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// sessionKeyPrefix is the Redis key prefix express-session (connect-redis) writes
// BFF session payloads under — mirrors server SESSION_KEY_PREFIX.
const sessionKeyPrefix = "alkemio:sid:"

// errSessionNotFound signals the cookie's session id has no Redis key
// (never-existed / reaped by TTL / logged-out via destroy). This is state (a) —
// "cookie present but no session": NOT a validation failure. The caller treats it
// as "no cookie credential" and falls through to the next credential, eventually
// to the anonymous sentinel (constitution §V: missing ≠ failed). Every other
// error is a validation/transport failure → 401/503 (never anonymous).
var errSessionNotFound = errors.New("oidc: session not found")

// alkemioSessionPayload is the subset of the server's AlkemioSessionPayload the
// cookie path validates against (session-store.redis.ts). Only the fields the
// handshake needs are decoded; unknown fields are ignored.
type alkemioSessionPayload struct {
	// AlkemioActorID is the resolved Alkemio actor id (nil/empty for an
	// anonymous BFF session — treated as a missing-claim validation failure here,
	// matching cookie-session.strategy.ts createAnonymous fall-through being a
	// distinct, non-authenticated identity).
	AlkemioActorID *string `json:"alkemio_actor_id"`
	// ExpiresAt is the sliding idle-window expiry (epoch seconds).
	ExpiresAt int64 `json:"expires_at"`
	// AbsoluteExpiresAt is the fixed absolute ceiling (epoch seconds); a session
	// past it is rejected regardless of the Redis key's remaining TTL
	// (absolute-ttl.guard.ts).
	AbsoluteExpiresAt int64 `json:"absolute_expires_at"`
	// TerminatedAt, when set, marks a tombstone left by BFF refresh-teardown —
	// state (b): had-a-session-now-invalid → 401, distinct from never-existed
	// (cookie-session.strategy.ts, FR-022c).
	TerminatedAt *int64 `json:"terminated_at"`
}

// sessionRedisClient is the narrow subset of *goredis.Client the session store
// needs, so tests can drive it with miniredis (or a fake) without a network and
// the adapter stays decoupled from the full client surface.
type sessionRedisClient interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
}

// sessionStore reads + validates BFF cookie sessions from Redis. It mirrors the
// server's read path (session-store.redis.ts get + cookie-session.strategy.ts
// validate); writes/TTL are owned by the BFF, never by collab.
type sessionStore struct {
	client sessionRedisClient
	// now is injectable for deterministic TTL tests; defaults to time.Now.
	now func() time.Time
}

// newSessionStore constructs a sessionStore over a redis client.
func newSessionStore(client sessionRedisClient) *sessionStore {
	return &sessionStore{client: client, now: time.Now}
}

// NewSessionStore is the exported constructor the composition root uses to build
// the cookie-session path over a *goredis.Client (or any compatible client). The
// returned store is wired into oidc.Config.Session.
func NewSessionStore(client sessionRedisClient) *sessionStore { //nolint:revive // returns an unexported type by design; constructed only via this func.
	return newSessionStore(client)
}

// Resolve looks the bare session id up in Redis and returns the session's
// alkemio_actor_id. It returns errSessionNotFound when the key is absent (→ fall
// through to anonymous), a validation error for a tombstoned/expired/missing-claim
// session (→ 401), and a transport error when Redis is unreachable (→ 503/reject).
// It NEVER returns a nil error with an empty actor id.
func (s *sessionStore) Resolve(ctx context.Context, sid string) (string, error) {
	raw, err := s.client.Get(ctx, sessionKeyPrefix+sid).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return "", errSessionNotFound
		}
		// Transport / store-unreachable: reject (503-equivalent), never anonymous.
		return "", fmt.Errorf("oidc: session store get: %w", err)
	}

	var p alkemioSessionPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return "", fmt.Errorf("oidc: decode session payload: %w", err)
	}

	// Tombstone (state (b)) → invalid. Mirrors cookie-session.strategy.ts.
	if p.TerminatedAt != nil {
		return "", fmt.Errorf("oidc: session terminated")
	}

	nowS := s.now().Unix()
	// Absolute ceiling breached → invalid even if the Redis key still exists
	// (absolute-ttl.guard.ts).
	if p.AbsoluteExpiresAt > 0 && nowS > p.AbsoluteExpiresAt {
		return "", fmt.Errorf("oidc: session absolute TTL exceeded")
	}
	// Sliding idle window expired → invalid.
	if p.ExpiresAt > 0 && nowS > p.ExpiresAt {
		return "", fmt.Errorf("oidc: session idle TTL exceeded")
	}

	// A live session without an actor claim is an anonymous BFF session; treat it
	// as a missing-claim validation failure rather than resolving an empty actor.
	if p.AlkemioActorID == nil || *p.AlkemioActorID == "" {
		return "", fmt.Errorf("oidc: session missing alkemio_actor_id")
	}
	return *p.AlkemioActorID, nil
}
