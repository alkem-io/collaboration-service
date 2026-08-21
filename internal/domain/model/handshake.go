package model

import "github.com/google/uuid"

// HandshakeCredentials is the domain-typed credential set read off the WebSocket
// handshake and handed to the Auth port. It carries every credential the
// configured AuthN adapter might inspect — the WS adapter populates it from the
// transport (Cookie / Authorization headers + the ?guestName= query param) and
// the adapter decides priority and validation, so credential resolution lives in
// the adapter, not the transport, and the Auth port stays infra-free (no
// *http.Request — constitution §I).
//
// Each AuthN adapter reads only the field it needs:
//   - header → ActorIDHeader (the gateway-stamped actor id);
//   - open   → nothing (everyone anonymous);
//   - oidc   → CookieSID → BearerToken → GuestName → anonymous sentinel, in the
//     same priority order the server's forward-auth controller uses.
type HandshakeCredentials struct {
	// ActorIDHeader is the value of the configured gateway actor-id header
	// (AUTH_TOKEN_HEADER, e.g. X-Alkemio-Actor-Id). The header AuthN adapter
	// trusts it directly; oidc/open ignore it.
	ActorIDHeader string
	// CookieSID is the bare BFF session id read from the session cookie
	// (alkemio_session[_<env>]); the oidc adapter looks it up in Redis. Empty
	// when the request carried no session cookie.
	CookieSID string
	// BearerToken is the raw value of the Authorization header (e.g.
	// "Bearer <jwt>"); the oidc adapter validates it as a Hydra RS256 access
	// token. Read ONLY from Authorization — there is no ?access_token= query
	// fallback (DROPPED, OPEN-7). Empty when absent.
	BearerToken string
	// GuestName is the ?guestName= query param; in oidc mode it yields a
	// named-anonymous identity (display name → presence only; principal = the
	// anonymous sentinel, OPEN-6). Empty when absent.
	GuestName string
}

// Empty reports whether no credential at all was presented — no actor-id header,
// no cookie session, no bearer, no guest name. A missing credential resolves to
// the anonymous sentinel (it is not an authentication FAILURE — constitution §V:
// missing ≠ failed).
func (c HandshakeCredentials) Empty() bool {
	return c.ActorIDHeader == "" && c.CookieSID == "" && c.BearerToken == "" && c.GuestName == ""
}

// AnonymousIdentity is the resolved sentinel identity a missing credential maps
// to in oidc mode (and a named-anonymous guest's authorization principal).
func AnonymousIdentity() Identity {
	// uuid.Nil mirrors the server's anonymous actor sentinel. It is a resolvable
	// principal, distinct from open mode's nil ActorID.
	id := uuid.Nil
	return Identity{ActorID: &id}
}
