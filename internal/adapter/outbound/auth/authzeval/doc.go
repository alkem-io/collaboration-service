// Package authzeval will implement the Auth + AuthZ ports (port.Auth,
// port.AuthZ) for Alkemio deployments: authentication from the handshake
// Alkemio token/cookie (Oathkeeper/Kratos), and per-document authorization via
// the authorization-evaluation-service (h2c HTTP/2 POST /internal/auth/evaluate,
// or NATS auth.evaluate), guarded by a sony/gobreaker circuit breaker. Failures
// fail closed (constitution §V). Implementation lands with task T006 of
// specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package authzeval
