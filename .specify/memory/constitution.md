<!--
Sync Impact Report
- Version change: 1.0.0 → 1.0.1 (PATCH — §V clarification, no principle change)
- 1.0.1 (2026-06-20): §V Security by Design — clarify the handshake-AuthN
  rule that *missing ≠ failed*. A credential that was **presented but is
  invalid** (malformed/expired/signature-rejected/tombstoned) is a FAILED
  handshake → 401; a **missing** credential (no cookie/bearer/guestName) is
  NOT a failure → it resolves to the anonymous sentinel and the per-document
  `AuthZ` port decides. Preserves the original intent (never silently
  downgrade a FAILED auth to anonymous); encodes the missing-vs-failed
  distinction the Wave-5 `oidc` AuthN adapter relies on (spec FR-022/FR-023).
- 1.0.0 (2026-06-18): initial ratification.
  Adapted from the Alkemio File Service (Go) constitution v1.3.0, inheriting
  the fleet's §I–XV principles verbatim where applicable, with service-specific
  adjustments for a CRDT/WebSocket service:
  - II. Storage Abstraction → Pluggable Ports (fan-out / persistence / auth)
  - III. Alkemio Integration First → adds standalone-first dual mode (open auth)
  - V. Security by Design → authoritative plaintext Y.Doc, handshake authN,
    per-document authZ, fail-closed
  - Technology Stack Constraints → CRDT/WS stack (coder/websocket, the forked
    y-crdt core, Prometheus); Postgres path keeps pgx/sqlc/golang-migrate
  - Added principles: I–XV (see Core Principles)
  - Added sections: Technology Stack Constraints, Integration Requirements,
    Anti-Patterns — Quick Reference, Governance
- Follow-up TODOs: none
-->

# Alkemio Collaboration Service (Go) Constitution

## Core Principles

### I. Hexagonal Architecture

All code MUST follow the hexagonal (ports and adapters) architecture
pattern. Business logic lives in the domain core and MUST NOT depend
on external infrastructure. External systems (fan-out bus, metadata
store, blob store, authorization service, WebSocket transport) are
accessed exclusively through well-defined ports (interfaces) with
concrete adapters.

- Domain types and interfaces MUST reside in dedicated domain packages
  with zero infrastructure imports.
- Each external dependency MUST have its own adapter implementing a
  domain-defined port.
- No adapter MAY import another adapter directly; cross-cutting
  concerns flow through the domain or application layer.

### II. Pluggable Ports — Scaling, Persistence, Auth

The service MUST keep horizontal scaling, persistence, and
authorization behind clean port interfaces so each is swappable by
configuration without touching business logic (FR-019/020/021/022).

- Cross-pod fan-out MUST go through a `ClusterBroadcaster` port
  (default `inmemory` single-pod; `redis` for multi-pod, R4).
- The document index MUST go through a `MetadataStore` port
  (default the Alkemio server RabbitMQ save/fetch bus; `postgres`
  for standalone).
- The encoded Y.Doc snapshot MUST go through a `BlobStore` port
  (default `inline`; `file-service` / `s3` / `local` optional).
- Authentication and authorization MUST go through `Auth` (handshake)
  and `AuthZ` (per-document) ports.
- Backend selection MUST be configuration-driven and the service MUST
  NOT leak backend details through its wire protocol or API.

### III. Standalone-First, Alkemio-Integrated

The service MUST run as a single binary with zero external
dependencies by default, AND integrate cleanly into the Alkemio
platform when configured. Both modes are first-class.

- The default configuration (`open` auth, `inmemory` fan-out,
  `inline` blob) MUST boot with no database, bus, or auth service.
- The Alkemio configuration MUST authenticate at the handshake from
  the Alkemio token/cookie (Oathkeeper/Kratos) and authorize per
  document via the authorization-evaluation-service.
- The service replaces `collaborative-document-service` and
  `whiteboard-collaboration-service`; it MUST serve both document
  conventions (memo `Y.XmlFragment`, whiteboard id-keyed `Y.Map`)
  over one protocol and one document id namespace.
- Actor identity MUST be referred to as `actorId`, never `userId`.

### IV. CRDT Correctness — One Core, Fuzz-Gated

The service MUST build on the single forked Go Yjs core
(`y-crdt`); it MUST NOT reimplement CRDT logic or carry a second
CRDT implementation.

- The core is trusted in production only after its cross-implementation
  fuzz gate against JS Yjs is green (WS-A, FR-011/SC-006).
- The live wire encoding is y-protocols v1; the durable snapshot
  encoding is v2 (v1 remains readable).
- Convergence MUST hold: all connected clients reach identical document
  state ≤1s after edits settle (SC-002). Malformed/hostile updates MUST
  be rejected without divergence.

### V. Security by Design

The service mediates document access and holds authoritative document
state, making security a non-negotiable concern at every layer.

- The server is authoritative and holds **plaintext** Y.Docs (FR-021).
- Authentication MUST happen at the WebSocket handshake. A **failed**
  authentication MUST NOT be silently downgraded to anonymous. A failed
  authentication means a credential was **presented but is invalid** —
  malformed, expired, signature-rejected, or tombstoned — and MUST be
  rejected (401). This is distinct from a **missing** credential (no
  cookie, no bearer, no `guestName`): a missing credential is not a
  failed handshake; it resolves to the **anonymous sentinel** and lets
  the per-document `AuthZ` port decide (a public-read document remains
  reachable, a protected one is refused by authorization). The principle
  is *missing ≠ failed*: never treat a credential that failed validation
  as anonymous, but absence of a credential is a legitimate anonymous
  identity, not a failure. (In `open` standalone mode everyone is
  anonymous by design; in `oidc` mode a presented-but-invalid credential
  is the 401 case while absence resolves to the sentinel; in `header`
  mode a missing/empty header means the gateway did not run and is
  rejected.)
- Per-document authorization (read vs. update-content → viewer vs.
  collaborator) MUST be evaluated via the `AuthZ` port and re-evaluated
  on `document.access_changed`.
- Authorization checks MUST **fail closed**: a transport failure, open
  circuit breaker, or degraded auth service is never treated as a
  healthy "allowed" or "denied" — the connection is refused.
- Secrets, tokens, and credentials MUST NOT be logged or included in
  error responses or control messages.
- All inter-service communication MUST use TLS in production.
- Configurable limits (max doc size, max connections per room, update
  rate) MUST be enforced; a violating client gets a control message +
  disconnect, others unaffected (FR-024).

### VI. Test-First Development

Tests MUST be written before implementation for all new features.
The red-green-refactor cycle is the standard workflow.

- Unit tests MUST cover domain logic with no infrastructure
  dependencies (use in-memory adapters or mocks for ports).
- Integration tests MUST verify adapter behavior against real
  dependencies (Redis, Postgres, the bus, file-service) where feasible.
- Convergence, presence, offline→reconnect catch-up, and both content
  conventions MUST be covered by the shared e2e harness in single-pod
  and two-pod modes (SC-011).

### VII. Root Cause Analysis (NON-NEGOTIABLE)

All debugging and bug fixing MUST be driven by root cause analysis.
Opportunistic or speculative code changes hoping they might resolve
an issue are strictly forbidden.

- Before any fix is applied, the actual root cause MUST be identified
  and documented with evidence.
- If the root cause is unclear, invest time in debugging first —
  guessing wastes more time than investigating.
- Fixes MUST directly address the identified root cause, not symptoms.
- Every bug fix commit MUST be traceable to a specific diagnosed cause.

### VIII. DRY — Single Source of Truth

Code duplication is treated as a defect. When two or more methods
share substantially the same logic, that logic MUST be extracted into
a shared helper or refactored to eliminate the duplication.

- No two methods MAY implement the same logic in different modules.
- When methods share partial logic, the common part MUST be extracted.
- Before implementing new logic, search for existing implementations —
  extend rather than duplicate.
- Configuration, constants, and type definitions MUST live in one
  canonical location.
- Three similar lines of inline code are acceptable; duplicated
  multi-line blocks are not.

### IX. Lint on Completion

Every piece of code MUST pass linting before it is considered ready.
Linting is not a CI-only gate — it MUST be run locally when a unit of
work (function, file, feature slice) is complete.

- Code MUST pass `golangci-lint run` with zero violations before
  committing.
- Linter configuration is part of the project and MUST NOT be bypassed
  with `nolint` directives unless justified in a comment.

### X. No Legacy Code

We control the full stack and all consumers. Never silently assume
backward compatibility is required.

- Dead, deprecated, or unused code MUST be removed — not left
  "just in case."
- Backward-compatibility hacks, unused exports, commented-out code, and
  defensive code for scenarios that no longer apply MUST be deleted.
- When a feature requires changes across multiple services, coordinate
  those changes (the workspace spec + contracts) rather than maintaining
  compatibility shims.
- The legacy `collaborative-document-service` and
  `whiteboard-collaboration-service` are decommissioned after the
  cutover confidence window — this service does not carry their code.
- Every line of code MUST justify its existence.

### XI. No Busywork

Every task, test, and artifact MUST deliver demonstrable value.

- Reject make-work activities that exist only to satisfy process
  checkboxes.
- Do not create documentation, comments, or abstractions "just in case."
- Specifications MUST be lean: only what is necessary to communicate
  intent.

### XII. Meaningful Tests Only

Tests MUST defend real invariants or catch real regressions. Unit test
coverage MUST be at least 95% (SC-008).

- The 95% coverage target is a minimum bar, not an excuse for padding —
  every test MUST still defend a real invariant.
- Never write tests for the sake of coverage metrics.
- Do not test implementation details, trivial getters/setters, or
  scenarios that cannot fail.
- If a test does not help catch bugs or document critical behavior, do
  not write it.

### XIII. Meaningful Success Criteria

Success criteria MUST be directly testable within this service.

- Never invent arbitrary metrics without baseline measurements or
  explicit stakeholder requirements.
- Avoid vanity metrics or external business outcomes that cannot be
  validated during development.

### XIV. Latest Dependencies Always

When adding or updating any dependency, the latest stable version MUST
be verified online (pkg.go.dev, GitHub releases, etc.).

- Never rely on AI training data for version numbers — it is likely
  outdated.
- Dependencies MUST be pinned to specific versions, but those versions
  MUST be current at time of addition.
- The forked `y-crdt` core is pinned by module `replace` to a specific
  fork commit whose fuzz gate is green; bumping it MUST re-verify the
  gate.

### XV. No Assumptions

Never assume requirements, behavior, or implementation details that are
not explicitly defined.

- If something is unclear or unknown, ask the user for clarification
  before proceeding.
- If factual information is needed (versions, API specs, library
  behavior), search online to verify.
- Do not guess — guessing leads to rework.

## Anti-Patterns — Quick Reference

The following are **strictly prohibited** (derived from principles
VII–XV):

1. Do not apply speculative fixes — find root cause first.
2. Do not keep code "just in case" or for backward compatibility unless
   explicitly requested.
3. Do not duplicate logic — find or create a single shared
   implementation.
4. Do not add superficial tests for coverage padding.
5. Do not invent performance SLAs without evidence.
6. Do not create abstractions for hypothetical future needs.
7. Do not add comments explaining obvious code.
8. Do not rely on training data for dependency versions — check online.
9. Do not create documentation files unless explicitly requested.
10. Do not assume — ask or search when something is unclear.
11. Do not use `map[string]any` for HTTP response bodies — use named
    structs with JSON tags and a `Render(w http.ResponseWriter)` method.
    This enables OpenAPI spec generation and compile-time type safety.
12. Do not reimplement CRDT logic — the forked `y-crdt` core is the
    single source of CRDT behavior.
13. Do not fail open on an authorization error — fail closed.

## Technology Stack Constraints

The following technology choices are fixed and MUST NOT be replaced
without a constitution amendment:

| Component         | Technology                                          |
|-------------------|-----------------------------------------------------|
| Language          | Go 1.26                                             |
| Architecture      | Hexagonal (ports/adapters)                          |
| CRDT core         | Forked `skyterra/y-crdt` (pure Go Yjs + v2 codec)   |
| WebSocket         | `coder/websocket`                                   |
| HTTP router       | chi v5                                              |
| Logging           | Zap (structured JSON)                               |
| Metrics           | Prometheus (`/metrics`)                             |
| Fan-out           | in-memory (default), Redis (multi-pod)              |
| Metadata store    | RabbitMQ→server (default), Postgres (standalone)    |
| Blob store        | inline (default), file-service / S3 / local         |
| DB driver (PG)    | pgx v5                                              |
| Query generation  | sqlc                                                |
| Migrations        | golang-migrate                                      |
| Messaging         | amqp091 (RabbitMQ), NATS (auth fallback)            |
| Authorization     | authorization-evaluation-service (h2c HTTP/2 preferred, or NATS); `open` for standalone |
| Circuit breaker   | sony/gobreaker v2                                   |

Additional dependencies SHOULD be minimized. The Go standard library
MUST be preferred over third-party packages when functionality is
equivalent.

## Integration Requirements

The collaboration service integrates with the following systems:

**Clients** (`client-web` memo via TipTap; `excalidraw-fork` whiteboard):
- Connect to `wss://<host>/collab/<documentId>` (one document per
  connection) carrying y-protocols sync + awareness + the custom
  ephemeral channel — see
  `../agents-hq/specs/003-unify-collab-yjs/contracts/ws-protocol.md`.

**Alkemio Server** (Node/TS):
- Owns document identity; the collab service reacts to lifecycle events
  (`document.deleted` cascade purge; optional `document.created` /
  `document.access_changed`) over RabbitMQ — see
  `.../contracts/lifecycle-events.md`.
- Holds the document metadata/index via the `save`/`fetch` bus extended
  with `content_pointer` + `blob_store` — see
  `.../contracts/persistence-ports.md`.

**Authorization Evaluation Service** (Go, h2c HTTP/2 or NATS):
- h2c (preferred): `POST {AUTH_SERVICE_URL}/internal/auth/evaluate`.
- NATS (fallback): subject `auth.evaluate`.
- Used at the handshake and per document to grant viewer vs.
  collaborator; guarded by a sony/gobreaker circuit breaker; fails
  closed.

**file-service** (Go, existing — no code change):
- Optional `BlobStore` backend via its existing PUT/GET API; expanding
  it is pre-authorized if the blob store needs a capability it does not
  yet expose.

## Governance

This constitution is the authoritative guide for all development
decisions in the Alkemio Collaboration Service (Go). It supersedes
informal conventions and ad-hoc decisions.

- **Amendments**: Any change to this constitution MUST be documented
  with a version bump, rationale, and migration plan for affected code.
- **Versioning**: The constitution follows semantic versioning. MAJOR
  for principle removals/redefinitions, MINOR for additions or material
  expansions, PATCH for clarifications.
- **Compliance**: All pull requests MUST be reviewed for compliance with
  these principles. Violations MUST be justified in the PR description
  and tracked as tech debt if accepted.
- **Review cadence**: The constitution SHOULD be reviewed quarterly or
  when significant architectural decisions arise.

**Version**: 1.0.1 | **Ratified**: 2026-06-18 | **Last Amended**: 2026-06-20
