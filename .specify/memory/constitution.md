<!--
Sync Impact Report
- Version change: 3.0.0 → 3.0.1 (PATCH — §III clarification; no principle change)
- 3.0.1 (2026-08-18): §III — widen the retained in-process path from a "test
  capability" to a "development and testing capability". 3.0.0 understated it:
  the same path is also the local development loop (real editors driven against
  the service without Alkemio infrastructure) and the documented zero-dependency
  smoke test that isolates the WebSocket path from authZ. All three are now
  enumerated, with an explicit instruction that the adapters serving them are
  retained on that basis and MUST NOT be pruned as unused — closing the gap
  where a later §X dead-code pass could have removed them on the strength of the
  narrower wording. No principle is redefined and no configuration changes
  status: the standalone deployment remains withdrawn.
- Version change: 2.0.0 → 3.0.0 (MAJOR — §III redefined; a supported product
  configuration is withdrawn)
- 3.0.0 (2026-08-18): Withdraw the zero-dependency standalone product promise.
  - MODIFIED §III "Standalone-First, Alkemio-Integrated" → "Alkemio-Integrated,
    In-Process Testable". The service targets Alkemio; the standalone deployment
    is no longer a supported configuration. Rationale: the promise was never
    satisfiable — the document index is owned by the Alkemio `server` and reached
    by RPC over RabbitMQ, so every real configuration depends on that external
    service, and no environment runs `server` without file-service — so it cost
    real complexity for a deployment nobody runs (§XI).
  - RETAINED as a distinct, narrower guarantee: the service MUST remain runnable
    entirely in-process for tests, using in-process fixtures and the core's
    shipped single-process defaults. Explicitly a test capability with no
    durability guarantee, not a deployment mode.
  - ADDED to §III: adapters existing SOLELY to serve the withdrawn promise are
    legacy under §X and MUST be removed; adapters that also serve the in-process
    test path are retained on that basis. Removal is tracked separately from the
    go-yjs core port so a foundational change does not also carry a multi-adapter
    deletion.
  - MODIFIED Technology Stack Constraints — metadata-store row now names the
    `MetadataStore` port and the RabbitMQ→server path as the system of record
    (the Postgres variant existed for standalone only); authorization row now
    scopes `open` to in-process tests. §V wording likewise rescoped.
  - Consequential: `001` FR-017 (standalone create/delete HTTP API) and FR-020
    (standalone as first-class) are superseded; the `postgres` metadata store and
    the non-file-service content adapters lose their only consumer.
  - MIGRATION PLAN (affected code):
    * NO DATA MIGRATION. Nothing is deployed and no production data exists, so
      this is deletion only — there is no stored state to convert or preserve.
    * ORDERING. This amendment MUST be committed before any of the removals
      below. Until it is in force, §III still mandates standalone and the same
      adapters are REQUIRED code, so deleting them would violate the constitution
      in effect at that moment. The removals are also deliberately NOT bundled
      into the go-yjs core port (`003`): a reviewer who disagrees with withdrawing
      standalone must be able to say so without rejecting a large deletion.
    * REMOVE — `internal/adapter/outbound/metastore/postgres/` with its
      migrations, pool, and sqlc output; drop `pgx`, `sqlc`, and `golang-migrate`
      from the build; drop the CI Postgres service. This is the service's ONLY
      database code, so afterwards it opens no database connection at all.
    * REMOVE — every content adapter other than `file-service` and the in-process
      store, with their config validation and env documentation.
    * REMOVE — the standalone create/delete REST API (`001` FR-017). It is the
      no-bus lifecycle equivalent and is already conditionally mounted; it shares
      the Manager path with the RabbitMQ lifecycle consumer, so only the HTTP
      surface goes and the lifecycle logic is untouched.
    * RETAIN — `inline` blob, `inmemory` metadata, `open` auth/authz, and
      `inmemory` fan-out. These serve the in-process test path that §III still
      requires, and are kept on that basis rather than as standalone remnants.
    * UPDATE — the `METADATA_STORE` / `BLOB_STORE` config enums and their
      validation, `.env.example`, `README.md`, and `CLAUDE.md`.
    * SUPERSEDE — mark `001` FR-017 and FR-020 superseded by this amendment in
      the `001` spec, so the earlier spec does not silently contradict it.
    * VERIFY — full gates green with no external services (`go build`, `go vet`,
      `go vet -tags integration`, `golangci-lint` at zero, `go test -race ./...`),
      and the `002` invariant suite still green and non-vacuous.
- Version change: 1.0.1 → 2.0.0 (MAJOR — §II and §IV redefined; code compliant
  with 1.0.1 violates 2.0.0)
- 2.0.0 (2026-08-18): Adopt `github.com/antst/go-yjs` as the CRDT core and its
  backend contracts. Driven by specs/003-go-yjs-core-port/ (FR-023), which is
  unimplementable under 1.0.1 because §IV mandated `y-crdt` by name.
  - MODIFIED §II Pluggable Ports — where the core defines a backend contract for
    a concern, that contract IS the port: `persistence.Store` (durable content,
    superseding the bespoke `BlobStore`), `hub.Hub` (fan-out, superseding
    `ClusterBroadcaster`), `memory.Registry` (document identity/lifetime).
    `MetadataStore` is retained and explicitly NOT superseded — the Alkemio index
    carries content type, authz policy, owner and bucket, which a byte-and-revision
    store does not model. Added two rules: implementations MUST be native (no
    translation shims; superseded ports removed, not wrapped) and MUST pass the
    core's conformance suites.
  - MODIFIED §IV CRDT Correctness — core re-pointed from the `y-crdt` fork to the
    first-party `go-yjs`. Substance preserved: one core, no reimplementation,
    differential fuzz gate against real Yjs, v1 live / v2 durable, ≤1s convergence.
    Clarified that the gate is the core's responsibility and is not re-verified
    here, and that a badly-fitting contract SHOULD be fixed upstream rather than
    worked around locally — but never diverged from silently.
  - MODIFIED §XIV Latest Dependencies — replaced the `y-crdt` module-`replace`
    clause. For the first-party pre-1.0 core the "latest stable" rule yields to an
    explicit version pin, so adopting an upstream change is always deliberate.
  - MODIFIED Technology Stack Constraints — CRDT core, fan-out, and durable-content
    rows re-stated against the new contracts; added a document-registry row.
  - Rationale: `y-crdt` proved inadequate and was rewritten into `go-yjs`, which
    ships backend contracts for concerns this service had hand-built. §II and §IV
    named the superseded core and ports directly, so they blocked the port.
  - MIGRATION PLAN (affected code):
    * NO DATA MIGRATION. The service has never been deployed and holds no
      production data, so no stored state must survive and no format continuity
      is owed. The work is a rebuild, not a conversion.
    * ORDERING. This amendment MUST be in force before implementation begins;
      under 1.0.1, §IV mandated `y-crdt` by name, so the port was unimplementable.
    * REPLACE — the core dependency and the module `replace` directive that
      redirected `skyterra/y-crdt` to the fork; `go-yjs` is a distinct module
      path, pinned to an explicit version (§XIV).
    * IMPLEMENT NATIVELY — `persistence.Store` over file-service, and `hub.Hub`
      over Redis. Both MUST reach their infrastructure directly. Implementing
      either by delegating to the port it supersedes is prohibited by §II.
    * REMOVE, NOT WRAP — the `BlobStore` and `ClusterBroadcaster` ports and every
      adapter behind them, once their replacements exist. No translation shim,
      compatibility layer, or adapter-over-adapter may survive.
    * ADOPT — `memory.Registry` for in-process document identity, acquisition,
      eviction, and invalidation; the collaboration session is rebuilt around its
      handle. Shutdown drain ordering, flush policy, presence, limits, authz, and
      lifecycle-event handling remain this service's own.
    * RETAIN — `MetadataStore`. It is NOT superseded: it carries the Alkemio
      document index, a different concern from a byte-and-revision store. It MUST
      NOT be repurposed as a persistence bridge.
    * WIRE — the core's conformance suites into CI for every implementation the
      service provides.
    * GATE — the `002` lifecycle properties MUST still hold. Tests reaching into
      rebuilt structures MAY be restructured but MUST NOT be weakened, and each
      MUST be re-proven non-vacuous with the proof recorded.
    * TRACKED BY — `specs/003-go-yjs-core-port/`.
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

Where the CRDT core (§IV) defines a backend contract for one of these
concerns, **that contract IS the port**. The service MUST NOT define a
parallel bespoke port for the same concern.

- Durable document content MUST go through the core's
  `persistence.CheckpointStore` contract. There are exactly TWO backends:
  `file-service` for production, and the in-process store for the test suite
  and local development. The in-process store is not durable across a restart
  and MUST never be presented as a deployment option.
- Cross-pod fan-out MUST go through the core's `hub.Hub` contract
  (shipped in-process default single-pod; `redis` for multi-pod, R4).
- In-process document identity, acquisition, eviction, and
  invalidation MUST go through the core's `memory.Registry` contract.
- The Alkemio document index MUST go through the `MetadataStore` port
  (default the Alkemio server RabbitMQ save/fetch bus; `postgres` for
  standalone). This is **not** superseded by `persistence.Store`: the
  index carries content type, authorization policy, owner, and bucket,
  which a byte-and-revision store neither models nor should.
- Authentication and authorization MUST go through `Auth` (handshake)
  and `AuthZ` (per-document) ports.
- Backend selection MUST be configuration-driven and the service MUST
  NOT leak backend details through its wire protocol or API.

**Implementations MUST be native.** A contract MUST be implemented
directly against its infrastructure. Implementing one by delegating to
a superseded port — a `Store` that calls an older snapshot/pointer
port, a `Hub` that wraps an older broadcaster — is prohibited.
Superseded ports MUST be removed, not wrapped; no translation shim,
compatibility layer, or adapter-over-adapter may survive a migration
(§VIII, §X).

**Custom implementations MUST be contract-validated.** Where the core
ships conformance suites for a contract, every implementation the
service provides MUST pass them in CI. Choosing a shape the contract
permits is conformant; misreporting a guarantee is not — an append
that reports success before its bytes are durable, a load that presents
a partial history as complete, or a fan-out that assumes ordering or
single delivery the contract does not promise, are all violations
regardless of whether the build and local tests pass.

### III. Alkemio-Integrated, In-Process Testable

The service targets the Alkemio platform. It MUST integrate cleanly
into it, and MUST remain runnable entirely in-process for tests.

**The zero-dependency standalone deployment is NOT a supported product
configuration.** That promise is withdrawn: it was never satisfiable,
because the document index is owned by the Alkemio `server` and reached
by RPC over RabbitMQ, so a real configuration always depends on that
external service. No environment runs `server` without file-service
either. Retaining the promise cost real complexity for a deployment
nobody runs (§XI — No Busywork).

This service holds **no database of its own and opens no database
connection**: `server` owns the durable rows, blobs live in
file-service, and every durable interaction is a service call.

- The service MUST run entirely **in-process**, with no database, bus,
  blob store, or auth service, using in-process fixtures and the core's
  shipped single-process defaults. This path serves three distinct
  purposes, all of which MUST keep working:
  1. the automated test suite;
  2. the local development loop, including driving real editors against
     the service without Alkemio infrastructure;
  3. the documented zero-dependency smoke test that isolates the
     WebSocket path from authZ.
  It is a **development and testing capability, not a deployment
  mode**: it carries no durability guarantee and MUST NOT be
  represented as a supported way to run the service in an environment
  that matters. Adapters serving it are retained on this basis and MUST
  NOT be pruned as unused.
- The Alkemio configuration MUST authenticate at the handshake from
  the Alkemio token/cookie (Oathkeeper/Kratos) and authorize per
  document via the authorization-evaluation-service.
- Adapters that exist **solely** to serve the withdrawn standalone
  promise are legacy under §X and MUST be removed. Adapters that also
  serve the in-process test path are retained on that basis.
- The service replaces `collaborative-document-service` and
  `whiteboard-collaboration-service`; it MUST serve both document
  conventions (memo `Y.XmlFragment`, whiteboard id-keyed `Y.Map`)
  over one protocol and one document id namespace.
- Actor identity MUST be referred to as `actorId`, never `userId`.

### IV. CRDT Correctness — One Core, Fuzz-Gated

The service MUST build on the single Go Yjs core
`github.com/antst/go-yjs`; it MUST NOT reimplement CRDT logic or carry
a second CRDT implementation.

- The core is **first-party**: it is this team's own product, created by
  rewriting the earlier `skyterra/y-crdt` fork after that fork proved
  inadequate for this service. It supplies both the CRDT and the backend
  contracts of §II.
- The core is trusted in production only after its cross-implementation
  differential fuzz gate against real JS Yjs is green (FR-011/SC-006).
  That gate is the core's own responsibility and is **not re-verified
  in this service**; encoding and merge semantics are out of scope here.
- Yjs wire compatibility is a design guarantee of the core. What this
  service MUST guarantee is that its own transport framing, sync
  handshake sequencing, and awareness handling do not break it.
- The live wire encoding is y-protocols v1; the durable snapshot
  encoding is v2 (v1 remains readable).
- Convergence MUST hold: all connected clients reach identical document
  state ≤1s after edits settle (SC-002). Malformed/hostile updates MUST
  be rejected without divergence.
- **Because the core is first-party, a contract that does not fit this
  service's genuine needs SHOULD be changed in the core** rather than
  worked around here; a poor fit is evidence about the contract, and the
  two are designed together. Diverging *silently* — working around a
  contract locally while leaving it unchanged upstream — is prohibited.

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
  identity, not a failure. (In `open` test mode everyone is
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
- The `go-yjs` core is **first-party and pre-1.0**, so its shape may
  change. It MUST be pinned to a specific version, and bumping it MUST
  re-verify the differential fuzz gate (§IV).
- For that core only, the "latest stable" rule yields to the explicit
  pin: upstream changes there are coordinated design decisions made by
  this team, not external releases to track. The pin exists so that
  adopting such a change is always a deliberate act here, never an
  implicit one.

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
12. Do not reimplement CRDT logic — the `go-yjs` core is the single
    source of CRDT behavior.
12a. Do not implement a core contract by delegating to a superseded
    port, and do not keep a superseded port alive behind a shim —
    implement natively and delete what it replaces (§II).
13. Do not fail open on an authorization error — fail closed.

## Technology Stack Constraints

The following technology choices are fixed and MUST NOT be replaced
without a constitution amendment:

| Component         | Technology                                          |
|-------------------|-----------------------------------------------------|
| Language          | Go 1.26                                             |
| Architecture      | Hexagonal (ports/adapters)                          |
| CRDT core         | `github.com/antst/go-yjs` (first-party Go Yjs, v1+v2 codecs) |
| WebSocket         | `coder/websocket`                                   |
| HTTP router       | chi v5                                              |
| Logging           | Zap (structured JSON)                               |
| Metrics           | Prometheus (`/metrics`)                             |
| Fan-out           | `hub.Hub`: shipped in-process (default), Redis (multi-pod) |
| Metadata store    | `MetadataStore`: RabbitMQ→server (the Alkemio system of record) |
| Document registry | `memory.Registry` (shipped in-process implementation) |
| Durable content   | `persistence.CheckpointStore`: file-service (deployed), in-process (tests/dev) |
| DB driver (PG)    | pgx v5                                              |
| Query generation  | sqlc                                                |
| Migrations        | golang-migrate                                      |
| Messaging         | amqp091 (RabbitMQ), NATS (auth fallback)            |
| Authorization     | authorization-evaluation-service (h2c HTTP/2 preferred, or NATS); `open` for in-process tests |
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
- Optional durable-content backend for `persistence.Store` via its
  existing PUT/GET API; expanding it is pre-authorized if the store
  needs a capability it does not yet expose.
- Its contract with this service is **store blob, read blob**. Blob
  retention, expiry, and reclamation of superseded blobs are the
  file-service's own concern; this service MUST NOT model or manage
  them, and exposes no document-history or restore surface.

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

**Version**: 3.0.1 | **Ratified**: 2026-06-18 | **Last Amended**: 2026-08-18
