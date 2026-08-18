# Feature Specification: Unified Real-Time Collaboration Server

**Feature Branch**: `feat/003-unify-collab-yjs`
**Created**: 2026-06-18
**Status**: Draft (Waves 1–4 delivered; Wave 5 — dual-adapter OIDC AuthN — spec/design forward)
**Workspace epic**: `../agents-hq/specs/003-unify-collab-yjs/` (WS-C — unified backend)
**Backlog Story**: https://github.com/alkem-io/alkemio/issues/1909 (#1909)
**Input**: The server slice of the epic — realize US1/US2/US3/US5 at the *server* for a single Go backend that serves both memos and whiteboards over one CRDT (Yjs) protocol, on the forked `y-crdt` core, with pluggable fan-out/persistence/auth ports.

> **Repo-local sub-spec (WS-C).** This is the `collaboration-service`'s own SpecKit
> spec — the same `sub_spec: true` treatment `y-crdt` and `excalidraw-fork` got. The
> **workspace epic** owns the cross-repo *why*, the frozen contracts
> (`contracts/{ws-protocol,persistence-ports,lifecycle-events}.md`), the
> data-model conventions, and the rollout sequencing. **This document owns the
> server's internals**: its user stories at the server boundary, its functional
> requirements, its measurable success criteria, and its task breakdown. It does
> NOT re-specify the CRDT core (that is `y-crdt`'s spec) or the client binding
> (that is `excalidraw-fork`'s / `client-web`'s). Where the epic says "the
> service MUST …", this spec says *how the server does it and in which wave*.

## Clarifications

### Inherited from the epic (Sessions 2026-06-17 / 2026-06-18)

The epic's clarifications are authoritative and not re-litigated here. The
server-relevant resolutions this spec builds on:

- **One protocol, both content types** — raw WebSocket carrying y-protocols sync + awareness; whiteboard ephemerals (cursor/emoji/countdown) ride awareness + a small ephemeral message type (epic FR-019).
- **Dual mode by design** — single-pod zero-dep default; multi-pod fan-out is an optional `redis` adapter behind a port (epic FR-020).
- **Server-trusted plaintext** — the server decodes and holds the authoritative `Y.Doc` and persists it; transport is TLS in flight; no server-blind/e2e requirement (epic FR-021).
- **Single id namespace, content-type in metadata; metadata/blob split** — small queryable index (id, content-type, version, content pointer, timestamps) separate from the encoded snapshot; content store independently pluggable, in-process by default, file-service offload for deployment (epic FR-022).
- **Full `Y.Doc` snapshot (v2), debounced/throttled** — fits the existing `save`/`fetch` contract; not an append-only log (epic R7, FR-010).
- **Owner-driven + lazy materialization** — the caller owns identity; the room is materialized on first connect and purged on owner-delete cascade; no orphans (epic FR-023).
- **Configurable limits** — max doc size, max connections per room (existing max-collaborators), per-connection update rate; reject/disconnect on breach (epic FR-024).
- **AuthN at handshake / AuthZ delegated** — Alkemio token/cookie at the WS handshake; per-document read/collaborator decisions delegated to the authorization-evaluation-service; both pluggable; standalone runs open (epic FR-021, R13).
- **Forward-compatible with versioning** — the persistence port abstracts a *version*, GC policy is deliberate/configurable, the metadata store leaves room for a version timeline (epic FR-025). v1 serves latest-only.

### Session 2026-06-18 (server-level — Wave 1 resolutions)

Resolved while building Wave 1 and recorded so Waves 2–4 do not re-decide them.
Detail and rationale live in `research.md`.

- Q: How are wire message types framed on the socket? → A: A single `[type as VarUint][payload]` envelope (the vendored `protocol.WriteMessage`/`ReadMessage`). Type `0`=sync, `1`=awareness, `2`=ephemeral, `3`=control. Types 2/3 reuse the same envelope, not a second framing.
- Q: How does the server originate the sync handshake? → A: The server sends `SyncStep1` (its state vector) on join; the client replies `SyncStep2` + its own `SyncStep1`. Sync is bidirectional and both directions reduce to a state-vector diff (`WriteSyncStep2`), which is also the US5 reconnect catch-up.
- Q: Where does a brand-new room's content type come from before any metadata exists? → A: From the `?type=` connection query param (defaulting to `memo`); once a snapshot is persisted, the stored metadata content-type wins. (Wave 3 sources it from the metadata index / authzeval instead — `handler.go` `contentTypeFromRequest`.)
- Q: How is the `Y.Doc` made single-writer without a lock? → A: Every mutation is serialized onto the room's single run-loop goroutine (`command` channel); the doc's `update` observer runs synchronously inside `ApplyUpdate` on that goroutine, so the member map and dirty flag are touched race-free. `-race` is clean.
- Q: Two ports beyond the epic's five were needed — are they breaking? → A: No. `service.Metrics` (observability, bridged to Prometheus by the HTTP adapter) and `service.Conn` (the room's narrow outbound view of a connection, implemented by the WS adapter) are *additive, non-breaking* domain ports that keep the room transport- and Prometheus-free.
- Q: Server-forced awareness eviction on disconnect — now or later? → A: **Deferred to Wave 3 (T013).** Wave 1 relies on the client's clean-close local-state-clear and awareness TTL (the y-websocket convention); the room does not yet map its room-local `connID` to the y awareness client id. Documented in `room.go` `dropMember`.

### OPEN — ✅ ALL RESOLVED (antst, 2026-06-18 clarify pass; detail + rationale under `## Clarifications → OPEN`)

- **OPEN-1 (T006 authzeval): RESOLVED →** `MetadataStore` carries the document's `authorizationPolicyId` (returned on `Load`); the `authzeval` adapter calls `evaluate(actorId, "read" | "update-content", policyId)`, reusing the file-service/wopi h2c+gobreaker client.
- **OPEN-2 (T005 fileservice blob): RESOLVED →** implement against file-service's existing `/internal/file` API as-is; **no expansion for v1**; mirror its content-addressed convention (fixed `storageBucketId` per deployment; size ceiling via `MAX_UPLOAD_SIZE`).
- **OPEN-3 (T005 rabbitmq metastore): RESOLVED →** target a **new unified contract** (`collaboration-save`/`collaboration-fetch`, index-only `{id, contentType, version, contentPointer, blobStore}` + `info`/`contribution`). **Cross-repo dependency:** the `server` owner must add the matching consumer — tracked as a follow-up; the rest of Wave 2 proceeds in parallel and the collab adapter is built to this contract.
- **OPEN-4 (T013/T014 limits + metric): RESOLVED →** epic **R9 limit defaults** (config-tunable); the FR-014 contribution metric ships **both** as a Prometheus gauge (always) and a RabbitMQ `contribution` event (Alkemio mode).

### Session 2026-06-20 (Wave 5 — dual-adapter handshake AuthN, option (c))

> **Decision recorded, not re-litigated (antst).** Wave 1–4 shipped a single
> handshake-AuthN posture: the `authzeval` adapter *trusts* the actor id carried
> in the gateway-stamped header (`AUTH_TOKEN_HEADER`, defaulting the deployment to
> `X-Alkemio-Actor-Id`) — i.e. **gateway-terminated AuthN**, matching file-service's
> `ActorHeaderExtractor`. Wave 5 adds a **second, config-selectable handshake-AuthN
> adapter that validates the credential itself**, without changing AuthZ. This is
> **option (c): support BOTH, config-selectable.**

The AuthN/AuthZ split, made explicit:

- **AuthN (handshake) is now a three-way config choice** — the existing single
  `AUTH_MODE=authzeval|open` enum is **split** so the handshake-AuthN strategy is
  named independently of AuthZ. Modes:
  - **`header`** *(Alkemio prod DEFAULT)* — **option (a), gateway-terminated.**
    Trust the actor id in the header stamped by Traefik's
    `strip-client-alkemio-headers`→`alkemio-resolve` forwardAuth (server
    `/rest/internal/forward-auth`). This is the canonical post-OIDC-cutover
    Alkemio pattern; it is exactly what the Wave-2 `authzeval` adapter's
    `Authenticate` already does (`model.Identity{ActorID: <header value>}`).
    **The `header` mode is the existing behaviour, renamed and made explicit** —
    no behavioural change to the gateway-terminated path.
  - **`oidc`** *(new — option (b), direct OIDC validation)* — a collab `Auth`
    adapter that **validates the credential ITSELF at the WS handshake** and
    extracts the actor id, mirroring the server's `forward-auth.controller.ts`:
    1. **`alkemio_session` BFF cookie** → look the bare session id up in Redis
       (`alkemio:sid:<id>`), read `alkemio_actor_id` from the
       `AlkemioSessionPayload`; reject tombstoned (`terminated_at` set) and
       expired (`expires_at`/`absolute_expires_at` in the past) sessions.
    2. **`Authorization: Bearer <jwt>`** → Hydra-issued **RS256** access token,
       **JWKS-validated** (issuer + audience allow-list + `alkemio_actor_id`
       claim + clock tolerance), extract `alkemio_actor_id`.
    3. **`?guestName=`** → anonymous guest with a display name (synthetic
       `guest-<uuid>` actor id is minted gateway-side today; standalone-direct
       collab mints/accepts a guest sentinel — see OPEN-6).
    4. **no credential** → the **nil-UUID anonymous sentinel**
       (`ANONYMOUS_ACTOR_ID` = `00000000-0000-0000-0000-000000000000`), which
       auth-evaluation-service resolves to `GLOBAL_ANONYMOUS`. The handshake is
       **not** 401'd for missing credentials in `oidc` mode — it resolves to
       anonymous, exactly as the gateway does (a malformed/expired credential
       that was *presented* is the 401 case; see OPEN-7).
    For standalone/off-gateway deployment AND defense-in-depth behind the gateway.
  - **`open`** *(unchanged — zero-dependency standalone)* — authenticate everyone
    as anonymous (empty actor id); the existing `open` adapter.
- **AuthZ is UNCHANGED in all three modes** — per-document `read`/`update-content`
  decisions still delegate to the authorization-evaluation-service via the
  `authzeval` AuthZ adapter (`evaluate(actorId, privilege, policyId)`,
  h2c+gobreaker, fail-closed). The standalone `open` AuthZ adapter is retained for
  zero-dependency runs. **AuthN mode and AuthZ adapter are now selected
  independently** (`AUTH_MODE` for AuthN, `AUTHZ_MODE` for AuthZ — see plan.md).
- **WS handshake credential surface** — the handshake reader is generalized from a
  single `AUTH_TOKEN_HEADER` to a credential set the `oidc` adapter inspects in the
  same priority order the server's forward-auth controller uses: **cookie →
  bearer → `?guestName=` → anonymous** (OPEN-7 — DECIDED). The bearer is read
  **only** from the `Authorization:` header; the `?access_token=` query-param token
  fallback is **deliberately NOT supported** (DROPPED — the browser cookie already
  rides the same-site WS upgrade, so a query token is unnecessary (YAGNI) and a
  URL-token log-leak surface).
- **Rollout coupling** — collab has **no k8s manifest yet**, and prod is **still on
  oathkeeper** (the OIDC cutover has not landed). Therefore the `header`-mode prod
  rollout is **coupled to the prod OIDC cutover** (the forward-auth gateway must be
  live before `header` mode trusts the stamped actor id end-to-end). `oidc` mode is
  the path that does **not** depend on the gateway being cut over — it validates
  Hydra/BFF credentials directly — and is therefore also the defense-in-depth
  option behind the gateway. This decision is **SPEC/DESIGN only**; the adapter
  implementation is deferred to Wave 5 tasks (T018), gated on a clean `/analyze`.

#### Wave-5 OPENs — ✅ ALL RESOLVED (antst, 2026-06-20 confirm pass)

The three Wave-5 OPENs are **DECIDED**, not open. The detailed grounding/rationale
remains under `## Clarifications → OPEN`; the locked decisions are:

- **OPEN-5 (AuthN/AuthZ mode split): DECIDED →** split the single
  `AUTH_MODE=authzeval|open` into **`AUTH_MODE`** (AuthN: `header`|`oidc`|`open`)
  and **`AUTHZ_MODE`** (AuthZ: `authzeval`|`open`). When `AUTHZ_MODE` is unset it is
  **derived** from `AUTH_MODE` (`open`→`open`; `header`/`oidc`→`authzeval`). The
  retired `AUTH_MODE=authzeval` value is accepted as a backward-compat **alias** for
  `header` AuthN + `authzeval` AuthZ. (See FR-021; plan.md "AuthN/AuthZ split".)
- **OPEN-6 (guest in `oidc` mode): DECIDED →** `?guestName=` in `oidc` mode is a
  **named anonymous** — the display name flows to awareness/presence only; the
  authorization principal is the **anonymous sentinel** (`ANONYMOUS_ACTOR_ID`). No
  real/distinct guest principal is minted. (See FR-022.)
- **OPEN-7 (validated-credential set + transport): DECIDED →** `oidc` validates
  **BOTH** the `alkemio_session` BFF cookie (→ Redis session → `alkemio_actor_id`)
  **AND** a Hydra **RS256** `Authorization: Bearer` token (→ JWKS → `alkemio_actor_id`
  claim); each path is **inert when its config is absent** (so the adapter degrades
  to cookie-only or bearer-only). Config: a separate **`SESSION_REDIS_URL`** for the
  cookie-session store that **defaults to `REDIS_URL`** when unset; the JWKS/issuer/
  audience/cookie-name env var names **mirror the server's** (see plan.md and
  data-model.md for the exact names). The **`?access_token=` query-param token
  fallback is DROPPED** — the bearer is read only from `Authorization:`; the
  browser cookie already rides the same-site WS upgrade (YAGNI) and a URL token is a
  log-leak surface. (See FR-022/FR-023.)

## User Scenarios & Testing *(mandatory)*

These are the epic's user stories realized **at the server boundary**. Each is
independently testable against the running service (a WebSocket client, in-memory
or durable adapters). The headline correctness stories (US1/US2/US5) are
**delivered in Wave 1** and proven by the tests named in `tasks.md`.

### User Story 1 - Property-granular whiteboard merge at the server (Priority: P1) — Wave 1 ✅

The server applies two clients' concurrent edits to *different properties of the
same whiteboard element* and both survive; concurrent edits to the *same*
property converge deterministically. The server holds the authoritative
plaintext `Y.Doc` (an id-keyed `Y.Map` of element `Y.Map`s), applies each
client's update through the CRDT core, and fans the resulting delta to the other
clients — never last-write-wins.

**Why this priority**: This is the epic's headline correctness win; the server is
where the merge actually happens.

**Independent Test**: Two WebSocket clients connect to one whiteboard room; each
mutates a different property of the same element; both properties are present on
the server's doc and on every client after convergence. (Proven by
`TestTwoClientWhiteboardConvergence`, `TestEndToEndWhiteboardConvergence`.)

**Acceptance Scenarios**:

1. **Given** two clients on the same whiteboard room, **When** client A sets `x` and client B sets `strokeColor` on the same element concurrently, **Then** the server's doc and every client converge to A's `x` *and* B's `strokeColor`.
2. **Given** two clients set the *same* property to different values, **When** both updates reach the server, **Then** all clients converge to one deterministic value with no divergence and no panic.
3. **Given** a connection sends a malformed frame, **When** the server reads it, **Then** the frame is dropped (logged) and the room keeps converging (no crash, no divergence).

---

### User Story 2 - Memo collaboration preserved at the server (Priority: P1) — Wave 1 ✅

The server serves memos (root `Y.XmlFragment` named `default`) over the same CRDT
protocol clients use today, with no regression: character-level concurrent
editing, and an existing persisted memo loads intact.

**Why this priority**: Memos already work well; the server consolidation must not
degrade them — the "do no harm" guarantee.

**Independent Test**: Two clients concurrently edit one memo room and converge;
a persisted v2 memo snapshot reloads to identical content. (Proven by
`TestTwoClientMemoConvergence`, `TestEndToEndTwoClientConvergence`,
`TestPersistenceRoundTrip`, `TestEndToEndPersistenceReload`.)

**Acceptance Scenarios**:

1. **Given** two clients editing one memo room concurrently, **Then** both insertions are preserved in a stable order on the server's doc and on all clients.
2. **Given** a memo snapshot persisted by the server (full v2 `EncodeStateAsUpdateV2`), **When** the room is re-materialized, **Then** its content loads intact from the blob store.

---

### User Story 3 - One server, operated once (Priority: P2) — Wave 1 partial / Waves 3–4

A single deployed service serves both content types with shared
presence/auth/persistence/limits paths and one observability surface. Wave 1
delivers the shared sync/persistence/observability core for both conventions;
presence/collaborator-mode/limits/lifecycle-cascade and the standalone API are
Wave 3; the two-pod fan-out and the ≥95% gate are Waves 2/4.

**Why this priority**: Halves the operational surface; high value but secondary
to correctness.

**Independent Test**: One running service serves a memo room and a whiteboard
room simultaneously, both through the same `Manager`/`Room`/persistence path and
the same `/metrics` endpoint. (Wave 1: `TestEndToEndTwoClientConvergence` +
`TestEndToEndWhiteboardConvergence` against the same handler; full presence/limits
proof lands in Wave 3.)

**Acceptance Scenarios**:

1. **Given** the unified service is running, **When** a memo and a whiteboard session run simultaneously, **Then** both are served by the same `Manager` with shared sync/persistence/metrics paths.
2. **Given** a configured collaborator limit, **When** a connection would exceed `max connections per room`, **Then** it is rejected/disconnected with a control message and other collaborators are unaffected. *(Wave 3, T014.)*
3. **Given** an authenticated viewer (read-only) and a collaborator, **When** each connects, **Then** the viewer receives a `read-only-state` control and its updates are not applied; the collaborator's are. *(Wave 3, T013/T014.)*

---

### User Story 5 - Offline edits merge on reconnect (Priority: P3) — Wave 1 ✅

A client that briefly disconnects keeps editing; on reconnect, its buffered edits
and the concurrent server-side edits both survive and converge. The server drives
this with the same `SyncStep1`→`SyncStep2` state-vector diff used for initial
sync — the reconnecting client sends its state vector and receives only the delta
it is missing, while its own buffered updates apply to the server.

**Why this priority**: A resilience win that falls out of the CRDT choice; not the
primary driver, but cheaply guaranteed by the same sync path.

**Independent Test**: Partition a client, edit on both sides, reconnect, and
confirm no lost edits on either side. (Proven by `TestOfflineReconnectNoLostEdits`.)

**Acceptance Scenarios**:

1. **Given** a client edits while disconnected, **When** it reconnects and exchanges `SyncStep1`/`SyncStep2` with the server, **Then** its edits and the concurrent server-side edits both appear, converged identically.

---

### Edge Cases

- **Malformed/hostile frame** → dropped + logged; room unaffected (Wave 1, `TestDispatchSyncMalformed`, `TestUnknownWireTypeIgnored`).
- **Slow consumer** → its outbound queue overflows; the room sheds that connection rather than stalling the run loop (Wave 1, `TestWSConnSlowConsumerDropped`, `TestSlowConsumerEvicted`).
- **Join race with idle release** → a connection acquiring a room that tears down between acquire and join retries once onto a fresh room (Wave 1, `Manager.Join` retry loop).
- **Document deleted while clients connected** → clients disconnected (`room-closed`), room released, metadata + blob purged; idempotent (Wave 3, T015; lifecycle-events.md).
- **Connection exceeds a configured limit** (doc size / conns / rate) → control message + disconnect; others unaffected (Wave 3, T014).
- **Late joiner** → immediately receives the current awareness snapshot so existing presence renders (Wave 1, `TestLateJoinerReceivesAwareness`).
- **Cross-pod client** (two-pod) → updates published on `doc:{id}`, ephemerals on `awareness:{id}`; a client on pod B sees pod A's edits (Wave 2 T004 / Wave 4 e2e, SC-011).
- **Persist failure mid-session** → `save-error` control; room keeps serving from memory; retries on the next debounce tick (Wave 1, `TestSaveErrorOnMetadataFailure`, `TestSaveErrorControlOnBlobFailure`).

## Requirements *(mandatory)*

Server-level functional requirements. Each traces to one or more epic FRs and to
tasks in `tasks.md`. **[Wave N]** marks the wave that delivers it.

### Functional Requirements

- **FR-001** [Wave 1 ✅]: The server MUST serve both memo (`Y.XmlFragment` "default") and whiteboard (id-keyed `Y.Map` `elements`/`files`/`appState`) documents from one `Manager`/`Room` core over one WebSocket protocol, content-type selecting the convention (epic FR-001/FR-019; `convention.go`).
- **FR-002** [Wave 1 ✅]: The server MUST hold the authoritative plaintext `Y.Doc` as a **single writer** — every mutation serialized through the room's run-loop goroutine — so the CRDT converges with no lock and `-race` stays clean (epic FR-002/FR-021; `room.go`).
- **FR-003** [Wave 1 ✅]: The server MUST apply each client's sync update through the forked `y-crdt` core and fan the resulting delta to the *other* members only (origin filtering), never echoing an update to its sender (epic FR-002/FR-003; `onDocUpdate`).
- **FR-004** [Wave 1 ✅]: The server MUST drive the y-protocols sync handshake — send `SyncStep1` on join, answer a client `SyncStep1` with a state-vector-diff `SyncStep2`, and apply `SyncStep2`/`Update` structs — using the vendored `protocol` subpackage for framing (epic FR-005/FR-019; `sync.go`, `handler.go`).
- **FR-005** [Wave 1 ✅]: The server MUST carry awareness (type 1) — applied to the room's awareness state and fanned out, with a snapshot sent to late joiners — and the custom ephemeral channel (type 2) — fanned out verbatim, never applied to the doc, never persisted (epic FR-007/FR-008; `handleMessage`).
- **FR-006** [Wave 1 ✅]: The server MUST persist the full `Y.Doc` as a **debounced v2 snapshot** to the BlobStore and upsert the MetadataStore index, emitting `saved`/`save-error` control messages; a persist failure keeps the room serving from memory (epic FR-010; `Room.persist`).
- **FR-007** [Wave 1 ✅]: The server MUST **lazily materialize** a room on first connect (loading the latest snapshot) and **release** it — persisting a final snapshot — on idle/empty, sharing one room across concurrent connections to the same id (epic FR-023; `Manager`, `newRoom`).
- **FR-008** [Wave 1 ✅]: The server MUST support **offline→reconnect** convergence via the `SyncStep1`-driven catch-up, with no lost edits on either side (epic FR-009; US5).
- **FR-009** [Wave 1 ✅]: The server MUST define its outbound dependencies as ports — `ClusterBroadcaster`, `MetadataStore`, `BlobStore`, `Auth`, `AuthZ` — plus the additive `service.Metrics` and `service.Conn` ports, with zero-dep default adapters (`inmemory`, `inline`, `open`) wired by config (epic FR-019/020/021/022; `port/ports.go`, `service/{doc,manager,room}.go`).
- **FR-010** [Wave 2]: The server MUST provide a **`redis`** `ClusterBroadcaster` that publishes updates on `doc:{id}` and ephemeral/awareness on `awareness:{id}`, and subscribes to deliver peer-pod payloads to local members — selected by `FANOUT_MODE=redis`, with `inmemory` the default (epic FR-020/R4; T004).
- **FR-011** [Wave 2]: The server MUST provide **durable MetadataStore adapters** — `rabbitmq` (the Alkemio `server` save/fetch dialect extended with `content_pointer`/`blob_store`) and `postgres` (sqlc/pgx, golang-migrate) for standalone — selected by `METADATA_STORE` (epic FR-022/R7; T005; OPEN-3).
- **FR-012** [Wave 2]: The server MUST provide a **durable content adapter** — `file-service` (offload via the existing file-service API) — alongside the `inline` default, selected by `BLOB_STORE`; the content pointer shape is the adapter's concern (epic FR-022/R7; T005; OPEN-2).
- **FR-013** [Wave 2 ✅ → split in Wave 5]: The server MUST provide an **`authzeval`** Auth+AuthZ adapter — handshake **header-trusting** authN (option (a), gateway-terminated: `model.Identity{ActorID}` from the gateway-stamped header) and per-document authZ via the authorization-evaluation-service (h2c HTTP/2 `POST /internal/auth/evaluate`, or NATS `auth.evaluate`), guarded by a sony/gobreaker circuit breaker and **failing closed** — alongside the `open` default. **[Wave 5]** the header-trusting AuthN half is promoted to a named **`header`** AuthN mode and the AuthZ half is selected independently of AuthN (`AUTHZ_MODE`); see FR-021–FR-023 (epic FR-021/R13; T006; OPEN-1).
- **FR-014** [Wave 3]: The server MUST manage **presence/collaborator mode** — viewer vs. collaborator, inactivity downgrade — and emit a **north-star contribution metric** (per-window contributing actors) equivalent to today's, including **server-forced awareness eviction** of a departed connection (epic FR-007/FR-014; T013; OPEN-4).
- **FR-015** [Wave 3 ✅ → refined in Wave 5]: The server MUST enforce **authN at the handshake** (401 on failure, never anonymous-downgrade except `open`/`oidc`-anonymous-fall-through modes) and **per-document authZ** via the `AuthZ` port (re-evaluated on `document.access_changed`), and enforce **configurable limits** — max doc size, max connections per room, per-connection update rate — rejecting/disconnecting on breach with a control message. **[Wave 5]** the "401 on failure" rule is refined per AuthN mode: `header` 401s on a missing/empty header (gateway didn't run); `oidc` resolves a missing credential to the anonymous sentinel and **only** 401s a *presented-but-invalid* credential (bad signature / expired / tombstoned), mirroring the gateway's forward-auth semantics (epic FR-021/FR-024; T014; FR-021–FR-023; OPEN-7).
- **FR-016** [Wave 3]: The server MUST consume the **`document.deleted`** lifecycle event and cascade — disconnect clients (`room-closed`), release the room, `MetadataStore.Delete` + `BlobStore.Delete` — idempotently, with no orphans (epic FR-012/FR-023; contracts/lifecycle-events.md; T015).
- **FR-017** [Wave 3]: The server MUST expose a **standalone create/delete HTTP API** (`POST /collab/<id>`, `DELETE /collab/<id>`) so it runs outside Alkemio without the lifecycle bus (epic FR-020/FR-023; contracts/lifecycle-events.md; T016).
- **FR-018** [Wave 4]: The server MUST be covered by a **single-pod and a two-pod e2e suite** (convergence for both content types, persistence round-trip, presence, cross-instance fan-out) that runs in CI and gates merges, and MUST reach **≥95% unit-test coverage** (epic FR-015/FR-016/SC-008/SC-009/SC-011; T017).
- **FR-019** [Wave 1 ✅ → all]: The server MUST inherit the fleet Go conventions — hexagonal architecture, DRY, structured logging (Zap), Prometheus `/metrics`, chi v5, `golangci-lint` clean, test-first — per its own constitution (epic FR-017/FR-018; constitution §I–XV).
- **FR-020** [Wave 1 ✅ → Wave 4]: The server MUST keep the **single-binary, zero-dependency standalone mode** a first-class configuration (`open`/`inmemory`/`inline`) and select every durable backend purely by configuration with no code change (epic FR-020/SC-011; `config.go`).

#### Wave 5 — dual-adapter handshake AuthN (option (c)) — forward, SPEC/DESIGN this pass

- **FR-021** [Wave 5]: The server MUST select its **handshake-AuthN strategy by configuration** from **three** modes — **`header`** (option (a), gateway-terminated: trust the actor id in the gateway-stamped header; the existing Wave-2 behaviour, now named; **Alkemio prod default**), **`oidc`** (option (b), direct credential validation at the handshake; FR-022), and **`open`** (anonymous; zero-dependency standalone default) — **independently of the AuthZ adapter selection** (`AUTH_MODE` selects AuthN; `AUTHZ_MODE` selects AuthZ). **`AUTHZ_MODE`** is `authzeval|open` and, when unset, is **derived** from `AUTH_MODE` (`open`→`open`; `header`/`oidc`→`authzeval`); the retired `AUTH_MODE=authzeval` value is accepted as a backward-compat **alias** for `header` AuthN + `authzeval` AuthZ. Renaming the existing AuthN posture to `header` MUST NOT change the gateway-terminated path's behaviour (Session 2026-06-20; T018; **OPEN-5 DECIDED**).
- **FR-022** [Wave 5]: In **`oidc`** mode the server MUST **validate the handshake credential itself** and resolve `model.Identity{ActorID}`, mirroring the server's `forward-auth.controller.ts`, trying credentials in order: **(1)** the **`alkemio_session` BFF cookie** — bare session id → Redis lookup (`alkemio:sid:<id>`, via `SESSION_REDIS_URL` defaulting to `REDIS_URL`) → `alkemio_actor_id`, rejecting **tombstoned** (`terminated_at` set) and **expired** (`expires_at`/`absolute_expires_at` past) sessions; **(2)** a **Hydra-issued RS256 `Authorization: Bearer` token** — JWKS-validated (issuer + audience allow-list + `alkemio_actor_id` claim + clock tolerance), extract `alkemio_actor_id`, read **only** from the `Authorization:` header (no `?access_token=` query fallback — DROPPED); **(3)** **`?guestName=`** — **named anonymous** (display name → presence only; principal = anonymous sentinel; **OPEN-6 DECIDED**); **(4)** **no credential** → the **nil-UUID anonymous sentinel** (`ANONYMOUS_ACTOR_ID`) which auth-eval resolves to `GLOBAL_ANONYMOUS`. The server MUST validate **BOTH** the cookie and the bearer (full forward-auth parity); each path is **inert when its config is absent** (no `SESSION_REDIS_URL`/`REDIS_URL` → cookie path off; no JWKS URL → bearer path off). The JWKS/issuer/audience/cookie-name env var names **mirror the server's** (**OPEN-7 DECIDED**) (epic FR-021/R13; T018; FR-013/FR-015; **OPEN-6/OPEN-7 DECIDED**).
- **FR-023** [Wave 5]: In **`oidc`** mode the server MUST treat AuthN failure exactly as the gateway forward-auth does: a **presented-but-invalid** credential (bad signature, expired/tombstoned session, missing `alkemio_actor_id` claim) → **401** at the handshake; a **missing** credential → resolve to the **anonymous sentinel** (never 401 for absence), so a public-read document remains reachable. The Redis-session and Hydra-JWKS dependencies MUST be behind the `Auth` port so the domain core is unchanged, MUST fail safely (Redis unreachable → 503/handshake-reject, not silent anonymous), and the `oidc` adapter MUST NOT be required for `header`/`open` modes (epic FR-021; T018; OPEN-7).

### Key Entities *(server view; details in `data-model.md`)*

- **Room** — the live in-memory session: authoritative plaintext `Y.Doc`, `Awareness`, member registry, dirty flag, debounce/idle timers, single run-loop goroutine. Runtime-only.
- **Manager** — the room registry/lifecycle owner: lazy-create, share-by-id, release-on-idle.
- **Metadata** — the queryable index row (id, content-type, version, content pointer, blob-store kind, owner ref, timestamps).
- **Snapshot** — the persisted full `Y.Doc` v2 encoding, written debounced; latest-only in v1.
- **Identity / AuthDecision / Privilege** — the authN principal and per-document authZ result. The `Identity.ActorID` is resolved per the selected **AuthN mode** (`header` = the gateway-stamped header value; `oidc` = validated from the BFF cookie session / Hydra bearer; `open` = empty/anonymous). The **anonymous sentinel** is the nil-UUID `ANONYMOUS_ACTOR_ID` (auth-eval → `GLOBAL_ANONYMOUS`), distinct from `open`-mode's empty actor id (Wave 5).
- **ControlMessage** — the server→client type-3 events (`saved`, `save-error`, `read-only-state`, `room-user-change`, `room-closed`).
- **Ports** — `ClusterBroadcaster`, `MetadataStore`, `BlobStore`, `Auth`, `AuthZ` (epic five) + `service.Metrics`, `service.Conn` (Wave-1 additive).

## Success Criteria *(mandatory)*

Server-level, directly testable in this repo. Each traces to an epic SC.

### Measurable Outcomes

- **SC-001** [Wave 1 ✅]: In concurrent whiteboard edits to **different properties of the same element**, **0%** are lost at the server — both survive on the authoritative doc and all clients (epic SC-001; `TestTwoClientWhiteboardConvergence`, `TestEndToEndWhiteboardConvergence`).
- **SC-002** [Wave 1 ✅ single-pod / Wave 4 two-pod]: After edits settle, all connected clients reach an **identical** doc state within **1s** for both content types, **0 divergent** (epic SC-002; convergence tests; two-pod in T017).
- **SC-003** [Wave 1 ✅]: A persisted **v2 snapshot round-trips** — re-materializing a room reloads identical content from the blob store (epic SC-007 supporting; `TestPersistenceRoundTrip`, `TestEndToEndPersistenceReload`, `TestIdleReleasePersistsFinalSnapshot`).
- **SC-004** [Wave 1 ✅]: Memo collaboration shows **no regression** — concurrent typing converges and existing snapshots load (epic SC-005; `TestTwoClientMemoConvergence`, `TestEndToEndTwoClientConvergence`).
- **SC-005** [Wave 1 ✅]: A malformed/hostile frame is rejected **without divergence or panic**, and a slow consumer is shed **without stalling** the room (epic SC-002/FR-024-supporting; `TestDispatchSyncMalformed`, `TestSlowConsumerEvicted`).
- **SC-006** [Wave 2]: A document persisted with the **`file-service` blob adapter** (metadata inline, blob offloaded) reloads to **identical content** with the index row holding only metadata + a content pointer (epic SC-012; T005 + e2e).
- **SC-007** [Wave 2]: Enabling **`FANOUT_MODE=redis`** makes a two-pod deployment converge cross-instance **with no code change**; `inmemory` remains the zero-dep default (epic SC-011; T004 + T017).
- **SC-008** [Wave 3]: An authenticated **viewer cannot mutate** a document and a **collaborator can**, decided by the `AuthZ` port and re-evaluated on `document.access_changed`; an **unauthenticated handshake is 401** (except `open`) (epic FR-021; T013/T014).
- **SC-009** [Wave 3]: A connection that **breaches a configured limit** (doc size / conns / rate) is rejected/disconnected with a control message and **other collaborators are unaffected** (epic FR-024; T014).
- **SC-010** [Wave 3]: A **`document.deleted`** event disconnects clients, releases the room, and purges metadata + blob **idempotently** — no orphan remains (epic FR-012/FR-023; T015).
- **SC-011** [Wave 4]: Unit-test coverage is **≥95%** for all server code, measured and **CI-gating**; `make openapi` is clean; the single-pod and two-pod e2e suites are green (epic SC-008/SC-009/SC-010/SC-011; T017).
- **SC-012** [Wave 1 ✅ → all]: The service **boots and serves both content types with zero external dependencies** in the default config (`open`/`inmemory`/`inline`) and selects durable backends purely by env config (epic SC-011; `config.go`, `cmd/server`).
- **SC-013** [Wave 5]: With `AUTH_MODE=oidc`, a handshake carrying a **valid `alkemio_session` cookie** resolves to the session's `alkemio_actor_id`, a **valid Hydra RS256 bearer** resolves to its `alkemio_actor_id` claim, a **tombstoned/expired session** or **invalid bearer** is **401'd**, and **no credential** resolves to the **nil-UUID anonymous sentinel** — all without the gateway running (validated directly by the `oidc` adapter) (FR-022/FR-023; T018).
- **SC-014** [Wave 5]: Switching `AUTH_MODE` between `header`, `oidc`, and `open` is a **pure config change** — the AuthZ adapter, the domain core, and the WS protocol are **unchanged** — and `header` mode behaves identically to the pre-Wave-5 gateway-terminated path (FR-021; T018).

## Assumptions & Dependencies

- **CRDT core** — the forked `y-crdt` (vendored via `replace skyterra/y-crdt => antst/y-crdt@…`) is the single source of CRDT behavior; the server never reimplements CRDT logic (constitution §IV, anti-pattern 12). Live wire = y-protocols **v1**; durable snapshot = **v2** (`EncodeStateAsUpdateV2`); v1 remains readable. Trusting the fork in production depends on **WS-A's cross-impl fuzz gate** being green (epic FR-011/SC-006) — a *workspace* gate, not a server task.
- **Frozen cross-repo contracts** — `ws-protocol.md`, `persistence-ports.md`, `lifecycle-events.md`, and the epic `data-model.md` are authoritative; this spec conforms to them and does not redefine them. The OPENs below are *implementation-detail* questions inside those contracts, not changes to them.
- **Wave-1 additive ports** — `service.Metrics` and `service.Conn` extend the port surface without breaking the epic's five; the epic's "ports held" finding is recorded in `tasks/collaboration-service.md`.
- **Out of scope (server)** — the migration job and big-bang cutover (WS-E, owned by `server`/infra); the CRDT core internals and fuzz harness (WS-A); the client bindings (WS-B/WS-D); cross-session version history (FR-025 forward-compat only). The standalone Postgres metastore exists so the service is reusable outside Alkemio, not because Alkemio needs it.

## Clarifications → OPEN-block grounding (ALL RESOLVED — retained as rationale)

Grounded by reading the sibling services (see `research.md` for the file
anchors). Each was an implementation detail inside an already-frozen contract; none
blocks Wave 1.

> **✅ ALL SEVEN OPENs are RESOLVED — none is open.** OPEN-1–4 were resolved in the
> 2026-06-18 clarify pass; **OPEN-5/6/7 are DECIDED in the 2026-06-20 confirm pass
> (antst)** — see the "OPEN — ✅ ALL RESOLVED" and "Wave-5 OPENs — ✅ ALL RESOLVED"
> summaries above for the locked decisions. The detailed analysis below is retained
> only as the grounding/rationale that informed each choice; it is **not** a list of
> open questions. The chosen option matches the **Recommendation** in each block,
> except where the summary notes otherwise. OPEN-3 carries a tracked cross-repo
> follow-up (the `server`-side consumer for the new unified
> `collaboration-save`/`-fetch`).

### OPEN-1 — authzeval request mapping (Wave 2, T006)

**Found in code:** the `authorization-evaluation-service` **exists** as a Go service
(`/Users/antst/work/alkemio/authorization-evaluation-service`). Contract is fully
knowable: `POST /internal/auth/evaluate` (h2c HTTP/2 on `:6060`, or NATS subject
`auth.evaluate`). Request `{actorId?, privilege, authorizationPolicyId}`; response
`{allowed, reason, error?}`. Privilege whitelist includes `read`, `update`,
`update-content`, `contribute`. **No auth on the endpoint itself** (in-cluster
trust). Working Go h2c clients already exist in `file-service`
(`internal/adapter/outbound/authhttp/client.go`) and `wopi-service` — both with a
gobreaker circuit breaker — so the adapter pattern is settled.

**Genuinely unknown:** a collaboration document is addressed by a `documentId`, but
the auth service evaluates against an **`authorizationPolicyId`**. *What policy id
does a memo/whiteboard document map to, and where does the server learn it?* And
which privilege names mean read vs. collaborate — `read` + `update-content`
(matches the server's `model.PrivilegeRead`/`PrivilegeUpdateContent`), or `read` +
`contribute`?

**Recommendation:** The `MetadataStore` (server-owned) carries the document's
`authorizationPolicyId` alongside content-type, returned on `Load`; the
`authzeval` adapter calls `evaluate(actorId, "read"|"update-content", policyId)`.
This reuses the file-service/wopi h2c+gobreaker client verbatim and keeps the
collab service from duplicating Alkemio's authorization model (DRY, constitution
§VIII). **Resolved & built (T006):** the policy id is sourced from `MetadataStore`
(`PolicyResolver`) and the privilege strings are `read` / `update-content`.

### OPEN-2 — fileservice BlobStore: existing API vs. expansion (Wave 2, T005)

**Found in code:** file-service exposes `POST /internal/file` (multipart),
`GET /internal/file/{id}/content`, `DELETE /internal/file/{id}` — **no auth on
`/internal/*`** (in-cluster trust). Blobs are content-addressed (SHA3-256), stored
on local disk, default max **32 MiB** (ceiling 1 GiB). Put returns a
document UUID + `externalID`. This **fully covers** Put/Get/Delete of a snapshot
blob; no expansion is needed for core persistence.

**Genuinely unknown:** only whether a **future public "export snapshot" download**
is wanted — that would need either the collab service to proxy it (apply its own
authZ, fetch internally) or a file-service capability-token expansion. Not needed
for v1 server persistence.

**Recommendation:** Implement the `fileservice` adapter against the **existing
`/internal/file` API** — `Put` = multipart `POST` (store the returned UUID as the
content pointer, with `externalID` for dedup), `Get` = `GET /{id}/content`,
`Delete` = `DELETE /{id}`. Pick a fixed `storageBucketId` per deployment. Snapshot
sizes (≈1–10 MB) sit well under 32 MiB; if a board can exceed that, set
`MAX_UPLOAD_SIZE` accordingly. **No file-service expansion for v1.** **Resolved &
built (T005.3):** a fixed `storageBucketId`+`authorizationId` per deployment, with
the `MAX_UPLOAD_SIZE` ceiling.

### OPEN-3 — RabbitMQ metastore dialect (Wave 2, T005)

**Found in code:** both legacy services use NestJS `Transport.RMQ` request/reply RPC
over one durable queue. **Memos** (`collaborative-document-service`): patterns
`collaboration-document-save` / `-fetch`; save `{documentId, binaryStateInBase64}`
(Yjs **v2** base64), fetch → `{contentBase64 | undefined}`. **Whiteboards**
(`whiteboard-collaboration-service`): patterns `save` / `fetch`; save
`{whiteboardId, content}` (**Excalidraw JSON**), fetch → `{content}`. Both emit a
fire-and-forget **contribution** event (`collaboration-memo-contribution` /
`contribution`) with `{id, users[]}` per window, and expose `info` →
`{read, update, maxCollaborators, isMultiUser?}` for the collaborator-mode logic.

**Genuinely unknown:** the unified server holds **one** representation (a v2 `Y.Doc`
snapshot) for **both** content types, but the two legacy dialects differ in pattern
names, id field, and payload (binary-v2 vs. JSON). *Does the unified server speak
**both** legacy dialects (route by content-type), or does `server` expose a **new
unified `save`/`fetch`** the collab service targets — extended per the epic with
`content_pointer` + `blob_store` so `server` stores the index, not the blob?* The
consumer side lives in `server` (not in these repos), so the wire shape `server`
will accept is not determinable from the collab side alone.

**Recommendation:** Target a **new unified contract** — patterns
`collaboration-save` / `collaboration-fetch`, payload
`{id, contentType, version, contentPointer, blobStore}` (index only; the blob goes
to the BlobStore), with `info`/`contribution` carried forward. This matches
`persistence-ports.md` (metadata/blob split) and avoids baking two legacy dialects
into the new service (constitution §X, no-legacy). **Resolved & built (T005.1):**
the collab adapter targets this unified contract (`contracts/unified-metadata-rmq.md`).
**Tracked cross-repo follow-up:** the `server`-side consumer for the unified
patterns does not exist yet (recorded in the Wave-2 phase note); if `server` cannot
expose the unified contract in time, the fallback is a content-type-routed adapter
speaking both legacy dialects with the blob inline.

### OPEN-4 — limits defaults + presence/collaborator-mode + FR-014 metric (Wave 3, T013/T014)

**Found in code / epic research:** the epic's R9 proposes defaults — **max doc size
~32 MB**, **max connections/room** carried from today's `maxCollaborators`,
**per-connection rate ~50 msg/s** token-bucket; inactivity downgrade as today. The
legacy services confirm the model: read-only downgrade when `update=false`, when
`collaboratorCount >= maxCollaborators` (ROOM_CAPACITY_REACHED), when
`isMultiUser=false` and a second joins; whiteboard adds **inactivity downgrade**
after `collaborator_inactivity` seconds. The **north-star metric** is a per-window
set of contributing actor ids flushed on an interval (`contribution_window`).

**Genuinely unknown:** the **exact default values** for the new service (the epic
calls them "starting defaults, all tunable") and whether the contribution metric is
emitted over the **same RabbitMQ event** (tied to OPEN-3) or a **Prometheus
counter** (or both).

**Recommendation:** Adopt the R9 defaults as the config defaults — `MAX_DOC_BYTES=32MiB`,
`MAX_CONNS_PER_ROOM` from metadata `maxCollaborators` (fallback e.g. 50),
`UPDATE_RATE=50/s` token-bucket, `COLLABORATOR_INACTIVITY=120s`,
`CONTRIBUTION_WINDOW=…` carried from the legacy config. Emit the contribution metric
**both** as a Prometheus gauge (`collaboration_contributing_actors`) *and*, in
Alkemio mode, as the RabbitMQ `contribution` event (so `server` analytics are
unbroken). **Resolved & built (T013/T014):** the R9 defaults are adopted
(`MAX_DOC_BYTES=32MiB`, `MAX_CONNS_PER_ROOM=50`, `UPDATE_RATE_PER_SEC≈50`,
`COLLABORATOR_INACTIVITY_SECONDS=120`, `CONTRIBUTION_WINDOW_SECONDS=60`) and the
metric ships **both** transports (Prometheus gauge + RMQ `collaboration-contribution`
event).

### OPEN-5 — AuthN-mode enum shape + AuthZ-mode independence (Wave 5, T018) — ✅ DECIDED

**DECIDED (antst, 2026-06-20):** AuthN is `header` | `oidc` | `open`, `header` =
Alkemio default. The existing single `AUTH_MODE=authzeval|open` enum is **split**
into `AUTH_MODE` (AuthN: `header`|`oidc`|`open`) and a separate **`AUTHZ_MODE`**
(`authzeval`|`open`). The Wave-2 `authzeval` *adapter* is decomposed: its
header-trusting `Authenticate` becomes the `header` AuthN adapter; its `Evaluate`
becomes the `authzeval` AuthZ adapter (selected by `AUTHZ_MODE`). **Coupling for
backward-compat:** when `AUTHZ_MODE` is unset, derive it from `AUTH_MODE`
(`open`→`open` AuthZ, `header`/`oidc`→`authzeval` AuthZ) so existing
`AUTH_MODE=authzeval` deployments keep working via a compatibility alias
(`authzeval`→`header` AuthN + `authzeval` AuthZ). **Env names:** `AUTH_MODE` /
`AUTHZ_MODE`; the `authzeval` alias is preserved. (Grounding below retained.)

### OPEN-6 — guest handling in standalone-direct `oidc` mode (Wave 5, T018) — ✅ DECIDED

**Found in code:** the gateway mints a synthetic `guest-<uuid>` actor id
(`ActorContextService.createGuest`) for `?guestName=` callers; collab never sees
the minting, only the resolved header today. In `oidc` mode collab is *off-gateway*,
so it must decide what a `?guestName=` handshake resolves to. **DECIDED (antst,
2026-06-20):** `oidc` mode treats `?guestName=` as a **named anonymous** — it
resolves to the **anonymous sentinel** (`ANONYMOUS_ACTOR_ID`) and carries the
display name only in awareness/presence (it is **not** a distinct authorization
principal). No real/distinct guest principal is minted. Reason: minting a
`guest-<uuid>` principal that auth-eval would not recognize standalone gains nothing
for authZ; the display-name UX is presence-only.

### OPEN-7 — `oidc` validated-credential set + WS-handshake transport (Wave 5, T018) — ✅ DECIDED

**Found in code:** the server's `forward-auth.controller.ts` tries **cookie →
bearer → guestName → anonymous**; both the cookie (BFF Redis session) and the
bearer (Hydra RS256 JWKS) are validated. A browser WebSocket cannot set arbitrary
request headers, but **does** send cookies on same-site upgrades and can pass query
params; native/M2M clients can set `Authorization`. **DECIDED (antst, 2026-06-20):**

1. **Which credentials does `oidc` validate — bearer-only, session-only, or both?**
   **DECIDED: BOTH** (full forward-auth parity — cookie session *and* Hydra
   bearer), so the same adapter serves browser (cookie) and M2M/native (bearer)
   clients. Either dependency is **disabled** by leaving its config unset
   (no `SESSION_REDIS_URL`/`REDIS_URL` → cookie path off; no JWKS URL → bearer path
   off), so the adapter degrades to cookie-only or bearer-only.
2. **Where does the WS handshake read each credential?** **DECIDED, mirroring the
   forward-auth priority:** cookie from the `Cookie:` header
   (`alkemio_session[_<env>]`, same-site upgrade); bearer from `Authorization:`
   **only**; guest from `?guestName=`. The **`?access_token=` (and `?token=`)
   query-param token fallback is DROPPED** — not supported. Rationale: the browser
   cookie already rides the same-site WS upgrade, so a query token is unnecessary
   (YAGNI), and a token in a URL is logged/cached far more readily than a header (a
   log-leak surface we decline to open). The corresponding T018 sub-task is removed.
3. **JWKS / issuer / audience / cookie-name config** — **DECIDED:** **mirror the
   server's** env var names — the Hydra JWKS URL / issuer, the audience allow-list,
   and the cookie name (`alkemio_session`, env-suffixed) use the **same names the
   server's OIDC config uses** (recorded in plan.md / data-model.md). The
   cookie-session store uses a **separate `SESSION_REDIS_URL`** that **defaults to
   the fan-out `REDIS_URL`** when unset — so a single-Redis deployment needs no extra
   config, while a deployment that isolates the session store can point it
   elsewhere.

## Wave map (server delivery)

| Wave | Scope | Tasks | Status |
|---|---|---|---|
| 1 | Live-sync server: room lifecycle, y-protocols sync+awareness+ephemeral, debounced v2 persistence, US5 reconnect, both conventions, ports + zero-dep adapters | T001–T003, T007–T012 | **DONE** (commit `57b79db`, PR #1) |
| 2 | Durable adapters: redis fan-out; rabbitmq+postgres metastore; file-service blob; authzeval auth | T004–T006 | Forward |
| 3 | Presence/collaborator-mode/inactivity + awareness eviction + contribution metric; authN/authZ + limits; lifecycle delete-cascade; standalone HTTP API | T013–T016 | Forward |
| 4 | Single-pod + two-pod e2e; ≥95% coverage gate; openapi clean | T017 | Forward |
| 5 | **Dual-adapter handshake AuthN (option (c)):** split AuthN/AuthZ mode selection; `header` (rename of gateway-terminated) + new `oidc` direct-validation adapter (BFF cookie session + Hydra RS256 bearer) + `open`; config + wiring | T018 | **Forward (spec/design only this pass)** |
