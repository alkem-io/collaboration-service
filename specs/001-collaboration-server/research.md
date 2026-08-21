# Research — collaboration-server (WS-C, Phase 0)

Consolidates the **server-level** decisions: the workspace research items the
server inherits (R4 fan-out, R7 persistence, R13 auth, R9 limits) plus the
resolutions made while building Wave 1. Format: **Decision · Rationale ·
Alternatives**. The workspace `research.md`
(`../agents-hq/specs/003-unify-collab-yjs/research.md`) is the source of truth for
the cross-repo decisions; this file records how the *server* realizes them and the
server-only choices the epic deferred.

## Inherited workspace decisions (server-relevant)

### R4 — Horizontal scaling: pluggable fan-out, Redis pub/sub
- **Decision**: `ClusterBroadcaster` port — `inmemory` default (zero deps), optional `redis` pub/sub on `doc:{id}` (durable updates) + `awareness:{id}` (ephemeral, TTL'd).
- **Rationale**: CRDT convergence is intra-process, so multi-master fan-out is safe and *optional*; correctness never depends on it. Decouples scaling from correctness → standalone-reusable.
- **Server realization**: the port is in place (`port/ports.go`); `inmemory` is a no-op (single pod). The room publishes applied updates and ephemerals through the port and applies peer-pod payloads as non-origin updates. `redis` adapter is T004 (Wave 2).
- **Alternatives**: socket.io redis-adapter (ties to socket.io, rejected by R3); per-doc affinity routing (simpler ops, but fan-out generalizes better).

### R7 — Persistence: metadata/blob split, pluggable blob store, debounced v2
- **Decision**: two ports — `MetadataStore` (the small queryable index, default via the `server` RabbitMQ save/fetch; `postgres` standalone) and the content store (the full v2 `Y.Doc` snapshot; in-process default, `file-service` for deployment). Persist **debounced/throttled** (~500 ms default).
- **Rationale**: keeps the relational DB lean for large snapshots; matches today's save cadence; standalone-friendly; fits the existing `save`/`fetch` contract.
- **Server realization**: `Room.persist` encodes `EncodeStateAsUpdateV2(doc, nil)`, calls `Blob.Put(pointer, snapshot)` then `Metadata.Save(meta)`, emits `saved`/`save-error`, and bumps version. The inline pointer == document id (`data-model.md`). Debounce via the run-loop `saveTimer`; a final save on idle/last-leave (`TestIdleReleasePersistsFinalSnapshot`). Durable adapters are T005 (Wave 2).
- **Alternatives**: append-only update log + compaction — rejected (heavier port, bigger change to save/fetch; not needed at v1).

### R13 — AuthN vs AuthZ: two separate, configurable, fail-closed ports
- **Decision**: `Auth` = Alkemio token/cookie validated at the WS handshake (Oathkeeper/Kratos). `AuthZ` = delegate per-document read/collaborator/update-content to the **authorization-evaluation-service** via **h2c HTTP/2** (`POST /internal/auth/evaluate`) or **NATS** (`auth.evaluate`). Adapters: `authzeval` (Alkemio) / `open` (standalone). AuthZ **fails closed** on any transport/breaker/degraded condition.
- **Rationale**: reuses Alkemio's centralized authZ (no duplicated rules — DRY); keeps the service runnable standalone with authZ off.
- **Server realization**: `Auth`/`AuthZ` ports + `model.{Identity,AuthDecision,Privilege}` in place; the WS handler calls `Auth.Authenticate` at the handshake (401 on failure) and the `open` adapter satisfies both ports for standalone. The h2c+gobreaker `authzeval` adapter is T006 (Wave 2). The fail-closed contract is documented on `port.AuthZ.Evaluate` and `model.AuthDecision`.
- **Alternatives**: embed authorization rules in the collab service — rejected (duplicates `server`/auth-eval logic, violates DRY, couples standalone reuse to Alkemio's model).

### R9 — Resource-limit defaults (server enforcement)
- **Decision (corrected after browser E2E against the native-Yjs client)**:
  configurable limits remain available, but the all-frame update-rate limiter and
  mutation-only inactivity downgrade default to off. They do not reproduce legacy
  behaviour: the legacy whiteboard had no server-side frame-rate disconnect, and
  volatile cursor activity reset its inactivity timer. The contribution window
  defaults to the legacy 600 seconds.
- **Server realization**: enforced in the room/handler at Wave 3 (T014); a breach → control message + disconnect, others unaffected. Exact defaults to confirm (OPEN-4).

## Wave-1 server resolutions

### D1 — Wire framing: one `[type as VarUint][payload]` envelope for all four types
- **Decision**: every WebSocket frame is a single `protocol.WriteMessage`/`ReadMessage` envelope with a leading type byte: `0` sync, `1` awareness, `2` ephemeral, `3` control. Types 2 and 3 reuse the *same* framing as 0/1, not a second scheme.
- **Rationale**: one framing keeps the inbound dispatch a single `switch model.WireMessageType(msgType)` (`room.handleMessage`), and the vendored `protocol` package stays the canonical reference for the y-protocols sub-types. Control (type 3) carries a JSON `ControlMessage` body so its event set is extensible without a new wire type.
- **Alternatives**: a bespoke framing for the custom channels — rejected (two parsers, DRY violation, more surface for malformed-frame bugs).

### D2 — Bidirectional sync; server originates `SyncStep1`; reconnect == state-vector diff
- **Decision**: on join the server sends `SyncStep1` (its state vector) plus an awareness snapshot; a client `SyncStep1` is answered with `WriteSyncStep2` (the diff against the client's vector); `SyncStep2`/`Update` structs apply to the doc.
- **Rationale**: the y-websocket model — both peers exchange state vectors and each sends only the delta the other lacks. The *same* `SyncStep1`→`SyncStep2` path is the US5 reconnect catch-up, so offline→reconnect needs no special code (`TestOfflineReconnectNoLostEdits`).
- **Implementation note**: `dispatchSync` (`sync.go`) decodes the sub-message itself rather than calling `SyncHandler.HandleMessage`, because the room must tag the applied transaction with the *connection's* origin (`updateOrigin{src}`) so `onDocUpdate` can skip echoing the delta to its sender. The `protocol` helpers (`EncodeSyncStep1/2`, framing) are still reused verbatim for the wire shape.
- **Alternatives**: use `SyncHandler.HandleMessage` directly — rejected (it applies updates with the handler as origin, defeating per-connection echo filtering).

### D3 — Content-type source: `?type=` query first, metadata second (authzeval later)
- **Decision**: a brand-new room's content type comes from the `?type=memo|whiteboard` connection query param (default `memo`); once a snapshot exists, the **persisted metadata content-type wins** at load (`loadMetadata` overrides `r.content`).
- **Rationale**: a never-persisted document has no metadata to read; the query param lets the first client declare the convention so the right root shape is materialized (`applyConvention`). For any saved document the stored type is authoritative, so a wrong/absent query param can't corrupt an existing doc.
- **Forward**: Wave 3 sources content-type (and `authorizationPolicyId`) from the metadata index / authzeval instead of the query param (`handler.go` `contentTypeFromRequest` TODO).
- **Alternatives**: bake content-type into the id — rejected by the epic (single id namespace, FR-022).

### D4 — Single-writer room via a run-loop goroutine (no lock around the CRDT)
- **Decision**: every mutation (join, leave, message, persist, close) is a `command` serialized onto the room's single `run()` goroutine. The doc's `update` observer fires synchronously inside `ApplyUpdate` on that goroutine, so the member map, dirty flag, and version are touched by exactly one goroutine.
- **Rationale**: the CRDT core is not concurrency-safe for concurrent writers; a single-writer loop is simpler and faster than locking the doc, and makes `-race` clean by construction. A bounded per-connection outbound queue (`service.Conn`/`wsConn`) keeps a slow client from stalling the loop — it is shed on overflow.
- **Alternatives**: a `sync.Mutex` around the doc — rejected (coarser, error-prone, and the observer callback would still need careful ordering); a goroutine-per-message — rejected (re-introduces concurrent writers).

### D5 — Two additive domain ports: `service.Metrics` and `service.Conn`
- **Decision**: introduce `service.Metrics` (room/conn gauges + snapshot counters, bridged to Prometheus by the HTTP adapter, `NopMetrics` default) and `service.Conn` (the room's narrow outbound view of a connection, implemented by `ws.wsConn`).
- **Rationale**: §I — the domain must not import Prometheus or `coder/websocket`. These ports keep the room observable and able to fan out while staying infra-free and unit-testable without a socket or a metrics registry. They are **additive and non-breaking** — the epic's five ports were unchanged by Wave 1 (the "ports held" finding), so Wave 2 adapters plug into the same seams.
- **Alternatives**: import Prometheus/`coder/websocket` into the room — rejected (breaks the hexagon, couples the core to transport and metrics libraries).

### D6 — Deferred: server-forced awareness eviction on disconnect
- **Decision**: Wave 1 does **not** force-clear a departed connection's awareness entry. It relies on the client's clean-close local-state-clear and awareness TTL (the y-websocket convention).
- **Rationale**: the y-protocols awareness client id is the client's own y client id carried in its updates; the room does not map its room-local `connID` to that y client id, so it cannot synthesize an eviction without extra bookkeeping. Deferring keeps Wave 1 minimal and correct for the common (clean-close) path.
- **Forward**: Wave 3 (T013) tracks the connection↔awareness-client-id mapping and emits a server-side awareness removal on disconnect (and on the delete cascade), so peers stop rendering a vanished cursor immediately. Documented in `room.go` `dropMember`.
- **Alternatives**: force a generic awareness clear on every leave — rejected (without the client-id mapping it could clear the wrong entries).

## Sibling-service findings grounding the OPENs

Read directly from the sibling repos to ground the Wave-2/3 contract questions
(see spec.md `## Clarifications → OPEN` for the decisions/recommendations).

### authorization-evaluation-service (OPEN-1) — **exists, contract knowable**
- Go service at `/Users/antst/work/alkemio/authorization-evaluation-service`. HTTP: `POST /internal/auth/evaluate` (h2c on `:6060`); NATS: subject `auth.evaluate` (envelope `{pattern, data, id}`).
- Request `{actorId?, privilege, authorizationPolicyId}`; response `{allowed, reason, error?}` (`internal/service/types.go`). Privilege whitelist (`internal/service/validation.go`) includes `read`, `update`, `update-content`, `contribute`. **No auth on the endpoint** (in-cluster trust).
- Working Go h2c clients (with `sony/gobreaker`) already exist: `file-service/internal/adapter/outbound/authhttp/client.go`, `wopi-service/internal/adapter/outbound/authhttp/auth_service.go`. → reuse this pattern verbatim.
- **Unknown:** the `documentId → authorizationPolicyId` mapping (where the server learns the policy id) and the exact privilege strings for read vs. collaborate. → OPEN-1.

### file-service (OPEN-2) — **existing `/internal` API covers Put/Get/Delete**
- chi v5 routes (`internal/adapter/inbound/http/router.go`): `POST /internal/file` (multipart), `GET /internal/file/{id}/meta`, `GET /internal/file/{id}/content`, `PUT /internal/file/{id}/content`, `DELETE /internal/file/{id}`, `PATCH /internal/file/{id}`, `POST /internal/file/copy`. **No auth on `/internal/*`** (in-cluster trust).
- Upload is multipart (`file`, `displayName`, `storageBucketId`, `authorizationId`, …); returns `{id (UUID), externalID (SHA3-256), mimeType, size, reused}`. Content-addressed dedup; local-disk storage; default max **32 MiB** (ceiling 1 GiB).
- **Verdict:** the existing API fully supports snapshot Put/Get/Delete. The "pre-authorized expansion" is only relevant to a future *public* snapshot export, not to v1 server persistence. → OPEN-2 (confirm bucket-id convention + size ceiling; no expansion for v1).

### collaborative-document-service + whiteboard-collaboration-service (OPEN-3) — **two legacy dialects**
- Both are NestJS `Transport.RMQ` request/reply RPC over one durable queue.
- **Memos:** patterns `collaboration-document-save` / `collaboration-document-fetch`; save `{documentId, binaryStateInBase64}` (Yjs **v2** base64 — `Y.encodeStateAsUpdateV2`); fetch → `{contentBase64 | undefined}`. Contribution event `collaboration-memo-contribution {memoId, users[]}`. `info` → `{read, update, isMultiUser, maxCollaborators}`.
- **Whiteboards:** patterns `save` / `fetch`; save `{whiteboardId, content}` (**Excalidraw JSON** string); fetch → `{content}`. Contribution event `contribution {whiteboardId, users[]}`; `contentModified` event. `info` → `{read, update, maxCollaborators}`; **inactivity downgrade** after `collaborator_inactivity` s.
- The `server`-side consumer is **not** in these repos, so the wire shape `server` will accept for the unified service is not determinable from the collab side. → OPEN-3 (recommend a *new unified* `collaboration-save`/`-fetch` index-only contract; confirm with the `server` owner).

### presence / collaborator-mode / north-star metric (OPEN-4)
- Read-only downgrade model confirmed by both legacy services (`update=false`; `count >= maxCollaborators`; `isMultiUser=false` + second joiner; whiteboard inactivity timer). North-star = per-window set of contributing actor ids flushed on `contribution_window`. Epic R9 supplies starting limit defaults. → OPEN-4 (confirm exact default values + whether the metric is RabbitMQ event, Prometheus, or both).

## Encoding summary (what crosses which boundary)

| Boundary | Encoding |
|---|---|
| Live sync over WS | y-protocols **v1** (`SyncStep1`/`SyncStep2`/`Update`) |
| Awareness over WS | y-protocols awareness (TTL'd, never persisted) |
| Ephemeral (type 2) | custom payload, fanned out verbatim, never applied/persisted |
| Control (type 3) | JSON `ControlMessage` body in the `[type][payload]` envelope |
| Durable snapshot | full **v2** `EncodeStateAsUpdateV2` (v1 also readable) |
| Cross-pod (redis, W2) | `doc:{id}` = applied update bytes; `awareness:{id}` = ephemeral/awareness bytes |
