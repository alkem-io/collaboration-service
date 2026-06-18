# Tasks: Unified Real-Time Collaboration Server (WS-C)

**Input**: Design documents in `specs/001-collaboration-server/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md
**Tests**: Included — constitution mandates test-first (§VI) and ≥95% coverage (§XII/SC-011).

> **Organized by the epic's waves.** Wave 1 (live-sync server + ports) is **DONE**
> (commit `57b79db`, PR #1) — tasks below carry file anchors and the tests that
> prove them. Waves 2–4 are **forward**, broken into concrete sub-tasks. Task ids
> (T001–T017) align with the milestone ids in
> `../agents-hq/specs/003-unify-collab-yjs/tasks/collaboration-service.md`.

## Format: `[ID] [P?] [Wave] Description`
- **[P]**: parallelizable (different files/packages, no ordering dependency).
- File paths are relative to the repo root.

---

## Phase 1 — Setup + ports — ✅ DONE (Wave 1)

- [X] **T001** [W1] Hexagonal Go skeleton — `cmd/server/{main.go,app.go}`, `internal/{config,domain,adapter}`; central `go-ci@v1` (lint/test/build), `.golangci.yml`, `apispec.yaml`, `CLAUDE.md` workspace hint, constitution (`.specify/memory/constitution.md`). Gates green; `/healthz` + `/metrics` (`internal/adapter/inbound/http/{router,health,metrics,middleware}.go`). ⏳ two-ruleset governance = org-admin follow-up.
- [X] **T002** [P] [W1] Vendor the forked `y-crdt` (`replace skyterra/y-crdt => antst/y-crdt@a2c966d` via Go proxy; WS handler constructs `ycrdt.NewDoc` + uses the `protocol` subpackage); structured JSON logging (`internal/config/logger.go`) + Prometheus `/metrics` with `collaboration_rooms_active` and the `service.Metrics`→Prometheus bridge (`internal/adapter/inbound/http/metrics.go` + `metrics_bridge_test.go`).
- [X] **T003** [W1] Define the five ports — `ClusterBroadcaster`, `MetadataStore`, `BlobStore`, `Auth`, `AuthZ` — in `internal/domain/port/ports.go`, each mapped to a frozen contract. Working zero-dep adapters: `fanout/inmemory` (no-op), `metastore/inmemory`, `blobstore/inline`, `auth/open` (authN+authZ). Durable adapters are `doc.go` placeholders tied to T004–T006. Domain types in `internal/domain/model/{document,room,control,auth,errors}.go`. Config validation in `internal/config/config.go` (`config_test.go`).

**Wave-1 additive ports (recorded, non-breaking):** `service.Metrics`
(`internal/domain/service/manager.go`) and `service.Conn`
(`internal/domain/service/room.go`) — the epic's five ports held unchanged.

---

## Phase 3 — Live-sync server — ✅ DONE (Wave 1)

> Built TDD on `feat/003-unify-collab-yjs` → DRAFT PR
> [collaboration-service#1](https://github.com/alkem-io/collaboration-service/pull/1)
> (base `develop`, `57b79db`). 31 tests, `-race` clean.

- [X] **T007** [W1] Room lifecycle — lazy materialize on connect (load latest snapshot), authoritative plaintext GC'd `Y.Doc`, single run-loop goroutine = single writer, release/purge on idle/empty (final snapshot on release). `internal/domain/service/{room.go,manager.go,convention.go}` (`newRoom`, `Manager.{Join,acquire,remove,Close}`, `Room.{run,finish,loadSnapshot}`). Proven: `TestManagerCloseReleasesRooms`, `TestIdleReleasePersistsFinalSnapshot`, `TestLifecycleMetrics`, `TestMaterializeFailsOnMetaLoadError`, `TestLoadSnapshotPropagatesBlobError`, `TestDoubleLeaveAndForwardAfterReleaseAreSafe`.
- [X] **T008** [W1] WS inbound adapter — raw WS at `/collab/{documentId}` (`coder/websocket`), authN at handshake (401), `Manager.Join`, per-conn read loop; server sends `SyncStep1` + awareness snapshot on join; bidirectional `SyncStep1`↔`SyncStep2`↔`Update` via the vendored `protocol` subpackage. `internal/adapter/inbound/ws/{handler.go,conn.go}` + `internal/domain/service/sync.go` (`dispatchSync`). Buffered single-writer outbound (`wsConn`, shed-on-overflow). Proven: `TestHandshakeRejectedOn401`, `TestMissingDocumentIDIs400`, `TestStrayTextFrameIgnored`, `TestWSConnDeliversFrames`, `TestWSConnCloseIdempotent`, `TestDispatchSyncStep1ProducesDelta`, `TestDispatchSyncUpdateAppliesToServer`, `TestOnDocUpdateSkipsOriginator`.
- [X] **T009** [W1] Ephemeral channel (cursor/emoji/countdown) over awareness + fan-out — wire type `2` fanned out verbatim, never applied/persisted; awareness type `1` applied to the room awareness + fanned out + late-joiner snapshot (FR-008). `internal/domain/service/room.go` (`handleMessage`, `awarenessSnapshot`). Proven: `TestAwarenessFanOutAndNotPersisted`, `TestLateJoinerReceivesAwareness`, `TestUnknownWireTypeIgnored`.
- [X] **T010** [W1] Document conventions — memo (`Y.XmlFragment` "default") + whiteboard (id-keyed `Y.Map` `elements`/`files`/`appState`); `ContentType` selects materialization; `?type=` query seeds a new doc, stored metadata wins thereafter. `internal/domain/service/convention.go` + `internal/adapter/inbound/ws/handler.go` (`contentTypeFromRequest`). Proven: `TestTwoClientMemoConvergence`, `TestTwoClientWhiteboardConvergence`, `TestEndToEndWhiteboardConvergence`.
- [X] **T011** [W1] Debounced v2 snapshot persistence — `EncodeStateAsUpdateV2` → `BlobStore.Put` + `MetadataStore.Save` (inline pointer = doc id); `saved`/`save-error` control; debounce timer + save-on-release; `collaboration_snapshots_total{outcome}` metric (R7). `internal/domain/service/room.go` (`Room.persist`). Proven: `TestPersistenceRoundTrip`, `TestEndToEndPersistenceReload`, `TestSaveErrorOnMetadataFailure`, `TestSaveErrorControlOnBlobFailure`, blob round-trip tests in `blobstore/inline/store_test.go`, version-bump in `metastore/inmemory/store_test.go`.
- [X] **T012** [W1] Offline→reconnect (US5) — `SyncStep1`-driven catch-up (state-vector diff `WriteSyncStep2`), client offline-buffer flush, converge with concurrent server-side edits (FR-009). `internal/domain/service/{sync.go,room.go}`. Proven: `TestOfflineReconnectNoLostEdits`.

**Wave-1 status.** Headline proofs pass: two-client convergence for *both* memo and
whiteboard (both edits survive — not LWW), persistence round-trip, US5 reconnect.
**The epic's five ports held**; two additive ports introduced. Deferred to later
waves: redis fan-out (T004), durable metastore/blob (T005), authzeval (T006),
presence + server-forced awareness eviction + collaborator-mode/limits (T013/T014),
delete-cascade (T015), standalone HTTP API (T016), two-pod e2e + gate (T017).

---

## Phase 2 — Durable adapters — ⬜ FORWARD (Wave 2)

> Each adapter plugs into a held port; the three sub-streams parallelize. Replace
> the `doc.go` placeholder in each package with a real implementation + tests.
> **Blocked-by:** OPEN-1 (T006), OPEN-2 (T005 blob), OPEN-3 (T005 metastore) — see
> spec.md `## Clarifications → OPEN`.

### T004 — Redis fan-out (`ClusterBroadcaster`, R4) — `internal/adapter/outbound/fanout/redis/`
- [ ] **T004.1** [P] [W2] Tests first (`broadcaster_test.go`): publish on `doc:{id}` round-trips to a subscriber on another logical pod; ephemeral publishes on `awareness:{id}`; a pod does **not** receive its own publish back; `cancel()` unsubscribes idempotently. Use a real Redis (miniredis or testcontainers) per constitution §VI integration-test guidance.
- [ ] **T004.2** [W2] Implement `Broadcaster{client *redis.Client}` satisfying `port.ClusterBroadcaster`: `Publish` selects `doc:{id}`|`awareness:{id}` by `ephemeral`, tags the payload with a pod/source id so the local pod can drop its own echo; `Subscribe` opens a `PSUBSCRIBE`/`SUBSCRIBE`, dispatches `(payload, ephemeral)` to the handler, returns a once-idempotent `cancel`.
- [ ] **T004.3** [W2] Wire `Room` to publish each applied update (`doc:`) and each awareness/ephemeral frame (`awareness:`) through `Deps.Broadcaster`, and to apply peer-pod payloads as **non-origin** updates (fanned to local members only). Add a fan-out lag metric (R10).
- [ ] **T004.4** [W2] `cmd/server` selects `redis` on `FANOUT_MODE=redis` from `REDIS_URL`; `inmemory` stays default. Add `redis/go-redis` (latest, version-checked §XIV).

### T005 — Durable metastore + blobstore (R7) — `metastore/{rabbitmq,postgres}/`, `blobstore/{fileservice,s3,local}/`
- [ ] **T005.1** [W2] **rabbitmq metastore** (Alkemio default) — tests + impl satisfying `port.MetadataStore` over `amqp091` request/reply RPC mirroring the `server` `save`/`fetch` pattern, extended with `content_pointer`+`blob_store` (index only; blob goes to the BlobStore). **Resolve OPEN-3 first** (new unified `collaboration-save`/`-fetch` contract vs. content-type-routed legacy dialects). `correlationId`/`replyTo` RPC; `Delete` on cascade.
- [ ] **T005.2** [P] [W2] **postgres metastore** (standalone) — `db/migrations/` (golang-migrate) for the metadata table (data-model.md fields + `authorizationPolicyId`); sqlc queries; pgx/v5 adapter satisfying `port.MetadataStore` (`Load`/`Save` upsert-and-bump-version/`Delete`). Tests against a real Postgres (testcontainers).
- [ ] **T005.3** [P] [W2] **fileservice blobstore** — adapter over file-service's existing `/internal/file` API: `Put` = multipart `POST /internal/file` (store the returned UUID as the content pointer; `externalID` for dedup), `Get` = `GET /internal/file/{id}/content`, `Delete` = `DELETE /internal/file/{id}`. Fixed `storageBucketId` per deployment; size within `MAX_UPLOAD_SIZE`. **Resolve OPEN-2** (no file-service expansion for v1). Tests against a stub file-service server.
- [ ] **T005.4** [P] [W2] **s3 blobstore** (standalone) — adapter satisfying `port.BlobStore` over an S3-compatible client (latest SDK, §XIV); content pointer = object key. Tests against a localstack/minio container.
- [ ] **T005.5** [P] [W2] **local blobstore** (standalone) — adapter writing the snapshot under a configured root; content pointer = relative path; atomic write (temp + rename). Tests against a temp dir.
- [ ] **T005.6** [W2] `cmd/server` selects metastore on `METADATA_STORE` and blobstore on `BLOB_STORE`; persist the chosen `BlobStoreKind` in metadata so a doc rehydrates from the right backend regardless of running config.

### T006 — authzeval auth (R13) — `internal/adapter/outbound/auth/authzeval/`
- [ ] **T006.1** [W2] Tests first: a granted privilege → `AuthDecision{Allowed:true}`; a clean denial → `{Allowed:false}`; a transport/breaker failure → **error** (caller fails closed, never a clean denial); handshake token resolves an `Identity` (401 mapping in the WS adapter). Stub auth-eval server.
- [ ] **T006.2** [W2] Implement the **authN** side: resolve `model.Identity{ActorID}` from the Alkemio handshake token/cookie (Oathkeeper/Kratos). Satisfy `port.Auth`.
- [ ] **T006.3** [W2] Implement the **authZ** side over **h2c HTTP/2** `POST {AUTH_SERVICE_URL}/internal/auth/evaluate` (request `{actorId, privilege, authorizationPolicyId}`, response `{allowed, reason, error?}`), NATS `auth.evaluate` fallback, guarded by `sony/gobreaker/v2`, **failing closed**. **Reuse the file-service/wopi h2c+gobreaker client pattern verbatim** (`file-service/internal/adapter/outbound/authhttp/client.go`). Satisfy `port.AuthZ`. **Resolve OPEN-1** (the `documentId→authorizationPolicyId` source + the read/collaborate privilege strings).
- [ ] **T006.4** [W2] `cmd/server` selects `authzeval` on `AUTH_MODE=authzeval` (`open` default); env `AUTH_SERVICE_URL`/`NATS_URL`/breaker tunables (already stubbed in `.env.example`).

---

## Phase 4 — Presence, auth, limits, lifecycle, standalone API — ⬜ FORWARD (Wave 3)

> **Shaped by OPEN-4** (limits defaults + presence/collaborator-mode + FR-014 metric).

### T013 — Presence + collaborator mode + contribution metric (FR-007/FR-014) — `internal/domain/service/`
- [ ] **T013.1** [W3] Tests first: a collaborator that goes idle past `COLLABORATOR_INACTIVITY` is downgraded to viewer (`read-only-state` control); a departed connection's awareness entry is evicted so peers stop rendering its cursor; the contribution metric counts distinct contributing actors within a window and flushes on the interval.
- [ ] **T013.2** [W3] Track the connection↔awareness-client-id mapping in the room (the gap noted in `room.go` `dropMember`) and emit a **server-forced awareness removal** on disconnect and on the delete cascade (closes the Wave-1 D6 deferral).
- [ ] **T013.3** [W3] Implement viewer/collaborator mode + inactivity downgrade: re-evaluate via `AuthZ`, emit `read-only-state`; reset the inactivity timer on any client mutation (mirror the legacy whiteboard `collaborator_inactivity` logic).
- [ ] **T013.4** [W3] Emit the **north-star contribution metric** (per-window contributing actor ids): a Prometheus gauge `collaboration_contributing_actors`, and — in Alkemio mode — the RabbitMQ `contribution` event so `server` analytics stay unbroken (**confirm transport in OPEN-4**).

### T014 — AuthN-at-handshake + per-doc authZ + configurable limits (FR-021/FR-024) — `ws/handler.go`, `service/room.go`
- [ ] **T014.1** [W3] Tests first: an unauthenticated handshake → 401 (except `open`); a viewer's update is **not** applied while a collaborator's is; a connection breaching a limit (doc size / conns-per-room / update rate) is disconnected with a control message and **other collaborators are unaffected**; authZ re-evaluated on `document.access_changed`.
- [ ] **T014.2** [W3] Enforce per-document authZ in the room/handler: gate `update-content` on `AuthZ.Evaluate` (fail closed); deny applying updates from a viewer; re-evaluate on `document.access_changed` (lifecycle).
- [ ] **T014.3** [W3] Enforce configurable limits — `MAX_DOC_BYTES` (~32 MB default), `MAX_CONNS_PER_ROOM` (from metadata `maxCollaborators`, fallback default), per-connection update rate (token bucket, ~50 msg/s default), inactivity timeout — breach → control + disconnect (**confirm defaults in OPEN-4**). Add to `config.go` with fail-fast validation.

### T015 — Lifecycle delete-cascade consumer (FR-012/FR-023) — `internal/adapter/inbound/lifecycle/`
- [ ] **T015.1** [W3] Tests first: a `document.deleted` event disconnects connected clients (`room-closed`), releases the room, and calls `MetadataStore.Delete` + `BlobStore.Delete`; deleting an absent doc is a **no-op** (idempotent); optional `document.created` pre-registers metadata; `document.access_changed` triggers a re-evaluation.
- [ ] **T015.2** [W3] Implement the RabbitMQ lifecycle consumer (same bus as persistence) per `contracts/lifecycle-events.md`: route `document.deleted`→cascade purge via the Manager, `document.created`→pre-register, `document.access_changed`→re-evaluate.

### T016 — Standalone create/delete HTTP API (FR-020/FR-023) — `internal/adapter/inbound/http/`
- [ ] **T016.1** [W3] Tests first: `POST /collab/<id>` pre-registers a document (content-type in body); `DELETE /collab/<id>` cascades the same purge as the bus event; responses are **named structs with a `Render()` method** (constitution anti-pattern 11 — never `map[string]any`) so `apispec.yaml` stays generatable.
- [ ] **T016.2** [W3] Implement the create/delete handlers on the chi router (the no-bus equivalent of the lifecycle events) and regenerate `apispec.yaml`.

---

## Phase 5 — E2E + coverage gate — ⬜ FORWARD (Wave 4)

### T017 — Single-pod + two-pod e2e; ≥95% coverage gate — `test/e2e/`, CI
- [ ] **T017.1** [W4] Single-pod e2e: spin up the service (`open`/`inmemory`/`inline`), drive ≥2 simulated WS clients, assert convergence within 1s for **both** memo and whiteboard, a persistence round-trip, and presence/awareness (SC-002/SC-009).
- [ ] **T017.2** [W4] Two-pod e2e: two service instances behind `FANOUT_MODE=redis`, clients split across pods, assert **cross-instance convergence** with no code change vs single-pod (SC-007/SC-011).
- [ ] **T017.3** [W4] file-service blob-offload e2e: persist with `BLOB_STORE=file-service`, assert reload-identical with the metadata row holding only metadata + a content pointer (SC-006/SC-012).
- [ ] **T017.4** [W4] Limits/authZ e2e: viewer-cannot-mutate, collaborator-can, 401 handshake, limit-breach-disconnect (SC-008/SC-009).
- [ ] **T017.5** [W4] Wire the **≥95% coverage gate** into CI (block merges below threshold) and verify `make openapi` is clean; run `golangci-lint` with zero violations across the whole tree (SC-011, §IX/§XII).

---

## Dependencies & execution order

- **Wave 1 (T001–T003, T007–T012)** — DONE; no remaining dependency.
- **Wave 2 (T004–T006)** — depends on Wave 1's held ports. T004/T005/T006 parallelize; resolve **OPEN-1/2/3** before the contract-touching sub-tasks (T006.3, T005.1, T005.3). Trusting the fork in production also depends on **WS-A's fuzz gate** (workspace, not a server task).
- **Wave 3 (T013–T016)** — T014 (per-doc authZ) depends on T006 (authzeval); T013/T015/T016 depend on Wave-1 lifecycle; resolve **OPEN-4** before T013/T014 defaults.
- **Wave 4 (T017)** — depends on all prior waves; T017.2 needs T004; T017.3 needs T005.3; T017.4 needs T006/T014.

## Counts

| Wave | Status | Milestone tasks | Fine-grained sub-tasks |
|---|---|---|---|
| 1 (Setup+ports, live-sync) | **DONE** | 9 (T001–T003, T007–T012) | — (each proven by named tests) |
| 2 (durable adapters) | Forward | 3 (T004–T006) | 14 (T004.1–4, T005.1–6, T006.1–4) |
| 3 (presence/limits/lifecycle/API) | Forward | 4 (T013–T016) | 11 (T013.1–4, T014.1–3, T015.1–2, T016.1–2) |
| 4 (e2e + gate) | Forward | 1 (T017) | 5 (T017.1–5) |
| **Total** | 9 done / 8 forward | 17 | 30 forward sub-tasks |
