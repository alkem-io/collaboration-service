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

## Phase 2 — Durable adapters — ✅ DONE (Wave 2)

> Built TDD on `feat/003-wave2-adapters` (off `feat/003-unify-collab-yjs`) → DRAFT
> PR stacked on #1. Each adapter plugs into a Wave-1 held port; the `doc.go`
> placeholders were replaced with real implementations + tests. OPEN-1/2/3 were
> resolved and built exactly to the resolved contracts. **The `BlobStore.Put` port
> now returns the resolved content pointer** (so file-service's assigned UUID is
> recorded in metadata) — a coherent, non-breaking extension across all four blob
> adapters. Gates green (`go build`/`vet`/`gofmt`/`golangci-lint run` = 0 issues,
> `go test -race` clean); the zero-dep standalone default still boots.
>
> **Cross-repo follow-up (OPEN-3):** the `server`-side consumer for the unified
> `collaboration-save`/`-fetch`/`-delete`/`-info`/`-contribution` patterns DOES NOT
> EXIST YET — see `contracts/unified-metadata-rmq.md` for the hand-off.

### T004 — Redis fan-out (`ClusterBroadcaster`, R4) — `internal/adapter/outbound/fanout/redis/`
- [X] **T004.1** [P] [W2] Tests first (`broadcaster_test.go`): publish on `doc:{id}` round-trips to a subscriber on another logical pod; ephemeral publishes on `awareness:{id}`; a pod does **not** receive its own publish back; `cancel()` unsubscribes idempotently. Real Redis via **miniredis** (faithful in-process); the two-instance convergence test (`TestTwoInstancesConverge`) proves SC-011.
- [X] **T004.2** [W2] Implement `Broadcaster{client redisClient}` satisfying `port.ClusterBroadcaster`: `Publish` selects `doc:{id}`|`awareness:{id}` by `ephemeral`, frames the payload with a pod/source id so the local pod drops its own echo; `Subscribe` opens the doc+awareness channels, dispatches `(payload, ephemeral)`, returns a once-idempotent `cancel`. `redis/go-redis/v9@v9.20.1`.
- [X] **T004.3** [W2] Wired `Room` to publish each applied **local** update (raw v1 bytes on `doc:`) and each awareness/ephemeral frame (on `awareness:`) through `Deps.Broadcaster`; peer-pod payloads apply as **peer-origin** updates (fanned to local members, never re-published — ping-pong guard). Fan-out lag metric (R10): `collaboration_fanout_total{outcome}` + `collaboration_fanout_lag_seconds`. Proven: `fanout_test.go` (`TestTwoPodDocUpdateConverges`, `TestTwoPodAwarenessConverges`, `TestPeerUpdateNotEchoedBackToBus`).
- [X] **T004.4** [W2] `cmd/server` selects `redis` on `FANOUT_MODE=redis` from `REDIS_URL`; `inmemory` stays default (a domain `noopBroadcaster` defaults a nil port).

### T005 — Durable metastore + blobstore (R7) — `metastore/{rabbitmq,postgres}/`, `blobstore/{fileservice,s3,local}/`
- [X] **T005.1** [W2] **rabbitmq metastore** (Alkemio default) — `port.MetadataStore` over `amqp091@v1.12.0` NestJS-style request/reply RPC (`{pattern,data,id}` + `correlationId`/`replyTo`). **OPEN-3 resolved → NEW UNIFIED contract** `collaboration-save`/`-fetch`/`-delete` (+ `-info`/`-contribution`), index-only payload `{id, contentType, version, contentPointer, blobStore, authorizationPolicyId, ownerRef}`; `info`→`{read,update,maxCollaborators,isMultiUser?}` and fire-and-forget `contribution` carried forward. Contract types in `contract.go`; documented in `contracts/unified-metadata-rmq.md` (the `server`-consumer hand-off). Unit-tested against the contract shape; live broker via build-tagged integration test.
- [X] **T005.2** [P] [W2] **postgres metastore** (standalone) — `migrations/` (golang-migrate@v4.19.1, embedded) for `collaboration_metadata` (+ `authorization_policy_id`); explicit column SQL; pgx/v5@v5.10.0 adapter (`Load`/`Save` upsert-and-bump-version/`Delete`). Unit-tested via a fake querier; real Postgres via build-tagged integration test.
- [X] **T005.3** [P] [W2] **fileservice blobstore** — over file-service's existing `/internal/file` API (OPEN-2): `Put` = multipart `POST` (returned UUID = content pointer; deletes the superseded snapshot), `Get` = `GET /{id}/content`, `Delete` = `DELETE /{id}`. Fixed `storageBucketId`+`authorizationId` per deployment; `MAX_UPLOAD_SIZE` ceiling. Tests against a faithful stub file-service.
- [X] **T005.4** [P] [W2] **s3 blobstore** (standalone) — `port.BlobStore` over aws-sdk-go-v2 (s3@v1.104.0); content pointer = object key (optional prefix). Unit-tested via a fake S3 API; minio/localstack via build-tagged integration test.
- [X] **T005.5** [P] [W2] **local blobstore** (standalone) — snapshot under a configured root; content pointer = relative path; atomic write (temp + fsync + rename); traversal-rejecting. Tests against a temp dir.
- [X] **T005.6** [W2] `cmd/server` selects metastore on `METADATA_STORE` (`inmemory` zero-dep default / `rabbitmq` / `postgres`) and blobstore on `BLOB_STORE`; the chosen `BlobStoreKind` is persisted per metadata row and rehydration reads it back, so a doc loads from the right backend regardless of running config.

### T006 — authzeval auth (R13) — `internal/adapter/outbound/auth/authzeval/`
- [X] **T006.1** [W2] Tests first: granted → `{Allowed:true}`; clean denial → `{Allowed:false}`; transport / 503-degraded / open-breaker / unresolvable-policy → **error** (caller fails closed); handshake token resolves an `Identity`; empty token rejected. Stub h2c auth-eval server.
- [X] **T006.2** [W2] **authN**: resolve `model.Identity{ActorID}` from the handshake token (gateway-authenticated actor id, Oathkeeper/Kratos). Satisfies `port.Auth`.
- [X] **T006.3** [W2] **authZ** over **h2c HTTP/2** `POST {AUTH_SERVICE_URL}/internal/auth/evaluate` (`{actorId, privilege, authorizationPolicyId}` → `{allowed, reason, error?}`), `sony/gobreaker/v2@v2.4.0`, **failing closed** — **reusing the file-service h2c+gobreaker client pattern verbatim**. **OPEN-1 resolved**: the policy id is resolved from `MetadataStore` (`PolicyResolver`), privileges `read`/`update-content`. Satisfies `port.AuthZ`.
- [X] **T006.4** [W2] `cmd/server` selects `authzeval` on `AUTH_MODE=authzeval` (`open` default); env `AUTH_SERVICE_URL` + breaker tunables. The authzeval adapter is wired with a `policyResolver` over the configured MetadataStore.

---

## Phase 4 — Presence, auth, limits, lifecycle, standalone API — ✅ DONE (Wave 3)

> **Shaped by OPEN-4** (limits defaults + presence/collaborator-mode + FR-014 metric).
> Built TDD on `feat/003-wave3` (off `feat/003-wave2-adapters`) → DRAFT PR stacked
> on #2. Gates green (`go build`/`vet`/`gofmt`/`goimports`/`golangci-lint run` = 0
> issues, `go test -race` clean, `make openapi` clean for the new REST surface);
> the zero-dep standalone binary still boots and the HTTP API works without a bus.

### T013 — Presence + collaborator mode + contribution metric (FR-007/FR-014) — `internal/domain/service/`
- [X] **T013.1** [W3] Tests first: a collaborator that goes idle past `COLLABORATOR_INACTIVITY` is downgraded to viewer (`read-only-state` control); a departed connection's awareness entry is evicted so peers stop rendering its cursor; the contribution metric counts distinct contributing actors within a window and flushes on the interval. Proven: `presence_test.go` (`TestAwarenessEvictedOnDisconnect`, `TestCollaboratorDowngradedOnInactivity`, `TestMutationResetsInactivity`, `TestContributionMetricFlush`).
- [X] **T013.2** [W3] Track the connection↔awareness-client-id mapping in the room (`roomMember.awarenessID`, learned via `trackAwarenessID`) and emit a **server-forced awareness removal** on disconnect and on the delete cascade (`room.go` `evictAwareness`/`forcedAwarenessRemoval` — null-state update with a bumped clock; closes the Wave-1 D6 deferral).
- [X] **T013.3** [W3] Viewer/collaborator mode + inactivity downgrade: mode resolved at join via `AuthZ` (`resolveMode`), `read-only-state` emitted; the inactivity sweep (`presence.go` `sweepInactive`) downgrades idle collaborators; any mutation resets `lastActivity` (`recordActivity`), mirroring the legacy whiteboard `collaborator_inactivity` logic.
- [X] **T013.4** [W3] North-star contribution metric (`presence.go` `flushContribution`): Prometheus gauge `collaboration_contributing_actors` (always) **and** — in Alkemio mode — the RabbitMQ `collaboration-contribution` event via the new `port.Contributor` (rabbitmq `Store.Contribution`); standalone uses a domain `noopContributor`. **OPEN-4 transport resolved: both Prom gauge + RMQ event.**

### T014 — AuthN-at-handshake + per-doc authZ + configurable limits (FR-021/FR-024) — `ws/handler.go`, `service/room.go`
- [X] **T014.1** [W3] Tests first: unauthenticated handshake → 401 (`handler_test.go` `TestHandshakeRejectedOn401`); a viewer's update is not applied while a collaborator's is (`authz_limits_test.go` `TestViewerUpdateNotApplied`/`TestCollaboratorUpdateApplied`); a limit breach (doc size / conns / rate) disconnects only the offender (`TestMaxDocSizeDisconnects`/`TestMaxConnsPerRoomCap`/`TestUpdateRateLimitDisconnects`, `TestRefusedJoinClosesSocket`); authZ re-evaluated on access change (`TestReEvaluateDowngradesOnAccessChange`/`TestReEvaluateUpgradesOnAccessChange`/`TestReEvaluateFailsClosed`).
- [X] **T014.2** [W3] Per-document authZ in the room: `update-content` gated on `AuthZ.Evaluate` (`canMutate`/`dispatchSync` viewer gate, fail closed via `resolveMode`); a viewer's update is dropped; `Manager.ReEvaluate`→`reEvaluateMembers` handles `document.access_changed`. Refused join close-status mapped in `handler.go` `joinCloseStatus`.
- [X] **T014.3** [W3] Configurable limits (`service/limits.go` token bucket + `Limits` in `RoomConfig`): `MAX_DOC_BYTES` (32 MiB), `MAX_CONNS_PER_ROOM` (50), `UPDATE_RATE_PER_SEC` (~50/s token bucket), `COLLABORATOR_INACTIVITY_SECONDS` (120s), `CONTRIBUTION_WINDOW_SECONDS` (60s) — all in `config.go` (`LimitsConfig`/`loadLimitsConfig`) with fail-fast validation (negative → error). **OPEN-4 defaults adopted (epic R9).**

### T015 — Lifecycle delete-cascade consumer (FR-012/FR-023) — `internal/adapter/inbound/lifecycle/`
- [X] **T015.1** [W3] Tests first: `document.deleted` disconnects clients (`room-closed`), releases the room, and `MetadataStore.Delete` + `BlobStore.Delete`; absent-doc delete is a no-op; `document.created` pre-registers; `document.access_changed` re-evaluates. Proven: `service/cascade_test.go` (`TestPurge*`, `TestPreRegisterWritesMetadata`, `TestReEvaluate*`) + `lifecycle/consumer_test.go`.
- [X] **T015.2** [W3] RabbitMQ lifecycle consumer (`lifecycle/{consumer,conn}.go`, same bus as persistence) per `contracts/lifecycle-events.md`: `document.deleted`→`Manager.Purge` (cascade), `document.created`→`Manager.PreRegister`, `document.access_changed`→`Manager.ReEvaluate`. Wired in `cmd/server` for `METADATA_STORE=rabbitmq`.

### T016 — Standalone create/delete HTTP API (FR-020/FR-023) — `internal/adapter/inbound/http/`
- [X] **T016.1** [W3] Tests first (`http/collab_api_test.go`): `POST /collab/<id>` pre-registers (content-type in body, defaults memo, rejects unknown); `DELETE /collab/<id>` cascades the same purge as the bus event; responses are **named structs with `Render()`** (`CreateDocumentResponse`/`DeleteCollabResponse`/`ErrorResponse` — never `map[string]any`, anti-pattern 11) so OpenAPI stays generatable.
- [X] **T016.2** [W3] Create/delete handlers (`http/collab_api.go` `CollabAPIHandler`) mounted on the chi router (no-bus equivalent of the lifecycle events, sharing `Manager.PreRegister`/`Purge`); `make openapi` regenerates `openapi.yaml` with `POST`/`DELETE /collab/{documentId}` + schemas. **The deferred OpenAPI gate is now ON.**

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
- **Wave 3 (T013–T016)** — DONE. T014 (per-doc authZ) built on T006 (authzeval); T013/T015/T016 on Wave-1 lifecycle; **OPEN-4 resolved** (epic R9 defaults + Prom gauge *and* RMQ contribution event).
- **Wave 4 (T017)** — depends on all prior waves; T017.2 needs T004; T017.3 needs T005.3; T017.4 needs T006/T014.

## Counts

| Wave | Status | Milestone tasks | Fine-grained sub-tasks |
|---|---|---|---|
| 1 (Setup+ports, live-sync) | **DONE** | 9 (T001–T003, T007–T012) | — (each proven by named tests) |
| 2 (durable adapters) | **DONE** | 3 (T004–T006) | 14 (T004.1–4, T005.1–6, T006.1–4) |
| 3 (presence/limits/lifecycle/API) | **DONE** | 4 (T013–T016) | 11 (T013.1–4, T014.1–3, T015.1–2, T016.1–2) |
| 4 (e2e + gate) | Forward | 1 (T017) | 5 (T017.1–5) |
| **Total** | 16 done / 1 forward | 17 | 5 forward sub-tasks |
