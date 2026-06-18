# Implementation Plan: Unified Real-Time Collaboration Server

**Branch**: `feat/003-unify-collab-yjs` | **Date**: 2026-06-18 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/001-collaboration-server/spec.md`
**Workspace epic**: `../agents-hq/specs/003-unify-collab-yjs/` (WS-C)

> **Repo-local plan.** Owns the server's **architecture and rollout in waves** —
> the hexagon, the ports/adapters, config-driven backend selection, single-pod vs
> multi-pod, standalone vs Alkemio modes. The cross-repo architecture and the
> frozen contracts live in the epic's `plan.md` and `contracts/`.

## Summary

A single Go service that serves both memos and whiteboards over one CRDT (Yjs)
protocol on the forked `y-crdt` core. The domain core owns the **room** — the
authoritative plaintext `Y.Doc` driven by a **single run-loop goroutine** (single
writer, no lock) — and orchestrates y-protocols sync + awareness, a custom
ephemeral channel, debounced v2 snapshot persistence, presence/limits, and the
delete cascade. Everything external is a **port**: cross-pod fan-out, metadata
store, blob store, authN, authZ — each swappable by configuration. The default
config (`open`/`inmemory`/`inline`) boots with **zero external dependencies** (the
standalone story); Alkemio wiring selects `authzeval`/`redis`/`rabbitmq`/`file-service`.
Wave 1 (live-sync server + ports + zero-dep adapters) is **done**; Waves 2–4 add
durable adapters, presence/auth/limits/lifecycle/standalone-API, and the e2e +
≥95% coverage gate.

## Technical Context

**Language/Version**: Go 1.26 (constitution Technology Stack)
**Primary Dependencies**: `coder/websocket` (WS), `go-chi/chi/v5` (HTTP), `go.uber.org/zap` (structured logging), `prometheus/client_golang` (`/metrics`), the forked **`y-crdt`** (`replace skyterra/y-crdt => antst/y-crdt@…` — CRDT core + `protocol` subpackage) · *Wave 2+*: `redis/go-redis`, `rabbitmq/amqp091-go`, `jackc/pgx/v5` + `sqlc` + `golang-migrate`, `golang.org/x/net/http2` (h2c), `nats-io/nats.go` (auth fallback), `sony/gobreaker/v2` (circuit breaker), an S3 SDK
**Storage**: main DB (metadata/index, via `server` RabbitMQ) + pluggable blob store (inline / file-service / S3 / local). Standalone: Postgres + local/S3.
**Testing**: `go test -race ./...`; in-memory port fakes for domain unit tests; the shared e2e harness (single-pod + two-pod) for cross-cutting behavior (Wave 4)
**Target Platform**: Linux server, single static binary (multi-arch container)
**Project Type**: Web service — hexagonal (ports/adapters)
**Performance Goals**: ≤1s convergence after edits settle (SC-002); a slow consumer never stalls the room (bounded per-conn queue, shed on overflow)
**Constraints**: single-pod **zero external dependency** default (FR-020/SC-012); multi-pod purely by enabling `redis` (no code change); server holds **plaintext** docs; authZ **fails closed**
**Scale/Scope**: thousands of elements per board / long memos per doc; many concurrent rooms per pod; horizontal scale via Redis fan-out

## Constitution Check

*GATE: re-checked after Wave-1 design. Constitution = `.specify/memory/constitution.md` (§I–XV).*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Hexagonal Architecture | PASS | Domain (`internal/domain/{model,port,service}`) has zero infra imports; adapters under `internal/adapter/{inbound,outbound}`; the room depends only on `service.Conn`/`service.Metrics`/the five ports — not on `coder/websocket` or Prometheus. |
| II. Pluggable Ports (scaling/persistence/auth) | PASS | `ClusterBroadcaster`/`MetadataStore`/`BlobStore`/`Auth`/`AuthZ` in `port/ports.go`; selected by `config.go` env. Backend never leaks through the wire protocol. |
| III. Standalone-First, Alkemio-Integrated | PASS | Default `open`/`inmemory`/`inline` boots with no DB/bus/auth (`cmd/server`); Alkemio config wires `authzeval`/`rabbitmq`/`redis`/`file-service`. `actorId` never `userId` (`model.Identity`). |
| IV. CRDT Correctness — one core, fuzz-gated | PASS | Single forked `y-crdt` import; no second CRDT impl; wire v1 / snapshot v2; convergence proven by tests. Production-trust gated on WS-A fuzz (workspace). |
| V. Security by Design | PASS (Wave 1 seam) / Wave 3 enforce | Plaintext authoritative doc; handshake authN seam (`Auth.Authenticate`, 401); `AuthZ` documented fail-closed (`model.AuthDecision`). Full per-doc authZ + limits land Wave 3 (T014). |
| VI. Test-First | PASS | Wave 1 was TDD (31 tests; convergence/persistence/reconnect proofs precede impl). In-memory adapters for domain tests; e2e harness Wave 4. |
| VII. Root Cause Analysis | PASS | N/A for spec authoring; bug fixes during impl must be RCA-traceable. |
| VIII. DRY | PASS | `protocol` framing reused (no re-implemented framing); `convention.go` single source for root-type shapes; `isNotFound` centralizes the sentinel branch. OPEN-1/OPEN-3 explicitly avoid duplicating `server`/auth-eval logic. |
| IX. Lint on Completion | PASS | Wave 1: `golangci-lint run` 0 issues + `gofmt`/`goimports` clean (gates green per `tasks/collaboration-service.md`). |
| X. No Legacy Code | PASS | The service does **not** carry the two decommissioned services' code; OPEN-3 recommends a *new unified* persistence contract over baking in two legacy dialects. |
| XI. No Busywork | PASS | Stub adapters are `doc.go` placeholders tied to a task, not speculative code; no abstraction without a wave that uses it. |
| XII. Meaningful Tests Only (≥95%) | PASS (Wave 1 partial) | Wave-1 new-code coverage: service 91.3%, ws 85.5%, http 97.4%. The ≥95% gate is T017 (Wave 4); tests defend real invariants (convergence, no-lost-edits, eviction). |
| XIII. Meaningful Success Criteria | PASS | Every SC is testable in-repo (convergence, round-trip, 401, limit-breach, cascade) or in the e2e harness; none is a vanity metric. |
| XIV. Latest Dependencies | PASS | Wave-2 deps version-checked online at add time (constitution §XIV); the `y-crdt` `replace` is pinned to a fork commit whose fuzz gate must be green. |
| XV. No Assumptions | PASS | The four OPENs are surfaced rather than guessed; sibling services were read to ground them. |

**No gate failures.** The Wave-1 additive ports (`service.Metrics`, `service.Conn`)
are consistent with §I (keep the domain free of Prometheus and the WS type).

## Architecture

### Hexagon

```text
                        ┌──────────────── inbound adapters ────────────────┐
   client (WS) ───────► │ ws/  : Accept → authN → Manager.Join → readLoop   │
                        │ http/: chi router, /healthz, /metrics, (T016 API) │
                        └───────────────────────┬──────────────────────────┘
                                                 ▼
        ┌──────────────────────── domain core (service) ────────────────────────┐
        │ Manager (registry/lifecycle) → Room (single run-loop goroutine)        │
        │   owns *ycrdt.Doc + *ycrdt.Awareness (authoritative, plaintext)        │
        │   sync.go (dispatch SyncStep1/2/Update) · convention.go (root shapes)  │
        │   debounce/idle timers · origin-filtered fan-out · limits/presence(W3) │
        │ depends only on: ports (below) + service.Conn + service.Metrics        │
        └───────────────────────────────┬───────────────────────────────────────┘
                                         ▼
        ┌──────────────────────── outbound ports ───────────────────────────────┐
        │ ClusterBroadcaster  MetadataStore  BlobStore   Auth        AuthZ        │
        │ ├ inmemory (def)    ├ rabbitmq(def)├ inline(def)├ open(def) ├ open(def) │
        │ └ redis (T004)      ├ postgres     ├ file-service          └ authzeval  │
        │                     (T005)         ├ s3 / local (T005)       (T006)     │
        └────────────────────────────────────────────────────────────────────────┘
```

### The seven ports

| Port | Where | Default (zero-dep) | Alkemio / durable | Wave |
|---|---|---|---|---|
| `ClusterBroadcaster` | `port/ports.go` | `inmemory` (no-op) | `redis` (`doc:`/`awareness:`) | T004 (W2) |
| `MetadataStore` | `port/ports.go` | `inmemory` | `rabbitmq` (server) / `postgres` | T005 (W2) |
| `BlobStore` | `port/ports.go` | `inline` | `file-service` / `s3` / `local` | T005 (W2) |
| `Auth` (handshake authN) | `port/ports.go` | `open` (anon) | `authzeval` (token) | T006 (W2) |
| `AuthZ` (per-doc) | `port/ports.go` | `open` (allow) | `authzeval` (auth-eval-svc, fail-closed) | T006 (W2) |
| `service.Metrics` *(W1 additive)* | `service/manager.go` | `NopMetrics` | Prometheus bridge (`http/metrics.go`) | T002 (W1 ✅) |
| `service.Conn` *(W1 additive)* | `service/room.go` | — | `ws.wsConn` (buffered writer, shed-on-overflow) | T008 (W1 ✅) |

**Why two added ports.** Keeping `service.Metrics` and `service.Conn` in the domain
preserves §I: the room never imports Prometheus or `coder/websocket`. The HTTP
adapter owns the concrete collectors and bridges them; the WS adapter owns the
socket and implements `Conn`. The epic's five ports were unchanged by Wave 1 — the
"ports held" finding — so Wave 2 durable adapters plug into the same seams and
parallelize cleanly.

### Config-driven adapter selection (`internal/config/config.go`)

`Load()` reads env and validates each enum, failing fast (§XV — no silent
half-config): `PORT` (4006), `FANOUT_MODE` (`inmemory`|`redis`),
`METADATA_STORE` (`rabbitmq`|`postgres`), `BLOB_STORE`
(`inline`|`file-service`|`s3`|`local`), `AUTH_MODE` (`authzeval`|`open`).
`cmd/server` maps each selection to a concrete adapter and constructs
`service.Deps`; the core consumes only interfaces. Adding a backend is a new
adapter package + one switch arm — no domain change.

### Deployment modes

- **Standalone (zero-dep):** `open` + `inmemory` + `inline` (+ `postgres`/`local`/`s3` optional). Single static binary, no bus/Redis/auth service. The reusable self-contained Yjs server (FR-020/SC-012).
- **Alkemio:** `authzeval` + `redis` (multi-pod) or `inmemory` (single-pod) + `rabbitmq` metadata + `inline`/`file-service` blob. AuthN at the handshake (Oathkeeper/Kratos token), per-doc authZ via the auth-eval-service.

### Single-pod vs multi-pod

Correctness is **intra-process** — the CRDT converges within one room's run loop
regardless of fan-out. `ClusterBroadcaster` only spans pods: with `inmemory` there
is no peer (single pod); with `redis`, the room publishes each applied update on
`doc:{id}` and each ephemeral/awareness frame on `awareness:{id}`, and a
subscription delivers peer-pod payloads into the local room as non-origin updates
(fanned to local members). A client may connect to **any** pod. Enabling multi-pod
is a config flip — **no code change** (SC-007/SC-011).

## Rollout in waves

- **Wave 1 — live-sync server (DONE).** T001–T003 (hexagonal skeleton + ports + zero-dep adapters), T007–T012 (room lifecycle, WS y-protocols sync+awareness, ephemeral channel, both conventions, debounced v2 persistence, US5 reconnect). TDD; gates green; PR #1 (`57b79db`). Headline proofs pass: two-client convergence for *both* content types, persistence round-trip, reconnect-no-lost-edits.
- **Wave 2 — durable adapters.** T004 `redis` fan-out; T005 `rabbitmq`+`postgres` metastore and `file-service`+`s3`+`local` blob; T006 `authzeval` auth. Each plugs into the held ports; parallelizable. Blocked-by OPEN-1/2/3 for contract detail.
- **Wave 3 — presence/limits/lifecycle/standalone-API.** T013 presence + collaborator mode + inactivity downgrade + server-forced awareness eviction + contribution metric; T014 authN-at-handshake + per-doc authZ + configurable limits; T015 `document.deleted` cascade consumer; T016 standalone create/delete HTTP API. Shaped by OPEN-4.
- **Wave 4 — e2e + gate.** T017 single-pod + two-pod e2e (convergence, both content types, persistence, presence, cross-pod fan-out), ≥95% coverage gate, `make openapi` clean.

## Project Structure

### Documentation (this feature)

```text
specs/001-collaboration-server/
├── plan.md            # This file
├── spec.md            # Server user stories, FRs, SCs, OPENs
├── research.md        # Server-level decisions (workspace R4/R7/R13 + Wave-1 resolutions)
├── data-model.md      # CRDT conventions + metadata/snapshot schema (server view)
├── quickstart.md      # Run/test standalone + Alkemio mode
├── tasks.md           # Fine-grained tasks by wave (done + forward)
└── checklists/
    └── requirements.md # Spec-quality checklist
```

### Source Code (repository root) — Wave 1 in place; Wave 2+ packages stubbed

```text
cmd/server/{main.go,app.go}                         # wiring: config → adapters → Manager → router; graceful shutdown
internal/config/{config.go,logger.go}               # env load/validate; zap logger
internal/domain/
├── model/{document,room,control,auth,errors}.go     # domain types (no infra imports)
├── port/ports.go                                    # the five epic ports
└── service/
    ├── doc.go            # Deps (port set)
    ├── manager.go        # registry/lifecycle + Metrics port + NopMetrics
    ├── room.go           # run-loop room, Conn port, fan-out, persist, timers
    ├── sync.go           # dispatchSync (SyncStep1/2/Update)
    └── convention.go     # newRoomDoc (GC'd), applyConvention, isNotFound
internal/adapter/
├── inbound/
│   ├── ws/{handler.go,conn.go}                       # Accept→authN→Join→readLoop; buffered writer
│   └── http/{router,health,metrics,middleware,metrics_bridge}.go
└── outbound/
    ├── fanout/{inmemory/broadcaster.go, redis/doc.go→impl T004}
    ├── metastore/{inmemory/store.go, rabbitmq/doc.go→impl T005, postgres/doc.go→impl T005}
    ├── blobstore/{inline/store.go, fileservice/doc.go→impl T005, s3/doc.go→impl T005, local/doc.go→impl T005}
    └── auth/{open/auth.go, authzeval/doc.go→impl T006}
# Wave 2+ additions: db/migrations/ (postgres), internal/adapter/inbound/lifecycle/ (T015),
# standalone API handlers on http/ (T016), test/e2e/ harness (T017).
```

**Structure Decision**: the fleet hexagonal layout (file-service/oidc/wopi shape).
Domain is infra-free; every external system is a port with a default zero-dep
adapter and one or more durable adapters selected by config. The `y-crdt` fork is
an external library import, never re-implemented here.

## Complexity Tracking

*No constitution violations to justify.* The one notable departure from the epic's
port list — adding `service.Metrics` and `service.Conn` — is additive and
*reduces* coupling (keeps Prometheus and `coder/websocket` out of the domain), so
it strengthens §I rather than violating it. The owning-a-Go-CRDT-core risk is a
**workspace** concern gated by WS-A's fuzz suite, not a server-plan risk.
