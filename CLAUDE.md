# Alkemio Collaboration Service (Go)

> **Workspace context.** This repo is part of the Alkemio polyrepo at
> [alkem-io/agents-hq](https://github.com/alkem-io/agents-hq). Cross-repo
> (vertical) feature specs live there under `specs/NNN-*/`. When working on a
> `feat/NNN-...` branch in this repo, the matching workspace spec is the single
> source of truth — for this service that is
> `../agents-hq/specs/003-unify-collab-yjs/`.

Unified real-time collaboration backend for Alkemio. **One Go service over one
CRDT protocol (Yjs)** that replaces both `collaborative-document-service`
(memos) and `whiteboard-collaboration-service` (whiteboards). Memos are a
`Y.XmlFragment`; whiteboards are an id-keyed `Y.Map` scene (per-property
concurrent merge). Transport is raw WebSocket + y-protocols (sync + awareness);
scaling, persistence, and auth are **optional pluggable ports** — single-binary
standalone by default, Redis fan-out + file-service blob offload + auth-
evaluation-service for Alkemio.

This is **WS-C** of the `003-unify-collab-yjs` epic.

## Tech Stack

- **Language**: Go 1.26
- **CRDT core**: `github.com/antst/go-yjs` (pure Go Yjs core + v2 codec, plus the
  `backend/{persistence,memory,hub,conformance}` contracts). Pinned to an EXPLICIT
  version — see [go.mod](./go.mod) — never floated to main: it is this team's own
  pre-1.0 product, so §XIV's "always latest" is satisfied by tracking its releases
  deliberately rather than by tracking its default branch.
- **WebSocket**: `coder/websocket`
- **HTTP Router**: chi v5
- **Logging**: Zap (structured, JSON)
- **Metrics**: Prometheus (`/metrics`)
- **Architecture**: Hexagonal (ports and adapters)
- **Persistence (pluggable)**: metadata store (RabbitMQ→server / Postgres),
  content store (in-process / file-service) — pgx v5 + sqlc + golang-migrate
  for the Postgres path
- **Auth (pluggable)**: handshake AuthN and per-document AuthZ are selected
  INDEPENDENTLY — `open` (standalone) / `header` (gateway-terminated, the prod
  default) / `oidc` (direct validation) for AuthN; `open` / `authzeval`
  (authorization-evaluation-service, h2c HTTP/2 or NATS) for AuthZ

## Architecture

Hexagonal. The domain core (`internal/domain/{model,service,port}`) holds the
CRDT room lifecycle, presence, limits, and lifecycle, and depends only on the
ports. Adapters implement them:

**Inbound** (`internal/adapter/inbound/`):
- `ws/` — raw WebSocket at `wss://<host>/collab/<documentId>` (one document per
  connection): y-protocols sync + awareness + the custom ephemeral channel.
- `http/` — operational surface: `/healthz`, `/metrics`.

**Outbound** (`internal/adapter/outbound/`) — one subpackage per adapter:
- `hub/redis` — `hub.Hub` (cross-pod fan-out, R4). The single-pod default is the
  core's shipped `hub.NewInProcess()`, used directly rather than re-implemented.
  **Multi-pod with a durable store is REJECTED at startup**: no ownership
  mechanism, so two pods flushing the same document overwrite each other. Use a
  single pod (`HUB_MODE=inmemory`) with the durable store.
- `metadatastore/{inmemory,rabbitmq,postgres}` — `MetadataStore` (document index)
- `persistence/{inprocess,fileservice}` — `persistence.CheckpointStore` (Y.Doc v2 state)
- `auth/{open,authzeval}` — `Auth` (handshake authN) + `AuthZ` (per-document)

### Ports (cross-repo contracts)

| Port | Contract |
|---|---|
| `hub.Hub` | `specs/003-go-yjs-core-port/contracts/hub.md` (multi-pod fan-out) |
| `MetadataStore` | `.../contracts/persistence-ports.md` (metadata/index) |
| `BlobStore` | `.../contracts/persistence-ports.md` (content-blob) |
| `Auth` | `.../contracts/ws-protocol.md` (handshake AuthN) |
| `AuthZ` | `.../contracts/ws-protocol.md` (per-document AuthZ, evaluated once per session) |
| lifecycle queue Q1 | `.../contracts/lifecycle-retry-runbook.md` (frozen args, retry ladder, DLQ replay) |

## Configuration (env vars)

See [.env.example](./.env.example). The two BACKEND SELECTORS are MANDATORY —
they have no default, and startup fails naming the missing key and its supported
values. Defaulting them would let an omitted (or renamed, which is the same thing
to the process) key boot healthy on the non-durable in-process store and lose
every document on restart, with nothing in the logs distinguishing "chose inline"
from "never said". Everything else is standalone-friendly (open auth); a
zero-dependency run costs one explicit line per selector:

- `PORT` (default 4006)
- `HUB_MODE` — **required**; `inmemory` | `redis` (redis + `CHECKPOINT_STORE=file-service` is rejected at startup)
- `METADATA_STORE` — `rabbitmq` | `postgres`
- `CHECKPOINT_STORE` — **required**; `inline` (non-durable, tests/local) | `file-service` (durable)
- `AUTH_MODE` — `header` | `oidc` | `open`. In `header` mode the actor id is read from
  `AUTH_TOKEN_HEADER`, which MUST be a gateway-owned header — the
  client-controllable `Authorization` default is rejected at startup.
- `AUTHZ_MODE` — `authzeval` | `open` (derived from `AUTH_MODE` when unset)

**Authorization is per WebSocket session.** READ and UPDATE are evaluated once, at
connection open and BEFORE the room is materialized, and the resulting capability
holds until that socket closes. There are no per-frame checks and no lease. A
revocation therefore takes effect on the client's next connection, not immediately —
see the runbook. A denied session closes `1008`; an authorization backend outage
closes `1011`, so clients keep retrying.

## Development Workflow

- Always run `golangci-lint run` before committing (constitution §IX).
- Tests must defend real invariants — no coverage-padding tests (§XII).
- Root cause analysis is mandatory before any bug fix; document the cause (§VII).
- Verify latest dependency versions online — never trust training data (§XIV).
- Use `actorId` internally, never `userId`.
- The server holds **plaintext** authoritative Y.Docs (FR-021).

## Status

The live collaboration behavior is implemented: room lifecycle, y-protocols
sync/awareness, the ephemeral channel, persistence with debounced flush + retry +
escalation, presence, limits, and the owner-delete cascade. Ports, adapters, CI,
lint, and governance are in place.

Persistence is durable-by-declaration: `Room.persist` writes a COMPLETE V2
snapshot and states its codec, the in-process store records the codec beside the
bytes, and the file-service store accepts V2 only because its blob is a bare Yjs
update other systems read. Nothing infers a codec from bytes — the wrong decoder
returns an empty state vector with no error.

**Broker requirement.** The lifecycle retry topology needs **RabbitMQ >= 3.13.2**
and the service refuses to start below it: on 3.9.13 a quorum queue accepts the TTL
and dead-letter arguments, echoes them back, and expires nothing. CI and
dev-orchestration now run **4.0.5**, so the floor is satisfied.

4.0 sets a default `delivery-limit` of 20 on quorum queues where 3.x was unlimited,
which would silently drop an event redelivered past it on a queue with no
dead-letter exchange — measured: the 21st delivery loses it. Q1 and the DLQ
therefore declare `x-delivery-limit: int32(-1)`; the retry tiers deliberately do
not. Q1's literal is mirrored byte-for-byte by `server`.

Every environment also needs its existing queue state checked before deploying:
queue arguments are immutable after declaration, so a queue that already exists
with different ones cannot be reconfigured, only deleted and recreated.
Preconditions and the check are in
[`specs/003-go-yjs-core-port/contracts/lifecycle-retry-runbook.md`](./specs/003-go-yjs-core-port/contracts/lifecycle-retry-runbook.md).
Do not lower the floor to make a local environment work: it does not buy
compatibility, it buys a service that looks healthy while silently dropping
deletions and revocations.

## Full Constitution

See `.specify/memory/constitution.md` for the complete set of principles and
governance rules (inherited §I–XV from the fleet, plus the CRDT/WS specifics).
