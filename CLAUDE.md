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
- **CRDT core**: the Alkemio fork of `skyterra/y-crdt` (pure Go Yjs core + v2
  codec), vendored via a module replace — see [go.mod](./go.mod). Fork branch
  `001-v2-encoding-and-sync-protocol` (cross-impl fuzz gate green = WS-A).
- **WebSocket**: `coder/websocket`
- **HTTP Router**: chi v5
- **Logging**: Zap (structured, JSON)
- **Metrics**: Prometheus (`/metrics`)
- **Architecture**: Hexagonal (ports and adapters)
- **Persistence (pluggable)**: metadata store (RabbitMQ→server / Postgres),
  content store (in-process / file-service) — pgx v5 + sqlc + golang-migrate
  for the Postgres path
- **Auth (pluggable)**: `open` (standalone) / `authzeval`
  (authorization-evaluation-service, h2c HTTP/2 or NATS)

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
  **Multi-pod with a durable store is unsupported**: no ownership mechanism, so
  two pods flushing the same document overwrite each other (startup warns).
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
| `AuthZ` | `.../contracts/ws-protocol.md` + `lifecycle-events.md` (per-document AuthZ) |

## Configuration (env vars)

See [.env.example](./.env.example). Defaults are standalone-friendly
(single-pod, inline blob, open auth):

- `PORT` (default 4006)
- `HUB_MODE` — `inmemory` | `redis` (redis + `CHECKPOINT_STORE=file-service` is unsupported)
- `METADATA_STORE` — `rabbitmq` | `postgres`
- `CHECKPOINT_STORE` — `inline` | `file-service`
- `AUTH_MODE` — `open` | `authzeval`

## Development Workflow

- Always run `golangci-lint run` before committing (constitution §IX).
- Tests must defend real invariants — no coverage-padding tests (§XII).
- Root cause analysis is mandatory before any bug fix; document the cause (§VII).
- Verify latest dependency versions online — never trust training data (§XIV).
- Use `actorId` internally, never `userId`.
- The server holds **plaintext** authoritative Y.Docs (FR-021).

## Status — Phase 1 (provisioning)

This repo is the fleet-consistent hexagonal skeleton: ports defined, the y-crdt
core vendored and compiling, CI/lint/governance/constitution in place. The live
collaboration behavior (room lifecycle, y-protocols sync/awareness, persistence,
presence, lifecycle cascade) lands with tasks **T004–T017** of
`../agents-hq/specs/003-unify-collab-yjs/tasks/collaboration-service.md`,
driven by this repo's own SpecKit spec/plan/tasks.

## Full Constitution

See `.specify/memory/constitution.md` for the complete set of principles and
governance rules (inherited §I–XV from the fleet, plus the CRDT/WS specifics).
