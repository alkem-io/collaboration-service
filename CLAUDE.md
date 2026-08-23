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
- **Persistence (pluggable)**: metadata store (RabbitMQ→server), content store
  (in-process / file-service). The service opens NO database of its own: the
  document index is `server`'s, reached by RPC over RabbitMQ, and blobs are
  file-service's.
- **Auth (pluggable)**: handshake AuthN and per-document AuthZ are selected
  INDEPENDENTLY — `open` (standalone) / `header` (gateway-terminated, the prod
  default) for AuthN; `open` / `authzeval`
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
- `metadatastore/{inmemory,rabbitmq}` — `MetadataStore` (document index)
- `persistence/{inprocess,fileservice}` — `persistence.CheckpointStore` (Y.Doc v2 state)
- `auth/{header,open,authzeval}` — `Auth` (handshake authN: `header` is the Alkemio
  production adapter, `open` is standalone) + `AuthZ` (per-document: `authzeval` /
  `open`)

### Ports (cross-repo contracts)

| Port | Contract |
|---|---|
| `hub.Hub` | `specs/003-go-yjs-core-port/contracts/hub.md` (multi-pod fan-out) |
| `MetadataStore` | `.../contracts/persistence-ports.md` (metadata/index) |
| `persistence.CheckpointStore` | `.../contracts/persistence-ports.md` (content-blob) — the CORE's contract, implemented by `persistence/{inprocess,fileservice}`; there is no `BlobStore` port in this repo |
| `Auth` | `.../contracts/ws-protocol.md` (handshake AuthN) |
| `AuthZ` | `.../contracts/ws-protocol.md` (per-document AuthZ, evaluated once per session) |
| lifecycle main queue | frozen argument table, mirrored by `server` — see `internal/adapter/inbound/lifecycle/topology.go` |

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
- `METADATA_STORE` — `inmemory` (default; non-durable, tests/local) | `rabbitmq`
- `CHECKPOINT_STORE` — **required**; `inline` (non-durable, tests/local) | `file-service` (durable)
- `AUTH_MODE` — `header` | `open`. In `header` mode the actor id is read from
  `AUTH_TOKEN_HEADER`, which MUST be named explicitly and MUST be gateway-owned.
  There is no default header name: startup rejects both an unset `AUTH_TOKEN_HEADER`
  and the client-controllable `Authorization`.
- `AUTHZ_MODE` — `authzeval` | `open` (derived from `AUTH_MODE` when unset)

**A document must EXIST before it can be joined.** After authorization succeeds and
BEFORE the room is materialized, `Join` requires a metadata row; an unknown id is
refused without loading a checkpoint, opening a room, or writing an index row. The
metadata store is the existence record and is durable wherever one is configured —
in the Alkemio topology `collaboration-fetch` resolves against the memo/whiteboard
rows in `server`'s own database, so the gate survives a restart. This is what stops
a deleted document from being resurrected by a RECONNECT.

A second guard covers what that one cannot: a Join already in flight, which read
the row before `server` deleted it. `Manager` keeps a monotonic **delete epoch**;
a Join captures it immediately before the existence read, and the acquisition is
refused if any delete landed in between. The two are complementary — the existence
gate catches later connections, the epoch catches in-flight ones — and a refused
in-flight Join is transient (`1011`), so the client reconnects into a fresh
existence read.

The refusal is deliberately **indistinguishable from a denial** — same close status,
same reason — so the service cannot be used to enumerate which document ids exist.
Only the server's own logs separate `ErrDocumentUnknown` from `ErrForbidden`.

The practical consequence: **create, then collaborate.** Production already works
this way (the entity long predates any socket). Standalone and tests must register
the document first, via `POST /collab/{documentId}` or `Manager.PreRegister`.

**Session ends are typed (`session-end`), never free text.** Every path that ends a
connection names a `code`, a `scope` (`member` | `document`) and a `disposition`
(`transient` | `manual` | `terminal`), and the control is delivered BEFORE the socket
closes — both travel one per-connection FIFO, so the client cannot see the close
without the reason. Codes: `update-rate-exceeded` (member, transient),
`document-size-limit-exceeded` (member, manual — the offending edit must be dropped,
or reconnecting re-trips it), `document-deleted` (document, terminal),
`edits-not-saved` (document, terminal — the no-flush teardowns: escalation and
panic), `server-shutdown` (document, transient), `update-not-accepted` (member,
transient — a live room's command buffer stayed full past its deadline, so an
inbound update was NOT applied, broadcast or saved and the client should reconnect
with backoff). That code is for BACKPRESSURE ONLY: an enqueue refused because the
room is tearing down is deliberately silent, because teardown sends its own
authoritative document-scoped end and a competing member-scoped one would preempt
it — reporting a deletion or a data-loss escalation as a retry.
The client branches on these literals, so changing one is a cross-repo change.

**Adding a session-end code is a THREE-STAGE DEPLOY, in this order.** client-web's
`classifySessionEnd` returns null for a code it does not know and the caller fails
CLOSED — terminal, no reconnect — so emitting a new code to a client that has not
shipped it turns a transient fault into a permanent disconnect. The order is
therefore (1) client-web learns the code, (2) this service emits it, (3) any other
caller that depends on it. `update-not-accepted` is at stage 2 as of this commit:
it exists in the code and MUST NOT be deployed before client-web ships its
classifier entry. `TestEverySessionEndCodeCarriesScopeAndDisposition` is the
tripwire — it fails on any change to the code set until the client disposition is
agreed and pinned.

**Authorization is per WebSocket session.** READ and UPDATE are evaluated once, at
connection open and BEFORE the room is materialized, and the resulting capability
holds until that socket closes. There are no per-frame checks and no lease. A
revocation therefore takes effect on the client's next connection, not immediately
(tracked as BASIC-015 in the canonical remediation ledger:
[alkem-io/agents-hq](https://github.com/alkem-io/agents-hq) →
`specs/006-collab-content-unification/kiss-remediation-ledger.md`). A denied session closes
`1008`; an authorization backend outage
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

**The assets-root validator is not independently rollout-safe, and it is not
rolling-deploy safe.** Whiteboard updates carrying inline `data:` file locators are
refused, so a client generation that can still emit them — or that ignores the
`update-rejected` control — must not be running when the validator is. The
client-side work is COMPLETE (`client-web` efd44a2a1 + 72686d930 dropped the dataURL
fallback; 8d69ef4ff + 5c6f4600f handle `update-rejected`; 620c41d2a fixed the
close-code routing), so what remains is verifying the DEPLOYED client generation
carries it, not waiting for the code. Beyond that, a mixed fleet
diverges: an old pod accepts poison and publishes it over the hub, a new pod refuses
that peer update, and the two hold different documents for the same id permanently.
Ordinary overlapping rolling replacement is not allowed — drain the old pods and cut
the service generation over as a boundary.

**Lifecycle queue topology — a cross-repo contract.** One durable quorum main
queue plus one diagnostic quorum DLQ. A transient failure is `Nack(requeue=true)`
and the broker redelivers; an envelope this service can never act on is
`Nack(requeue=false)` and the main queue's DLX records it. There is no retry
ladder, no replay tooling, and no broker-version floor — all three existed to
support delay tiers that no longer exist.

Both queues declare `x-delivery-limit: int32(-1)`. RabbitMQ 4.0 defaults quorum
queues to 20 where 3.x was unlimited, and at the limit a message on a queue with
no dead-letter route is DROPPED (measured on 4.0.5: the 21st delivery). On the
main queue every requeue is another delivery. On the DLQ nothing consumes, but the
management UI's "Get messages" with Requeue=yes issues `basic.get`, which counts —
so repeated operator inspection could destroy the record being inspected.

**`server` declares the main queue too**, with the same arguments (server
`eb12d945` carries the matching dead-letter pair). An inequivalent redeclaration
fails `PRECONDITION_FAILED` and the declaring party does not start, and queue
arguments are immutable — a mismatch is fixed by deleting and recreating the
queue, not by redeploying. Change that table only in lockstep with `server`, as a
planned cutover. Neither side's test can see the other's code, so keeping the two
tables identical is a manual obligation.

## Full Constitution

See `.specify/memory/constitution.md` for the complete set of principles and
governance rules (inherited §I–XV from the fleet, plus the CRDT/WS specifics).
