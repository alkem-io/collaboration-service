# Alkemio Collaboration Service

Unified real-time collaboration backend for the Alkemio platform — one Go
service over one CRDT protocol (Yjs). It replaces both
`collaborative-document-service` (memos) and `whiteboard-collaboration-service`
(whiteboards):

- **Memos** are a `Y.XmlFragment` (TipTap/ProseMirror binding).
- **Whiteboards** are an id-keyed `Y.Map` scene (Excalidraw), giving
  per-property concurrent merge instead of whole-element last-write-wins.

Transport is **raw WebSocket + y-protocols** (sync + awareness) at
`wss://<host>/collab/<documentId>`. Horizontal scaling (fan-out), persistence
(metadata + blob), and auth sit behind **pluggable ports** for testability and
bounded adapter choice. The supported Alkemio deployment is integrated: the
authoritative index is in `server` via RabbitMQ, durable content is in
file-service, and authN/authZ use `header` + `authzeval`. Durable deployment
today is one pod with the in-process hub — Redis fan-out with file-service is
rejected at startup until the service has a multi-pod ownership mechanism.

This is **WS-C** of the `003-unify-collab-yjs` epic. The cross-repo spec is the
source of truth:
[`agents-hq/specs/003-unify-collab-yjs/`](https://github.com/alkem-io/agents-hq/tree/main/specs/003-unify-collab-yjs).

## Architecture

Hexagonal (ports and adapters). See [CLAUDE.md](./CLAUDE.md) for the full map.

```
cmd/server/                     # boot + adapter wiring
internal/
├── config/                     # env config + zap logger
├── domain/
│   ├── model/                  # Metadata, Snapshot, Room, Awareness, Identity
│   ├── port/                   # MetadataStore, Auth, AuthZ, Contributor
│   └── service/                # CRDT room mgmt, presence, lifecycle, limits
└── adapter/
    ├── inbound/
    │   ├── ws/                 # raw WS + y-protocols sync/awareness/ephemeral
    │   └── http/               # /healthz, /metrics
    └── outbound/
        ├── hub/redis/
        ├── metadatastore/{inmemory,rabbitmq}/
        ├── persistence/{inprocess,fileservice,metapointer}/
        └── auth/{open,authzeval}/
```

The CRDT core is the Alkemio fork of `skyterra/y-crdt` (pure Go Yjs core + v2
codec), vendored via a module `replace` in [go.mod](./go.mod).

## Quick start

```bash
make build        # build ./bin/collaboration-service
# The two backend selectors are MANDATORY and have no default, so `make run`
# alone fails by design. This is the explicit tests/local fixture — non-durable,
# open auth, never a deployment:
HUB_MODE=inmemory CHECKPOINT_STORE=inline make run
make test         # race-enabled unit tests + coverage summary
make lint         # golangci-lint
make setup-hooks  # install the .githooks pre-commit hook
```

Configuration is via environment variables — see [.env.example](./.env.example).

## Endpoints

| Path | Purpose |
|---|---|
| `GET /healthz` | Process-alive probe (does NOT check Redis/RabbitMQ/file-service/authzeval) |
| `GET /metrics` | Prometheus scrape |
| `GET /collab/{documentId}` | WebSocket collaboration (one document per connection) |

## Status

**Phase 1 (provisioning).** Fleet-consistent hexagonal skeleton with ports
defined and the y-crdt core vendored. The live collaboration behavior lands
with tasks T004–T017 of the workspace spec, driven by this repo's own SpecKit
spec/plan/tasks.

## License

EUPL-1.2
