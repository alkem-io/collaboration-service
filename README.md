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

An always-on private listener serves `GET /internal/memos/{memoId}/markdown`.
It defaults to `127.0.0.1:4016`. `MEMO_MARKDOWN_ADDR` only changes the bind
address: it must be a private or loopback IP on a port different from `PORT`.
It reads one saved snapshot and returns raw Markdown with `Cache-Control: no-store`.
A valid saved empty memo returns an empty body; a missing or unreadable checkpoint
is an error. The current limit is 1 MiB per encoded checkpoint and two concurrent
reads. It neither joins a room nor writes a file. The endpoint has no
application authentication or authorization: do not route it publicly. A rollout
requires network-level trusted-service access and TLS; current consumers are unchanged.
See [the memo verification commands](specs/004-native-collab-previews/quickstart.md#4-verify-the-memo-route-boundary).

The native preview worker refreshes stored files during editing. The first
changed, successful save opens a five-second window; the worker takes the
latest saved revision at its end. Normal room release also submits a final
revision that has not been published, without waiting for another window.
The worker publishes two PNGs for a whiteboard or one Markdown file for a
memo. It creates missing private files in the
document's own bucket and subsequently replaces their content at stable IDs.
A failed overwrite does not allocate another file or block editing, saving
or close. No current frontend or server consumer is switched over.

Publication is always on in the supported single-writer, file-service-backed
topology. See the
[artifact contract and future consumer handoff](specs/004-native-collab-previews/contracts/facility.md#documents-automatic-publication-no-new-http-read-endpoint)
and [real file-service verification](specs/004-native-collab-previews/quickstart.md#3-verify-the-whiteboard-artifact-not-just-worker-completion).

## Status

**Phase 1 (provisioning).** Fleet-consistent hexagonal skeleton with ports
defined and the y-crdt core vendored. The live collaboration behavior lands
with tasks T004–T017 of the workspace spec, driven by this repo's own SpecKit
spec/plan/tasks.

## License

EUPL-1.2
