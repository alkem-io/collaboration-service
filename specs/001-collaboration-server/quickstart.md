# Quickstart — collaboration-server

Run and test the unified collaboration server in both modes: **standalone**
(single binary, zero external dependencies) and **Alkemio** (durable adapters).
Module: `github.com/alkem-io/collaboration-service`. Default port: **4006**.

## Build / lint / test

```bash
make build       # go build the server binary
make vet         # go vet ./...
make lint        # golangci-lint run (zero violations expected — constitution §IX)
make test        # go test -race ./...
make openapi     # regenerate/verify apispec.yaml (clean is a Wave-4 gate, SC-011)
make run         # build + run with the current environment
```

Wave-1 gates (all green on `57b79db`): `go build`/`vet`/`gofmt`/`goimports`,
`golangci-lint` (0 issues), `go test -race ./...` (31 tests). New-code coverage:
service 91.3%, ws 85.5%, http 97.4% (the ≥95% gate is Wave 4, T017).

## Standalone mode (zero external dependencies) — Wave 1 ✅

The defaults are standalone-friendly: `open` auth, `inmemory` fan-out, `inline`
blob. No database, bus, Redis, or auth service required.

```bash
# Defaults already select the zero-dep adapters; explicit for clarity:
export PORT=4006
export FANOUT_MODE=inmemory
export BLOB_STORE=inline
export AUTH_MODE=open
# METADATA_STORE defaults to rabbitmq; for a pure-standalone run use the
# in-process metastore via the standalone build path (postgres lands T005).

make run
```

Operational surface:

```bash
curl -s localhost:4006/healthz     # liveness
curl -s localhost:4006/metrics     # Prometheus (collaboration_rooms_active, …)
```

Connect a client (one document per connection, y-websocket model):

```text
wss://localhost:4006/collab/<documentId>?type=memo        # rich-text memo
wss://localhost:4006/collab/<documentId>?type=whiteboard  # Excalidraw scene
```

- `?type=` seeds a brand-new document's convention; a previously-saved document's
  stored content-type wins (research.md D3).
- The server sends `SyncStep1` + an awareness snapshot on connect; the client
  replies `SyncStep2` + its own `SyncStep1`, then both stream `Update`s. Edits are
  fanned to the *other* clients; debounced v2 snapshots are persisted; `saved`/
  `save-error` arrive as type-3 control messages.

## Alkemio mode (durable adapters)

Wave 2 wires the durable adapters; the env keys below are stubbed in
`.env.example` and activate as T004–T006 land.

```bash
export AUTH_MODE=authzeval
export AUTH_SERVICE_URL=http://authorization-evaluation-service:6060   # h2c HTTP/2
# or NATS fallback: export NATS_URL=nats://nats:4222

export METADATA_STORE=rabbitmq        # the server save/fetch bus (OPEN-3)
export BLOB_STORE=inline              # or file-service (OPEN-2)

export FANOUT_MODE=redis              # multi-pod; inmemory for single-pod
export REDIS_URL=redis://redis:6379
```

- AuthN is the Alkemio token/cookie at the WS handshake (401 on failure).
- AuthZ is delegated to the authorization-evaluation-service (per-document
  read/collaborator), guarded by a circuit breaker, **failing closed**.
- Enabling `FANOUT_MODE=redis` makes a multi-pod deployment converge
  cross-instance **with no code change** (SC-007/SC-011).

## Test the server behaviors

Domain/adapter unit tests (in-memory port fakes — no infra):

```bash
go test -race ./internal/...
```

Key Wave-1 proofs (see `tasks.md` for the full map):

| Behavior | Test |
|---|---|
| Two-client memo convergence | `TestTwoClientMemoConvergence`, `TestEndToEndTwoClientConvergence` |
| Two-client whiteboard per-property merge | `TestTwoClientWhiteboardConvergence`, `TestEndToEndWhiteboardConvergence` |
| Persistence round-trip (v2 snapshot reload) | `TestPersistenceRoundTrip`, `TestEndToEndPersistenceReload`, `TestIdleReleasePersistsFinalSnapshot` |
| Offline→reconnect, no lost edits (US5) | `TestOfflineReconnectNoLostEdits` |
| Awareness fan-out, not persisted | `TestAwarenessFanOutAndNotPersisted`, `TestLateJoinerReceivesAwareness` |
| Malformed frame rejected, no divergence | `TestDispatchSyncMalformed`, `TestUnknownWireTypeIgnored` |
| Slow consumer shed, room not stalled | `TestWSConnSlowConsumerDropped`, `TestSlowConsumerEvicted` |
| Save-error control on persist failure | `TestSaveErrorOnMetadataFailure`, `TestSaveErrorControlOnBlobFailure` |
| Handshake 401 / missing doc id 400 | `TestHandshakeRejectedOn401`, `TestMissingDocumentIDIs400` |

## E2E + coverage gate (Wave 4 ✅)

The full service is proven end to end through its real composition root
(`internal/app.New`) by the build-tagged e2e suite, including a real **JS-client
y-protocols interop** harness:

```bash
# Install the JS-interop harness deps once (real yjs + y-protocols + ws).
( cd test/e2e/jsinterop && npm ci )

# Single-pod, two-pod (in-process miniredis), file-service offload, authZ/limits,
# and the JS-interop convergence proof — all hermetic (no external backends):
go test -tags e2e -race ./test/e2e/...

# The build-tagged integration suites against live backends:
export POSTGRES_TEST_DSN=postgres://collab:secret@localhost:5432/collab_test?sslmode=disable
export RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/
go test -tags integration -race ./...

# The ≥95% combined coverage gate (unit + integration + e2e, merged), as CI runs it:
./.scripts/coverage-gate.sh 95.0      # currently 95.8%
```

CI wires this in `.github/workflows/ci-integration.yml` (postgres/rabbitmq
service containers + the Node interop harness), alongside the central lint·race·
build lane in `ci-test.yml`.

> **y-protocols compatibility:** the JS-interop harness validates the Go server
> against ACTUAL `yjs` clients. It caught (and the fix restored) the canonical
> awareness-channel framing (`[type][writeVarUint8Array(body)]`); the sync channel
> was already canonical post the codec revert. Any future framing regression fails
> the harness immediately — the highest-value compat signal in the suite.
