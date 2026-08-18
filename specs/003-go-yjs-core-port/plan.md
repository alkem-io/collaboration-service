# Implementation Plan: go-yjs Core Port & Backend-Contract Adoption

**Branch**: `feat/003-go-yjs-core-port` | **Date**: 2026-08-18 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/003-go-yjs-core-port/spec.md`
**Constitution**: v3.0.1 (amended for this feature — see Constitution Check)

## Summary

Replace the `y-crdt` fork with `github.com/antst/go-yjs`, adopting its backend
contracts as this service's ports rather than bridging the new library behind the
old port shapes. `persistence.Store` (checkpoint-only, over file-service) replaces
`BlobStore`; `hub.Hub` (over Redis) replaces `ClusterBroadcaster`; `memory.Registry`
takes ownership of in-process document identity and lifetime, and the collaboration
session is rebuilt around its handle. `MetadataStore` survives untouched in role — it
carries the Alkemio document index over the `server` RabbitMQ RPC, a different concern
from a byte-and-revision store.

The service has never been deployed to production and holds no production data, so
this is finishing the first version on the right foundation rather than migrating a
running system. It **does** run today — in `server`'s 006 quickstart, against real
Alkemio spaces — so the work must land without breaking that.

The `002` lifecycle invariant suite is CRDT-core-independent by deliberate design and
is the regression net for the whole rebuild.

## Technical Context

**Language/Version**: Go 1.26 (unchanged; `go-yjs` declares `go 1.26.0`)
**Primary Dependencies**: `github.com/antst/go-yjs` (first-party, **pre-1.0, pinned** — §XIV); `coder/websocket`; `chi/v5`; `zap`; Prometheus; `redis/go-redis/v9`; `amqp091`; `golang.org/x/sync`
**Storage**: blobs via `persistence.Store` over **file-service**; the document index via `MetadataStore` RPC to the Alkemio `server` over RabbitMQ. **This service opens no database connection of its own** once the `postgres` adapter is removed
**Testing**: `go test -race`; the library's `backend/conformance` suites; the existing e2e harness (single-pod, two-pod) and JS-interop suite; the `002` invariant suite as the regression net
**Target Platform**: Linux container; k8s (manifests exist on `feat/003-migration`, **not in this branch's history**)
**Project Type**: WebSocket collaboration backend, hexagonal (ports & adapters)
**Performance Goals**: convergence ≤1s after edits settle (SC-002); cold-load cost bounded by document size, not edit count (SC-012)
**Constraints**: crash loss ≤ one configurable flush window; `MAX_DOC_BYTES` 32 MiB; sustained write volume ≈ `document size ÷ flush interval × active documents` (FR-010a); ≥95% unit coverage (§XII); `golangci-lint` zero (§IX)
**Scale/Scope**: 50 connections/room default; **single-pod initially** — multi-pod fan-out ships but is not durability-supported until ownership leases land (FR-022a)

*No NEEDS CLARIFICATION remain: five `/speckit-clarify` passes resolved every open decision (see spec Clarifications).*

## Constitution Check

*GATE: must pass before Phase 0, re-checked after Phase 1.*

| # | Principle | Gate for this feature | Pre-Phase-0 | Post-Phase-1 |
|---|---|---|---|---|
| I | Hexagonal Architecture | Domain core depends only on ports; adapters implement them. The registry/handle types enter the domain as a dependency-inverted port, not an adapter import | ✅ | ✅ |
| II | Pluggable Ports | Adopted contracts **are** the ports; implementations native; superseded ports removed not wrapped; conformance suites in CI | ✅ (amended v2.0.0) | ✅ |
| III | Alkemio-Integrated, In-Process Testable | The in-process path keeps working for all three roles (tests, local dev with real editors, zero-dep smoke test) | ✅ (amended v3.0.1) | ✅ |
| IV | One Core, Fuzz-Gated | `go-yjs` only; no second CRDT; the differential oracle is the core's job and is **not** re-verified here | ✅ (amended v2.0.0) | ✅ |
| V | Security by Design | Auth/authz behaviour unchanged, fail-closed preserved, plaintext authoritative documents (FR-004, FR-020) | ✅ | ✅ |
| VI | Test-First | Contract and invariant tests precede each implementation slice | ✅ | ✅ |
| VII | Root Cause Analysis | Any defect found mid-port is diagnosed before fixing | ✅ | ✅ |
| VIII | DRY | One vocabulary per concept — drives the `MetadataStore` canonicalisation (FR-009a) and the config-key rename (FR-022c) | ✅ | ✅ |
| IX | Lint on Completion | `golangci-lint run` zero before each commit | ✅ | ✅ |
| X | No Legacy Code | `BlobStore`/`ClusterBroadcaster` deleted, not wrapped; `y-crdt` fully removed (FR-001, SC-008/008a) | ✅ | ✅ |
| XI | No Busywork | Adaptive flush scheduling and a durable standalone store explicitly not built | ✅ | ✅ |
| XII | Meaningful Tests Only | ≥95% coverage; every restructured `002` test re-proven non-vacuous (FR-018a) | ✅ | ✅ |
| XIII | Meaningful Success Criteria | All 23 SCs testable within this service | ✅ | ✅ |
| XIV | Latest Dependencies | `go-yjs` pinned to an explicit version; bumping re-verifies the oracle | ✅ | ✅ |
| XV | No Assumptions | Deployment reality verified against the 006 worktree, not inferred (spec Dependencies) | ✅ | ✅ |

**No violations.** Complexity Tracking is therefore empty and omitted.

Two gates deserve emphasis because they are the ones most likely to be silently
failed during implementation:

- **§II / FR-007 (native, no shims).** The cheapest wrong implementation of every
  slice below is a wrapper around the port it replaces. That is the defect this
  feature exists to prevent, and it compiles.
- **§XII / FR-018a (non-vacuity).** The rebuild removes structures several `002`
  tests reach into. Restructuring them is permitted; weakening them is not, and each
  restructured test must be re-proven to fail when its guarantee is reverted.

## Project Structure

### Documentation (this feature)

```text
specs/003-go-yjs-core-port/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── persistence-store.md
│   ├── hub.md
│   └── registry-session.md
├── checklists/
│   └── requirements.md  # 16/16, from /speckit-specify + /speckit-clarify
└── tasks.md             # Phase 2 — NOT created by /speckit-plan
```

### Source Code (repository root)

The existing hexagonal layout is kept. The deltas below are the feature:

```text
cmd/server/

internal/
├── domain/
│   ├── model/                    # unchanged
│   ├── port/                     # ports.go: DELETE BlobStore + ClusterBroadcaster;
│   │                             #   KEEP MetadataStore, Auth, AuthZ, Contributor
│   └── service/
│       ├── room.go               # REBUILT around memory.Handle; teardown-flush matrix
│       ├── manager.go            # registry ownership moves to memory.Registry
│       ├── lifecycle_state.go    # RETIRED where it duplicates registry semantics
│       ├── sync.go               # REBUILT on protocol.SyncHandler + overrides
│       ├── awareness_wire.go     # REBUILT on the core's awareness dispatch
│       ├── doc.go, convention.go, limits.go, presence.go   # re-pointed to go-yjs
│       └── flush.go              # NEW: batching above the Store; escalation policy
│
└── adapter/
    ├── inbound/
    │   ├── ws/                   # transport kept; framing/dispatch delegated to core
    │   ├── http/                 # collab_api.go REMOVED with the standalone withdrawal
    │   └── lifecycle/            # unchanged
    └── outbound/
        ├── persistence/          # NEW — persistence.Store implementations
        │   ├── fileservice/      #   the real one
        │   └── inprocess/        #   the test/dev fixture
        ├── hub/                  # NEW — hub.Hub implementation
        │   └── redis/            #   (single-process uses the core's shipped InProcess)
        ├── metastore/            # RENAMED metadatastore/ (FR-009a)
        │   ├── rabbitmq/         #   kept — the Alkemio system of record
        │   ├── inmemory/         #   kept — test/dev fixture
        │   └── postgres/         #   REMOVED (separate change, authorised by v3.0.0 §III)
        ├── blobstore/            # REMOVED entirely (superseded by persistence/)
        ├── fanout/               # REMOVED entirely (superseded by hub/)
        └── auth/                 # unchanged
```

**Structure Decision**: keep the hexagonal layout and the one-subpackage-per-adapter
convention already in use. New adapter families (`persistence/`, `hub/`) mirror the
directory shape of the ones they replace, so the diff reads as substitution rather
than reorganisation. `blobstore/` and `fanout/` are deleted outright — under §X and
FR-007 they may not survive as wrappers. The `postgres` removal and the
`metastore/`→`metadatastore/` rename are authorised here but land as their own
changes (see research.md, Sequencing).

## Complexity Tracking

No constitutional violations to justify.
