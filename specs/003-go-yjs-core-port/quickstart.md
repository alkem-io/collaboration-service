# Quickstart — Validating the go-yjs Core Port

**Feature**: `003-go-yjs-core-port` | **Date**: 2026-08-18

How to prove this feature works. Each scenario names the success criteria it
discharges. This is a validation guide — implementation belongs in `tasks.md`.

## Prerequisites

- Go 1.26
- `golangci-lint`
- Docker, for the real-Alkemio scenario (`server`'s 006 quickstart stack)
- `SPECIFY_FEATURE=003-go-yjs-core-port` when running Spec Kit scripts — the branch is
  `feat/003-…` and `check-prerequisites.sh` expects `003-…`

## 0. Gates — run before every commit

```bash
go build ./...
go vet ./...
go vet -tags integration ./...
golangci-lint run          # MUST be 0 issues (§IX)
go test -race ./...
```

**Expected**: all clean. These are the standing gates, not a final check.

---

## 1. The `002` regression net — the gate between slices

```bash
go test -race -count=1 ./internal/domain/service/
```

**Expected**: every `002` invariant passes. Run this **after each implementation
slice**, not once at the end — it is the mechanism that catches the rebuild damaging
the coordination layer.

**Non-vacuity ledger (FR-018a, SC-005/005a)**: for every restructured invariant test,
revert its guarantee in isolation and confirm the test goes RED, then restore. A test
that stays green with its guarantee removed is vacuous and does not count. Record the
proof per test.

> Precedent: `002` built exactly such a ledger — five fixes, each reverted
> individually, each ratchet observed failing in the predicted way.

---

## 2. Contract conformance — the core's own adversarial suites

```bash
go test -race ./internal/adapter/outbound/persistence/...
go test -race ./internal/adapter/outbound/hub/...
```

**Expected** (SC-006):
- `conformance.Persistence` passes for every store implementation
- `conformance.PersistenceFencing` passes **against a fenced instance**, even though no
  deployment enables fencing (FR-008a, SC-017)
- `conformance.Hub` passes — it injects reordering, duplication, and redelivery, so
  passing means documents converge under hostile delivery (SC-007)
- `conformance.PersistenceCompaction` is **not run**; the store implements no
  `Compactor` by design. Record this as a stated decision, not an omission (FR-008b)

---

## 3. In-process smoke — zero external services

The path that must keep working for all three of its roles: tests, local development
with real editors, and this smoke test (constitution §III).

```bash
# in-process fixtures; no database, bus, blob store, or auth service
make run
curl -s localhost:4006/healthz
curl -s localhost:4006/metrics | head
```

Connect a client:

```text
wss://localhost:4006/collab/<documentId>?type=memo
wss://localhost:4006/collab/<documentId>?type=whiteboard
```

**Expected** (SC-009): collaboration works with nothing else running. Two browsers
converge; presence updates; no client-side change (SC-001, SC-002).

**Note**: documents do **not** survive restart here — the fixtures are in-process by
design. That is the documented behaviour, not a defect.

---

## 4. Real Alkemio — whiteboards in spaces

The topology that matters: `hub`=redis, `MetadataStore`=rabbitmq, persistence over
file-service, auth=authzeval, identity resolved at the gateway
(`X-Alkemio-Actor-Id`).

```bash
# in the server repo, 006 branch
docker compose -f quickstart-services.yml up -d
```

**Expected**: open a whiteboard in an Alkemio space and edit it collaboratively.
Per-property merge holds for the whiteboard scene and rich-text merge for memos
(SC-001, FR-003).

**Watch for**: the `server`-side consumer **does** exist on `feat/006-collab-content-unification`
(`collaboration-integration.controller.ts`). If metadata calls fail, check the branch
before concluding it is missing — a check against `develop` will wrongly suggest so.

---

## 5. Durability — bounded crash loss

```bash
# with a durable store configured, drive edits, then kill without graceful shutdown
kill -9 <pid>
# restart and cold-load the same document
```

**Expected** (SC-004): the document recovers exactly the last completed flush, with no
corruption and no manual intervention. Repeat across several kill/restart cycles.

**Also verify**:
- Changing the configured flush interval changes the loss bound, with no code change and
  no stored-format change (User Story 2, scenario 2)
- Cold-load time for a long-lived, heavily-edited document tracks **document size**, not
  accumulated edit count (SC-012)
- A never-before-saved document opened concurrently by many sessions is seeded **exactly
  once**, with content identical to a single-session open (SC-015)

---

## 6. Degraded durability — the silent state made visible

Make the blob backend fail while a session is live.

**Expected**:
- The session **keeps serving**; the document stays dirty; flushes retry with backoff;
  collaborators are told their edits are not yet durable (FR-013, FR-027)
- The not-yet-durable condition is visible **via metrics alone, before anyone is
  disconnected** (SC-013) — flush outcome, consecutive-failure count, time-in-state
- Past the threshold: invalidation, and a disconnect reason that specifically means
  *recent edits could not be saved* — distinguishable from an ordinary disconnect, with
  the discarded edits counted and logged alongside the undurable duration (SC-016)
- No persistence signal available before the rebuild is missing after it (SC-014)

---

## 7. Teardown flush matrix

Prove each path behaves oppositely where it must (SC-018):

| Path | Expected |
|---|---|
| graceful shutdown | **persists** before backends close |
| idle release with unsaved changes | **persists** |
| generation invalidation | **does not persist** |
| escalation after repeated failure | **does not persist** |
| panic on the processing path | **does not persist** |

The three "does not persist" cases are the ones worth an explicit test: writing a
document of doubtful integrity over good stored content is the failure this matrix
exists to prevent.

---

## 8. Hostile frames — offender-only failure

Fuzz malformed and truncated frames at a live connection.

**Expected** (SC-019): the sending connection errors and closes. **Zero room teardowns,
zero effect on other members, zero process crashes.** This is a behavioural improvement
over the previous fallback, where a bad frame relied on a run-loop recover that saved
the pod by destroying the room.

---

## 9. Configuration rename — fail fast, never silently

```bash
# start with a removed/renamed key still set
BLOB_STORE=file-service make run
```

**Expected** (SC-021): startup **fails with an error naming the replacement key**. It
MUST NOT be ignored.

**Why this scenario exists**: the renamed keys have silent defaults. An ignored stale
key would fall back to in-process storage, sending blobs to memory and losing every
document on restart **while the service reported healthy**. Verify against the full
coordination set (FR-022e) — this repo, the manifests on `feat/003-migration`, and
`server`'s 006 quickstart.

---

## 10. Removal completeness

```bash
grep -rn "y-crdt" --include="*.go" .        # expect: no hits
go list -m all | grep -i "y-crdt"           # expect: no hits
grep -rn "MetaStore" --include="*.go" .     # expect: no hits (FR-009a)
```

**Expected** (SC-008, SC-008a, SC-008b): zero references to the previous core; zero
translation shims — each adopted contract has exactly one implementation per backend,
reaching its infrastructure directly; `MetadataStore` is the only spelling.

Verify by inspection that no implementation of a core contract delegates to a port it
supersedes. This is the one failure mode that compiles, passes tests, and still defeats
the purpose of the feature.

---

## 11. Coverage

```bash
go test -race -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1
```

**Expected** (SC-011): ≥95% (§XII). Coverage-padding tests do not count — every test
must defend a real invariant.
