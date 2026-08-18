# Phase 0 — Research: go-yjs Core Port & Backend-Contract Adoption

**Feature**: `003-go-yjs-core-port` | **Date**: 2026-08-18

All Technical Context unknowns were resolved before planning: five `/speckit-clarify`
passes closed every open decision, and the deployment reality was verified directly
rather than inferred. This document records **why** each decision was taken, what was
rejected, and the sequencing constraints that follow.

---

## D1 — Persistence shape: checkpoint-only `Store`

**Decision**: implement `persistence.Store` as `Appender` + `Loader` + `FenceMode`,
deliberately **not** `Compactor`. One flush writes one whole-document blob; one load
reads one blob and returns it as a single `Checkpoint` with no trailing records and an
empty `Next`.

**Rationale**: the blob backend (file-service) is immutable by contract — every write
is a new blob. Per-transaction appends would produce a blob per keystroke-burst.
Whole-document checkpoints fit that medium exactly. `Compactor` is optional in the
contract and a `Loader` may legitimately return a checkpoint covering all history, so
this is a permitted point in the contract's design space, not a workaround. It also
satisfies FR-012 structurally: recovery cost tracks document size, never edit count.

**Alternatives rejected**:
- *Delta records with compaction* — minimises bytes written, but needs a tracked record
  sequence, a real `Compactor` with compare-and-swap semantics, and unbounded blob
  growth against an immutable store. Cost far exceeds the benefit at current sizes.
- *A degenerate store that keeps only the latest record while claiming log semantics* —
  this is the duct-taping FR-007a forbids.

**Cost accepted**: every flush rewrites the whole document (FR-010a). The configurable
interval is the control; the envelope must be documented (SC-020).

---

## D1a — CONFLICT FOUND IN IMPLEMENTATION: checkpoint-only cannot conform

**Status**: D1's premise is **wrong**, discovered while implementing T025/T045.
Recorded here rather than silently resolved, because it overturns a decision
taken in clarification (Q1) and an assumption written into FR-008b.

**What D1 assumed**: that a checkpoint-only store is "a legitimate point in the
contract's design space" because `Compactor` is optional and a `Loader` may
return a checkpoint covering all history — so `conformance.PersistenceCompaction`
would simply not apply.

**What the suite actually requires** (`backend/conformance/persistence.go`):

- **Per-record fidelity.** It appends `[]byte("first")` then `[]byte("second")`
  and asserts `history[0].Update == "first"`, `history[1].Update == "second"`.
  Records are **opaque bytes**, not CRDT updates — so a checkpoint-only store
  cannot merge them into one covering checkpoint. There is nothing to merge.
- **Pagination with a fixed recovery view.** `Limit: 1` must return one record
  plus a continuation token; the continuation must exclude appends that landed
  after the first page, while a fresh load must include them.
- **Caller-owned slices**, `ErrNotFound` for absent history, `ErrUnexpectedFence`
  for a fenced write to an unfenced store, and context cancellation honoured.

None of that is satisfiable by storing only the latest whole-document blob.

**The conflict**: FR-008/SC-006 require every implementation to pass its
conformance suites; Q1/FR-008b specify a shape that cannot.

**Reconciliation adopted** (satisfies both, and preserves Q1's *intent*):

Implement a **genuine append-log store WITH compaction** — `CompactingStore`,
not the bare `Store` D1 described. Then:

- `conformance.Persistence` and `conformance.PersistenceCompaction` both pass,
  so FR-008/SC-006 hold and FR-008b's "compaction does not apply" is withdrawn.
- The service's *usage* is unchanged from Q1's intent: a flush still writes one
  whole-document update per window, and compaction installs it as the checkpoint
  immediately. Steady state is therefore exactly what Q1 asked for — one
  checkpoint, no trailing records, one blob read on load.
- FR-012 (bounded recovery cost) is now satisfied *by compaction* rather than
  by the absence of history, which is the stronger guarantee: without compaction
  a conforming log would grow without bound.

**What the user should confirm**: this makes `Compactor` required rather than
excluded. The alternative — keep checkpoint-only and skip `conformance.Persistence`
— was rejected because it violates FR-008 and would leave the one contract with
no shipped implementation entirely unvalidated.

## D2 — Flush batching lives **above** the `Store`

**Decision**: the service merges a flush window and calls `Append` **once** per window.
The interval is operator configuration (~500ms–10s range, documented default), armed
only when the document changed; shutdown flush is unconditional.

**Rationale**: `Append` returning nil means the bytes crossed the durability boundary.
A store that buffered internally and returned early would misreport durability —
non-conforming under FR-007a, and `conformance.Persistence` would likely catch the
`Append`→`Load` inconsistency. Batching above the contract keeps `Append` honest while
delivering the same write-amplification reduction.

**Alternatives rejected**: buffering inside the `Store` (dishonest); per-transaction
appends (wrong for an immutable blob medium — see D1).

---

## D3 — Full adoption of `memory.Registry`

**Decision**: the registry owns document identity, coalesced acquisition, eviction,
invalidation, and handle lifetime. `Room` is rebuilt around `Handle`/`Handle.Done`;
`002`'s explicit state machine retires wherever it duplicates registry semantics.

**Rationale**: one lifecycle vocabulary instead of two. `002` rebuilt the coordination
layer precisely because a grown-by-accretion seam kept regenerating defects; carrying
the registry's vocabulary *and* the hand-built one would recreate that condition.

**What the registry does NOT absorb** — these stay this service's own: shutdown drain
ordering that persists before backends close (FR-001); flush policy and its timer;
presence; limits and the byte budget; authz re-evaluation; control messages; the
lifecycle-event consumer's bounded idempotent handling.

**Forced, not chosen**: `InProcessRegistry` documents that it *starts no goroutines*
and that `Evict` never invalidates an outstanding handle. It therefore has no eviction
*policy* — the `002` idle-release policy remains this service's responsibility and must
continue to drive `Evict`.

**Alternatives rejected**: keeping `Manager`'s registry and using the core for CRDT only
(violates FR-005); a partial split with both owning lifetime (two vocabularies — the
`002` failure mode).

**Risk**: several `002` invariant tests reach into structures this removes. FR-018a
governs: restructure permitted, weakening forbidden, non-vacuity re-proven per test.

---

## D4 — Transport rebuilt on the core's dispatch

**Decision**: the WebSocket adapter keeps the socket and connection lifecycle but
delegates framing and dispatch to `protocol.SyncHandler`, registering type overrides
for the checks that must precede application (byte budget, view-only write rejection,
rate limiting).

**Rationale**: three concrete gains. `HandleMessage` recovers panics from malformed or
truncated frames so one bad frame fails one connection — today the fallback is `002`'s
run-loop recover, which survives by tearing down the **whole room**. `InspectMessage`
is allocation-free, so the byte-budget pre-check works from frame length without
decoding. And it retires a parallel implementation of a state machine the core owns
(§VIII, FR-007).

**Alternatives rejected**: keeping the current dispatch on `ReadMessage`/`WriteMessage`
(leaves the room-teardown failure mode and duplicate state machine in place); treating
transport as out of scope (the parallel state machine is exactly what FR-007 targets).

**Note**: the core deliberately ships no transport; the socket layer stays ours.

---

## D5 — Redis fan-out ported now, not deferred

**Decision**: implement `hub.Hub` over Redis in this feature.

**Rationale**: the existing `ClusterBroadcaster` and `hub.Hub` are near-isomorphic —
`Publish(id, payload, ephemeral)` ↔ `Publish(Message{DocumentID, SourceID, Kind, Payload})`,
`Subscribe(...) (cancel, err)` ↔ `Subscribe(...) (Subscription, err)`, and the existing
per-pod source-id echo suppression is exactly the contract's `SourceID` obligation. §II
requires configuration-driven cross-pod fan-out and §X forbids leaving the adapter dead,
so "defer" would mean *delete and rewrite later* — more work than porting now.

**Two contract deltas to handle**:
1. `hub.Handler` returns an `error` to signal backpressure, and an implementation
   "must not silently discard a message merely because an active local subscriber queue
   is full". The current handler has no error path and Redis pub/sub is fire-and-forget.
2. The contract promises neither ordering nor single delivery; completeness comes from
   persistence and state-vector catch-up. Any place the current code leans on pub/sub
   ordering must stop doing so.

**Risk posture**: ships behind the hub-mode configuration key but is **not load-bearing
on day one** — the initial deployment is single-pod.

---

## D6 — Ownership leases deferred; the store is built fence-capable and **tested**

**Decision**: `cluster.Coordinator` is out of scope. The persistence implementation is
nonetheless constructible in both fence modes, and `conformance.PersistenceFencing`
must pass against a fenced instance in CI.

**Rationale**: single-pod first makes `Fence` zero — the normal non-clustered mode — so
fencing is inert. The library supports reopening unfenced data as fenced with no
rewriting of stored history, which makes deferral genuinely cheap. But the *reason* to
build fence-capability now is to avoid that migration later, and untested capability
does not deliver it — it relocates the work and risks discovering the design is wrong
while it guards live documents. `FenceMode` is fixed at construction (deliberately, so
one omitted fence cannot silently disable stale-owner protection), so both modes must
be constructible anyway.

**Consequence**: durable multi-pod is **not supported** until ownership lands
(FR-022a). Only the originating pod persists, but edits originating on different pods
make several pods writers of one whole-document blob; the later write supersedes the
earlier, self-healing only if a live pod flushes again. A startup warning names the
unsupported combination (FR-022b).

---

## D7 — Standalone withdrawn as a product; in-process path retained

**Decision**: the zero-dependency standalone *deployment* is no longer supported
(constitution v3.0.0 §III). The in-process path is retained for three roles: the test
suite, the local development loop including real editors, and the documented
zero-dependency smoke test.

**Rationale**: the promise was never satisfiable — the document index is owned by the
Alkemio `server` and reached by RPC, so every real configuration depends on that
external service, and no environment runs `server` without file-service. It cost real
complexity for a deployment nobody runs (§XI).

**Consequence**: exactly **one** `persistence.Store` is written for real use
(file-service), plus an in-process fixture. No durable standalone store.

---

## D8 — Configuration keys renamed to match the contracts

**Decision**: the keys selecting the `persistence.Store` backing medium and the
`hub.Hub` mode are renamed for the contracts they select. `METADATA_STORE` is unchanged
(`MetadataStore` survives as a port in its own right).

**Rationale**: chosen against the recommendation to keep names stable, on §VIII
consistency grounds — the configuration surface should not name a deleted port.

**The hazard this creates, and its mitigation**: the affected keys have **silent
defaults**. `BLOB_STORE` unset falls back to `inline`, an in-process map. A stale
manifest setting the old key after the rename would be ignored, blobs would go to
memory, and every document would be lost on restart **while the service reported
healthy**. FR-022d therefore requires a removed key to fail startup with an error
naming its replacement. This requirement exists *because* of the rename.

**Coordination set** (FR-022e — all must move together): this repo's config,
`.env.example`, README/CLAUDE.md; `deploy/k8s/base/configmap.yaml` on the **unmerged**
branch `feat/003-migration`; `server`'s 006 `quickstart-services.yml` `collaboration`
block. A partial rollout is a data-loss risk, not a cosmetic inconsistency.

---

## D9 — Teardown flushes only when the document is believed good

**Decision**: **flush** on graceful shutdown and on idle release. **Do not flush** on
generation invalidation, on escalation after repeated write failure, or after a panic on
the document's processing path.

**Rationale**: resolves an apparent conflict between "shutdown flush is unconditional"
(FR-010) and the handle contract's requirement that a session stop reading or mutating a
poisoned document. *Unconditional* scopes to the graceful path. `002` already set this
precedent for panics — tear down without persist, so a mid-panic document is never
written over the last good snapshot. Invalidation and escalation are the same hazard.

---

## D10 — Backend unavailable: keep serving, retry, escalate

**Decision**: a transient durable-write failure does **not** invalidate. The session
keeps serving, the document stays dirty, the flush retries with backoff, and
collaborators are told their edits are not yet durable. After a bounded, configurable
number of consecutive failures, escalate: invalidate, signal holders, disconnect with a
reason meaning *recent edits could not be saved*. Known divergence invalidates
immediately, without waiting.

**Rationale**: a failed flush means *not yet durable*, which is not *diverged* — the
in-memory document remains authoritative and correct. Invalidation exists for the case
where durable state moved out from under the session.

**Loss is accepted but never silent** (FR-028): escalation discards unsaved edits, so it
must produce a distinct counter, a log entry naming the document and its undurable
duration, and a non-generic disconnect reason. **No secondary storage fallback** is
built (FR-029) — escalation fires because the store is unreachable, and a fallback would
reintroduce the `local`/`s3` adapters being deleted.

**Escalation must not depend on the backend being reachable**: if invalidation cannot
reload because storage is still down, the session is still torn down rather than left
serving unbacked state.

---

## D11 — `MetadataStore` retained, and canonicalised

**Decision**: `MetadataStore` is **not** superseded by `persistence.Store`. It is
retained, and `MetadataStore` becomes the single canonical name — port, config
identifiers, and package path alike (`MetaStore*` → `MetadataStore*`,
`metastore/` → `metadatastore/`).

**Rationale**: the index carries content type, authorization policy id, owner ref, and
storage bucket — none of which a contract "expressed only in bytes and revisions"
models or should. Three names for one concept (port `MetadataStore`, config
`MetaStoreMode`, package `metastore/`) is the permanent translation surface §VIII
forbids; `config.go` already has to translate between them in a comment, which is the
tell.

**Trap to avoid**: `MetadataStore` must never become the persistence bridge. That is the
shim FR-007 forbids, and it is the most natural-looking wrong turn in this feature.

---

## Verified deployment reality

Checked against the **006 worktree**, not `develop` — an earlier check of `develop`
plus a stale Wave-2 code comment produced the opposite (wrong) conclusion:

- The unified RabbitMQ contract **has a working consumer**: `server`'s
  `src/services/collaboration-integration/collaboration-integration.controller.ts`,
  `@MessagePattern(SAVE|FETCH|DELETE|INFO, Transport.RMQ)`.
- The service **is deployed and runs against real Alkemio spaces** — `server`'s 006
  `quickstart-services.yml` runs it as `alkemio_dev_collaboration`
  (`ghcr.io/alkem-io/collaboration-service:pr-5`) behind Traefik, identity resolved at
  the gateway and forwarded as `X-Alkemio-Actor-Id`.
- Real topology: `FANOUT_MODE=redis`, `METADATA_STORE=rabbitmq`,
  `BLOB_STORE=file-service`, `AUTH_MODE=authzeval`.
- k8s manifests exist on `feat/003-migration` and are **not in this branch's history**.

The stale comments in `metastore/rabbitmq/contract.go` and `003`'s task file have been
corrected at source so the next reader does not repeat the error.

---

## Sequencing constraints

1. **Constitution amendments precede implementation.** v2.0.0 (core + contracts) and
   v3.0.0 (standalone withdrawn) are committed (`c2d3e12`); v3.0.1 (§III clarification)
   is in the working tree. Under v1.0.1 this feature was unimplementable — §IV mandated
   `y-crdt` by name.
2. **Adapter removals are authorised but land separately.** The `postgres` metadata
   adapter (with `pgx`/`sqlc`/`golang-migrate` and the CI Postgres service), the
   `local`/`s3` blob adapters, and the standalone create/delete HTTP API are legacy
   under v3.0.0 §III + §X. They are kept out of this feature so a foundational port does
   not also carry a multi-adapter deletion — and so a reviewer who disagrees with
   withdrawing standalone can say so without rejecting the port.
3. **The config-key rename is atomic across repos** (D8). It must not land here alone;
   the unmerged `feat/003-migration` manifests are the specific risk.
4. **`002` must be present as the regression net** — satisfied: this branch forks from
   the commit carrying it.
5. **Within the feature**: core swap → `persistence.Store` → registry/session rebuild →
   transport dispatch → `hub.Hub`. Each slice keeps the `002` suite green; the suite is
   the gate between slices, not a final check.

## Tooling note (not a blocker)

`.specify/scripts/bash/resolve-template.sh` calls `resolve_template_content`, which
`common.sh` does not define — a Spec Kit version skew. `/speckit-plan` is unaffected
(`setup-plan.sh` uses `resolve_template`, which exists). `check-prerequisites.sh`
requires `SPECIFY_FEATURE=003-go-yjs-core-port` because the branch is `feat/003-…` and
the script expects `003-…`.
