# Phase 0 — Research: go-yjs Core Port & Backend-Contract Adoption

**Feature**: `003-go-yjs-core-port` | **Date**: 2026-08-18

All Technical Context unknowns were resolved before planning: five `/speckit-clarify`
passes closed every open decision, and the deployment reality was verified directly
rather than inferred. This document records **why** each decision was taken, what was
rejected, and the sequencing constraints that follow.

---

## D1 — Persistence shape: `CheckpointStore`

**Decision**: implement `persistence.CheckpointStore` — one current state per document,
replaced on every save. The log profile (`Appender` + `Loader`) and `Compactor` are
deliberately not implemented. One flush writes one whole-document blob; one load reads
one blob and returns the whole document.

**Rationale**: the blob backend (file-service) is immutable by contract — every write
is a new blob. Per-transaction appends would produce a blob per keystroke-burst.
Whole-document checkpoints fit that medium exactly: the contract defines the checkpoint
profile precisely for a medium whose durable unit is a blob rewritten in place, so this
is the sanctioned profile rather than a narrowed log. It also
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

## D1a — Why the checkpoint profile, not the log profile

**Design result**: a medium that keeps ONE current state per document cannot
implement the log profile, and the two are not interchangeable by narrowing.

`conformance.Persistence` mandates log semantics: it appends OPAQUE bytes — not
valid Yjs updates — and requires them back verbatim, in order, through a paginated
view whose `Through` is fixed by the first page. No merge folds opaque bytes into
one covering state, so a single-current-state store cannot satisfy it, and
compaction does not help because the assertion runs before any compaction could
occur.

Nor can the shape be faked on this medium. A framed record envelope inside the
blob is not available: the stored blob is a bare Yjs update by cross-repo
contract, so an envelope would leave other writers producing bytes this service
could not parse. Multi-file logs would abandon the stable-pointer model the medium
exists to provide.

**Result**: the contract offers a `CheckpointStore` profile — `SaveCheckpoint` /
`LoadCheckpoint` / `FenceMode`, same error sentinels, same caller-owned-slice
discipline, same returning-nil-means-durable rule — with
`conformance.CheckpointPersistence` asserting what is meaningful for that shape
(round-trip fidelity, monotonic revisions, alias-safety in both directions,
cancellation, `ErrNotFound`, declared encoding) and NOT per-record history or
pagination.

**Consequences for this feature**:

1. One whole-document blob per flush, one blob read on load, no record sequence,
   no compaction. The compaction suite does not apply because the store is not a
   log (FR-008b).
2. **The state vector is derived on read**, with `EncodeStateVectorFromUpdateV2`.
   The contract sanctions ignoring the supplied bytes; what `LoadCheckpoint`
   returns must be correct for the stored update, not identical to what the caller
   passed. file-service has nowhere to put it — `ContentMetadata` is a typed
   image-specific JSONB view, not a free-form bag.
3. **The file-service store reports `Unfenced`, by design** — see D6a.
4. **Both stores implement the checkpoint profile**, so the test and production
   paths cannot diverge in profile.

**Process note**: the spec's rule — a contract that does not fit this service's
genuine needs SHOULD be changed in the core, and silent local divergence is
prohibited — is what produced this outcome. Working around it locally would have
left a permanent unvalidated seam in the one path that carries real documents.

---

## D6a — Fencing is NOT reachable over file-service, and that is conforming

**Decision**: the file-service `CheckpointStore` reports `Unfenced`. Stale-owner
protection comes from the cluster lease, not from persistence.

**Rationale**: a fence epoch is per-document state that must be persisted and
compared on every write. file-service has nowhere to hold it — a file row carries
`ExternalID`, `MimeType`, `Size`, `DisplayName`, `StorageBucketID`,
`AuthorizationID`, `TagsetID`, `Version`, and a typed image-specific
`ContentMetadata`. Holding the epoch in the Alkemio metadata index instead would
work mechanically but is **explicitly not a substitute**: the contract is meant to
be the FINAL stale-owner rejection precisely because a partitioned holder can stay
alive, and a rejection that must first reach another service is not that backstop.

**Consequence**: the file-service store cannot hold an epoch, and neither store
carries a fenced path. Both report `Unfenced` and reject a non-zero `Fence` with
`ErrUnexpectedFence` (FR-008a), which is the property a caller depends on.

**Operational consequence carried upstream**: on lease loss a node must **shed its
clients**, not merely decline to write. One that keeps serving reads and presence
while silently failing to persist is split-brain the CRDT cannot resolve — the
bytes converge, but presence, limits and authorization do not.

**Hazard to guard in the implementation**: file-service deduplicates on content
hash within a bucket (`unique(externalID, storageBucketID)`). Identical bytes to
the SAME file succeed; identical bytes to a DIFFERENT file in the bucket 409. That
coincidence can look like stale-owner protection during testing while providing
none, so it must be commented where the 409 is handled.

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

## D0 — Pinned core version and the oracle-reverification rule (T004)

**Pinned**: `github.com/antst/go-yjs v0.0.3` in [go.mod](../../go.mod). An exact
version, not a range, per §XIV's carve-out for a first-party pre-1.0 core: the
usual "track the latest" rule assumes an upstream that versions defensively
against strangers, and this one does not — it is our own product, moving fast,
and a floating dependency would let a change land in this service without anyone
choosing it.

**Reverification rule.** The core is the ORACLE for CRDT behaviour: this service
asserts nothing about merge semantics itself, it runs the core's conformance
suites. So a version bump is not a dependency update, it is a change of oracle,
and it MUST re-run every adopted suite before it is accepted —
`CheckpointPersistence`, `CheckpointPersistenceFencing`,
`CheckpointPersistenceDeletion`, `Memory`, and `Hub` — plus the JS-interop e2e,
which is the only check that the core still agrees with real Yjs rather than
merely with itself. [conformance-coverage.md](./conformance-coverage.md) records
which suites apply and why each non-applicable one does not.

**Sequencing.** Four contract gaps were found by building against the core rather
than by reading it: the missing checkpoint profile, a false backstop claim in the
cluster documentation, a conformance suite that rejected a sanctioned
implementation strategy, and the absent deletion capability. Each was escalated
upstream and fixed there rather than accommodated locally — silent local
divergence from the core is prohibited, because it would make this service's
behaviour depend on a contract nobody else can read.

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

**Outcome (T010)**: on inspection, nothing in `lifecycle_state.go` duplicated
registry semantics — the two govern different objects. `roomState` is about
whether one serving entity may accept work and who owns its teardown; the
registry is about which live document exists and who holds it. A document can be
healthy while its room drains, and a room can be Active while its document is
invalidated underneath it (precisely what `Handle.Done()` signals). What `002`
carried that the registry HAS absorbed — the `released` bool and the hand-built
handle lifetime — is gone. The boundary is now documented at the top of
`lifecycle_state.go` so it is not re-litigated.

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

## D6 — Ownership leases deferred; the stores run unfenced

**Decision**: `cluster.Coordinator` is out of scope, and no fenced path is built.
Every store reports `Unfenced` and rejects a non-zero `Fence` with
`ErrUnexpectedFence`.

**Rationale**: single-pod first makes `Fence` zero — the normal non-clustered mode
— so fencing is inert. The library supports reopening unfenced data as fenced with
no rewriting of stored history, so fencing can arrive with the coordinator that
needs it, against a store built to hold an epoch at that point. A fence arbitrates
between multiple owners of one document; this service writes none, and durable
multi-pod operation is out of scope (FR-022a/b).

**Consequence**: rejecting a fence this service cannot honour is the property that
matters, and it is tested. A caller must never believe it has stale-owner
protection that is not there.

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

**Consequence**: exactly **one** durable `persistence.CheckpointStore` is written for real use
(file-service), plus an in-process fixture. No durable standalone store.

---

## D8 — Configuration keys renamed to match the contracts

**Decision**: the keys selecting the `persistence.CheckpointStore` backing medium and the
`hub.Hub` mode are renamed for the contracts they select. `METADATA_STORE` is unchanged
(`MetadataStore` survives as a port in its own right).

**Rationale**: chosen against the recommendation to keep names stable, on §VIII
consistency grounds — the configuration surface should not name a deleted port.

**A separate defect this exposed**: the selectors had **silent defaults** — unset
`CHECKPOINT_STORE` meant `inline`, an in-process map. A deployment that says
nothing, or misspells the key, gets the non-durable store, serves normally,
reports healthy, and loses every document on restart. Resolved by making the
canonical selectors mandatory (spec FR-022f). Nothing to do with the rename; the
rename is what made us look.

**Consistency set** (FR-022e): this repo's config, `.env.example`, CLAUDE.md;
`deploy/k8s/base/configmap.yaml` on the **unmerged** branch `feat/003-migration`;
`server`'s 006 `quickstart-services.yml` `collaboration` block. Ordinary internal
consistency while the shape settles.

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
reintroduce the content adapters being deleted.

**Escalation must not depend on the backend being reachable**: if invalidation cannot
reload because storage is still down, the session is still torn down rather than left
serving unbacked state.

---

## D11 — `MetadataStore` retained, and canonicalised

**Decision**: `MetadataStore` is **not** superseded by `persistence.CheckpointStore`. It is
retained, and `MetadataStore` becomes the single canonical name — port, config
identifiers, and package path alike (`MetadataStore*` → `MetadataStore*`,
`metastore/` → `metadatastore/`).

**Rationale**: the index carries content type, authorization policy id, owner ref, and
storage bucket — none of which a contract "expressed only in bytes and revisions"
models or should. Three names for one concept (port `MetadataStore`, config
`MetadataStoreMode`, package `metastore/`) is the permanent translation surface §VIII
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
- Real topology: `HUB_MODE=redis`, `METADATA_STORE=rabbitmq`,
  `CHECKPOINT_STORE=file-service`, `AUTH_MODE=header` (AuthZ derives to `authzeval`).
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
   the non-file-service content adapters, and the standalone create/delete HTTP API are legacy
   under v3.0.0 §III + §X. They are kept out of this feature so a foundational port does
   not also carry a multi-adapter deletion — and so a reviewer who disagrees with
   withdrawing standalone can say so without rejecting the port.
3. **The config-key rename is atomic across repos** (D8). It must not land here alone;
   the unmerged `feat/003-migration` manifests are the specific risk.
4. **`002` must be present as the regression net** — satisfied: this branch forks from
   the commit carrying it.
5. **Within the feature**: core swap → `persistence.CheckpointStore` → registry/session rebuild →
   transport dispatch → `hub.Hub`. Each slice keeps the `002` suite green; the suite is
   the gate between slices, not a final check.

## Tooling note (not a blocker)

`.specify/scripts/bash/resolve-template.sh` calls `resolve_template_content`, which
`common.sh` does not define — a Spec Kit version skew. `/speckit-plan` is unaffected
(`setup-plan.sh` uses `resolve_template`, which exists). `check-prerequisites.sh`
requires `SPECIFY_FEATURE=003-go-yjs-core-port` because the branch is `feat/003-…` and
the script expects `003-…`.
