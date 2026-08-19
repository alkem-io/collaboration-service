# Feature Specification: go-yjs Core Port & Backend-Contract Adoption

**Feature Branch**: `feat/003-go-yjs-core-port`
**Created**: 2026-08-17
**Status**: Draft — clarifications resolved; constitutional amendment adopted (v2.0.0); ready for `/speckit-plan`
**Workspace epic**: `../agents-hq/specs/003-unify-collab-yjs/` (WS-C); follows `001-collaboration-server` and `002-room-lifecycle-redesign`
**Backlog Story**: [alkem-io/alkemio#1909 — Unified Real-Time Collaboration Service (Yjs/CRDT)](https://github.com/alkem-io/alkemio/issues/1909) *(existing epic, found by board search per Principle XI — not a new story; `001` and `006` link the same ticket)*
**Input**: Replace the `y-crdt` package with `github.com/antst/go-yjs`. This must not be duct-taping — the new package takes a genuinely different approach to persistence, hub, and memory interfaces, so the service must be reworked where it matters to use those contracts properly, rather than stitching the new package behind the old shapes.

> **Repo-local sub-spec.** This document owns the *requirements* — the behavioural outcomes the service MUST satisfy once built on `go-yjs`. It does **not** specify the implementation model (which adapter implements which port, package layout, work sequencing); that is `plan.md`. The lifecycle safety/liveness properties established by `002` are **inherited unchanged** and act as this feature's regression net.

## Context — why this spec exists

### Development status — this is greenfield, not a live-system migration

**The service has never been deployed and carries no production data.** It is still being written for the first time. `y-crdt` was adopted early, proved inadequate for what this service needs, and was rewritten by this team into what became `go-yjs` — a different product, under this team's own control. This feature is therefore **finishing the first version on the right foundation**, not migrating a running system onto a new one.

Three consequences shape the whole spec:

1. **There is no backward-compatibility obligation.** No stored documents must survive, no format continuity is owed, no rollback path or zero-downtime deployment is required. Development and test data is disposable. This removes what would otherwise be the largest and riskiest part of the work.
2. **The design is free to be correct rather than compatible.** No decision here needs to accommodate a choice made earlier in development; superseded code is deleted outright.
3. **The dependency is not external.** If a `go-yjs` contract does not fit this service's genuine needs, **changing `go-yjs` is a legitimate option** — often the better one — rather than something to work around in this repo. A contract that forces an awkward implementation here is evidence about the contract, and the two are designed together. What remains forbidden is *silently* diverging: working around a contract locally while leaving it unchanged upstream.

The service is built on `y-crdt` (the Alkemio fork of `skyterra/y-crdt`), which supplies a CRDT core and nothing else. Everything a server needs around that core — document registry, cross-pod fan-out, snapshot persistence, eviction — was hand-built in this repo as bespoke ports (`ClusterBroadcaster`, `MetadataStore`, `BlobStore`) and a bespoke coordination layer (`Manager`, `Room`). `002` has just rebuilt that coordination layer to be correct-by-construction.

`go-yjs` is the successor of that same lineage, and it changes the division of labour. It ships **backend contracts** for precisely the concerns this repo hand-built:

| go-yjs contract | Purpose | Ships an implementation? | What it overlaps here |
|---|---|---|---|
| `persistence.Store` (LOG profile) | Durable append-only update log + checkpoints, with revisions, paged recovery reads, optional compaction and fencing | **No — contract only.** Not implemented here: the medium's durable unit is a blob rewritten in place | *(not adopted)* |
| `persistence.CheckpointStore` (CHECKPOINT profile) | One current state per document, replaced on every save; one complete read per load | **No — contract only.** **This is the profile this service implements**, over file-service | `BlobStore` (whole-document snapshot + pointer) |
| `memory.Registry` | Document identity, coalesced acquisition, eviction, and `Invalidate` poison-and-reload recovery | Yes — `InProcessRegistry`, **single-process** | `Manager`'s room registry + singleflight `acquire` |
| `hub.Hub` | Ephemeral fan-out with source-echo suppression; explicitly permits duplication and reordering | Yes — `InProcess`, **single-process** | `ClusterBroadcaster` |
| `cluster.Coordinator` | Optional multi-node document ownership via time-bounded, fenced leases | No — contract only | *(no equivalent — the service has no ownership model)* |
| `backend/conformance` | Importable adversarial suites validating any port implementation against the documented contract | Yes — importable suites | *(no equivalent)* |

**These are contracts first, implementations second.** The shipped defaults exist so that a single-process deployment does not have to write a registry and a fan-out map — busywork with one correct answer. They are explicitly single-process, and persistence ships no implementation at all. The Alkemio deployment runs against Postgres/RabbitMQ/Redis/file-service backends, so this service implements its own `persistence.CheckpointStore` over file-service and its own `hub.Hub` over the selected backend (in-process or Redis), against the defined interfaces; that is the intended use of the library, not a departure from it. "Adopt the contract" therefore means *implement the interface faithfully* — it does not mean *use the shipped default*.

The critical difference is **the durability model, not the API surface**. The service today persists a whole encoded document as a blob and commits a pointer to it (delete-after-commit), coupling two ports. `go-yjs` expresses durability in terms of bytes and revisions, and offers two profiles: a LOG profile (append updates, read back a paginated recovery view, optionally compact) and a CHECKPOINT profile (replace the document's complete state on every save, read it back whole). This service selects the checkpoint profile.

**What "not duct-taping" means here.** The failure mode this feature exists to avoid is *architectural*: keeping the existing `BlobStore` / `ClusterBroadcaster` implementations and bridging them to `go-yjs`'s interfaces with translation shims — a `persistence.CheckpointStore` whose body delegates to a superseded snapshot port, a `hub.Hub` that wraps `ClusterBroadcaster`, a transport glued on through an adapter-over-adapter layer. That would compile, and it would leave the service carrying two overlapping abstractions for every concern, with the old model's assumptions preserved underneath a new vocabulary — a permanent translation surface to maintain and reason through, in violation of §VIII (single source of truth) and §X (no legacy code).

The requirement is instead to write **proper, native implementations of the `go-yjs` contracts, directly over the underlying infrastructure** — a real `persistence.CheckpointStore` that talks to the blob backend, a real `hub.Hub` that talks to Redis, a transport adapter that drives the library's sync/awareness handling directly. The ports superseded by those contracts are **removed, not wrapped**. (`MetadataStore` is not superseded — it carries the Alkemio document index, a genuinely different concern from a byte-and-revision store — so it survives on its own merits, never as a persistence bridge.)

Within that constraint the contract offers two profiles rather than mandating one: a
LOG profile (`Store` — append-only records with paged recovery and optional compaction)
and a CHECKPOINT profile (`CheckpointStore` — one current state per document, replaced
on every save). The checkpoint profile is defined for a medium whose durable unit is a
document-sized blob rewritten in place, which is exactly file-service, so it is the
sanctioned choice here rather than a narrowed log: one whole-document blob per flush,
one blob read on load.

A separate obligation is honesty about guarantees: an `Append` that returns success before the bytes are durable, a `Load` that presents a partial history as complete, or a fan-out that assumes ordering or single delivery the contract never promised, are non-conforming regardless of how natively they are written.

A second difference is **failure semantics**. `go-yjs` gives persistence failure a defined recovery path: `Registry.Invalidate` poisons the current generation, closes outstanding handles via `Handle.Done`, and forces concurrent acquisitions to reload from storage. The service currently has no equivalent concept — a failed save broadcasts a control message and leaves the in-memory document authoritative and diverged from durable state.

### Constitutional impact — amendment required

This feature **cannot be implemented under the current constitution**; it requires an amendment, adopted alongside it:

- **§IV (CRDT Correctness — One Core, Fuzz-Gated)** mandates the service build on `y-crdt` *by name* and carry exactly one core. The amendment must re-point the named core to `go-yjs` while preserving the substance of the principle: one core only, no reimplementation of CRDT logic, and a differential fuzz gate against real Yjs (which `go-yjs` supplies as a 13-surface bidirectional oracle).
- **§II (Pluggable Ports)** enumerates `ClusterBroadcaster`, `MetadataStore`, and `BlobStore` as *the* required ports. Where a `go-yjs` backend contract supersedes one, the principle must name the surviving contract instead, preserving the intent: scaling, persistence, and auth remain swappable and configuration-driven.
- **§III (Standalone-First)** requires a **second amendment**: the zero-dependency standalone *product promise* is withdrawn (see Clarifications, Session 2026-08-18). It was never satisfiable anyway — the document index requires the Alkemio DB in every configuration — and no environment exists that has that DB without file-service. The in-process path is retained explicitly as a **test capability**, served by the core's shipped `InProcessRegistry` / `InProcess` defaults.
- **§XIV (Latest Dependencies Always)** interacts with the target's pre-1.0 status — see Assumptions.

The amendment is a **deliverable of this feature**, not a follow-up. Implementation MUST NOT begin against a constitution that forbids it.

> **Status: adopted.** Constitution **v3.0.0** (2026-08-18) — two amendments. **v2.0.0** amends §II, §IV, §XIV, the Technology Stack Constraints, the Anti-Patterns quick reference, and the file-service integration note. §II now states that where the core defines a backend contract, that contract *is* the port, and adds two rules this spec depends on: implementations MUST be native (no translation shims; superseded ports removed, not wrapped) and MUST pass the core's conformance suites. **v3.0.0** then withdrew the zero-dependency standalone product promise (§III), retaining the in-process path explicitly as a test capability, and marked adapters serving only the withdrawn promise as legacy under §X. FR-023 is satisfied.

## Clarifications

### Session 2026-08-17

- **Q**: Durability model — adopt the append-only log + checkpoint contract natively, or preserve snapshot-only semantics behind the new interface? → **A**: Adopt the contract, with the flush **batched above the `Store`**: the service merges a flush window (armed only when there are changes) plus a shutdown flush, and calls `Append` **once** per window. `Append` therefore never claims durability it does not have. The service's own blob storage is the medium; no new infrastructure is introduced.
- **Q**: What does a flush write — incremental delta records, or the whole document? → **A**: **One whole-document blob per flush; one blob read on load.** The implementation is `persistence.CheckpointStore` — one current state per document, replaced on every save — so a load returns the whole document in one read. The log profile and `Compactor` are deliberately not implemented. No record sequence is tracked and no compaction runs. The service's entire contract with the blob backend is **store blob, read blob** — nothing beyond that.
- **Q**: What flush interval? → **A**: **Configuration, not specification.** It is an operational decision spanning at least ~500ms to ~10s; the service MUST expose it as a configurable knob with a sane default and MUST NOT hard-code a value or assume a particular magnitude. Shutdown flush is unconditional.
- **Q**: Coordination boundary — how much of the `002` layer is replaced by `memory.Registry`, and how much is preserved? → **A**: **Full adoption.** `memory.Registry` becomes the owner of document identity, coalesced acquisition, eviction, invalidation, and handle lifetime; `Room` is rebuilt around `Handle`/`Handle.Done`, and the `002` explicit state machine is **retired wherever it duplicates** those semantics. One lifecycle vocabulary, not two.
  - **What the Registry does NOT absorb, and therefore stays this service's own**: the shutdown drain ordering that persists before backends close (FR-001); the flush policy and its timer; presence; limits and the byte budget; authz re-evaluation; control messages; and the lifecycle-event consumer's bounded, idempotent handling. `Room` continues to exist as the collaboration session (**`Room`** is the canonical term throughout this spec, plan, and tasks) — it simply no longer owns document identity or doc teardown ordering.
  - **Consequence for the regression net** (see FR-018a): the `002` invariants are stated as properties, but several of its tests reach into structures this decision removes. Those tests are restructured, never weakened, and each is re-proven non-vacuous against the new internals.
- **Q**: Is `cluster.Coordinator` (fenced single-owner leases) in scope this iteration? → **A**: **Deferred.** The initial deployment is single-pod, where `Fence` zero is the normal non-clustered mode, so fencing is inert. The library supports reopening unfenced data as fenced with no rewriting of stored history, so enabling it later needs no migration. The stores run unfenced and reject a fence they cannot honour (FR-008a).
- **Resolved (no question needed)**: Multi-pod Redis fan-out is **ported in this feature, not deferred** — the existing `ClusterBroadcaster` and `hub.Hub` are near-isomorphic, §II requires configuration-driven cross-pod fan-out, and §X forbids leaving the adapter dead. It ships behind `HUB_MODE=redis` but is not load-bearing on day one, since the initial deployment is single-pod.
- **Resolved (no question needed)**: The service's contract with the blob backend is **store blob, read blob**. What the backend does with superseded blobs — retention, expiry, reclamation — is its own contract and is explicitly **not this service's concern**. This service MUST NOT model, track, expose, or reason about blob history.

### Session 2026-08-18

- **Q**: What is the acceptable write volume, given that each flush rewrites the entire document? → **A**: **Document the envelope; do not build adaptive behaviour.** Sustained write volume is approximately `document size ÷ flush interval × actively-edited documents`. That relationship MUST be documented alongside the flush-interval setting, and the shipped default MUST be defensible against the configured document-size limit rather than chosen arbitrarily.
  - The tension is **pre-existing, not introduced here**: the current model also rewrites the whole document, at a 500ms debounce, with `MAX_DOC_BYTES` defaulting to 32 MiB — a limit-sized document at that interval is ~64 MiB/s for one document. A longer configurable interval strictly improves the situation.
  - Adaptive per-size flushing is deliberately **not** built: it would add complexity for a problem nobody has measured (§XI). If real document sizes later show the envelope is too tight, that is a measured follow-up.
- **Q**: Should the WebSocket layer be rebuilt on the core's own message-dispatch handler, or keep this service's hand-written dispatch? → **A**: **Rebuild on the core's dispatch handler.** The sync state machine, awareness routing, and malformed-frame recovery come from the core; this service registers overrides only where a domain check must run before anything is applied.
  - Gains: a malformed or truncated frame is recovered by the core and surfaced as an error on that one connection, rather than relying on `002`'s run-loop recover — which saves the pod but tears the **whole room** down for one bad client. Frame validation is allocation-free, so the byte-budget pre-check works from frame length without decoding. And it retires a parallel implementation of a state machine the core already owns (§VIII, FR-007).
  - Unchanged: transport remains this service's responsibility (the core deliberately ships none), and every domain check — byte budget before apply, view-only write rejection, presence, rate limiting — is preserved via handler overrides.
- **Q**: Should the backend-selection environment variable names stay as they are, given that one is named after a port this feature deletes? → **A**: **Rename them to match the adopted contracts.** The configuration vocabulary follows the contracts, not the superseded ports: the `persistence.CheckpointStore` backing medium and the `hub.Hub` mode are named for what they now are. `METADATA_STORE` is unaffected — `MetadataStore` survives as a port in its own right (FR-009a).
  - This is a **coordinated cross-repo change**, not a local edit: the current keys appear in this repo's config, `.env.example`, and docs; in `deploy/k8s/base/configmap.yaml` (on the unmerged branch `feat/003-migration`); and in `server`'s 006 `quickstart-services.yml` `collaboration` service block. All must move together.
  - **Critical hazard**: an absent selector used to fall back to a default — unset meant `inline`, an in-process map. A key that is misspelled or simply omitted is silence to `os.Getenv`, so the service stored blobs in memory, losing every document on restart while appearing healthy. **Resolved by making the canonical selectors MANDATORY** (FR-022f): `HUB_MODE` and `CHECKPOINT_STORE` have no defaults, so an omitted, misspelled, or renamed key fails startup naming the missing key and its supported values.
- **Q**: When a document is being torn down, when should the service write a final save — and when must it deliberately not? → **A**: **Flush only when the document is known-good.** Graceful shutdown and idle release flush; invalidation, escalation, and a panicking run loop tear down **without** flushing.
  - Rationale: a poisoned or half-mutated in-memory document may have diverged from stored content, so writing it would overwrite good content with suspect content. `002` already set this precedent for the panic path (tear down without persist, so a mid-panic document is never written over the last good snapshot); invalidation and escalation are the same hazard. Meanwhile FR-001's shutdown flush and idle release act on documents believed good, so they must flush or a window of edits is lost.
  - This resolves an apparent conflict between "shutdown flush is unconditional" and the handle contract's requirement that a session stop reading or mutating a poisoned document: *unconditional* scopes to the graceful path, not to every teardown.
- **Q**: Should the stale-owner (fencing) path be tested now, while it is switched off? → **A**: **Yes.** The library's fencing conformance suite MUST pass against a fenced instance of the store as part of this feature, even though every deployment runs unfenced.
  - Rationale: the sole justification for building fence-capability now is to avoid rewriting stored history later. Untested capability does not deliver that — it relocates the work and risks discovering the design is wrong at the moment it is guarding live documents. Testing it while it is inert is cheap; testing it under a coordinator rollout is not.
  - **Stale-owner protection**, for the record: an ownership lease alone cannot stop a partitioned-but-alive pod from writing after its lease expired, so every durable write carries a monotonically increasing `Fence` epoch and a fenced store rejects any write bearing an older one — storage, not the coordination layer, is the final arbiter. `FenceMode` is fixed at construction rather than per-write, precisely so a single omitted fence cannot silently disable the protection.
- **Q**: When a document is torn down because saving has failed repeatedly, what should happen to the edits that were never saved? → **A**: **The loss is accepted, but never silent.** Escalation discards the unsaved edits; the service MUST count the loss, log it with the document id and how long the document had been undurable, and disconnect members with a **distinct reason meaning "your recent edits could not be saved"** — not a generic disconnect.
  - Rationale: escalation fires precisely *because* the store is unreachable, so any write-it-elsewhere fallback needs a second storage path this service deliberately no longer has. Building one to serve an outage would reintroduce the very adapters being deleted. The thing to prevent is silent discarding, not discarding.
- **Q**: With ownership leases deferred, what is the durability posture for multi-pod deployments where two pods can save the same document? → **A**: **Multi-pod is fan-out-supported but NOT durability-supported until `cluster.Coordinator` lands.** Single-writer ownership is a documented precondition for durable multi-pod operation, not something this iteration provides.
  - The hazard is concrete: only the originating pod persists on the flush timer, but edits originating on different pods make several pods writers of the same document. Each flush writes the **whole** document, so if pod B flushes a state that has not yet received pod A's latest edits by fan-out, B's blob supersedes A's. It self-heals when A flushes again — unless A dies first. This is pre-existing rather than introduced here; deferring the coordinator is what makes stating it necessary.
  - Day one is single-pod, so nothing is lost by declaring this now, and a later operator cannot enable multi-pod believing durability is safe.
- **Q**: When a document is opened for the first time, where is its stored state loaded in? → **A**: **Inside the registry's open path.** The open function restores from the checkpoint store, reached through the document's `contentPointer`. Because the registry coalesces concurrent cache misses into one call, first-open restore is exactly-once **by construction** rather than guarded by a check that races. A document with no pointer has no stored state and opens EMPTY; the metadata reply carries no document bytes.
- **Q**: What must an operator be able to see about durability — specifically, that edits are being accepted but are not yet durable? → **A**: **Parity plus the new failure modes.** Every persistence signal that exists today must survive the rebuild, and the states this feature introduces must be observable in their own right: flush outcome, consecutive-failure count and escalation, generation invalidation, and how long a document has been accepted-but-not-durable.
  - Rationale: FR-013's retry-with-backoff creates a **silent degraded state** that did not previously exist — a document accepting edits while every flush fails looks identical to a healthy one from outside, until escalation disconnects its members. Today's `SnapshotFailed` counter cannot express "consecutive", "escalating", or "invalidated". Parity alone would leave the state most worth seeing invisible.
- **Q**: `MetadataStore` or `MetadataStore` — which is canonical? → **A**: **`MetadataStore`**, everywhere. The codebase currently carries three names for one concept: the port `port.MetadataStore`, the config identifiers `MetadataStoreMode`/`MetadataStoreInMemory`/`MetadataStoreRabbitMQ`/`parseMetaStore`, and the package path `metastore/`. `config.go` even has to translate between them in a comment ("MetadataStoreMode selects the MetadataStore adapter") — the tell that two vocabularies exist for one thing (§VIII).
- **Q**: Is the zero-dependency standalone mode a supported product configuration, or only a development/test convenience? → **A**: **Test/dev convenience only.** "Standalone" conflated two things: a product promise nobody wants, and in-process test fixtures that are genuinely needed. The promise is dropped (requires a §III amendment); the fixtures stay.
  - The "zero external dependencies" claim was never true for any useful configuration: the document index is owned by the Alkemio `server` and reached by RPC over RabbitMQ, so a real configuration always depends on that external service. Every environment running `server` — test environments included — also runs file-service, so the other content adapters serve a deployment that does not exist.
  - **The service holds no database of its own.** `pgx`/`database/sql` appear only in the `postgres` metadata adapter; removing it leaves the service with zero direct database access, and drops `pgx`, `sqlc`, and `golang-migrate` with it. Every durable interaction becomes a service call: index via `server` (RabbitMQ RPC), blobs via file-service, authz via authorization-evaluation-service.
  - **Consequence for this feature**: exactly **one** durable `persistence.CheckpointStore` is written, over file-service. No durable standalone store is built. Tests use in-process fixtures and the shipped `InProcessRegistry` / `InProcess` hub. Auth, authz, and durable backends are not exercised by the fixture path.
- **Q**: When the persistence backend is unavailable, what should a live `Room` do? → **A**: **Keep serving; retry the flush with backoff; escalate to invalidate + disconnect only after a bounded number of consecutive failures.** A failed flush means "not yet durable", which is *not* the same as "diverged" — the in-memory document remains authoritative and correct. Invalidation is reserved for known divergence from durable state, not for transient write failure.

## User Scenarios & Testing *(mandatory)*

> "Users" here are the **collaborators** whose documents and edits the service must not lose, and the **operator** running it. Every story is an observable outcome, independently testable.

### User Story 1 — Collaboration works over the new core (Priority: P1)

A collaborator editing a memo or whiteboard gets working real-time collaboration over the new core: browsers converge, presence works, and no client-side accommodation is required.

**Why P1**: this is the entire premise. The core exists to speak Yjs to browsers; if wire compatibility or convergence does not hold, nothing else about the design matters.

**Independent Test**: run the existing e2e harness (single-pod and two-pod) and the JS-interop suite against the service; assert convergence.

**Acceptance Scenarios**:

1. **Given** two browsers editing one document, **When** both submit concurrent edits, **Then** they converge to identical state.
2. **Given** a Yjs client of the currently supported version, **When** it performs the sync handshake and awareness exchange, **Then** it interoperates with no client-side change.
3. **Given** a whiteboard's id-keyed `Y.Map` scene and a memo's `Y.XmlFragment`, **When** each is edited concurrently, **Then** per-property merge behaviour is preserved for both document conventions.

---

### User Story 2 — Crash loss is bounded and operator-controlled (Priority: P1)

A collaborator's edits survive an abrupt process termination up to a bounded, operator-configured window, and the document recovers cleanly and quickly on restart no matter how long it has been edited.

**Why P1**: durability is the property most visible to users when it fails. The port changes how it is achieved, so the guarantee must be an explicit bound the operator controls rather than an incidental consequence of a save timer.

**Independent Test**: drive edits, kill the process without a graceful shutdown at a controlled point, restart, and assert the document recovers exactly the last completed flush.

**Acceptance Scenarios**:

1. **Given** a document with a completed flush, **When** the process is killed abruptly and restarted, **Then** the document recovers exactly that flushed state, with no corruption and no manual intervention.
2. **Given** the configured flush interval, **When** an operator changes it, **Then** the crash-loss bound changes accordingly, with no code change and no stored-format change.
3. **Given** a document edited heavily over a long period, **When** it is cold-loaded, **Then** recovery cost is bounded by the document's size rather than by how many edits it has accumulated.
4. **Given** a durable write that fails, **When** the failure is observed, **Then** the session keeps serving and retries, and collaborators are told their edits are not yet durable.
5. **Given** flushes that keep failing, **When** the consecutive-failure threshold is crossed, **Then** the generation is invalidated and members are disconnected with a reason.
6. **Given** a document accepting edits while every flush fails, **When** an operator inspects metrics, **Then** the not-yet-durable condition is visible before any member is disconnected.

---

### User Story 3 — The `002` lifecycle guarantees still hold (Priority: P1)

Every safety and liveness property established by the lifecycle redesign continues to hold on the new core: no edit loss on graceful shutdown, no hung join or purge, no room or goroutine leak, no multi-pod teardown deadlock, limits enforced on every entry point.

**Why P1**: `002` is the most expensive correctness work in this repo, and the coordination layer is exactly where this work cuts. Silently regressing it would trade a hard-won guarantee for an internal rearrangement.

**Independent Test**: the `002` invariant suite — deliberately built CRDT-core-independent (transport relay fidelity, persistence byte round-trip, lifecycle/concurrency) — runs unchanged and green against the rebuilt service.

**Acceptance Scenarios**:

1. **Given** the `002` invariant suite, **When** it runs against the rebuilt service, **Then** every property still holds, with no assertion weakened.
2. **Given** a guarantee now delivered by a `go-yjs` contract rather than hand-built code, **When** its invariant is exercised, **Then** it still holds — the guarantee moved, it did not disappear.
3. **Given** an invariant test that reached into a structure the rebuild removed, **When** it is restructured to the new internals, **Then** it asserts the same or a stronger property and is re-proven non-vacuous by reverting its guarantee and observing it fail.
4. **Given** the rebuilt coordination layer, **When** a full adversarial review runs over it, **Then** none of the seam/concurrency/lifecycle defect class `002` closed has reappeared.

---

### User Story 4 — Port implementations are contract-validated (Priority: P2)

Every custom backend port this service implements is validated against the library's own adversarial conformance suites, not only against this repo's tests.

**Why P2**: the contracts permit behaviour this service has never had to tolerate (duplicated and reordered fan-out, paged recovery reads, fencing). Self-authored tests would encode our assumptions rather than the contract.

**Independent Test**: the conformance suites for each implemented port run in CI and pass.

**Acceptance Scenarios**:

1. **Given** each custom persistence implementation, **When** the applicable persistence conformance suites run, **Then** all pass.
2. **Given** a fenced instance of the store, **When** the fencing conformance suite runs, **Then** it passes — despite no deployment enabling fencing — so the capability is proven while it is still cheap to correct.
3. **Given** each custom fan-out implementation, **When** the hub conformance suite runs — which deliberately injects reordering, duplication, and redelivery — **Then** documents still converge.
4. **Given** the in-process test path, **When** the core's shipped single-process defaults satisfy it, **Then** they are used unmodified and no bespoke reimplementation of them is carried in this repo (§X, §XI).
5. **Given** the deployed configuration, **When** its custom fan-out and persistence implementations are used, **Then** they are validated by the same conformance suites as any other implementation — a custom implementation is held to the contract, not exempted from it.

---

### User Story 5 — Multi-pod collaboration survives hostile fan-out (Priority: P2)

Collaborators spread across pods converge even though the fan-out contract explicitly promises neither ordering nor single delivery.

**Why P2**: production runs multi-pod. The current implementation may lean on properties the new contract does not guarantee.

**Independent Test**: two-pod e2e with a fan-out layer injecting duplication, reordering, and redelivery; assert convergence and correct echo suppression.

**Acceptance Scenarios**:

1. **Given** collaborators on different pods, **When** fan-out duplicates and reorders messages, **Then** all clients still converge.
2. **Given** a pod publishing an update, **When** that message returns to its own source, **Then** it is suppressed rather than reapplied or echoed to its originator.
3. **Given** a subscriber that misses messages while disconnected, **When** it reconnects, **Then** completeness is restored through persistence and state-vector catch-up, not by assuming fan-out delivered everything.

---

### Edge Cases

Each maps to a requirement and a non-vacuous test:

- A deployment sets only the pre-rename configuration keys → startup fails because the canonical selector is absent, rather than defaulting to in-process storage and silently losing documents. The config carries no knowledge of the abandoned names. *[stale config key]*
- A client sends a malformed or truncated frame → that connection errors and closes; the room, its other members, and the process are unaffected. *[offender-only frame failure]*
- A document is invalidated while dirty → it is torn down **without** a final flush; the unsaved edits are discarded rather than written over stored content whose relationship to them is unknown. *[no flush on poisoned teardown]*
- An idle document with unsaved changes is released → it **does** flush before release, so idling out never silently costs a window of edits. *[flush on idle release]*
- A durable write fails transiently mid-session → the session keeps serving, the document stays dirty, the flush retries with backoff, and collaborators are told their edits are not yet durable. *[transient write failure]*
- Durable writes keep failing past the escalation threshold → the generation is invalidated, holders are signalled, and members are disconnected with a reason that specifically means their recent edits could not be saved. The discarded edits are counted and logged with the undurable duration; the teardown is never reported as a clean one. Storage may still be unreachable, so no reload is attempted. *[escalation without a reachable backend]*
- Durable state is found to have diverged from the in-memory document → invalidate and reload immediately, without waiting for the escalation threshold. *[poison-and-reload]*
- A session ignores the invalidation signal and keeps using a document it already holds → it is a contract violation; the service must not depend on cooperative holders for correctness. *[uncooperative holder]*
- A recovery read returns a partial history with no continuation token → a contract violation that must be detected, not silently accepted as a complete document. *[truncated recovery]*
- A document is acquired concurrently by many sessions after a cache miss → acquisition coalesces into one load; a caller abandoning its wait does not cancel the shared initializer for the others. *[coalesced acquire]*
- Two pods flush the same document concurrently → the later whole-document write supersedes the earlier; edits present only in the superseded blob survive solely because a live pod flushes again. Tolerated only because multi-pod is not durability-supported (FR-022a). *[concurrent writers]*
- Two sessions open the same document simultaneously → exactly one materialization occurs; a stored checkpoint is restored once and never applied twice. *[double-restore]*
- Fan-out redelivers an already-applied update → applying it again is a no-op and never corrupts or duplicates content. *[redelivery]*
- Ephemeral awareness is conflated with durable updates → awareness must never enter the durable log. *[kind separation]*
- The pre-1.0 dependency changes shape on upgrade → the service pins a version and upgrades deliberately. *[dependency drift]*

## Requirements *(mandatory)*

### Functional Requirements

**Core**

- **FR-001**: The service MUST use `go-yjs` as its single CRDT core; `y-crdt` MUST be fully removed, with no compatibility shim, adapter, or vendored remnant left behind (§X — no legacy code).
- **FR-002**: The service MUST remain wire-compatible with existing Yjs browser clients — sync handshake, update encoding, and awareness — requiring no client-side change.
- **FR-003**: Both document conventions (memo `Y.XmlFragment`, whiteboard id-keyed `Y.Map` scene) MUST work correctly, including per-property concurrent merge for whiteboards.
- **FR-004**: The server MUST continue to hold plaintext authoritative documents (inherited from `001`, FR-021).
- **FR-004a**: Document restoration MUST happen in the registry's open path: load from the checkpoint store, or initialise an empty document when nothing is stored. A session MUST NOT observe a partially initialised document.
- **FR-004b**: Materialization MUST be exactly-once per document even under concurrent opens, achieved by the registry's coalescing of cache misses rather than by an emptiness check performed after acquisition. A stored checkpoint MUST NOT be applied twice, and an empty document MUST NOT be initialised twice.

**Contract adoption — the anti-duct-taping requirements**

- **FR-005**: Where `go-yjs` defines a backend contract for a concern this service implements, the service MUST adopt that contract as its port, rather than wrapping the library behind a pre-existing bespoke port shape.
- **FR-006**: Where a shipped default satisfies a deployment's needs, it MUST be used as shipped rather than reimplemented (§VIII, §X, §XI). Where it does not — notably multi-pod fan-out and all durable persistence, for which the library ships nothing or ships a single-process-only default — the service MUST provide its own implementation of the defined interface. Implementing a contract is the library's intended extension path and MUST NOT be treated as a deviation; what is forbidden is bypassing the contract, not implementing it.
- **FR-007**: Contract implementations MUST be native — written directly against the underlying infrastructure. The service MUST NOT implement a `go-yjs` contract by delegating to a superseded port (for example a `CheckpointStore` that calls a superseded snapshot blob/pointer port, or a `Hub` that wraps `ClusterBroadcaster`). Ports superseded by an adopted contract MUST be **removed, not wrapped**; no translation shim, compatibility layer, or adapter-over-adapter may survive (§VIII, §X).
- **FR-007a**: Choosing a profile the contract offers (here `CheckpointStore` rather than the log profile) is conformant. **Misreporting a guarantee is not**: a `SaveCheckpoint` returning success before the bytes are durable, a `LoadCheckpoint` returning less than the document's complete state, or a fan-out assuming ordering or single delivery the contract does not promise, are non-conforming regardless of how natively they are written or whether this repo's own tests pass.
- **FR-008**: Every custom port implementation MUST pass the library's corresponding conformance suites in CI.
- **FR-008a**: Every persistence implementation MUST report `Unfenced` and MUST reject a non-zero `Fence` with `ErrUnexpectedFence`, before mutating or erasing anything. A store that silently accepted an epoch it cannot honour would let a caller believe it has stale-owner protection that does not exist. Fencing arrives with the coordinator that needs it; unfenced data reopens as fenced without a history migration, so nothing is lost by not building it now.
- **FR-008b**: Which suites apply MUST be recorded explicitly, so an unrun suite is a stated decision rather than an oversight. For the checkpoint profile adopted here (research.md D1a), the applicable suite is `conformance.CheckpointPersistence`; the log-shaped `conformance.Persistence` and `conformance.PersistenceCompaction` do **not** apply, because the store is not a log and implements no `Compactor`.
- **FR-009**: Durable and ephemeral traffic MUST remain separated: awareness and other ephemeral state MUST never be written to durable storage.
- **FR-009a**: `MetadataStore` MUST be the single canonical name for that concept — port, config identifiers, and package path alike. The `MetadataStore*` config identifiers and the `metastore/` package path MUST be renamed to match. Carrying two vocabularies for one concept is the same defect FR-007 forbids for shims: a permanent translation surface (§VIII).
- **FR-009b**: The WebSocket layer MUST drive the core's message-dispatch handler rather than carrying a parallel sync state machine. Domain checks that must precede application — the byte budget, view-only write rejection, rate limiting — MUST be preserved by registering type overrides, not by reimplementing dispatch (FR-019 continues to apply on every entry point).
- **FR-009c**: A malformed, truncated, or hostile frame MUST fail the offending **connection** only. It MUST NOT tear down the room, disturb other members, or crash the process. Relying on a run-loop panic recover for this is insufficient, because that remedy destroys the room to survive one bad frame.

**Durability**

- **FR-010**: Crash loss MUST be bounded by one flush window. *(Terminology: the **flush interval** is the configured period; a **flush window** is one such period's accumulated changes — the unit of loss. They are related but not interchangeable, and this spec uses them in exactly these senses.)* The flush interval MUST be operator-configurable across at least the ~500ms–~10s range, MUST NOT be hard-coded, and MUST have a documented default; a flush MUST be armed only when the document has changed, and a shutdown flush is unconditional.
- **FR-010a**: Because a flush rewrites the whole document, sustained write volume scales as `document size ÷ flush interval × actively-edited documents`. This relationship MUST be documented wherever the flush interval is configured, and the shipped default MUST be justified against the configured document-size limit. The service MUST NOT ship a default combination whose worst case is knowingly unserviceable.
- **FR-011**: Recovery MUST restore a document to its last completed flush after an abrupt termination, with no manual intervention.
- **FR-011a**: Teardown MUST flush only when the document is believed good. **Flush**: graceful shutdown, and release of an idle document with unsaved changes. **Do NOT flush**: generation invalidation, escalation after repeated write failure, and teardown following a panic on the document's own processing path. A document whose integrity is in doubt MUST NOT be persisted over stored content.
- **FR-011b**: Every teardown path MUST be explicit about which of the two it is; a path that is neither MUST NOT default to flushing. FR-010's "shutdown flush is unconditional" scopes to the graceful path only.
- **FR-012**: **Recovery cost** MUST stay bounded as a document's edit history grows — a long-lived document MUST NOT require replaying unbounded history to open. This design satisfies the requirement structurally — a load reads one whole-document blob, so recovery cost tracks document size, not edit count — rather than by a compaction policy. Should a future implementation introduce compaction, it MUST NOT drop records appended after its basis. Reclaiming superseded storage is the blob backend's concern, not this service's (see Non-Goals).
- **FR-013**: The service MUST distinguish **not-yet-durable** from **diverged**, and MUST NOT treat the first as the second.
  - A **transient durable-write failure** MUST NOT by itself invalidate the document. The session continues serving, the document stays dirty, and the flush is retried with backoff. Collaborators MUST be informed that their edits are not yet durable.
  - After a **bounded, configurable number of consecutive failed flushes**, the service MUST escalate: invalidate the in-memory generation, signal outstanding holders, and disconnect members with a reason, so edits cannot accumulate unbacked indefinitely. The threshold MUST have a documented default.
  - A document **known to have diverged** from durable state MUST be invalidated and reloaded immediately, without waiting for the escalation threshold.
  - Escalation MUST NOT depend on the backend being reachable: if invalidation cannot reload from storage because the backend is still down, the session MUST still be torn down rather than left serving unbacked state.
- **FR-014**: A recovery read that violates its contract (notably a truncated history presented as complete) MUST be detected and surfaced as an error, never accepted as a valid document.

**Preserved guarantees**

- **FR-018**: All `002` lifecycle safety and liveness **properties** MUST continue to hold. Where a property is now delivered by a library contract rather than hand-built code, it MUST still be asserted — a guarantee that moves MUST NOT quietly disappear.
- **FR-018a**: Because the coordination layer is rebuilt (see Clarifications Q2), an invariant test that reaches into a removed structure MAY be restructured to the new internals, but MUST NOT be weakened: the property it asserts, and the failure it would catch, MUST be preserved or strengthened. Every restructured test MUST be re-proven **non-vacuous** — demonstrably failing when its guarantee is removed — and that proof recorded, exactly as `002` required. A test deleted rather than restructured MUST be justified by the property having become unreachable by construction, never by inconvenience.
- **FR-019**: All configured limits (byte budget, update rate, connection cap) MUST remain enforced on every mutation entry point, local and cross-pod alike.
- **FR-020**: Authentication and authorization behaviour MUST be unchanged, including fail-closed evaluation and handshake authentication.
- **FR-021**: The service MUST remain runnable in-process for tests without any external service, using in-process fixtures and the core's shipped single-process defaults. This is a **test capability, not a supported deployment mode** — no durable standalone `CheckpointStore` is required or written.
- **FR-022**: Backend selection MUST remain configuration-driven, with no code change required to switch a backend.
- **FR-022a**: Durable multi-pod operation MUST NOT be represented as supported until single-writer document ownership exists. Cross-pod fan-out is supported; concurrent durable writers are not. This precondition MUST be documented wherever multi-pod configuration is described.
- **FR-022b**: When configured for multi-pod fan-out together with a durable store and no ownership mechanism, the service MUST emit a startup warning naming the unsupported combination, so the precondition is visible at run time and not only in documentation. **Concretely**: logged at WARN or above, during startup and before the service begins serving, naming both the configuration keys involved and the unsupported combination in the message. This is stated precisely so it is verifiable — "prominent" alone is not testable.
- **FR-022c**: Backend-selection configuration keys MUST be named for the contracts they select, not for superseded ports. The key selecting the `persistence.CheckpointStore` backing medium and the key selecting the `hub.Hub` mode MUST both be renamed accordingly; the `MetadataStore` key is unchanged.
- **FR-022f**: The backend selectors `HUB_MODE` and `CHECKPOINT_STORE` MUST be explicitly set; there is no default for either. Startup MUST fail naming the missing key and its supported values.
  - **Why**: where a document's bytes live is not a question the service may answer on the operator's behalf. An unset `CHECKPOINT_STORE` defaulting to `inline` means a deployment that says nothing gets the NON-DURABLE in-process store, serves normally, reports healthy, and loses every document on restart — and nothing in the logs distinguishes "chose inline" from "never said". A typo in a helm value produces exactly the same silence. Durability is not a sensible default to guess at, in either direction: defaulting to `file-service` instead would fail every test and local run at the first save rather than at boot. So the operator states it, and standalone states `inline` out loud.
  - **Cost, stated plainly**: zero-CONFIG standalone is given up. Zero-DEPENDENCY standalone is unaffected — `CHECKPOINT_STORE=inline` with `HUB_MODE=inmemory` still requires nothing running (constitution §III). One explicit line per selector is not worth silent data loss.
- **FR-022e**: The selector names MUST be consistent across every consumer in one change — this repo's config, `.env.example`, and documentation; the k8s manifests on `feat/003-migration`; and `server`'s 006 `quickstart-services.yml`. This is ordinary internal consistency during development, not a migration: nothing has shipped, so there is no deployment to keep working and no compatibility to preserve. FR-022f is what makes a missed consumer fail loudly rather than silently.

**Observability**

- **FR-025**: Every persistence-related signal the service emits today MUST have an equivalent after the rebuild; the change of core MUST NOT reduce what an operator can see.
- **FR-026**: The **not-yet-durable** state MUST be observable in its own right, without waiting for escalation. At minimum the service MUST expose: flush outcome (success/failure), the current consecutive-failure count, escalation events, generation-invalidation events, and how long a document has been accepted-but-not-durable. These MUST be available as metrics — not only log lines — so the condition can be alerted on.
- **FR-027**: Collaborators MUST be informed when their edits are accepted but not yet durable, and when durability is restored, so the degraded state is not silent at the client either.
- **FR-028**: When escalation discards unsaved edits, the loss MUST be explicit: a distinct counter, a log entry carrying the document id and the duration the document had been undurable, and a disconnect reason that specifically means *recent edits could not be saved*. A generic disconnect reason MUST NOT be used, and the loss MUST NOT be reported only as a successful teardown.
- **FR-029**: The service MUST NOT introduce a secondary storage path as a fallback for durable-write failure; escalation accepts the loss rather than writing the document elsewhere.

**Governance**

- **FR-023**: The constitution MUST be amended — at minimum §IV and §II — to reflect the new core and the adopted contracts, and that amendment MUST be adopted before implementation begins. **✅ Satisfied by constitution v2.0.0 (2026-08-18).**
- **FR-024**: The dependency MUST be pinned to a specific version, given its pre-1.0 status (§XIV).

### Key Entities

- **Document** — one collaborative document's authoritative CRDT state, identified by a backend-neutral document identifier.
- **Checkpoint** — the whole encoded document state written by one flush, positioned by an opaque monotonically increasing revision, carrying the equivalent state coverage so a reader need not instantiate a document to understand it. Under the chosen design this is the *only* durable record kept per document.
- **Checkpoint** — what a load returns: the document's complete state in one read, with the codec it was saved under.
- **Registry & handle** — the owner of in-process document identity, coalesced acquisition, eviction and invalidation, and the acquisition lease a session holds including its invalidation signal. Under the chosen design this is the single lifecycle authority for a document.
- **Room (collaboration session)** — the members, presence, limits, authz state, and flush policy around one acquired document. It holds a handle rather than owning the document's identity or teardown ordering.
- **Fan-out message** — a transport-neutral published value, tagged as durable-update or ephemeral-awareness, attributed to a logical source for echo suppression.
- **Ownership lease** *(not implemented)* — a time-bounded, fenced claim by one node over one document. Named here only to define the term.
- **Conformance suite** — the library's adversarial validation of a port implementation against its documented contract.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Existing Yjs browser clients interoperate with zero client-side changes; the JS-interop and e2e suites pass unmodified in single-pod and two-pod modes.
- **SC-002**: Collaborators converge to identical document state within 1s after edits settle (inherited from `001` SC-002), in both single- and multi-pod modes.
- **SC-004**: After an abrupt termination, documents recover to their last completed flush across repeated kill-restart cycles, with zero corruption and zero manual intervention.
- **SC-005**: Every `002` invariant property holds against the rebuilt service, and the suite remains non-vacuous — every test, restructured or not, still fails when its guarantee is removed, with the non-vacuity proof recorded per test.
- **SC-005a**: No `002` invariant is dropped without a written justification that the property became unreachable by construction.
- **SC-006**: Every implemented port passes the library's conformance suites in CI.
- **SC-007**: Documents converge under a fan-out layer that actively duplicates, reorders, and redelivers messages.
- **SC-008**: Zero references to the previous core remain in the codebase, dependency graph, or build.
- **SC-008a**: Zero translation shims remain — no port superseded by an adopted contract survives in any form, and no implementation of a `go-yjs` contract delegates to one. Verifiable by inspection: each adopted contract has exactly one implementation per backend, reaching the infrastructure directly.
- **SC-008b**: Zero occurrences of `MetadataStore` remain; `MetadataStore` is the only spelling in ports, config, and package paths.
- **SC-009**: The test suite runs end to end with no external service, using in-process fixtures and the core's shipped defaults.
- **SC-010**: A full adversarial review of the rebuilt service returns zero findings — the convergence metric inherited from `002` SC-002.
- **SC-011**: Unit test coverage remains at or above 95% (§XII).
- **SC-012**: Cold-load time for a long-lived, heavily-edited document is bounded by document size and does not grow with accumulated edit count.
- **SC-013**: A document that is accepting edits while all flushes fail is distinguishable from a healthy document via metrics alone, before any member is disconnected.
- **SC-014**: No persistence signal available before the rebuild is absent after it.
- **SC-015**: A document opened concurrently by many sessions materializes exactly once — one checkpoint restore, or one empty initialisation — with content identical to a single-session open.
- **SC-016**: Every escalation that discards unsaved edits produces a distinct counter increment, a log entry naming the document and its undurable duration, and a disconnect reason distinguishable from an ordinary disconnect.
- **SC-017**: The set of applicable conformance suites is recorded with a reason for each suite NOT run, and a non-zero `Fence` is rejected with `ErrUnexpectedFence` by every store.
- **SC-018**: No teardown path writes a document whose integrity is in doubt: invalidation, escalation, and post-panic teardown are each proven not to persist, while graceful shutdown and idle release are each proven to persist.
- **SC-019**: A malformed-frame fuzz run affects only the sending connection: zero room teardowns, zero effects on other members, zero process crashes.
- **SC-020**: FR-010a's documentation obligation is discharged: the flush-interval guidance states the write-volume relationship and shows the worst case for a document at the size limit, so an operator can see the cost of a chosen interval before setting it.
- **SC-021**: A process started without an explicit `HUB_MODE` or `CHECKPOINT_STORE` MUST fail startup naming the missing key and its supported values; a process with both set explicitly MUST start. Both are asserted, and both fail if the selectors regain defaults.

## Assumptions

- `go-yjs` is not a third-party dependency in the ordinary sense: it is **this team's own product**, created by rewriting `y-crdt` after that fork proved inadequate for this service's needs. Its origins trace to `skyterra/y-crdt`, so this is a move within one lineage, onto a foundation the team controls.
- The in-process path (`open` auth, `inmemory` metadata, `inline` blob, `inmemory` fan-out) serves **three** distinct roles, all of which must keep working: the automated test suite; the local development loop, including driving real editors; and a documented zero-dependency smoke test that isolates the WebSocket path from authZ (`server`'s quickstart records this override explicitly, pointing at `docs/local-collab-testing.md`). Retaining these adapters is therefore not merely a test convenience.
- **Yjs compatibility is a design guarantee of the library, not a risk this feature carries.** `go-yjs` is 100% compatible with Yjs by design — that is its reason to exist — and it is gated by its own 13-surface bidirectional differential oracle against real Yjs. Encoding, merge semantics, and the V2 codec are therefore **out of scope for verification here**, consistent with `002`'s treatment of the core as externally gated.
- What this spec *does* assert is that **this service's own usage** preserves that compatibility: its transport framing, sync handshake sequencing, and awareness handling must not break what the core guarantees. FR-002 and SC-001 exist to catch service-level mistakes, never to re-test the library.
- The stored blob is a raw Yjs-V2 state snapshot with no envelope or compression, shared with `server` (JS Yjs) and already in use today. `EncodeStateAsUpdateV2` / `ApplyUpdateV2` / `EncodeStateVectorFromUpdateV2` are all available, so a checkpoint-only `Store` can also derive `Checkpoint.StateVector` from the bytes it already holds, without constructing a document.
- **Eviction policy stays this service's own.** `InProcessRegistry` documents that it *starts no goroutines* and that `Evict` never invalidates an outstanding handle, so the registry owns the eviction *mechanism* but has no policy of its own. The idle-release policy that `002` built to satisfy its no-room-leak invariant therefore remains this service's responsibility and must continue to drive `Evict`. This is not a design choice left open — the library forces it.
- The `002` invariant suite is CRDT-core-independent by deliberate design and therefore remains a valid regression net across the core change — this is the primary safety mechanism for the whole feature.
- The library is **pre-1.0 and its shape may change** — but it is this team's own, so shape changes are a coordinated design activity rather than external churn imposed on the service. The service still pins a version and upgrades deliberately, so that a change upstream is an explicit decision here; tension with §XIV (latest dependencies) is resolved in favour of an explicit pin.
- The library declares Go 1.26 and no runtime dependencies; the repo already targets Go 1.26, so no toolchain change is implied and the dependency footprint does not grow.
- Transport stays this service's responsibility — the core deliberately ships none — so the WebSocket adapter is retained. Its **framing and dispatch**, however, are rebuilt on the core's message-dispatch handler (see Clarifications); the adapter keeps the socket, the connection lifecycle, and the domain checks, and stops carrying a parallel sync state machine.
- Existing backend infrastructure (Postgres, RabbitMQ, Redis, file-service, authorization-evaluation-service) remains available; this feature changes which contracts are implemented over them, not which systems are deployed.
- The service implements **exactly one** durable `persistence.CheckpointStore` — over file-service — and one multi-pod `hub.Hub` over Redis. Writing these is the intended use of the library's extension points, and the conformance suites exist precisely to validate them. The core's shipped single-process defaults serve the in-process test path; no durable store is written for it.
- **A flush writes one whole-document update; a load reads one checkpoint.** The implementation is `persistence.CheckpointStore`: one current state per document, replaced on every save. Recovery cost tracks document SIZE, never accumulated edit count.
- Auth, presence, limits, and the lifecycle-event consumer are **out of scope** except where the adopted contracts force a change; they were cleared by prior review.
- The whiteboard/memo document conventions themselves are unchanged; only the core beneath them moves.

## Non-Goals

Explicitly out of scope. Each is listed because it is adjacent enough to be mistaken for scope:

- **Backward compatibility with anything already written by this service.** No stored document, durable format, or configuration from the in-development state must survive. There is no production data, no rollback requirement, and no zero-downtime constraint.
- **Blob retention, expiry, and reclamation of superseded blobs.** This service's contract with the blob backend is exactly **store blob, read blob**. What the backend retains or reclaims is its own contract: not this service's concern, not its configuration surface, and not something it should attempt to manage or model.
- **Any notion of document history, snapshots-over-time, or restore.** This feature builds no interface — internal or external — to enumerate, browse, diff, or restore prior states, and MUST NOT be designed as though one were pending. A load returns *the* current document, not a choice among stored states. Treat any design pressure toward history-awareness as out of scope by default.
- **A supported zero-dependency standalone deployment.** Withdrawn as a product promise (Clarifications, Session 2026-08-18); the in-process path exists for tests only. No durable standalone `Store`, and no work to make the fixture path production-viable.
- **Adaptive or size-aware flush scheduling.** The interval is a flat configured value; varying it by document size is explicitly not built, pending measurement of real document sizes (§XI).
- **Multi-pod ownership and fencing at runtime** — not implemented: the stores run unfenced and reject a fence they cannot honour (FR-008a). Consequently **durable multi-pod operation is out of scope for this feature**: fan-out is delivered and tested, single-writer durability is not, and the combination is documented as unsupported rather than silently shipped (FR-022a/b).
- **Changes to auth, presence, limits, or the lifecycle-event consumer**, except where an adopted contract forces one.
- **Changes to the document conventions** (memo `Y.XmlFragment`, whiteboard id-keyed `Y.Map`).
- **Re-verifying CRDT merge correctness** — the library's differential oracle against real Yjs owns that.

## Dependencies

- `github.com/antst/go-yjs` — pinned pre-1.0 dependency; its contracts and conformance suites are the reference for FR-005 through FR-009.
- The constitutional amendment (FR-023) blocks implementation.
- `002` must be merged (or this branch rebased onto it) so its invariant suite is present as the regression net. *(Satisfied: this branch forks from the commit carrying `002`.)*

### Verified deployment reality (checked 2026-08-18)

Recorded because an earlier reading of a stale Wave-2 code comment suggested otherwise. All three were verified against the 006 worktree, not `develop`:

- **The unified RabbitMQ contract HAS a working consumer.** `server` (branch `feat/006-collab-content-unification`) implements it at `src/services/collaboration-integration/collaboration-integration.controller.ts` — `@MessagePattern(SAVE|FETCH|DELETE|INFO, Transport.RMQ)` against `collaboration-save`/`-fetch`/`-delete`/`-info` — with a `collaboration-metadata` domain type and a migration adding `contentVersion` to memo and whiteboard. The "consumer does not exist yet" note in `metastore/rabbitmq/contract.go` and in `003`'s task file is **stale**; it was true on `develop` only.
- **The service is deployed and runs against real Alkemio.** `server`'s 006 `quickstart-services.yml` runs it as `alkemio_dev_collaboration` (`ghcr.io/alkem-io/collaboration-service:pr-5`) behind Traefik, with identity resolved at the gateway and forwarded as `X-Alkemio-Actor-Id`. Whiteboards in real Alkemio spaces have been opened against it.
- **The real topology is** `HUB_MODE=redis`, `METADATA_STORE=rabbitmq`, `CHECKPOINT_STORE=file-service`, `AUTH_MODE=header` (AuthZ derives to `authzeval`) — the `deploy/k8s/base/configmap.yaml` on `feat/003-migration` still carries the OLD key names and values and MUST be updated before that branch merges. **Those k8s manifests exist on branch `feat/003-migration` and are NOT in this branch's history**; `plan.md` MUST NOT assume they need authoring. It MUST also sequence the configuration-key rename (FR-022c/d/e) across them, since those manifests are on a branch that could otherwise be merged carrying stale keys.

This also corrects the earlier inference that the Postgres metadata adapter was the working local path: it is not. `METADATA_STORE=rabbitmq` is, exactly as `001` intended, so the Postgres adapter's standalone-only classification stands and its removal carries no ordering constraint.
