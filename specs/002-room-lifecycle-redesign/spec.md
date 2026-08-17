# Feature Specification: Room Lifecycle & Coordination-Layer Redesign

**Feature Branch**: `feat/002-room-lifecycle-redesign` _(planned; branch-vs-PR#10 strategy is open — see Assumptions)_
**Created**: 2026-06-25
**Status**: Draft (specify) — clarify pending
**Workspace epic**: `../agents-hq/specs/003-unify-collab-yjs/` (WS-C); follows `001-collaboration-server`
**Backlog Story**: _[link or create per Principle 11 — search board before creating]_
**Input**: Redesign the collaboration room lifecycle / coordination layer — the room run-loop teardown, the `Manager` registry coordination, and the lifecycle-consumer interaction — so it is correct-by-construction under teardown, backpressure, multi-pod fan-out, and graceful shutdown, eliminating the seam/concurrency defect *class* that repeated full reviews found clustered in `room.go` / `manager.go` / lifecycle and that one-path patching does not converge.

> **Repo-local sub-spec.** Same treatment as `001`. This document owns the *requirements* — the safety + liveness properties the lifecycle MUST satisfy — and the acceptance scenarios (the enumerated failure modes). It does **not** specify the implementation model (the explicit state machine, the single ordering owner); that is `plan.md`. It does **not** re-specify the CRDT core (`y-crdt`'s own gate) or the backend adapters / sync framing / auth (reviewed sound — out of scope). The proven single-writer command-loop **core is KEPT**; only the coordination/lifecycle layer around it is redesigned.

## Context — why this spec exists

The coordination layer was grown feature-by-feature (and agent-by-agent) without an explicit lifecycle design. Two independent full adversarial reviews found **every** HIGH-severity defect clustered at the *edges* of the single-writer run loop — teardown ordering, backpressure/enqueue races, lock-held-across-I/O, auxiliary-goroutine coordination, missed state transitions — never in the doc-serialization core itself (which is `-race`-clean). The defects are a **class**: a guard added to one path (the byte budget on local writes; the handler timeout on delete/create events) repeatedly left the sibling path (cross-pod updates; access-changed events) exposed. Review-and-patch does not converge because each patch closes an *instance* while the class regenerates. This spec re-states the lifecycle as explicit, testable safety + liveness requirements so the layer can be rebuilt correct-by-construction and gated by a deterministic, **y-crdt-independent** invariant suite.

## Clarifications

### Session 2026-06-25

- **Q**: No-edit-loss-on-shutdown (FR-001) — guarantee strength? → **A**: Bounded drain + deadline backstop — block shutdown until every dirty room persists or a configured deadline elapses, then log + proceed (guaranteed termination beats hanging into a SIGKILL that loses more).
- **Q**: Multi-pod fan-out subscription vs the run loop (the finish()/enqueue deadlock root)? → **A**: Decouple — the subscribe goroutine writes peer updates into a bounded queue the run loop drains; teardown closes the queue and never calls back into `enqueue` (removes the circular wait by construction).
- **Q**: Command-channel backpressure on full? → **A**: Bounded-block — block until space OR `done` OR a deadline, never unbounded and never lossily shed a sync update; the loop is kept drained by bounding every handler (FR-008), so the block stays short and ordering is preserved.
- **Q**: Multi-pod scope this iteration? → **A**: Solve single + multi-pod together — one coherent state-machine + decoupled-fan-out design covering both modes; the multi-pod HIGHs (teardown deadlock, lock-across-I/O) are prod-path load-bearing.
- **Resolved (no question needed)**: `persist()` ordering → delete the predecessor blob AFTER the metadata commit (delete-after-commit), and `loadSnapshot` tolerates a pointer whose blob is missing (defense-in-depth, FR-002); a failed delete then leaks a benign orphan rather than stranding a fatal pointer.

## User Scenarios & Testing *(mandatory)*

> "Users" here are the **operator** running the service and the **collaborators** whose edits must not be lost. Each story is a safety/liveness *outcome*, independently testable via the invariant suite — and y-crdt-independent (no dependency on CRDT merge correctness).

### User Story 1 — No edit loss on graceful shutdown (Priority: P1)
A collaborator's recent edits survive a normal shutdown (deploy/rollout): when a pod stops, every room with unsaved edits persists its final snapshot **before** the durable backends it writes to are torn down.
**Why P1**: silent data loss is the highest-impact failure — invisible until a user reports lost work.
**Independent Test**: drive a dirty room, trigger graceful shutdown with a gated backend, assert the final snapshot commits before any backend closes and that shutdown blocks until it does.
**Acceptance Scenarios**:
1. **Given** a room with unsaved edits, **When** the service shuts down gracefully, **Then** the room's final snapshot is durably persisted before the metadata/blob backends close.
2. **Given** a room materialized in the instant after shutdown begins, **When** the service is draining, **Then** that room is refused or also drained — never silently dropped with unsaved edits.

### User Story 2 — A join (and purge) never hangs (Priority: P1)
A client connecting to a document always gets a session or a clean error within bounded time, even when the target room is concurrently tearing down; likewise an owner-delete.
**Why P1**: a hung join leaks the connection goroutine + socket and degrades the pod over time — a compounding liveness failure.
**Independent Test**: race a join (and a purge) against teardown; assert it returns within bounded time, never blocks.
**Acceptance Scenarios**:
1. **Given** a room tearing down, **When** a client joins concurrently, **Then** the join retries into a fresh room and succeeds, or returns a clean error — never blocks indefinitely.
2. **Given** a room tearing down, **When** an owner-delete arrives, **Then** the purge completes (idempotently) within bounded time.

### User Story 3 — A slow/stalled client never freezes the room (Priority: P1)
One unresponsive collaborator cannot stall the room's single run loop or affect other members.
**Why P1**: a single slow client freezing the loop is a cheap DoS against everyone else in the room.
**Independent Test**: a stalled-peer connection; assert the run loop keeps serving other members within bounded time (shed/close does not block the loop).
**Acceptance Scenarios**:
1. **Given** a member whose socket is full/stalled, **When** the room sheds it, **Then** the run loop is not blocked and other members continue uninterrupted.

### User Story 4 — Multi-pod teardown never deadlocks (Priority: P1)
In fan-out (multi-pod) deployments a room tears down cleanly without deadlocking against its cross-pod subscription.
**Why P1**: a teardown deadlock leaks the room + goroutines, loses the final snapshot, and stalls shutdown to its backstop — in the production path.
**Independent Test**: a room with an active fan-out subscription under backpressure; assert teardown completes with no circular wait between run loop, subscribe goroutine, and finish.
**Acceptance Scenarios**:
1. **Given** a multi-pod room whose command channel is full and whose subscribe goroutine is mid-enqueue, **When** the room is closed, **Then** teardown completes without deadlock and the final snapshot persists.

### User Story 5 — Persistence never strands a document (Priority: P1)
A snapshot save that partially fails never leaves a document referencing a deleted blob.
**Why P1**: a stranded pointer makes the document permanently unopenable on cold load — unrecoverable loss.
**Independent Test**: fail the metadata commit after the blob write; assert the durable row never references a non-existent blob.
**Acceptance Scenarios**:
1. **Given** a snapshot upload that succeeds but whose metadata commit fails, **When** the document is later cold-loaded, **Then** it still opens (no stranded pointer).

### User Story 6 — Limits & lifecycle events enforced on every path (Priority: P2)
Configured limits (byte budget, rate, connection cap) and lifecycle handling (timeout, idempotency) apply to **every** entry point — local writes, cross-pod updates, and lifecycle events alike — not just the path that happened to be wired.
**Why P2**: a guard on one path and not its sibling is a silent hole (the class-vs-instance defect) — high impact, narrower trigger than P1.
**Independent Test**: an entry-point × limit matrix; assert each limit holds on every entry point.
**Acceptance Scenarios**:
1. **Given** the byte budget, **When** a mutation arrives via the cross-pod path, **Then** it is enforced exactly as on the local path.
2. **Given** the handler timeout, **When** an access-changed event wedges a room, **Then** the consumer is not head-of-line-blocked beyond the timeout.

### User Story 7 — No room or goroutine leak (Priority: P2)
An empty room is always released; no run-loop goroutine or Y.Doc is pinned by a non-cooperative client or a missed state transition.
**Why P2**: a slow resource leak / DoS that accumulates over time.
**Independent Test**: trip a self-disconnect (rate/size) on a solo client that ignores the close; assert the room is eventually released (RoomCount→0).
**Acceptance Scenarios**:
1. **Given** a solo member self-disconnected by a limit, **When** it ignores the close-control and holds the socket, **Then** the empty room is still released.

### Edge Cases — the enumerated failure modes (the bugs ARE the spec)

The redesign MUST structurally prevent each mode found by review (each maps to an FR + a non-vacuous invariant test):

- Shutdown returns before a dirty room's final snapshot completes → backends close mid-save → lost edits. *[shutdown-loss]*
- `enqueue` wins the buffered-send race into a room whose run loop then exits → `Join`/`Purge` block forever. *[join/purge-hang]*
- The slow-consumer shed performs a synchronous blocking close on the run loop → seconds-long room-wide stall per shed. *[shed-block]*
- `finish()` tears down the fan-out subscription before closing `done` while the subscribe goroutine is parked in `enqueue` on a full channel → circular-wait deadlock. *[finish-order deadlock, multi-pod]*
- `acquire()` holds the global registry mutex across blocking backend I/O (incl. an unbounded fan-out subscribe) → one unresponsive backend wedges every Manager op, incl. shutdown. *[lock-across-I/O]*
- `persist()` deletes the predecessor blob before the metadata commit → a failed commit strands the pointer → unopenable document. *[delete-before-commit]*
- The byte budget is enforced on the local write path but not the cross-pod update path. *[budget one-path]*
- The lifecycle handler timeout covers delete/create but not access-changed→re-evaluate → consumer HOL-block. *[timeout one-path]*
- A self-disconnect on the message path never re-arms the idle timer → empty-room / goroutine leak. *[idle-leak]*
- A cascading disconnect recurses through the send/evict/drop path. *[eviction recursion]*
- A redelivered or blank lifecycle event clobbers live metadata (content-type / pointer / owner / bucket). *[redelivery clobber]*
- _(plus any further seam/lifecycle finding from the in-progress and future full reviews — the edge-case list is append-only as the ratchet runs.)_

## Requirements *(mandatory)*

### Functional Requirements — safety + liveness the lifecycle MUST satisfy

**Safety**
- **FR-001**: The service MUST persist a dirty room's final snapshot before any durable backend it writes to is closed (no edit loss on graceful shutdown). Shutdown bounds the drain: it blocks until every dirty room has persisted OR a configured shutdown-drain deadline elapses, then logs and proceeds (guaranteed termination over an unbounded hang).
- **FR-002**: The service MUST NOT leave a persisted document referencing a non-existent blob; a partially-failed save MUST remain recoverable (the document stays openable). Achieved by delete-after-commit (the predecessor blob is deleted only after the new pointer commits) plus `loadSnapshot` tolerating a missing blob behind a pointer — a failed delete leaks a benign orphan, never a fatal stranded pointer.
- **FR-003**: A redelivered or blank lifecycle event MUST NOT clobber populated live metadata (idempotent lifecycle).
- **FR-004**: The authoritative Y.Doc MUST have a single writer; no path may mutate it concurrently with the run loop (preserved from `001`, re-asserted as an invariant).
- **FR-005**: Every configured limit (byte budget, update rate, connection cap) MUST be enforced on EVERY mutation entry point (local AND cross-pod) — the class, not one path.

**Liveness**
- **FR-006**: A join MUST return a session or a clean error within bounded time, even racing room teardown (never block indefinitely).
- **FR-007**: An owner-delete (purge) MUST complete within bounded time, even racing room teardown.
- **FR-008**: No operation on the room's single run loop may block unbounded — every backend call and every close/shed MUST be time-bounded or asynchronous, so a slow client/backend cannot freeze the loop or other members. Producers MUST bounded-block on a full command channel (until space OR teardown OR a deadline) — never unbounded, and never lossily shed a sync update.
- **FR-009**: A room MUST tear down without deadlock in ALL deployment modes, including multi-pod. The fan-out subscription MUST be decoupled from the run loop via a bounded queue the loop drains (the subscribe goroutine never calls back into the loop's `enqueue`), so the run loop, the subscription, and finish cannot form a circular wait by construction.
- **FR-010**: Manager-level operations (join, purge, re-evaluate, shutdown) MUST NOT be blockable by a single unresponsive backend; no lock may be held across blocking I/O.
- **FR-011**: An empty room MUST always be released; no missed state transition may pin a run-loop goroutine or Y.Doc.

**Structure** *(stated as requirements; the model is `plan`)*
- **FR-012**: The room lifecycle MUST be an explicit state machine with centrally-enforced transitions; illegal transitions MUST be unrepresentable, not merely avoided by convention.
- **FR-013**: Teardown ordering (stop-accepting → drain → flush snapshot → tear down auxiliary goroutines → release) MUST be owned in a single place, not re-derived per call site.
- **FR-014**: Lifecycle-event processing MUST be bounded per delivery on EVERY event type, so one wedged room cannot head-of-line-block the consumer.

**Foundational correctness** *(the transport/persistence properties the y-crdt-independent suite asserts)*
- **FR-015**: The service MUST deliver every accepted update to every other member of the room exactly once — no dropped, duplicated, or reordered-within-a-sender update (fan-out relay fidelity) — in both single- and multi-pod modes.
- **FR-016**: A persisted snapshot MUST round-trip byte-identically — the bytes stored are the bytes returned, and a reloaded document re-encodes to what was stored (persistence fidelity), independent of CRDT merge semantics.

### Key Entities
- **Room** — owns one document's authoritative Y.Doc + member set; has an explicit lifecycle state (Materializing → Active → Draining → Closed). The single writer.
- **Manager** — room registry + lifecycle owner; lazily materializes, shares, releases rooms; coordinates the shutdown drain. MUST NOT hold a lock across I/O.
- **Run loop** — the single goroutine serializing commands against a Room's Y.Doc (the proven core, KEPT).
- **Lifecycle consumer** — translates bus events (created/deleted/access-changed) into bounded, idempotent Manager operations.
- **Auxiliary goroutines** — the fan-out subscription (multi-pod); their teardown MUST be coordinated with the run loop without circular wait.
- **Invariant suite** — the y-crdt-independent deterministic gate encoding FR-001…FR-014 as non-vacuous tests.

## Success Criteria *(mandatory)*

### Measurable Outcomes
- **SC-001**: The y-crdt-independent invariant suite (encoding every FR + every enumerated edge case) is green AND non-vacuous — each test demonstrably fails if its guarantee is removed.
- **SC-002**: A fresh full adversarial review of the redesigned coordination layer returns ZERO new seam/concurrency/lifecycle defects — a clean terminal pass (the convergence metric).
- **SC-003**: Zero edits lost across N graceful-shutdown cycles under load.
- **SC-004**: Zero hung joins/purges and zero room/goroutine leaks across a teardown-race stress run.
- **SC-005**: Multi-pod teardown completes with zero deadlocks across a fan-out-backpressure stress run.
- **SC-006**: Every limit holds on every entry point (no one-path gap), verified by an entry-point × limit matrix.

## Assumptions
- The proven single-writer command-loop **core is correct and KEPT**; only the coordination/lifecycle layer is redesigned (this is a bounded redesign, not a from-scratch rewrite).
- The backend adapters (metastore RPC, blobstore, auth, fan-out transport) and the y-protocols sync framing are sound (cleared by review) and out of scope EXCEPT where the lifecycle interacts with them (ordering, bounding, idempotency).
- CRDT merge/convergence correctness is `y-crdt`'s responsibility, validated by its own differential-vs-real-Yjs gate; this spec's invariant suite is deliberately **y-crdt-INDEPENDENT** (transport relay fidelity + persistence byte round-trip + lifecycle/concurrency properties).
- **Branch strategy is open**: the redesign may land on the PR #10 branch as the way its coordination layer reaches mergeable quality, or on its own branch off main — decided in `plan` (it interacts with #10's fate). The current uncommitted #10 fixes are a partial, one-path mitigation the redesign supersedes.
- Both standalone (single-pod / in-memory / inline / open) and Alkemio (multi-pod redis fan-out + postgres/rabbitmq + file-service + authzeval) deployments are in scope; the multi-pod path carries the deadlock and lock-across-I/O risks.
