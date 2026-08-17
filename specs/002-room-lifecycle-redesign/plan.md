# Implementation Plan: Room Lifecycle & Coordination-Layer Redesign

**Branch**: `feat/002-room-lifecycle-redesign` _(strategy in §Migration)_ | **Date**: 2026-06-25 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/002-room-lifecycle-redesign/spec.md` (clarify-converged 2026-06-25)

## Summary

Rebuild the collaboration room **coordination/lifecycle layer** — keeping the proven single-writer command-loop core — as an **explicit lifecycle state machine** with a **single teardown-ordering owner**, a **decoupled fan-out subscription** (bounded inbound queue, never a run-loop callback), **bounded-block backpressure**, a **single guarded mutation chokepoint**, and a **lock-free-across-I/O Manager** (per-id singleflight). Every safety/liveness FR maps to a mechanism and to a **y-crdt-independent invariant test**; the suite is built FIRST (red against today's code), and the redesign turns it green edge-by-edge. This eliminates the seam/concurrency defect *class* structurally rather than patching instances.

## Technical Context

**Language/Version**: Go 1.26 · **Primary deps**: `coder/websocket`, chi v5, the `skyterra/y-crdt` fork (opaque to this layer), `go-redis` (fan-out), pgx v5 (postgres), `amqp` (rabbitmq). **Storage**: pluggable metastore + blobstore (out of scope except at the lifecycle seam). **Testing**: `go test -race` + the new **y-crdt-independent invariant suite**; ≥95% coverage gate (constitution §XII). **Target**: Linux server, single-pod (default) AND multi-pod redis fan-out (Alkemio prod). **Project type**: hexagonal Go service. **Performance**: the run loop must never block unbounded; shutdown drain ≈ one `BackendTimeout`. **Constraints**: no lock across I/O; illegal lifecycle transitions unrepresentable; limits on every entry point. New timeouts are configurable with defaults: enqueue-deadline ≈ `BackendTimeout`, subscribe-connect ≈ 10s, shutdown-drain ≈ `BackendTimeout + grace`. **Scope**: `internal/domain/service/{room,manager}.go` lifecycle + `internal/adapter/inbound/lifecycle` interaction + the fan-out subscribe seam — NOT the CRDT core, sync framing, auth, or backend adapter internals.

## Constitution Check

*GATE: pass before design; re-check after.*

- **§ Single-writer / no concurrent doc mutation** — PRESERVED and re-asserted as invariant INV-SW (the core is kept; the redesign only changes how work enters/leaves the loop).
- **§XII tests defend real invariants, ≥95%** — the redesign is gated by the invariant suite; every test is non-vacuous (red-on-revert) by mandate.
- **§VII root-cause before fix** — each mechanism cites the finding(s) it structurally eliminates.
- **No-MVP ([[no-mvp-build-the-product]])** — the redesign delivers the WHOLE lifecycle (single + multi-pod, all entry points), not a single-path slice (clarify Q4).
- **y-crdt independence** — the suite asserts SERVER properties (relay fidelity, byte round-trip, lifecycle/concurrency) with the CRDT treated as opaque deterministic bytes; merge correctness is delegated to y-crdt's differential gate.
- No violations → Complexity Tracking empty.

## Design

### 1. Room lifecycle state machine (FR-012)

A single `state` field + ONE central `transition(to)` method that enforces legal edges and runs entry/exit actions in one place. Illegal transitions are a programming error (panic in tests / logged-and-ignored in prod), never silently mis-sequenced.

```
Materializing ──ready──▶ Active ──close|purge|idle-empty──▶ Draining ──drained──▶ Closed
      │                                                                              ▲
      └──────────────────────── materialize-failure ────────────────────────────────┘
```

| State | Accepts | Invariant |
|---|---|---|
| **Materializing** | nothing on the loop yet (snapshot load + subscribe wiring, OFF the registry lock — §5) | not yet in the registry as joinable |
| **Active** | all commands; peer updates via the bounded queue | single writer; all mutations go through the guarded chokepoint (§4) |
| **Draining** | NO new work (`enqueue` refuses; `acquire` won't hand it out); finishes the teardown sequence (§2) | exactly one entry, idempotent |
| **Closed** | nothing; `done` closed; removed from registry | terminal |

The teardown trigger (cmdClose / cmdPurge / idle-empty) transitions Active→Draining; the **transition owns the ordering** (§2), so finish-ordering can't be mis-sequenced per-site (kills the `finish()` deadlock and the idle-leak class).

### 2. Single teardown-ordering owner (FR-013, FR-001, FR-009)

The Active→Draining→Closed transition runs ONE ordered sequence, in one function, replacing the scattered `finish()` / idle / close / purge ordering:

1. **Stop accepting** — set state=Draining (under the room's own guard): `enqueue` now refuses, `acquire` won't share it.
2. **Flush** — `persistNow()` the final snapshot, bounded by `BackendTimeout` (the doc is still intact; backends still live — Manager drains rooms BEFORE closing backends, §6).
3. **Tear down aux goroutines** — cancel `subCtx` → the fan-out subscribe goroutine exits (it never called `enqueue`, §3 — no circular wait); cancel the room ctx.
4. **`close(done)`** — AFTER flush, so the Manager's drain-wait and any `Join`/`Purge` waiter observe completion only once the snapshot is durable.
5. **`onReleased`** — remove from the registry (idempotent via the existing `released` guard).

Ordering invariants this bakes in: *flush before close(done)* (no-loss), *close(done) before/independent-of cancelSub is now safe* (decoupling removed the hazard), *aux-goroutine teardown is cancel-not-callback* (no deadlock).

### 3. Decoupled fan-out subscription (FR-009 — clarify Q2)

The subscribe goroutine **never calls back into the run loop's `enqueue`**. It writes peer updates into a bounded `peerUpdates chan []byte` that the run loop drains alongside `commands`:

```go
// subscribe goroutine (per room, multi-pod only):
for msg := range pubsubCh {
    select {
    case peerUpdates <- msg:        // bounded; backpressure is local & safe
    case <-subCtx.Done():           // teardown cancels → goroutine exits, no callback
        return
    }
}
// run loop:
select {
case cmd := <-commands:      r.dispatch(cmd)
case upd := <-peerUpdates:   r.applyUpdate(upd, peerOrigin)   // SAME chokepoint (§4)
case <-saveTimer.C: ...; case <-idleTimer.C: ...
}
```

Teardown cancels `subCtx` (goroutine unblocks from the queue-write and returns) then `pubsub.Close()` — a cancel, not a wait-on-a-parked-enqueue. **The circular wait is gone by construction** (kills the `finish()` redis deadlock). Bonus: peer updates now flow through the same guarded chokepoint (§4), closing the budget-bypass.

### 4. Single guarded mutation chokepoint (FR-005 — clarify-resolved nuance)

All doc mutations — local sync updates, peer updates, seed — route through ONE `applyUpdate(update, origin)`:

1. **Account** size (`docBytes += len(update)`) for every origin (local + peer) — universal accounting.
2. **Enforce** the byte budget: **local** origin → reject pre-commit (disconnect offender, FR-024). **Peer** origin → cannot reject (rejecting a peer update another pod accepted would diverge) → **accept + log/metric on breach**; correctness relies on **uniform `MaxDocBytes` across pods** (documented operational constraint; config-skew is the only exposure).
3. Apply to the doc, mark dirty, broadcast (local origin only; peer origin never re-broadcast — echo guard).

This makes "limit on every entry point" structural: a guard added at the chokepoint covers every path (kills the budget-one-path class).

### 5. Lock-free-across-I/O Manager (FR-010)

`acquire` never holds `m.mu` across backend I/O. Per-id singleflight:

1. `m.mu.Lock()`: if room exists → return it; else register an in-flight materialization promise for the id; `m.mu.Unlock()`.
2. **Off the lock**: `newRoom(...)` — snapshot load, **bounded** subscribe (`subCtx` with a connect deadline — no unbounded redis Subscribe). Concurrent acquires for the same id await the promise, not the global lock.
3. `m.mu.Lock()`: insert the finished room; resolve the promise; `m.mu.Unlock()`.

A wedged/unresponsive backend now blocks only that id's materialization (bounded by the connect deadline), never every Manager op or shutdown (kills the lock-across-I/O wedge). `Manager.Close` can always take `m.mu` to snapshot the room list.

### 6. Manager shutdown drain (FR-001 — clarify Q1) + no-new-room-during-shutdown

`Manager.Close`: set `m.closed=true` under `m.mu` → **`acquire` refuses new rooms while shutting down** (kills the new-room-during-shutdown loss); snapshot rooms; enqueue cmdClose each; **wait on each `room.done` with a deadline backstop** (`BackendTimeout + grace`), then log + proceed (clarify Q1: bounded drain). LIFO closer order (consumer → manager → backends) keeps backends live during the drain (preserved).

### 7. Bounded backpressure + teardown-safe enqueue (FR-006, FR-007, FR-008 — clarify Q3)

`enqueue`: refuse immediately if state≠Active; else `select { case commands<-cmd: true; case <-done: false; case <-time.After(enqueueDeadline): false }` — bounded-block, never unbounded, never lossy. `Manager.Join`/`Purge` select on `result` vs `room.done` (retry into a fresh room on teardown — preserved from this session's fix, now reinforced by the state check). Lifecycle `ReEvaluate`/access-changed go through the same bounded enqueue with the handler's timeout ctx (kills the timeout-one-path HOL block).

### 8. persist() delete-after-commit (FR-002)

Reorder: upload new blob → **commit metadata** → only then delete the predecessor blob; and `loadSnapshot` tolerates a pointer whose blob is `ErrNotFound` (treat as empty/seed, not fatal). A failed delete leaks a benign orphan (GC-able), never strands a fatal pointer.

### FR / edge-case → mechanism → invariant traceability

| Finding (edge case) | FR | Mechanism | Invariant test |
|---|---|---|---|
| shutdown-loss | FR-001 | §2 flush-before-close, §6 drain | INV-NOLOSS-SHUTDOWN |
| new-room-during-shutdown | FR-001 | §6 `m.closed` guard | INV-NOLOSS-SHUTDOWN-RACE |
| join/purge-hang | FR-006/007 | §7 state-check + select-on-done | INV-JOIN-LIVE, INV-PURGE-LIVE |
| shed-block | FR-008 | async/bounded close (this session's `go CloseNow`) + §7 | INV-LOOP-LIVENESS |
| finish() redis deadlock | FR-009 | §3 decoupled fan-out | INV-TEARDOWN-NODEADLOCK |
| acquire lock-across-I/O | FR-010 | §5 singleflight | INV-MGR-LIVENESS |
| persist delete-before-commit | FR-002 | §8 delete-after-commit + load-tolerance | INV-PERSIST-NOSTRAND |
| budget one-path (handlePeer) | FR-005 | §4 chokepoint | INV-BUDGET-ALLPATHS |
| timeout one-path (access_changed) | FR-014 | §7 bounded enqueue + ctx | INV-LIFECYCLE-BOUNDED |
| idle-leak | FR-011 | §1/§2 state machine (idle→Draining) | INV-NOLEAK |
| eviction recursion | FR-011 | deregister-before-evict (this session) | INV-EVICT-BOUNDED |
| redelivery clobber | FR-003 | COALESCE/insert-if-absent (this session) | INV-LIFECYCLE-IDEMPOTENT |
| single-writer | FR-004 | §4 one chokepoint on the loop | INV-SW (`-race` + assertion) |

### y-crdt-independent invariant suite (the deterministic gate — built FIRST)

Server properties only; the CRDT is opaque deterministic bytes (relay/round-trip), so the suite is unaffected by y-crdt's merge state:
- **INV-RELAY-FIDELITY** (FR-015) — a client's opaque update bytes reach every other member exactly once, no skip/dup/reorder-within-sender.
- **INV-PERSIST-ROUNDTRIP** (FR-016) — `Blob.Put(b)→Get==b`; metastore field round-trip; snapshot reload byte-identical; INV-PERSIST-NOSTRAND.
- **INV-NOLOSS-SHUTDOWN(-RACE)**, **INV-JOIN/PURGE-LIVE**, **INV-LOOP-LIVENESS**, **INV-TEARDOWN-NODEADLOCK** (multi-pod, miniredis), **INV-MGR-LIVENESS**, **INV-BUDGET-ALLPATHS** (entry-point × limit matrix), **INV-LIFECYCLE-BOUNDED/IDEMPOTENT**, **INV-NOLEAK**, **INV-EVICT-BOUNDED**, **INV-SW**.

Each authored to **fail red against today's code** first (proves non-vacuity + baselines the defects), then driven green by the redesign.

### ADR

Add **ADR: "Explicit room lifecycle state machine + decoupled fan-out + single ordering owner"** to the repo's ADR registry — records the model decision, the rejected alternative (keep ad-hoc primitives, patch ordering per-site), and why (the seam-defect class doesn't converge under instance-patching). _To author alongside this plan._

## Project Structure

```text
internal/domain/service/
├── room.go            # lifecycle → state machine + transition owner + applyUpdate chokepoint + decoupled peerUpdates
├── manager.go         # singleflight acquire (no lock across I/O) + m.closed shutdown guard + bounded drain (kept)
├── lifecycle_state.go # NEW: the state enum + central transition() (extracted for clarity + unit test)
└── *_test.go          # the invariant suite (built first, red→green)
internal/adapter/inbound/lifecycle/
└── consumer.go        # bounded ctx threaded into EVERY event type (incl. access_changed)
internal/adapter/outbound/fanout/redis/
└── broadcaster.go     # subscribe goroutine writes to the bounded queue; cancel-not-callback teardown
internal/adapter/outbound/blobstore/fileservice/
└── store.go           # delete-after-commit ordering (caller in room.persist)
specs/002-room-lifecycle-redesign/
├── plan.md (this) · tasks.md (next) · contracts/invariants.md (the suite catalog, optional split)
```

**Structure Decision**: keep the hexagon; the redesign is confined to the domain lifecycle + the two seams (fan-out subscribe, lifecycle consumer). The single-writer core and all adapter internals are untouched.

## Migration approach (informs tasks)

1. **Suite first** — author the full invariant suite; run it red against current code (baseline + non-vacuity proof).
2. **State machine + transition owner** — introduce `lifecycle_state.go`; route the existing teardown paths through it.
3. **Decouple fan-out** — the bounded `peerUpdates` queue + cancel-not-callback teardown.
4. **One chokepoint** — route local + peer updates through `applyUpdate`; budget on all paths.
5. **Singleflight acquire** + `m.closed` guard. **6.** delete-after-commit + load-tolerance. **7.** bounded enqueue + lifecycle ctx-on-every-type.
8. Green the suite edge-by-edge; **single-owner implementation** (one author holds the state machine; NO parallel-agent edge writing — that is the diagnosed defect source). Each step keeps `-race` + the suite green.

**Branch strategy**: the redesign is how PR #10's coordination layer reaches mergeable quality → land it **on the #10 branch** (the current one-path uncommitted fixes are superseded by §1–§8; keep the new tests, replace the ad-hoc fixes). Confirm at tasks time.

## Complexity Tracking

*No constitution violations — table intentionally empty. The state machine reduces complexity (centralizes scattered ad-hoc coordination); it does not add a layer.*
