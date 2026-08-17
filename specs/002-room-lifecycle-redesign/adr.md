# ADR-002: Explicit room lifecycle state machine + decoupled fan-out + single ordering owner

**Status**: Accepted (2026-06-25) · **Spec**: [spec.md](./spec.md) · **Plan**: [plan.md](./plan.md)

## Context

Two independent full adversarial reviews found **every** HIGH-severity defect in the collaboration server clustered at the *edges* of the room's single-writer run loop — teardown ordering, backpressure/enqueue races, lock-held-across-I/O, auxiliary-goroutine coordination, missed state transitions — never in the doc-serialization core itself (which is `-race`-clean). The defects are a **class**: a guard added to one path (the byte budget on local writes; the handler timeout on delete/create events) repeatedly left the sibling path (cross-pod updates; access-changed events) exposed. Review-and-patch did **not** converge — each patch closed an *instance* while the class regenerated (a full pass found 12 defects *after* an integration review called the diff "clean"). The coordination layer had been **grown** feature-by-feature (and agent-by-agent) without an explicit lifecycle design.

## Decision

Rebuild the **coordination/lifecycle layer** correct-by-construction, **keeping the proven single-writer command-loop core**:

1. **Explicit lifecycle state machine** (`Materializing → Active → Draining → Closed`) with a centrally-enforced transition table (`lifecycle_state.go`); illegal transitions are unrepresentable (`Active→Closed` is illegal so the final-snapshot flush always runs). `beginTeardown` replaces the scattered `released` bool as the idempotent teardown guard.
2. **Single teardown-ordering owner** (`Room.teardown`) — stop-accepting → flush → cancel ctx → tear down the subscription → close(done) → release — written once, not re-derived per call site.
3. **Decoupled fan-out** — the subscribe goroutine writes to a bounded `peerUpdates` queue (cancellable on `roomCtx`), never `enqueue`; teardown cancels the ctx *before* `cancelSub`. The run-loop↔subscribe↔finish circular wait is impossible by construction.
4. **Lock-free-across-I/O Manager** — per-id `singleflight` so `newRoom`'s backend I/O runs off `m.mu`; an unresponsive backend can no longer wedge every Manager op or shutdown. A `closed` flag refuses new rooms during the shutdown drain.
5. **Single guarded mutation chokepoint** (`Room.applyUpdate`) — local + peer updates route through one budget gate (reject local, log+apply peer); the limit covers every entry point, not one path.
6. **Bounded enqueue + lifecycle-ctx on every event type** — a producer never blocks unbounded; the lifecycle consumer cannot be head-of-line-blocked.
7. **delete-after-commit** persistence — the predecessor blob is dropped only after the new pointer commits.

Every FR + every finding maps to a mechanism and to a **y-crdt-independent invariant test**; the suite is the deterministic gate (built red against the old code, driven green).

## Consequences

- The seam/concurrency defect **class** is eliminated structurally rather than by instance-patching; future regressions fail an invariant test (MGR-LIVENESS, NODEADLOCK, BUDGET-ALLPATHS, LIFECYCLE-BOUNDED, NOLEAK, NOLOSS-SHUTDOWN, JOIN/PURGE-LIVE, …), several proven non-vacuous (red→green).
- The proven single-writer core is unchanged; this is a bounded redesign of the edges, not a from-scratch rewrite.
- Merge/convergence correctness remains `y-crdt`'s responsibility (its own differential gate); this layer's suite is deliberately y-crdt-independent (transport relay fidelity + persistence byte round-trip + lifecycle/concurrency properties).

## Alternatives rejected

- **Keep the ad-hoc primitives and patch per-site.** Demonstrably non-converging (the 12-after-"clean" pass); each fix is one-path while the class regenerates.
- **Full from-scratch rewrite of the subsystem.** Discards the proven single-writer core and the accumulated edge-case knowledge (the bugs *are* that knowledge — fed in here as requirements). Higher risk, no benefit.
