# Tasks: Room Lifecycle & Coordination-Layer Redesign

**Spec**: [spec.md](./spec.md) · **Plan**: [plan.md](./plan.md) · **Date**: 2026-06-25

**Strategy** (from plan §Migration): **suite-first** (author every invariant red against today's code → drive green), **single-owner** (one author holds the state machine; NO parallel-agent edge-writing — that is the diagnosed defect source, so `[P]` is used ONLY within the test-authoring phase, never across implementation edges), **edge-by-edge** (each mechanism keeps `-race` + the whole suite green before the next). Land on the PR #10 branch, superseding the one-path uncommitted fixes (keep their tests).

## Implementation status (2026-06-25)

**DONE + verified green** (`-race`, `golangci-lint` 0, integration-tag vet clean; ~1095/-154 across 32 files + 16 new): the lifecycle state machine (`lifecycle_state.go`) + single `teardown` owner (T002/T015/T016), singleflight `acquire` + `m.closed` shutdown guard (T019/T020), decoupled fan-out + cancel-before-cancelSub (T017), single `applyUpdate` budget chokepoint (T018), delete-after-commit persistence (T023), bounded `enqueueCtx` + lifecycle-ctx on every event type (T021/T022), idle re-arm on self-disconnect (NOLEAK), the ADR (T024). NON-VACUOUS invariant tests (red→green proven) landed for **MGR-LIVENESS, NODEADLOCK, BUDGET-ALLPATHS, LIFECYCLE-BOUNDED, NOLEAK**, plus PERSIST-ROUNDTRIP + the fileservice delete-after-commit contract; this session's earlier suite already covers NOLOSS-SHUTDOWN, JOIN/PURGE-LIVE, EVICT-BOUNDED, LIFECYCLE-IDEMPOTENT.

**REMAINING**: T026 the fresh full adversarial re-review (terminal-pass gate, SC-002) — running; T025 the merged ≥95% coverage gate (CI, needs backends/e2e); T027 branch reconcile once the gate passes.

---

## Phase 1: Setup

- [ ] T001 Create the y-crdt-independent test harness in `internal/domain/service/lifecycle_harness_test.go`: opaque-byte update generator, gated blob (Put/Delete hold-open), recording `Conn`, a stalled-peer `Conn`, and a miniredis two-pod fan-out fixture. (Reuse `gateBlob`/`wedgeRoom`/`fakeClient` already present.)
- [ ] T002 [P] Add `internal/domain/service/lifecycle_state.go`: the `roomState` enum (`Materializing|Active|Draining|Closed`) + a central `transition(to roomState)` skeleton (legal-edge table, no behavior wired yet) + a focused unit test for the edge table.

## Phase 2: Invariant suite — FOUNDATIONAL, authored RED first (the deterministic gate)

> Author each to FAIL against current `f4bc8bd`-state code, proving non-vacuity + baselining the defect. Some already pass (this session's fixes — shutdown-drain, join/purge, eviction); mark those and KEEP them. These `[P]` are safe (independent test files, test-authoring only).

- [ ] T003 [P] `invariant_noloss_test.go` — INV-NOLOSS-SHUTDOWN (have) + INV-NOLOSS-SHUTDOWN-RACE (new: room materialized after `Close` snapshot must be refused/drained).
- [ ] T004 [P] `invariant_join_purge_test.go` — INV-JOIN-LIVE + INV-PURGE-LIVE (have; extend to the state-machine refuse path).
- [ ] T005 [P] `invariant_loop_liveness_test.go` — INV-LOOP-LIVENESS (stalled peer never freezes the loop; shed bounded).
- [ ] T006 [P] `invariant_teardown_nodeadlock_test.go` — INV-TEARDOWN-NODEADLOCK (multi-pod miniredis; full command channel + subscribe mid-enqueue; teardown completes). **Expected RED.**
- [ ] T007 [P] `invariant_mgr_liveness_test.go` — INV-MGR-LIVENESS (unresponsive backend in `newRoom` must not block other Manager ops / shutdown). **Expected RED.**
- [ ] T008 [P] `invariant_budget_allpaths_test.go` — INV-BUDGET-ALLPATHS (entry-point × limit matrix: local AND peer update paths accounted; local rejected). **Expected RED on peer path.**
- [ ] T009 [P] `invariant_lifecycle_test.go` — INV-LIFECYCLE-BOUNDED (every event type incl. access_changed bounded) **Expected RED on access_changed** + INV-LIFECYCLE-IDEMPOTENT (redelivery no clobber; have).
- [ ] T010 [P] `invariant_noleak_test.go` — INV-NOLEAK (solo self-disconnect that ignores close → room still released). **Expected RED (idle-leak).**
- [ ] T011 [P] `invariant_persist_test.go` — INV-PERSIST-NOSTRAND (failed metadata commit must not strand the pointer) **Expected RED** + INV-PERSIST-ROUNDTRIP (Put→Get byte-identical; reload byte-identical).
- [ ] T012 [P] `invariant_relay_fidelity_test.go` — INV-RELAY-FIDELITY (opaque update bytes reach every peer exactly once, no skip/dup/reorder-within-sender).
- [ ] T013 [P] `invariant_evict_sw_test.go` — INV-EVICT-BOUNDED (have) + INV-SW (single-writer assertion + `-race`).
- [ ] T014 Run the full suite; record the RED baseline (T006/T007/T008/T009-bounded/T010/T011-nostrand expected red) and confirm every test fails when its guarantee is removed (non-vacuity ledger in `contracts/invariants.md`).

## Phase 3: State machine + teardown-ordering owner (US1, US7) — FOUNDATIONAL

- [ ] T015 [US7] Wire `transition()` into `room.go`: add the `state` field; gate `enqueue`/`acquire`-sharing on `state==Active`; idempotent Active→Draining→Closed.
- [ ] T016 [US1] Move ALL teardown (cmdClose/cmdPurge/idle-empty/finish) into the single Draining sequence: flush → cancel `subCtx` + room ctx → `close(done)` → `onReleased`. Green **INV-NOLEAK**; keep INV-NOLOSS-SHUTDOWN green.

## Phase 4: Decoupled fan-out (US4)

- [ ] T017 [US4] Add bounded `peerUpdates chan []byte`; run-loop `select` drains it; `redis/broadcaster.go` subscribe goroutine writes via `select{ peerUpdates<- ; <-subCtx.Done() }` (cancel-not-callback). Green **INV-TEARDOWN-NODEADLOCK**.

## Phase 5: Single guarded mutation chokepoint (US6)

- [ ] T018 [US6] Route local + peer + seed updates through one `applyUpdate(update, origin)` in `room.go`: account `docBytes` all origins; reject local on breach, accept+log peer (uniform-`MaxDocBytes` constraint documented). Green **INV-BUDGET-ALLPATHS**.

## Phase 6: Lock-free-across-I/O Manager (US1, US2)

- [ ] T019 [US2] `manager.go` `acquire`: per-id singleflight; release `m.mu` before `newRoom` I/O; bounded subscribe connect deadline (`subCtx`). Green **INV-MGR-LIVENESS**.
- [ ] T020 [US1] `m.closed` set under `m.mu` in `Close`; `acquire` refuses new rooms when closed; keep the bounded drain + deadline. Green **INV-NOLOSS-SHUTDOWN-RACE**.

## Phase 7: Bounded enqueue + lifecycle-ctx on every type (US2, US6)

- [ ] T021 [US2] `enqueue`: refuse if `state≠Active`, else `select{ commands<- ; <-done ; <-deadline }` (bounded-block). Keep Join/Purge select-on-done. Green **INV-JOIN-LIVE/INV-PURGE-LIVE**.
- [ ] T022 [US6] `lifecycle/consumer.go`: thread the per-delivery timeout ctx into EVERY handler incl. `handleAccessChanged`; bounded enqueue for ReEvaluate. Green **INV-LIFECYCLE-BOUNDED**.

## Phase 8: persist delete-after-commit (US5)

- [ ] T023 [US5] `room.persist`: upload → commit metadata → THEN delete predecessor blob; `loadMetadata` tolerates a pointer whose blob is `ErrNotFound`. Green **INV-PERSIST-NOSTRAND**.

## Phase 9: Polish & Validate

- [ ] T024 Author the ADR ("Explicit room lifecycle state machine + decoupled fan-out + single ordering owner") in the repo ADR registry.
- [ ] T025 Whole-suite green + the non-vacuity ledger re-verified (revert each mechanism → its invariant goes red); `go test -race ./...`, `golangci-lint run` 0, coverage ≥95% (constitution §XII).
- [ ] T026 SC-002 terminal pass: a fresh full adversarial review of the redesigned coordination layer returns ZERO new seam/concurrency/lifecycle defects. Ratchet any finding → a new invariant test + back to the relevant phase.
- [ ] T027 Branch reconciliation: supersede the one-path uncommitted #10 fixes with the redesign on the #10 branch; keep all new tests; commit signed.

---

## Dependencies & order

- Phase 1 → Phase 2 (harness before tests) → Phase 3 (state machine is the spine everything hangs on) → Phases 4–8 (mechanisms; each green-keeps the suite). Phase 9 last.
- **Within a phase**, implementation tasks are **sequential single-owner** (no `[P]`). `[P]` appears only in Phase 2 (independent test files).
- Phases 4–8 each depend on Phase 3 (the state machine) but are otherwise independent in code surface — still done sequentially by one owner to preserve seam coherence (the writing-process fix).

## Independent-test mapping (every US is gated)

US1→T016,T020 (INV-NOLOSS*) · US2→T019,T021 (INV-JOIN/PURGE/MGR-LIVE) · US3→T005 (INV-LOOP-LIVENESS) · US4→T017 (INV-TEARDOWN-NODEADLOCK) · US5→T023 (INV-PERSIST-NOSTRAND) · US6→T018,T022 (INV-BUDGET-ALLPATHS, INV-LIFECYCLE-BOUNDED) · US7→T015,T016 (INV-NOLEAK).

## Suggested MVP / increment boundary

Phase 1–3 + the RED suite is the foundational increment (the state machine + the gate). Phases 4–8 are each an independently-shippable green step. T026 (the fresh full review) is the convergence gate — the redesign is "done" only when it returns zero new seam defects.
