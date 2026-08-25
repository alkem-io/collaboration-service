# #10 Convergence — Handoff (collaboration-service, branch `feat/006-collab-content-unification`)

**Goal:** drive this branch to a CONVERGED state and stop only there.

## THE GATE (hard rule — do not soften)

Converged = **a FULL adversarial code-review of the COMPLETE current HEAD returns ZERO findings of ANY kind** — correctness defects AND observations AND nitpicks. Not a delta review, ever. After **any** code change the content changed → the prior review is VOID → re-run the full review. "0 correctness defects + N observations" is **NOT** converged; N>0 of anything = OPEN. Do not claim done/clean/converged before a genuinely-zero full pass. Every fix becomes a permanent **non-vacuous** test (proven to fail when the fix is reverted).

## Where we are (read first)

This branch carries the **002 room-lifecycle redesign** (specs/002-room-lifecycle-redesign/) — the proven single-writer command-loop core is KEPT; the coordination/lifecycle layer was rebuilt (explicit lifecycle state machine, single `teardown` ordering owner, decoupled `peerUpdates` fan-out, singleflight `acquire`, single `applyUpdate` budget chokepoint, delete-after-commit persistence, bounded enqueue). All uncommitted.

A **full adversarial re-review ran against HEAD `a4171b0`** (the redesign + 5 earlier review fixes + 4 ratchet tests). Result: **0 confirmed correctness defects** (3 candidate findings — budget GC skew, RabbitMQ frame interleaving, no-recover — were independently REFUTED), gates green, the 5 fixes + 4 ratchet tests verified correct & non-vacuous. BUT it surfaced **5 observations (OBS-1..5)** → **NOT zero, NOT converged.** Those 5 are the remaining work.

**In-flight (this session, uncommitted, NOT fully verified):** OBS-1, OBS-2, OBS-4 are **edited** in `internal/domain/service/room.go`; they **compile** (`go build ./...` = 0, `go vet ./internal/domain/service/` = 0) but the new fixes are **not yet tested, not lint-checked, and have no ratchet tests yet.** OBS-3 and OBS-5 are **not started.**

## The 5 observations — status & what each needs

- **OBS-1 — no `recover()` on the run loop.** A panic in any handler killed the `run()` goroutine with the room still registered, `r.done` never closed, `Manager.Close` blocking to its deadline (one panicking handler took down the whole pod's graceful shutdown).
  - ✅ FIX APPLIED: `defer recover()→r.teardown(nil)` at the top of `room.go run()` (no flush — a mid-panic doc must not be persisted over the good snapshot).
  - ⬜ NEEDS: a non-vacuous test — a handler that panics (e.g. a blob/metrics double that panics on a triggered persist) ⇒ the room tears down (`RoomCount→0`, `Manager.Close` returns promptly) instead of hanging. (Revert the `defer` ⇒ the goroutine panic crashes the test process ⇒ non-vacuous.)

- **OBS-2 — unguarded `cmd.done <- res`** (cmdJoin dispatch). The only producer always supplies a buffered `done`, but a nil channel would panic the loop (vs the nil-guarded `cmd.done2`).
  - ✅ FIX APPLIED: `if cmd.done != nil { cmd.done <- res }`.
  - ⬜ NEEDS: a small non-vacuous test (dispatch a cmdJoin with `done == nil` ⇒ no panic).

- **OBS-3 — `Manager.Close` close-signal is unbounded by the shutdown deadline.** `Close` **serially** does `room.enqueue(cmdClose)` per room; `enqueue` blocks up to `enqueueDeadline` (30s) on a full command buffer ⇒ worst case **N×30s** before the drain phase even starts. ALSO a latent bug: the drain uses a one-shot `deadline.C` timer — after it fires once for one room, later iterations have an empty timer channel and can block on `room.done` indefinitely.
  - ⬜ NOT FIXED. PLAN: bound the whole signal+drain by ONE shutdown deadline via a context. Create `shutdownCtx, cancel := context.WithTimeout(context.Background(), budget+shutdownDrainGrace)`; signal each room **concurrently** with `go room.enqueueCtx(shutdownCtx, command{kind: cmdClose})`; drain with `select { case <-room.done: case <-shutdownCtx.Done(): log; return }` (a closed-channel deadline that stays closed for all remaining rooms). See `internal/domain/service/manager.go` Close (~line 371) and `enqueueCtx` (room.go ~298, already ctx-aware).
  - ⬜ NEEDS: a non-vacuous test — a room with a saturated command buffer ⇒ `Close` returns within ~one deadline, not N×30s.

- **OBS-4 — `cmdPeer` dead code.** After the `peerUpdates` decoupling, `cmdPeer` had no producer (the subscribe goroutine writes `r.peerUpdates`, drained directly in `run()`); its enum entry, dispatch branch, and the now-orphaned `command.ephemeral` field were dead.
  - ✅ FIX APPLIED: removed the `cmdPeer` enum entry, the `case cmdPeer` dispatch branch, the `command.ephemeral` field + comment, and updated the `peerUpdates` drain comment. (`peerUpdate.ephemeral` — the live one — is untouched.) Compiles clean.
  - ⬜ NEEDS: nothing beyond the suite passing (no behavior change). Confirm `go test -race ./...` still green.

- **OBS-5 — two ratchet-coverage gaps** (invariants asserted in prose, not gated by a non-vacuous test).
  - (a) The byte-budget `docBytes`-accumulation soundness — the cheap `applyWouldExceedMaxDocBytes` bound (room.go ~149, `docBytes`). Add a test that fails if the accumulation over-counts/under-bounds.
  - (b) The **delete-after-commit ordering** in `persist` (room.go ~1437: `r.deps.Blob.Delete(ctx, oldPointer)` runs AFTER the metadata commit; a Save failure must leave the old blob+pointer intact). Add a test that fails if the delete moves before the commit.
  - ⬜ NOT STARTED. Add both as non-vacuous tests.

## Files

- Edited this round: `internal/domain/service/room.go` (OBS-1/2/4).
- Pending: `internal/domain/service/manager.go` (OBS-3).
- Ratchet tests: add to `internal/domain/service/` (the existing review-residual ratchet is `invariant_review_residual_test.go`; an `invariant_obs_*_test.go` is fine). Each must be revert-proven non-vacuous.

## Gates (run all; all must pass)

```
go build ./...
go vet ./...
go vet -tags integration ./...
golangci-lint run            # must be 0 issues
go test -race ./...
```
Then revert each fix individually → confirm its ratchet test goes RED (non-vacuity ledger).

## Hard rules

- **The gate is the full review of HEAD → zero of everything.** Re-run it after any change. Never a delta. Never claim converged before a clean full pass.
- **No two writers on the same seam.** Only ONE agent edits `room.go`/`manager.go` at a time (the redesign exists because parallel edge-edits were the defect source).
- **Commits MUST be SSH-signed** (verify `%G?` = `G`; never `--no-gpg-sign`; no `Co-Authored-By` trailers).
- Scope is ONLY this repo / #10. #2 (excalidraw-yjs) and #3 are parked.
- Subagents: Opus 4.8, high effort; still verify their output against the real goal.

## First step for the picking-up agent

1. `go build ./...` + `go test -race ./internal/domain/service/` to confirm the inherited OBS-1/2/4 base is green.
2. Implement OBS-3 (manager.go) + OBS-5 (two tests) + the OBS-1/2/3 ratchet tests.
3. Run all gates; build the non-vacuity ledger.
4. Re-run the FULL adversarial review of the complete HEAD. If it returns anything (defect OR observation), fix + ratchet + re-review. Loop until it returns literally zero. Only then commit (SSH-signed) + push.
