# Preserved behaviour — `003-go-yjs-core-port` (T041–T044)

The port replaced the CRDT core, the persistence port and the fan-out port. These
are the properties that had to survive that unchanged, and the evidence that they
did. "Verified by inspection" is recorded as such and never as a test result.

---

## T041 — shutdown drain ordering (FR-001)

**Property.** Dirty documents are persisted before the durable backends close.

`App.Close` drains its closers last-in-first-out: `Manager.Close` first, then
postgres/rabbitmq/redis. Returning early from the Manager drain would let those
backends close out from under a room's in-flight save-on-shutdown persist —
losing precisely the last debounce window of edits the final snapshot exists to
save.

**Expressed over handles now.** A room's document is a registry handle, and the
handle must stay valid for the save-on-release encode. `teardown` therefore
releases the handle LAST, after the flush, and `Manager.closeRegistry` runs after
the drain rather than alongside it.

**Evidence.** `TestManagerCloseWaitsForFinalSnapshotDrain` (behavioural) and
`TestInvShutdownAbortNoGaugeUnderflow` (the abort path). Both pass.

## T042 — the single mutation chokepoint (FR-019)

**Property.** Limits apply on every entry point, local and cross-pod alike.

**Verified by inspection.** `applyUpdate` has exactly two callers — the client
path (`sync.go`) and the peer path (`room.go`) — and the only
`ycrdt.ApplyUpdate*` against the live document is inside `applyUpdate` itself.
The other call sites operate on a `scratch` document (the budget measurement) or
run during materialization before the room serves anyone (`restoreInto`), so
neither is a mutation entry point.

**The two paths differ deliberately, and both are checked.** An over-budget
CLIENT update is rejected and the sender disconnected. An over-budget PEER update
is applied and logged: refusing it would diverge from the pod that already
accepted it, which is a worse outcome than exceeding the cap. What is forbidden
is skipping the check.

**Evidence.** `TestInvBudgetAllPaths` — a surviving `002` invariant, passing
unchanged through the port.

## T043 — auth and authz unchanged (FR-020)

**Property.** Handshake authentication and per-document authorization behave
exactly as before, including fail-closed evaluation.

**Evidence, and it is unusually strong.** `git diff 45d8267^..HEAD` over
`internal/adapter/outbound/auth/` reports **no changes at all** — the port did
not touch a single line of any auth adapter. The only change under
`internal/adapter/inbound/ws/handler.go` is five lines mapping the new
`ErrDocumentPurging` to a policy close status, which adds a refusal reason and
alters none of the existing ones.

The behaviour is additionally pinned by `TestAuthZErrorFailsClosed`,
`TestReadDeniedRefusesJoin`, `TestViewerUpdateNotApplied`,
`TestCollaboratorUpdateApplied`, and the four auth adapter suites. All pass.

## T044 — invariants deleted rather than restructured (SC-005a)

Exactly one: **`TestPurgeDurableSurfacesMetadataLoadError`**.

It induced a metadata-load failure inside `purgeDurable` and asserted the error
surfaced. `purgeDurable` no longer loads the metadata row — the file pointer is
resolved inside the store, behind `persistence.Deleter` — so there is no load
there to fail. The error is **unreachable by construction**, not merely untested,
which is the only ground on which SC-005a permits deletion.

The surrounding property (a failing durable purge surfaces rather than reporting
success) is still covered by `TestPurgeLiveRoomSurfacesBlobError` and
`TestPurgeDurablePropagatesBlobDeleteError`, and the removal is annotated at the
call site naming both.

No other `002` invariant was deleted. The restructured ones and their RED proofs
are in [non-vacuity-ledger.md](./non-vacuity-ledger.md).
