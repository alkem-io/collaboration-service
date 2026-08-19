# Conformance coverage (T048, FR-008b)

Which of the core's conformance suites this service runs, and **why each
non-applicable suite is not run** — the second half matters more. A suite that is
simply never mentioned is indistinguishable from one that was quietly skipped
because it failed.

Core version: `github.com/antst/go-yjs v0.0.3`.

## Run

| Suite | Against | Where |
|---|---|---|
| `conformance.CheckpointPersistence` | in-process store | `internal/adapter/outbound/persistence/inprocess/store_conformance_test.go` |
| `conformance.CheckpointPersistenceFencing` | in-process store, `NewFenced()` | same |
| `conformance.CheckpointPersistenceDeletion` | in-process store | same |
| `conformance.CheckpointPersistence` | file-service store | `internal/adapter/outbound/persistence/fileservice/store_conformance_test.go` |
| `conformance.Memory` | `memory.NewRegistry()` | `internal/domain/service/registry_conformance_test.go` |
| `conformance.Hub` | `hub.NewInProcess()` | same |

The registry and hub suites validate a **dependency**, not our code — we use both
shipped implementations unmodified (§X/§XI). They are run anyway because two
requirements rest on their exact behavior: FR-004b leans on `Acquire`'s coalescing
for exactly-once first-open restore, and FR-011a on `Invalidate`'s
poison-and-signal. A future core bump that changed either would otherwise break
those requirements silently.

## Not run, and why

**`conformance.Persistence`, `PersistenceFencing`, `PersistenceDeletion`,
`PersistenceDeletionFencing`** — the LOG profile (`persistence.Store`). We
implement the CHECKPOINT profile. The log profile appends opaque records and
demands them back verbatim, in order, paginated; a store that keeps one current
state per document cannot satisfy it and should not pretend to. This is not a
gap: the checkpoint profile exists *because* of this port (antst/go-yjs PR #8),
after the log profile turned out to describe a persistence model we do not use
and cannot adopt — `server` writes a bare Yjs-V2 snapshot on create and in the
T009 migration, so a record envelope was never available.

**`Compactor`** — no compaction suite is run because neither store implements
`Compactor`. Compaction is a log-profile concern: it folds accumulated records
into a covering state. A checkpoint store has nothing to compact — every save
already writes the document's complete state, so the compacted form is the only
form it has ever held.

**Fenced CHECKPOINT deletion** — the core ships `PersistenceDeletionFencing` for
the log profile only; there is no checkpoint equivalent. The property is covered
locally instead by `TestFencedDeleteRejectsAStaleOwnerAndLeavesStateIntact`,
which asserts a stale owner's delete is refused, fence-zero on a fenced store is
refused, and — the part worth pinning — a **rejected delete leaves the state
intact**. An error return that had already removed the blob would be the same data
loss with a better error message.

## Deletion is optional, so it is asserted at startup

`persistence.Deleter` is deliberately optional: some media are forbidden to delete
(WORM storage, object locks, regulated archival tiers), and a mandatory `Delete`
cannot express that. `buildCheckpoint` therefore returns
`persistence.DeletingCheckpointStore`, so a store that cannot delete fails
**startup** rather than surfacing the first time an owner deletes a document and
the cascade cannot complete.

## Fencing in production

Only the in-process store can be fenced. The file-service store reports
`Unfenced` by design: a file row has nowhere durable to hold the epoch, and
keeping it in a separate service is not a substitute for a persistence-level
backstop (research.md D6a). Exercising the fenced path against the in-process
store keeps the capability honest without pretending production has it.
