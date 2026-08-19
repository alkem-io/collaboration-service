# Contract: `memory.Registry` + the collaboration session

**Used from**: the core's shipped `InProcessRegistry`, **as shipped** — no
reimplementation (§X, §XI).
**Conformance gate**: `conformance.Memory`.

This is the seam where the rebuild is riskiest: it is the layer `002` rebuilt to close a
recurring defect class, and full adoption reopens it deliberately. FR-018a governs what
that costs.

## Division of responsibility

| The registry owns | The session (Room) owns |
|---|---|
| document identity | members, presence |
| coalesced acquisition | limits and the byte budget |
| eviction (mechanism) | authz state and re-evaluation |
| invalidation | flush policy and its timer |
| handle lifetime | control messages |
| | shutdown drain ordering (FR-001) |
| | the idle-release **policy** that drives eviction |
| | lifecycle-event consumer handling |

**The registry has no eviction policy of its own** — it starts no goroutines, and
`Evict` never invalidates an outstanding handle. This is forced by the library, not a
design choice: `002`'s idle-release policy remains this service's responsibility and
must continue to drive `Evict`, or the no-room-leak invariant regresses.

## Document restoration (the open function)

Ordered, and inside the coalesced open:

1. restore the stored checkpoint via `persistence.CheckpointStore`;
2. if nothing is stored, initialise an EMPTY document.

**A session MUST NOT observe a partially initialised document** (FR-004a).

**Materialization is exactly-once by construction** (FR-004b): concurrent first-opens
coalesce into a single open, so a checkpoint cannot be applied twice and an empty
document cannot be initialised twice. It MUST NOT be guarded by an emptiness check
performed *after* acquisition — that check races, and two simultaneous first-opens
would both materialize.

## Handle obligations

- A session MUST stop reading or mutating the document and release its handle when the
  invalidation signal fires. The registry has poisoned that instance and will reopen
  from persistence for new acquisitions.
- The signal is **cooperative, not revocation**: a holder that ignores it can keep using
  a document it already obtained, but doing so violates the contract and may silently
  diverge from durable state. **Correctness MUST NOT depend on cooperative holders.**
- Release is idempotent.

## Teardown flush matrix (FR-011a)

| Trigger | Flush |
|---|---|
| graceful shutdown | **yes** |
| idle release with unsaved changes | **yes** |
| invalidation | **no** |
| escalation after repeated write failure | **no** |
| panic on the processing path | **no** |

A teardown path that is neither MUST NOT default to flushing. "Shutdown flush is
unconditional" (FR-010) scopes to the graceful path only.

## Preserved `002` properties

Every property survives; several tests will need restructuring because they reach into
removed structures.

| Property | Now delivered by |
|---|---|
| no edit loss on graceful shutdown | this service (drain ordering) |
| join/purge never hang | registry coalescing + bounded handlers |
| slow client never freezes the room | this service (non-blocking shed) |
| no multi-pod teardown deadlock | decoupled fan-out + handle lifetime |
| no stranded document | `persistence.CheckpointStore` semantics |
| limits on every entry point | this service (single chokepoint) |
| no room or goroutine leak | idle policy driving `Evict` |

**FR-018a**: restructuring a test to new internals is permitted; weakening it is not.
The property it asserts and the failure it would catch MUST be preserved or
strengthened, and every restructured test MUST be **re-proven non-vacuous** — reverted
guarantee, observed failure, proof recorded. A test deleted rather than restructured
MUST be justified by the property having become unreachable by construction, never by
inconvenience (SC-005a).
