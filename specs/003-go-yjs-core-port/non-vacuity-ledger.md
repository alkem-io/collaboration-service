# Non-vacuity ledger — `003-go-yjs-core-port`

**FR-018a / SC-005.** A test that passes for a reason its author has not verified
is worse than no test, because it is trusted. Every guarantee restructured or
added during this port was proved by the same procedure:

1. revert the guarantee in isolation (one edit, in the production code),
2. run the test and confirm it goes **RED**,
3. restore,
4. re-run and confirm **GREEN**,
5. record the revert and the observed failure below.

Where a probe did **not** produce RED, that is recorded too — those are the
entries that changed a test.

---

## Restructured `002` invariants

The `002` suite passed unchanged across the core swap except where a test reached
into a structure the port removed. Those were restructured to the same or a
stronger property, never weakened.

| Invariant | Restructured because | Revert → observed RED |
|---|---|---|
| `TestInvBudgetAllPaths` | none — passed unchanged | remove the budget check from `applyUpdate` → the peer path stops logging over-budget, assertion fails |
| `TestInvTeardownNoDeadlock` | the fake was a `ClusterBroadcaster`; became a `hub.Hub` whose `Subscription.Close` blocks (the same pubsub.Close semantics) | swap teardown's `cancelSub` ahead of the context cancel → deadlocks, test times out |
| `TestInvPersistRoundtrip` | `BlobStore` → `persistence.CheckpointStore` | make `persist` write a delta instead of complete state → round-trip reads back short |
| `TestInvObs*` (5) | `panicOnSaveStore` embedded two different stores; now embeds one | remove `recover()` from the run loop → the induced panic crashes the test binary |
| `TestInvNoLeak`, `TestInvLifecycleBounded`, `TestInvMgrLiveness*`, `TestInvShutdownAbort*`, `TestInvReview*` | passed unchanged | — |

## Guarantees added by this port

| Guarantee | Revert applied | Observed RED |
|---|---|---|
| Handshake frames are never shed (`sendInitial`) | replace `sendInitial` with `Send` | "second handshake frame returned ws: connection closed instead of waiting for queue space" |
| The purge tombstone prevents resurrection | remove the three `acquire` guards | "a Join admitted inside the cascade wrote durable content back for a deleted document" |
| Failed flushes are retried | remove `armRetryTimer` | "timeout waiting for flush retried after a failure"; escalation never fires |
| Restore happens inside the registry's `OpenFunc` | move `restoreInto` after `Acquire` | "LoadCheckpoint called 8 times for 8 concurrent first-opens" |
| A room's teardown evicts its registry slot | remove `evictFromRegistry` | both eviction tests fail: the document is still resident after release |
| A failed seed leaves the room clean | hoist `dirty`/`seededPending` above the error return | "a FAILED seed marked the room dirty" |
| `metapointer.Record` creates a missing row | restore the "missing row is an error" branch | "Record on a document with no index row: no row" |
| `statusWriter.Unwrap` | delete the method | "Hijack through the status-recording wrapper: feature not supported" |
| Every metrics hook moves its series | empty `GenerationInvalidated`; empty `DocumentDurabilityRestored` | "left collaboration_generation_invalidations_total unchanged at 0"; "left collaboration_undurable_flush_failures unchanged at 7" |
| A pre-rebuild metric still exists | drop `FanoutLagSeconds` from `InitMetrics` | "metric collaboration_fanout_lag_seconds existed before the persistence rebuild and is not exported now" |
| Unsupported `CHECKPOINT_STORE` values fail startup | widen `parseCheckpointStore`'s accepted set | the startup error disappears |
| An unknown checkpoint codec is refused, not stored | accept the `default:` branch | "saving an unknown encoding = <nil>, want ErrUnsupportedEncoding" |
| A fence this store cannot honour is refused BEFORE erasure | drop `checkFence` from `Delete` | "Delete with a fence on an Unfenced store = <nil>, want ErrUnexpectedFence" |
| A blank `contentType` on upsert preserves the stored one | drop the preserve branch | "a blank contentType overwrote the stored one ... could materialize a memo root for a whiteboard" |
| The index `Delete` removes the row and is idempotent | drop `delete(s.rows, id)` | "Load after Delete = <nil>, want ErrNotFound" |
| A selected backend without its required settings fails at STARTUP | remove the `REDIS_URL` guard | "expected startup to fail; the backend is selected but unconfigured, so the failure would surface at first use instead" |
| The checkpoint restore is bounded by the ROOM, not by the caller | drop the `WithTimeout` in `restoreBounded` | "restore never returned" — the probe hangs to the 5s guard rather than mismatching an assertion |
| Writes survive repeated release → evict → re-materialize cycles | load the checkpoint, then discard it instead of applying | "2 of 24 writes survived 12 release/re-materialize cycles; a branch was overwritten" |
| The room DECLARES its checkpoint codec | drop `Encoding: EncodingV2` from `Room.persist` | 4 tests fail with "persistence: checkpoint encoding required" |
| The file-service store refuses a codec it cannot record | accept `EncodingV1` alongside V2 | "saving a V1 update = <nil>, want ErrUnsupportedEncoding" |
| A deleted blob whose index row survives loads as CORRUPT, not missing (required by `ErrNotFound`'s own contract) | clear the pointer in `Delete` | load reports `ErrNotFound`, `restoreInto` seeds the row's create-time content, and the deleted document returns |
| `CHECKPOINT_STORE` is mandatory | restore `getenv("CHECKPOINT_STORE", inline)` | "expected startup to fail, got nil — the service would run on the non-durable store and lose every document on restart" |
| `HUB_MODE` is mandatory | restore `getenv("HUB_MODE", inmemory)` | "HUB_MODE unset: expected startup to fail, got nil" |

### Why the room bounds its own restore

The core hands `OpenFunc` a context owned by the **registry**, not by any one
acquirer: it is cancelled when the *last* waiter stops waiting. That bounds an
open nobody wants any more; it does **not** bound an open somebody is still
waiting for. A document that keeps attracting joiners renews the clock on every
arrival, so a wedged `LoadCheckpoint` can outlive every deadline the acquirers
themselves carry.

`TestMaterializationIsBoundedWhenTheCheckpointStoreHangs` (the F7 regression)
cannot detect that gap: under a core that still propagates the acquirer's
context, the inherited deadline satisfies it either way. The added test therefore
passes `context.Background()` — no deadline, no cancellation — so **only** the
room's own bound can end the call. That is what makes it non-vacuous against the
pinned core rather than only against a future one.


## Probes that did NOT go RED — and what changed as a result

These are the entries that matter most: in each case the test was passing for a
reason other than the one it claimed.

| Test | Probe that should have failed it | What was actually wrong | Fix |
|---|---|---|---|
| `TestConcurrentFirstOpensRestoreExactlyOnce` | move `restoreInto` after `Acquire` | it drove `Join`, and `Manager.acquire`'s singleflight collapses concurrent first-connects regardless — it measured the singleflight and reported it as evidence about the registry | call `newRoom` directly, bypassing the singleflight |
| `TestMalformedFramesAreDropped` | — | `handleMessage` drops frames from unregistered senders *before* parsing, so a bare room never reached the parser; it proved "unknown senders are ignored" while claiming to prove "malformed frames are dropped" | register the member first; confirmed against the coverage profile, not the green tick |
| `TestColdLoadCostTracksDocumentSize` | make the store append rather than replace | the debounce coalesced both documents into a single flush, so a log-shaped store was indistinguishable | flush after every edit, which is also the honest model of accumulated history |
| `TestNoPersistenceSignalWasLostInTheRebuild` | empty a `PrometheusMetrics` method body | an unlabelled counter is exported at zero whether or not anything increments it | added `TestEveryMetricsHookMovesItsSeries`, which asserts movement rather than presence |
| `TestReleasedRoomsDoNotAccumulate` (first version) | remove `evictFromRegistry` | it inspected a registry of its own, but `NewManager` overwrites `Deps.Registry` with its own — so it reported a clean registry no room had ever touched | observe `mgr.registry` |
| `TestRejoinRacingAnIdleReleaseLosesNoWrites` (first version) | remove the flush before teardown | 40 concurrent joiners all coalesced onto ONE room: the document materialized **exactly once**, never evicted, and the release/rejoin cycle it claimed to stress never happened. The probe stayed green because the debounce had already flushed, and the test would have stayed green against any registry lifetime bug whatsoever | rounds made sequential, each waiting for release so the next must rebuild from the checkpoint; the materialization count is now **asserted in-test**, so the test fails if it ever decays back into the coalesced shape |
| `FuzzMalformedFramesAreOffenderOnly` (first version) | — | it checked the observer with `Ping`, and coder/websocket only processes pongs while a read is in flight, so it failed with **no offence at all** — it would have been reported as a server defect | the observer reads in the background, as a real client does |

## Deleted rather than restructured (SC-005a)

The **fenced variant of the in-process store** (`NewFenced`, the mode branching,
the fence high-water map, `CheckpointPersistenceFencing`, and the two fenced-only
tests). A fence exists to arbitrate between multiple owners of one document; this
service never writes one, and the topology it would protect — multiple pods
flushing the same document — is explicitly unsupported. It was a production
adapter for a configuration we refuse to run, kept alive by the tests that
exercised it. Those semantics belong to go-yjs's own conformance suite and its
planted implementations, not to a consumer that has no fences.

What REMAINS is the interface obligation: `FenceMode()` reports `Unfenced`, and a
non-zero fence is rejected with `ErrUnexpectedFence` rather than silently
accepted — a caller must not believe it has stale-owner protection it does not
have.


`TestPurgeDurableSurfacesMetadataLoadError`. `purgeDurable` no longer loads the
metadata row — the pointer is resolved inside the store — so the error it induced
became **unreachable by construction**, not merely untested. Two tests still
cover the surrounding property (that a failing durable purge surfaces rather than
reporting success), and the removal is annotated at the call site naming them.

No other `002` invariant was deleted.
