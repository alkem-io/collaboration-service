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
| `metapointer.Record` creates a missing row | restore the "missing row is an error" branch | "Record on a document with no index row: no row" |
| `statusWriter.Unwrap` | delete the method | "Hijack through the status-recording wrapper: feature not supported" |
| Every metrics hook moves its series | empty `GenerationInvalidated`; empty `DocumentDurabilityRestored` | "left collaboration_generation_invalidations_total unchanged at 0"; "left collaboration_undurable_flush_failures unchanged at 7" |
| A pre-rebuild metric still exists | drop `FanoutLagSeconds` from `InitMetrics` | "metric collaboration_fanout_lag_seconds existed before the persistence rebuild and is not exported now" |
| Unsupported `CHECKPOINT_STORE` values fail startup | widen `parseCheckpointStore`'s accepted set | the startup error disappears |
| An unknown checkpoint codec is refused, not stored | accept the `default:` branch | "saving an unknown encoding = <nil>, want ErrUnsupportedEncoding" |
| A non-zero fence is refused BEFORE erasure | drop `checkFence` from `Delete` | "Delete with a fence on an Unfenced store = <nil>, want ErrUnexpectedFence" |
| A blank `contentType` on upsert preserves the stored one | drop the preserve branch | "a blank contentType overwrote the stored one ... could materialize a memo root for a whiteboard" |
| The index `Delete` removes the row and is idempotent | drop `delete(s.rows, id)` | "Load after Delete = <nil>, want ErrNotFound" |
| A selected backend without its required settings fails at STARTUP | remove the `REDIS_URL` guard | "expected startup to fail; the backend is selected but unconfigured, so the failure would surface at first use instead" |
| Corrupt stored state fails materialization and never opens EMPTY | add `ErrCorrupt` to the `ErrNotFound` branch | "materialization succeeded against unreadable stored state; the room would serve an EMPTY document and the next save would overwrite the last good state" |
| The checkpoint restore is bounded by the ROOM, not by the caller | drop the `WithTimeout` in `restoreBounded` | "restore never returned" — the probe hangs to the 5s guard rather than mismatching an assertion |
| Writes survive repeated release → evict → re-materialize cycles | load the checkpoint, then discard it instead of applying | "2 of 24 writes survived 12 release/re-materialize cycles; a branch was overwritten" |
| The room DECLARES its checkpoint codec | drop `Encoding: EncodingV2` from `Room.persist` | 4 tests fail with "persistence: checkpoint encoding required" |
| The file-service store refuses a codec it cannot record | accept `EncodingV1` alongside V2 | "saving a V1 update = <nil>, want ErrUnsupportedEncoding" |
| A deleted blob whose index row survives loads as CORRUPT, not missing (required by `ErrNotFound`'s own contract) | clear the pointer in `Delete` | load reports `ErrNotFound`, the room opens the document EMPTY, and the next save writes that over content whose blob was just erased |
| `CHECKPOINT_STORE` is mandatory | restore `getenv("CHECKPOINT_STORE", inline)` | "expected startup to fail, got nil — the service would run on the non-durable store and lose every document on restart" |
| `HUB_MODE` is mandatory | restore `getenv("HUB_MODE", inmemory)` | "HUB_MODE unset: expected startup to fail, got nil" |
| redis + file-service is REJECTED at startup | remove the pair check | "must fail startup; the service would serve happily while two pods overwrote each other's flushes" |
| ...and the supported combinations still load | make the check reject everything | "single-pod durable must still load" |
| A save NEVER recreates a file the index still points at | restore the create-on-rewrite-404 fallback | "saving against a pointer whose file is gone SUCCEEDED; a missing referenced checkpoint was silently replaced with current in-memory state, hiding the corruption" |
| ...and a document with NO pointer still gets its first checkpoint | refuse every save | "first save for a document with no pointer must succeed" |

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

`TestPurgeDurableSurfacesMetadataLoadError`. `purgeDurable` no longer loads the
metadata row — the pointer is resolved inside the store — so the error it induced
became **unreachable by construction**, not merely untested. Two tests still
cover the surrounding property (that a failing durable purge surfaces rather than
reporting success), and the removal is annotated at the call site naming them.

No other `002` invariant was deleted.

---

# Lifecycle retry/DLQ topology (Q1–Q5)

35 mutation probes over the new lifecycle consumer: each guarantee was reverted in
the production source, the package suite was run, RED was observed, and the source
was restored. Every probe named below went RED. The harness re-runs the suite after
restoring and asserts GREEN, so a probe that leaves the tree dirty is detectable.

## Topology declaration

| Guarantee reverted | Test that caught it |
|---|---|
| retry tier declared without `x-queue-type: quorum` | `TestConnectDeclaresTheWholeTopologyDurablyAsQuorumQueues` |
| Q1 declared non-durable | ″ |
| retry tiers declared non-durable | ″ |
| Q1 given an extra argument (inequivalent to the producer's declaration) | ″ |
| `x-dead-letter-strategy: at-least-once` dropped | `TestRetryTiersCarryTheirScheduleAndTheDLQIsTerminal` |
| `x-overflow: reject-publish` dropped | ″ |
| `x-message-ttl` narrowed from `int32` to `int` (type drift) | ″ |
| tier dead-letters to the wrong routing key | ″ |
| DLQ given a TTL (no longer terminal) | ″ |
| `tierCount` drifts from `len(retryTiers)` | `TestTierCountMatchesTheSchedule` |

## Connect ordering

The ordering claims are asserted against a recorded call log, not merely against
"both happened". Each of these reverts moved a verb after `Consume` or removed it:

| Guarantee reverted | Test that caught it |
|---|---|
| `Qos` never applied (prefetch unlimited) | `TestConnectBoundsPrefetchAndDeclaresBeforeConsuming` |
| `Qos` applied after `Consume` | ″ |
| topology declared after `Consume` | ″ |
| publisher confirms never enabled | ″ |
| a zero configured prefetch reaches `Qos` as 0 | ″ + `TestResolvePrefetchDefaults` |

`DefaultPrefetch`'s exact value is **not** asserted. The earlier assertion compared
`ch.prefetch` against `DefaultPrefetch` itself, which is self-referential and stayed
green when the constant changed — it was a change-detector, not an invariant. The
invariant is that a positive prefetch is applied at all (0 means *unlimited* in
AMQP); the configured branch is covered separately by
`TestConnectHonoursConfiguredPrefetch`.

## Confirmed transfer

| Guarantee reverted | Test that caught it |
|---|---|
| publish loses `mandatory=true` | `TestFailureTransfersToTheNextTierBeforeAcking` |
| a broker RETURN is ignored (unroutable counts as success) | `TestTransferFailureLeavesTheEventBrokerOwned` |
| a broker NACK counts as success | ″ |
| an unconfirmed transfer acks the original anyway | ″ |
| the attempt header is not incremented | `TestFailureTransfersToTheNextTierBeforeAcking` |
| an exhausted schedule wraps to tier 0 instead of the DLQ | `TestExhaustedScheduleGoesToTheDeadLetterQueue` |
| a terminal verdict is routed into the retry ladder | `TestUnactionableEnvelopeGoesToTheDeadLetterQueue` |
| a successful handler transfers instead of just acking | `TestSuccessfulHandlerAcksWithoutTransferring` |

## Attempt-header totality

`x-collab-attempt` crosses a wire, so the routing decision must be defined for
every value that can arrive — not just the ones this consumer writes.

| Guarantee reverted | Test that caught it |
|---|---|
| header narrowed to `int32` without clamping (`1<<32` wraps to 0 → back to tier 0) | `TestAttemptHeaderRoutingIsTotalOverWhateverArrives` |
| negative attempt not floored | ″ |
| wider (`int64`) header types ignored | ″ |
| byte-width (`uint8`) header types ignored | ″ |

The negative-value case is the one worth spelling out. Routing alone did **not**
defend the floor: `nextTarget` already maps any `attempt <= 0` to tier 0, so
deleting the floor left the suite green. The observable defect is in the *outgoing*
header — the transfer writes `attempt+1`, so `-7` becomes `-6`, still "before the
ladder", and the event cycles the first tier forever without ever reaching the DLQ.
The test therefore asserts the header it publishes as well as the queue it targets:
**every transfer must strictly advance toward the DLQ.**

## Broker version floor

| Guarantee reverted | Test that caught it |
|---|---|
| the version floor is not enforced | `TestVersionFloorRejectsABrokerThatSilentlyIgnoresTheTopology`, `TestVersionFloorRejectsBelowTheBaselineOnEveryComponent` |
| a missing broker version is tolerated (fails open) | `TestVersionFloorFailsClosedOnAnUnreadableVersion` |
| an unreadable broker version is tolerated | ″ |

The first attempt at the missing-version probe was **invalid, not passing**: it
disabled the missing-version branch but left `raw` empty, so `parseBrokerVersion("")`
failed and the *unreadable* branch produced an error anyway. The guarantee was never
unprotected; the probe simply did not revert it. Re-run with the branch returning
`nil`, it went RED.

## Handler verdicts

| Guarantee reverted | Test that caught it |
|---|---|
| unparseable body acked as success | `TestUnactionableEnvelopeGoesToTheDeadLetterQueue`, `TestHandleVerdictsSeparateSuccessFromUnactionable` |
| unknown pattern acked as success | `TestHandleVerdictsSeparateSuccessFromUnactionable` |
| malformed `document.access_changed` acked as success | `TestMalformedAndEmptyEventsAreTerminal` |
| a purge failure acked instead of retried | `TestFailureTransfersToTheNextTierBeforeAcking`, `TestTransferFailureLeavesTheEventBrokerOwned`, `TestExhaustedScheduleGoesToTheDeadLetterQueue` |

`handleAccessChanged` was changed as a result of this pass. It previously swallowed
a malformed payload and returned nothing, so `handle` acked it as a success while
`handleDeleted` sent the identical failure to the DLQ. The asymmetry was not a
design choice — an unparseable `access_changed` is exactly as unactionable, and
acking it meant a lost revocation left no trace anywhere.

## Deleted rather than tested

- `headerReplays` (`x-collab-replays`). No producer and no consumer: nothing writes
  it and nothing reads it. It existed to support a replay procedure that is
  documentation, not code. Deleted rather than kept as a named-but-inert constant.
- `PatternDocumentCreated`, `CreatedEvent`, `handleCreated`, `normalizeContentType`,
  and `Manager.PreRegister` *on the lifecycle adapter's port*. `PreRegister` itself
  survives on `service.Manager` — it has a live second caller in the HTTP adapter —
  but the bus no longer carries a create event, so the lifecycle adapter's copy of
  the method was a port with no caller.
- `fakeAcker.requeued`. Production has no `Nack` and no `Reject` at all: a failed
  transfer leaves the delivery unacknowledged and recycles the channel. Tracking a
  requeue flag recorded a value no code path can produce. The `nacks` counter stays,
  asserted at zero — "must never reject" is a live invariant, since rejecting turns a
  transient publish failure into terminal handling behind an unconfirmed DLX hop.

## Supervisor (broker re-attachment)

Found while wiring metrics, not by a test: `recycle()` closes the channel, but
nothing re-opened it. The consume loop's only exit is the delivery stream ending,
so the first unconfirmable transfer permanently stopped the consumer — no purges,
no revocations — behind a process that stayed healthy in every other respect. The
same hole swallowed any broker restart or network blip. `Connect` now brings up one
`session` and a supervisor re-opens it on a bounded backoff until `Close`.

| Guarantee reverted | Test that caught it |
|---|---|
| supervisor removed (a recycle is terminal again) | `TestTheConsumerReAttachesAfterTheDeliveryStreamEnds` |
| dead session's connection not released before re-attaching | ″ |
| supervisor keeps re-attaching after `Close` | `TestCloseTearsDownChannelAndConnectionAndStopsSupervising` |

`TestCloseTearsDownChannelAndConnection` had to be rewritten rather than kept: it
asserted the channel and connection were each closed **exactly once**, which the
supervisor makes racily false (it releases the dead session too) while the actual
invariant — they end up released — still holds. Exact-count assertions on an
idempotent teardown are arithmetic, not invariants. The replacement asserts release
plus the property that Close actually stands the supervisor down; without that,
shutdown is indistinguishable from a broker blip and the consumer dials its way back
up forever behind a process trying to exit.

One claim was **weakened rather than defended**. `transfer` reads the channel and its
confirm/return streams in a single locked read, and the comment said this guarded
against a re-attach landing between the publish and the wait. It cannot: the
supervisor re-attaches from the same goroutine `transfer` runs on, so the interleaving
has no reachable instance. The probe that split the read stayed green for that reason,
and the comment was corrected instead of a test being invented to justify it. The lock
is there for `Close`, which does run on another goroutine.

## Lifecycle observability

Two signals, because they answer two questions a single metric cannot. Twelve
probes, all RED.

| Guarantee reverted | Test that caught it |
|---|---|
| transfers are never reported | `TestEveryTransferIsReported` |
| an unconfirmed transfer is reported as confirmed | ″ |
| every transfer reports the same queue | ″ |
| success also reports a transfer | `TestSuccessIsNotReportedAsATransfer` |
| depth polling never starts | `TestQueueDepthIsPolledForEveryQueueInTheTopology` |
| only the DLQ depth is polled | ″ |
| the depth reading is discarded | ″ |
| every queue reports the main queue's depth | ″ |
| a configured poll interval is ignored | ″ + `TestDepthPollIntervalResolution` |
| a negative poll interval falls back to the default | `TestDepthPollIntervalResolution` |
| the Prometheus bridge collapses the outcome label | `TestLifecycleObserverBridgeMovesItsSeries` |
| queue depth accumulates instead of replacing | ″ |

Two of those probes were **VACUOUS on the first run**, and both times the test was
at fault:

- *"the depth reading is discarded"* — the fake reported `4` for every queue and
  the probe hardcoded `4`, so the mutation was indistinguishable from the truth.
  The fake now reports a **different count per queue**, which also catches a
  reading taken from the wrong queue.
- *"a negative poll interval falls back to the default"* — `TestDepthPollingCanBeDisabled`
  waited 20ms and saw no poll. So would a 30-second default. A window-based test
  cannot tell *disabled* from *slow*, so the three-way meaning of the interval is
  now pinned on the resolver itself, with the behavioural test kept only for the
  part it can actually observe (no poller, no panic on a negative ticker).

`QueueDepth` is a level and `EventTransferred` is a rate, deliberately. A counter
alone cannot answer even "is READY work waiting": it only goes up, so the increment
that put ten events in the DLQ scrolls out of the alert window while the events sit
there. A gauge alone cannot answer "did anything just fail", because a transfer that
lands in a tier and expires back out is invisible between polls. Per-tier depth also
substitutes for message age — the ladder quantizes it, and AMQP offers no age reading
short of the management API or consuming the queue to peek.

Depth is read by **re-declaring** each queue, not by a passive declare. An equivalent
re-declaration is a no-op that returns the current count; a passive declare takes the
channel down whenever a queue is missing, which is exactly the situation worth
reporting rather than dying on. The declare and the poll share one `topologyFor` list,
so the arguments used to poll are by construction the ones used to declare — otherwise
a drift between them would be an inequivalent redeclaration that kills the channel.

## Real-broker integration (RabbitMQ 3.13.2)

The unit tests prove what this code does; these prove what the **broker** does with
it, which is a separate question and the one that actually bit. Run against a real
3.13.2 with the management plugin; CI is pinned to that exact version so it proves
the declared floor is *sufficient*, not merely that some newer broker works.

| Test | What only a real broker can answer |
|---|---|
| `TestConsumerConsumesLivePublishedEvents` | the producer's frozen Q1 declaration and the consumer's coexist on one broker |
| `TestQ1RejectsAnInequivalentProducerDeclaration` | the frozen contract is enforced by RabbitMQ (PRECONDITION_FAILED), not merely agreed in a document |
| `TestAFailedEventLandsOnTheFirstRetryTier` | the transfer publish actually ROUTES — a confirm alone does not prove it, since a default-exchange publish to a missing queue is a silent discard that still confirms |
| `TestAnUnactionableEventLandsInTheDeadLetterQueue` | an unreadable envelope is recorded, and skips the ladder |
| `TestBrokerExpiresARetryTierBackToItsTarget` | the broker honours `x-message-ttl` + `x-dead-letter-strategy` on a quorum queue at all |
| `TestAnExpiredRetryIsRetainedWhenItsTargetIsMissing` | at-least-once retains an unroutable dead-letter, against an at-most-once control that loses it |
| `TestTheRealLadderRedeliversAfterTheFirstTierExpires` | the shipped 30s tier round-trips: Q1 → tier → Q1 → next tier, with the attempt header surviving the broker's own republish |
| `TestDepthPollingReportsRealBrokerCounts` | the depth gauge publishes the broker's number, not ours |

### Negative control: the same tests on 3.9.13

Run against the dev broker (`rabbitmq:3.9.13-management`):

- `Connect` fails closed: *"broker is RabbitMQ 3.9.13; this topology requires >= 3.13.2…"*
- `TestBrokerExpiresARetryTierBackToItsTarget` **fails**: after the TTL the tier
  still holds its message and the target holds none. The broker accepted every
  argument, echoed them back, and expired nothing.

That is the whole justification for the version floor, reproduced as a test rather
than as a claim. It is also why dev-orchestration must move off 3.9.13 before this
ships.

### Two assertions that were WRONG before they were right

Both failures were mine, not the broker's, and both are the same mistake in
different clothing: measuring a proxy instead of the property.

**Reading the ready count for retention.** The first version asserted
`queue.declare-ok`'s message count on the source tier and reported that at-least-once
had *dropped* the message. It had not. A message held for a pending dead-letter hop
is neither ready nor unacknowledged, so the ready count reads 0 for a message that
is very much still there — the broker's own log said so at the time
(*"to prevent dead-lettered messages from piling up in the source quorum queue"*)
and `rabbitmqctl list_queues` confirmed `messages=1, messages_ready=0`. The fix was
the total, from the management API. The review instruction had said "total count …
not internal ready state"; I asserted the ready state anyway and then believed the
result.

**Sampling an eventually-consistent statistic once.** With the total wired up, the
test passed two runs in three. The management API refreshes queue statistics on
`collect_statistics_interval` (5s default), so a single sample is stale in both
directions. The assertion now requires the count to *hold* across the refresh
interval, and the control's 2-second TTL had to grow to 30 seconds — with a 2s TTL
and 5s statistics, "the control message was here before it expired" is not an
observable state, so the control was proving nothing.

### A measured limit — and a correction to it

at-least-once **retains** an expired message whose target is missing.

I first reported that it does **not** resume when the target is later created,
"measured over two minutes". That was wrong: the observation window was too short.
The broker retries the parked hop on a cycle of about three minutes and releases the
message on its own. Measured at **2m58s**, twice, on 3.13.2 — once on a tmpfs
container and once on a persistent volume. The two-minute probe stopped roughly
fifty seconds early and I reported the absence of an event as its impossibility.

So the recovery action is simply "declare the missing queue", and nothing else is
needed or effective in the meantime. That is documented AND demonstrated end to end
by `TestConnectReleasesAnEventParkedForAMissingTarget`, which parks an event for a Q1
that does not exist, starts the consumer, and watches the event be applied.

In production the service performs the recovery itself: it re-declares all five
queues at startup, on every supervisor re-attach, and on every depth poll
(`TestTheDepthPollRecreatesADeletedQueue`), so a deleted queue is back within one
poll interval and the parked message follows.

## Review round two — four blockers from the peer review

### 1. The confirm/return race (HIGH)

`transfer` selected on the return channel and the confirm channel. Both can be ready
at once — a mandatory publish to a missing queue produces `basic.return` then
`basic.ack`, and by the time the select runs the connection reader may have
dispatched both — and **Go picks at random between two ready cases**. Roughly half
the time the confirm won and an unroutable publish reported success, after which the
event was acked: transferred nowhere, deleted from the only queue holding it. The
worst outcome the ladder exists to prevent, and intermittent.

The fix is a non-blocking drain of the return channel after an ack, and it is
*sufficient* rather than merely helpful: amqp091-go dispatches every frame from one
connection-reader goroutine, and both a return and a confirm are delivered by a
synchronous send from inside it (`Channel.dispatch` → `ch.returns` /
`confirms.confirm`), in frame order. AMQP puts `basic.return` before `basic.ack` for
the same publish, and both channels are buffered so the dispatcher never blocks
part-way. A return for this publish is therefore already buffered when its
confirmation becomes readable. Verified by reading the vendored source, not assumed.

| Guarantee reverted | Test that caught it |
|---|---|
| the return drain after an ack is removed | `TestAnAckNeverOutvotesAReturnThatIsAlreadyWaiting` |
| the drain reads the return but accepts success anyway | ″ |
| the same drain in the replay path | `TestAReplayAckNeverOutvotesAReturnThatIsAlreadyWaiting` |

Both tests run 200 iterations against a fake that delivers both answers *before*
the publish call returns. A real broker cannot be made to lose this race on demand,
and one iteration could win the coin toss.

### 2. The depth metric claimed a total it cannot see (HIGH)

`collaboration_lifecycle_queue_depth` was documented as "current message count" and
"unattended work", but its source is `queue.declare-ok`, which reports only the
READY count — and this repo's own integration test proves a message parked for a
pending dead-letter hop is neither ready nor unacknowledged. In precisely the state
worth alerting on, the metric published zero.

Renamed to `collaboration_lifecycle_queue_ready_depth`, with the exclusion stated in
the metric help, the port doc, and the runbook, and the broker-side reading named
for the total. The claim that per-tier depth "stands in for age" was kept but its
justification corrected: AMQP has no age reading at all, not "none without the
management API".

The blind spot is also bounded, which is now stated rather than hoped: the consumer
re-declares the whole topology on every re-attach and every poll, so the only cause
of a parked message — a missing dead-letter target — is repaired within one poll
interval.

### 3. The documented recovery was not executable (BLOCKING)

"Recreate the queue and replay the source tier" could not work: a parked message is
unreachable by any consumer or shovel, so there is nothing to replay. Candidates
were tested against real 3.13.2 rather than reasoned about — see the correction
above. Declaring the target is the whole action, and the service already performs it.

| Guarantee reverted | Test that caught it |
|---|---|
| the depth poll does not re-declare (a deleted queue stays missing) | `TestTheDepthPollRecreatesADeletedQueue` |
| — (end-to-end demonstration, no mutation) | `TestConnectReleasesAnEventParkedForAMissingTarget` |

One candidate was **discarded as invalid rather than reported**: restarting the
broker appeared to lose everything, but that container used a tmpfs for
`/var/lib/rabbitmq`, so the restart wiped the data directory. The experiment measured
the test rig, not the broker. Re-run on a persistent volume.

### 4. The replay procedure was unsafe (BLOCKING)

The runbook's primary path was a shovel, followed by a warning that the shovel
preserves `x-collab-attempt` and therefore does not work. Removed. Replaced by
`cmd/lifecycle-replay`, which clears the attempt count, increments a replay count,
publishes `mandatory` with a confirm, and acks the dead-letter copy **only after**
the republish is confirmed — the opposite order from `get`-then-publish, which
destroys the last copy whenever the publish fails.

`x-collab-replays` is reinstated, but with a real producer and a real consumer this
time: written by the replay command, read by the consumer when an event reaches the
DLQ, and exported as the `replays` label on
`collaboration_lifecycle_dead_lettered_total`. That label is the escalation signal —
after a replay the attempt count is deliberately zero again, so nothing else
separates "this just failed" from "the fix applied before the last replay did not
work".

| Guarantee reverted | Test that caught it |
|---|---|
| replay does not clear the attempt count | `TestReplayReturnsDeadLetteredEventsToTheLadder` |
| replay does not increment the replay count | `TestASecondReplayIsDistinguishableFromTheFirst` |
| replay drops the replay header entirely | both |
| replay acks the dead-letter copy before the republish is confirmed | both |
| replay leaves its consumer attached to the DLQ | `TestASecondReplayIsDistinguishableFromTheFirst` |
| replay publishes without `mandatory` | `TestAReplayThatCannotLandLeavesTheEventInTheDeadLetterQueue`, `TestAReplayClearsTheAttemptCountAndCountsItself` |
| replay ignores a broker return | `TestAReplayThatCannotLandLeavesTheEventInTheDeadLetterQueue` |
| the replay counter wraps / passes through negatives | `TestTheReplayCounterSurvivesWhateverArrives` |
| the dead-letter signal is never raised | `TestTheDeadLetterSignalCarriesThePriorReplayCount` |
| the dead-letter signal drops the replay count | ″ |
| a retry-ladder hop also raises the dead-letter signal | `TestOnlyTheDeadLetterQueueRaisesTheDeadLetterSignal` |
| the Prometheus bridge collapses the `replays` label | `TestDeadLetterBridgeSeparatesReplayCounts` |

`Replay` leaving its consumer attached was **found by a test, not by review**: the
three-round replay test failed at round 2 because the lingering consumer swallowed
the next dead-letter message the instant it arrived — held unacknowledged on a
channel nobody was reading, invisible to the ready depth and unreachable by a second
replay. A bounded operation has to release the queue when it finishes.

### Comment sweep

`consume`'s doc still described the retired two-attempt mechanism ("acks/nacks per
the outcome … requeued once, then dropped"), `patternOf` still spoke of a "dropped
event", the handler-timeout note still said "nack/requeue", and
`TestHandleNacksRequeueOnPurgeFailure` was named for it. Nothing nacks or rejects any
more. All four corrected.

### Coverage: a pre-existing gate failure, measured rather than assumed

The combined coverage gate (≥95%) does **not** pass, and did not pass before this
work either. Measured both sides against the same broker:

| Tree | Combined coverage (scoped) |
|---|---|
| PR head before this work (`3176012`) | **94.8%** |
| after the lifecycle work, before its failure-path tests | 93.6% |
| after them | **94.8%** |

So the lifecycle work is gate-neutral: it dipped the number and the failure-path
tests brought it back. The 0.2% shortfall is pre-existing and spread across
subsystems this branch does not touch — `postgres.Migrate` (59%), `app.buildMetadata`
(66%), `room.measureLiveDoc` (60%), `manager.acquire` (74%), and a long tail in
`ws`, `oidc`, `fileservice`, and `metapointer`. It is reported rather than closed:
padding it from a lifecycle PR would mean writing tests for unrelated code to move a
number, and lowering the threshold would be worse.

The tests added to reach 94.8% are failure paths, not padding — every one has a
story about what breaks without it: a transfer that waits forever on a silent
broker, a replay that starts without publisher confirms, a recycle that outlives
shutdown, a supervisor that gives up on a broker that is merely restarting, a depth
poll whose failure takes the consumer with it.

`cmd/lifecycle-replay` is excluded from the gate's scope on the same grounds as
`cmd/server` — flag parsing and dialling in front of `lifecycle.Replay`, which is
itself inside the bar and covered against a real broker. `NopObserver` moved to
`observer_nop.go` and is excluded exactly as `NopMetrics` already was: empty method
bodies that cannot fail.

### An unrelated CI flake, fixed

The integration job on PR #10 was also failing on
`TestAwarenessAndDocumentUpdatesStayOnSeparateChannels` (redis hub) — at 3.01s,
against a 3-second `waitFor` deadline. It passes 28/28 locally including under
`-race`, so the failure was the budget, not the behaviour: the test waits on a
goroutine hop through miniredis pub/sub, which takes microseconds on an idle machine
and much longer on a loaded runner. A tight deadline there detects nothing — the
adapter has no latency requirement — it only fails the build when the runner is
busy. Raised to 30s with that reasoning recorded at the call site.

## Redis hub: Subscribe returned before the subscription existed

The CI flake I had "fixed" by widening a test deadline was a real message-loss
bug, and the deadline could never have fixed it.

`go-redis`'s `Client.Subscribe` does two unhelpful things:

```go
func (c *Client) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)   // error DISCARDED
	}
	return pubsub
}
```

- The error is thrown away, so a subscription that failed outright is
  indistinguishable from one that worked. The pump then sits on a connection that
  never delivers, for the life of the document, and it looks like a quiet document.
- Even on success, `pubsub.Subscribe` has only **written** the SUBSCRIBE command;
  `_subscribe` does not read the server's confirmation. A publish issued right
  after `Subscribe` returns can be processed first, and Redis pub/sub has no replay
  or backlog — **that message is gone**. Cross-pod, that is a document update or an
  awareness state that simply never arrives.

No deadline on the receiving side can wait out a message that was never queued,
which is why `TestAwarenessAndDocumentUpdatesStayOnSeparateChannels` failed at
exactly 3.01s: not slowness, a lost message. The 30s workaround is reverted and the
budget is back to 3s.

`Hub.Subscribe` now returns only after the server has confirmed **every** channel
it subscribed. The count comes from the same slice used for the SUBSCRIBE, so
"confirm every channel we subscribed" holds by construction rather than by two
numbers agreeing.

| Guarantee reverted | Test that caught it |
|---|---|
| Subscribe returns with no handshake (the original race) | `TestSubscribeIsReadyBeforeItReturns`, `TestSubscribeFailsWhenTheSubscriptionCannotBeEstablished`, `TestAFailedSubscribeLeavesNoTraceBehind` |
| only one channel is confirmed (awareness still races) | `TestSubscribeWaitsForAConfirmationPerChannel` + two others |
| a keep-alive Pong is counted as a confirmation | `TestAKeepAliveIsNotASubscriptionConfirmation` |
| a read failure is counted as a confirmation | `TestAwaitSubscribedSurfacesAReadFailure` + two others |
| a message arriving between confirmations is not collected | `TestAwaitSubscribedKeepsMessagesThatArriveBetweenConfirmations`, `TestAMessageArrivingDuringTheHandshakeIsStillDelivered` |
| …or is collected and then dropped | `TestAMessageArrivingDuringTheHandshakeIsStillDelivered` |
| a failed Subscribe leaves its subscriber registered | `TestAFailedSubscribeLeavesNoTraceBehind` |

Three of those were **VACUOUS on the first attempt**, and the reason is worth
recording: they live at the call site, and the call site could only be driven
through a real miniredis, which never produces the interleaving or the keep-alive.
The fix was not to write cleverer tests but to widen the seam — `client.Subscribe`
now returns a `pubsubConn` interface rather than the concrete `*goredis.PubSub`, so
a scripted connection can hand the hub exactly the reply sequence a real server
produces only occasionally. Same principle as the existing
`blockingSubscribeClient`: prove it from outside the production code, but make sure
the outside can actually reach it.

The handshake introduces a narrow loss of its own, which is why the collected
messages exist at all: a two-channel SUBSCRIBE is confirmed one channel at a time,
so a publish on the first can land before the second is confirmed. That message is
already off the connection — `PubSub.Channel()` will never redeliver it — so the
handshake hands it to the pump. Dropping it would lose the message exactly as the
original race did, in a smaller window.

The subscriber-cleanup entry is not cosmetic. `Subscribe` registers its subscriber
before doing I/O, and pump refs are **derived** from the live subscriber count. A
failed subscription that left its entry behind would give the next successful pump a
ref count that never reaches zero, leaking the Redis subscription and its goroutine
for the life of the pod — the same leak class as the race-window finding above it.
