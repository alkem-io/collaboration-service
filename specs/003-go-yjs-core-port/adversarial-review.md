# Adversarial review — `003-go-yjs-core-port` (T067, SC-010)

Reviewed `internal/...` and `cmd/...` after the port, hunting for defects rather
than confirming the design. Four found, all fixed, all with a regression test.

Every one is on a FAILURE path — the branch that runs when something else has
already gone wrong. That is the class ordinary testing misses, because nothing
exercises what happens *after* the error return.

---

## 1. A failed materialization leaked its registry handle

`newRoom` acquires the document, then subscribes to fan-out. A subscribe failure
returned the error without releasing the handle.

**Why it is worse than a leak.** A held handle makes `Evict` return `ErrInUse`
forever, so that document could never be evicted or invalidated again, and every
later open was handed the same stale in-memory copy. Nothing else could clean it
up: the room never entered the Manager's map, so no teardown would run for it.
A process restart was the only recovery, and the symptom is one document quietly
refusing to pick up changes from storage while every other document behaves.

**Fixed** by routing both release sites through one helper. Regression:
`TestFailedMaterializationReleasesTheRegistryHandle`, RED before the fix with
`memory: document in use`.

## 2. The Redis hub could leave a subscription with no subscribers

`Subscribe` registers the subscriber, starts the document's Redis subscription
off the lock (it does I/O), then installs it. A `Close` landing in that window
found no pump to decrement, and the install then set `refs = 1` for a subscriber
already gone. `refs` never reaches zero afterwards, so no later `Close` can tear
it down either — the pod holds that subscription and its goroutine for life.

**Fixed** by deriving `refs` from the live subscriber count instead of assuming
it, which is what `removeSubscriber`'s arithmetic already relied on. The two
sites now agree by construction rather than by coincidence.

**Proven, not argued.** A 600-iteration concurrent stress test never reproduced
it — the window is only as wide as one `client.Subscribe` call — so it was
initially recorded as resting on inspection. It is now deterministic:
`TestSubscriberClosingInsideThePumpStartWindowLeavesNoPump` widens the window
from OUTSIDE the hub, by supplying a Redis client whose `Subscribe` blocks until
the test releases it, and closes the only subscriber while a goroutine is parked
in there. Verified RED against the original `p.refs = 1` with "a Redis
subscription was installed for a document with NO subscribers".

No production seam was added for this. The hub already takes its client as an
interface, which is what made the window reachable from a test — a hook in
production code to prove a lock-window bug is a change nobody can justify at
review time.

## 3. Escalation spun instead of terminating — the one with real blast radius

Durability escalation tears the room down from inside the save-timer branch of
the run loop. That branch had no exit, so it fell through, re-armed the retry
timer on a dead room, and looped: save fails, threshold still crossed, escalate
again.

**Measured: 1 → 5 escalations in 300 ms for a single document, climbing.**

`DurabilityEscalationsTotal` means *"we discarded someone's unsaved edits"* — the
signal an operator pages on. One document on a broken store would drive it into
the thousands, so the metric that exists to make data loss visible would instead
make one incident look like a cluster-wide catastrophe, while the room's
goroutine and its failing I/O continued indefinitely.

**Fixed** by returning when the room is no longer Active. Every other branch that
tears down already returned; this one now does too. Regression:
`TestEscalationTerminatesTheRoomRatherThanSpinning`.

## 4. Duplicate `saved` notification on recovery (minor)

`onFlushSucceeded` broadcast a `saved` control, and `persist` broadcast one
unconditionally immediately after — so every recovery sent the client two
identical frames with the same version, and anything counting saves
double-counted exactly when it was already reporting an incident.

**Fixed** by dropping the redundant one. FR-027's restore half is carried by
`persist`'s own broadcast; the transition the client reads is save-error → saved.

---

## Reviewed and found sound

- **Publish delivers outside the lock.** A subscription closed concurrently can
  receive one late delivery. Correct as-is: handlers are synchronous, so holding
  the lock across delivery would let one slow handler block every other
  document's publish, and the room's handler drops late deliveries on
  `roomCtx.Done()`.
- **Purge ordering.** `beginPurge` precedes the room lookup, and `acquire` checks
  the tombstone at all three points including after off-lock materialization.
- **Shutdown closer order.** LIFO gives lifecycle consumer → Manager drain →
  durable backends, so rooms persist before their backends close.
- **Every goroutine is bounded.** The ws writer (exits on close or write error),
  the off-loop `CloseNow`, the hub pump (ctx or channel close), the room run loop
  (every teardown branch returns — after fix 3), the lifecycle and RPC consumers
  (exit when their delivery channel closes), and the HTTP server.
- **`metapointer.Record` read-modify-write.** Saves for one document are
  serialized by that room's run loop, and one document has one room. Concurrent
  first-saves would require two pods on one document, which is the durable
  multi-pod combination the service warns is unsupported.
- No `TODO`/`FIXME`/`XXX` markers remain in production code.

## Gates at the close of review

`go build` · `go vet` · `go vet -tags integration` · `go vet -tags e2e` ·
`golangci-lint run` **0 issues** · `go test -race ./...` **18 packages green** ·
e2e lane green · coverage gate **95.2%** (threshold 95.0%).
