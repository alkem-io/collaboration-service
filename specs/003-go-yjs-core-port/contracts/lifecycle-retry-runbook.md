# Lifecycle retry ladder — contract and operations runbook

The lifecycle consumer applies ONE event from `server`:

| Pattern | What it does | Why losing it matters |
|---|---|---|
| `document.deleted` | runs the owner-delete cascade (disconnect, release, purge durable state) | the only path that purges a document; losing it orphans content the owner believes is gone |

Anything else — an unrecognised pattern, an unparseable body — goes straight to the
dead-letter queue; see "What lands where".

**There is no `document.access_changed`.** Authorization is decided once per
WebSocket session, before the room is materialized, and holds until that socket
closes; a revocation takes effect when the client next connects. There is nothing
for a mid-session event to do, so the pattern, its handler, and the re-evaluation
they drove were removed rather than left inert.

The delete cascade is not recoverable by any other route, so the consumer never
drops an event it could not apply. It moves the event down a delay ladder instead, and finally to a
queue where a person can see it.

## Authorization is per WebSocket session

Worth knowing before anything below, because it decides what an operator can and
cannot fix by changing permissions:

**A session's capability is decided once, when the socket opens, and lasts until it
closes.** The service authenticates, then asks the authorization-evaluation service
for READ and UPDATE on that document, derives viewer/collaborator, and uses that for
the life of the connection. It does not re-check per frame, and there is no lease or
TTL.

**Revoking someone's access does not disconnect them.** It takes effect the next
time they connect. A WebSocket session can last hours, so a user whose permission
was removed may keep reading — and writing, if they had write — until they close the
tab, lose their network, or the pod restarts. That is the deliberate contract, not a
gap: authorization is a property of the connection.

If a revocation must take effect NOW, end the sessions rather than changing
permissions and waiting: delete the document (`document.deleted` disconnects
everyone), or restart the pods serving it. There is no in-band mechanism for
mid-session revocation, by design.

A reconnect is a new session and is evaluated afresh, so a revoked user is refused
with a `1008` policy close on their next attempt. An authorization backend outage
closes with `1011` instead, so clients keep retrying rather than treating an outage
as a permanent denial.

## The assets-root validator is NOT independently rollout-safe

The service refuses whiteboard updates whose `files` locators are inline `data:`
URIs. **Do not deploy that ahead of the client.** The currently shipped client can
still publish an inline dataURL as an upload fallback and ignores the
`update-rejected` control, so a validator-first rollout would refuse those updates
and leave that user's editor with a gap in its own clock sequence — every
subsequent write pending behind the refused one, with no message it acts on.

### It is also not safe under a rolling deployment

A mixed fleet DIVERGES. An old pod has no validator, so it accepts a poison update
and publishes it over the hub; a new pod refuses that peer update and never applies
it. The two pods then hold different documents for the same id, permanently — the
CRDT cannot heal a struct one side never received, and whichever pod checkpoints
last decides what is stored.

Client-first does not remove this. It removes *compliant* producers, not an old
client, a malicious one, or a session already open against an old pod while the
rollout is in progress.

**Ordinary overlapping rolling replacement is therefore not allowed for this
change.** The service generation must be cut over as a boundary.

Required order:

1. The deployed `client-web` generation must already drop the collaborative dataURL
   fallback and handle `update-rejected` by discarding that editor generation and
   reloading server state.

   **Both are implemented** — `efd44a2a1` + `72686d930` for the fallback removal,
   `8d69ef4ff` + `5c6f4600f` for the `update-rejected` handling. So this step is a
   verification, not a wait: confirm the generation actually serving users contains
   them. A merged commit is not a deployed client, and this gate is about what is
   RUNNING when the validator starts refusing updates.
2. **No mixed validator / non-validator collaboration-service fleet.** Drain and
   stop the old pods and cut the service generation over as a coordinated boundary
   — or otherwise prove old pods cannot accept connections or updates before new
   pods begin serving.
3. New pods start and rooms rematerialize from the same durable state.

This is sequencing and an operations gate, not a compatibility window: the old path
was never shipped to production, so there is no dual-schema support to build, no
peer special-case to add, and none should be added.

## Before deploying — preconditions, not suggestions

The service **fails closed** on both of these. That is deliberate: each failure is
silent and unrecoverable if it is allowed through, so it is a startup refusal
instead.

**1. Every target broker runs RabbitMQ >= 3.13.2.** On 3.9.13 a quorum queue
*accepts* `x-message-ttl` and `x-dead-letter-strategy`, reports both back on
inspection, and never expires anything. Every retry piles up in its tier, is
redelivered never, and produces no error anywhere.

CI and dev-orchestration run **4.0.5**, so the floor is met — and the topology is
declared for it. 4.0 applies a default `delivery-limit` of 20 to quorum queues where
3.x was unlimited, which would silently drop an event that has been redelivered past
it on a queue with no dead-letter exchange. Q1 and the DLQ therefore set
`x-delivery-limit: -1` explicitly; the retry tiers do not, because they have a DLX
(the limit diverts rather than drops) and no consumer to increment it.

**Do not weaken the floor to make a local environment work.** Lowering it does not
buy compatibility, it buys a service that looks healthy while silently dropping
deletions and revocations. Upgrade the broker.

**2. No pre-existing queue conflicts with the frozen arguments.** RabbitMQ queue
arguments are **immutable after declaration**: a queue that already exists with
different arguments cannot be reconfigured, and re-declaring it fails
`PRECONDITION_FAILED`. There is no in-place migration — the only fix is to delete
and recreate the queue, which discards whatever it holds.

So before deploying to an environment, check what is already there:

```bash
rabbitmqctl list_queues name type arguments | grep -iE 'collab|lifecycle'
```

Every queue this service will declare must either **not exist** or already match
exactly:

| Queue | Required |
|---|---|
| `<queue>` (Q1) | `quorum`, args exactly `{"x-queue-type":"quorum","x-delivery-limit":-1}` |
| `<queue>.retry.{30s,5m,30m}` | `quorum` + the TTL/dead-letter/overflow args below |
| `<queue>.dlq` | `quorum`, args exactly `{"x-queue-type":"quorum","x-delivery-limit":-1}` |

A `classic` queue under any of those names is a hard conflict. The pre-Yjs services
own **differently named** classic queues — on dev-orchestration today,
`collaboration-document-service` and `alkemio-whiteboard-collaboration`, both
`classic` with no arguments — so the default `alkemio-collaboration-lifecycle` does
not collide with them. It would collide if `LIFECYCLE_QUEUE` were pointed at an old
name to "reuse" it. Don't.

Q1 is additionally declared by `server`, so its arguments have to match on **both**
sides; see the frozen contract below.

## Topology

Everything routes through the **default exchange**, where the routing key *is* the
queue name. No exchange is declared or bound.

```
                    ┌──────────────────────────────────────────┐
                    │                                          │  expires after TTL
  server ─────────► <queue>  ──fail──► <queue>.retry.30s ──────┤  (broker dead-letters
   (Q1 producer)      ▲   │                                    │   back to Q1)
                      │   ├──fail(2)─► <queue>.retry.5m  ──────┤
                      │   ├──fail(3)─► <queue>.retry.30m ──────┘
                      │   │
                      └───┘
                          └──exhausted / unactionable──► <queue>.dlq   (terminal)
```

- Total covered outage before the DLQ: **~35 minutes**.
- `<queue>` defaults to `alkemio-collaboration-lifecycle` (`RABBITMQ_LIFECYCLE_QUEUE`).
- All five queues are **durable quorum** queues.
- The retry tiers have **no consumer**. A message sits for its TTL and the broker
  dead-letters it back to Q1.

### Q1's arguments are frozen — the cross-repo contract

`<queue>` is declared by **both** `server` and this service, with exactly:

```json
{ "x-queue-type": "quorum", "x-delivery-limit": -1 }
```

Nothing else, and nothing missing: **omitting** an argument the queue already has is
refused exactly as a **changed value** is. Both sides must declare the same set with
the same values.

Integer width is NOT part of that — verified on 4.0.5 across languages: `-1` sent as
an 8-, 16-, 32- or 64-bit integer all redeclare equivalently, because RabbitMQ
normalizes them. This repo writes `int32` by convention, matching the type the broker
reports back; a producer in another language does not have to match the width, only
the value. An inequivalent redeclaration on either side fails
`PRECONDITION_FAILED` and the declaring party does not start. In particular Q1
carries **no** dead-letter arguments: transfers out of it are explicit confirmed
publishes by the consumer, not broker dead-lettering.

`TestQ1RejectsAnInequivalentProducerDeclaration` asserts this against a real broker
— the contract is enforced by RabbitMQ, not merely agreed in this document.

### Broker version floor

**RabbitMQ >= 3.13.2 is required.** The consumer refuses to start below it,
including when the broker reports no readable version.

This is not caution. On 3.9.13 a quorum queue *accepts* `x-message-ttl` and
`x-dead-letter-strategy`, reports both back on inspection, and then **never expires
anything** — measured, not assumed. Every retry would pile up in its tier, be
redelivered never, and produce no error anywhere. The failure is completely silent,
which is why it is a startup refusal rather than a warning.

`TestBrokerExpiresARetryTierBackToItsTarget` is the test that keeps the floor
honest: it passes on 3.13.2 and fails on 3.9.13.

## Signals

| Metric | Kind | Question it answers |
|---|---|---|
| `collaboration_lifecycle_transfers_total{queue,outcome}` | counter | is anything failing right now, and how far down the ladder |
| `collaboration_lifecycle_queue_ready_depth{queue}` | gauge | is READY work waiting (a lower bound on unattended work — see below) |
| RabbitMQ's own `messages` column / `rabbitmq_queue_messages` | gauge | is there unattended work, INCLUDING messages parked for a dead-letter hop |
| `collaboration_lifecycle_dead_lettered_total{pattern,replays}` | counter | has a person already tried to fix this |

Suggested alerts:

```promql
# Page-worthy: an event has exhausted the ladder. A deletion or revocation is
# NOT applied and will not be retried without a person. Ready depth is exact for
# the DLQ — nothing consumes it and nothing dead-letters out of it, so no message
# there is ever in the parked state.
collaboration_lifecycle_queue_ready_depth{queue=~".+\\.dlq"} > 0

# Escalate rather than repeat: this event has already been replayed and failed
# again, so the fix applied before the last replay did not work.
increase(collaboration_lifecycle_dead_lettered_total{replays!="0"}[1h]) > 0

# Warn: something is failing. ~35 minutes of runway before it reaches the DLQ.
increase(collaboration_lifecycle_transfers_total{queue=~".+\\.retry\\..+"}[5m]) > 0

# Warn: transfers are not landing at all. The event is still an unacknowledged
# delivery and the consumer is recycling its channel to have it redelivered.
increase(collaboration_lifecycle_transfers_total{outcome="unconfirmed"}[5m]) > 0
```

Alert from the **first** retry, not from the DLQ. By the time the DLQ fills, the
window in which anyone could have fixed the backend has already closed.

Per-tier depth doubles as message age: an event in `.retry.30m` has already
survived `.retry.30s` and `.retry.5m`, so it has been failing for at least five and
a half minutes. AMQP exposes no message-age reading at all, so the ladder's own
quantization is the age signal.

### What the depth gauge does not see

It is **ready** depth — how many messages the broker would hand to a consumer right
now — and that is a *lower bound* on unattended work, not the whole of it. AMQP's
`queue.declare-ok` reports only the ready count, and a message the broker is holding
for a *pending dead-letter hop* is neither ready nor unacknowledged. It reads as
zero here while being very much present.

**Use RabbitMQ's own reading for the parked state**, because only the broker can see
it:

```bash
rabbitmqctl list_queues name messages messages_ready
#   <queue>.retry.30s   1   0     <- one message parked, invisible to ready depth
```

Where the management/prometheus plugin is scraped, `rabbitmq_queue_messages` is the
same total per queue. The broker also *says so in its log* while a hop is stuck:

```
Cannot forward any dead-letter messages from source quorum queue '<queue>.retry.30s'
… Fix this issue to prevent dead-lettered messages from piling up in the source
quorum queue.
```

That state has exactly one cause: an expired retry whose dead-letter target is
missing. It is bounded rather than open-ended — the consumer re-declares every
queue in the topology on each re-attach and on each depth poll, so a deleted queue
comes back within one poll interval and the broker then releases the parked message
on its own (see below).

## What lands where

| Situation | Destination |
|---|---|
| handler succeeded | acked, nothing republished |
| `Purge` returned an error (transient backend failure) | next retry tier |
| already on the last tier | `.dlq` |
| body is not JSON, or not a lifecycle envelope | `.dlq` immediately |
| `pattern` is outside the contract | `.dlq` immediately |
| payload has no `id`, or is not an object | `.dlq` immediately |

The last four skip the ladder because no amount of redelivery makes them
actionable — but they are *recorded* rather than dropped, because they mean the
producer is emitting something this service cannot read, and that has to be visible
somewhere.

## Replaying from the DLQ

The DLQ is terminal: no TTL, no dead-lettering. Messages wait for a person.

Before replaying, **fix the cause**. A replay into a still-broken backend walks the
whole ladder again and returns to the DLQ ~35 minutes later.

Use the shipped command. **Do not use a shovel**, and do not use
`rabbitmqadmin get` followed by a publish; both get this wrong in ways that are
hard to see afterwards.

```bash
# 1. See how much is waiting.
lifecycle-replay -url "$RABBITMQ_URL" -queue alkemio-collaboration-lifecycle -dry-run

# 2. Fix the underlying failure. Confirm with
#    increase(collaboration_lifecycle_transfers_total{queue=~".+\\.retry\\..+"}[5m])
#    that new events are no longer entering the ladder.

# 3. Put the events back on the ladder.
lifecycle-replay -url "$RABBITMQ_URL" -queue alkemio-collaboration-lifecycle
#   replayed 4 event(s) onto alkemio-collaboration-lifecycle

# -limit N moves at most N, for a cautious first pass.
```

It is a command rather than a documented broker recipe because the move has two
requirements neither alternative meets:

- **`x-collab-attempt` must be cleared.** A shovel preserves message headers
  (`dest-add-forward-headers: false` suppresses only the shovel's *own* headers), so
  a shovelled event arrives still marked `x-collab-attempt: 3` and goes straight
  back to the dead-letter queue on its first failure — indistinguishable from a
  replay that worked.
- **The dead-letter copy must be released only after the republish is confirmed.**
  `get` with ack followed by a publish does the opposite: a publish that fails after
  the ack destroys the last copy of the event. The command publishes with
  `mandatory`, requires a confirm with no return, and only then acks — and on any
  failure leaves the dead-letter copy untouched.

The command also increments `x-collab-replays`. Clearing the attempt count erases
the event's history, and that counter is what puts it back: it reaches the
`collaboration_lifecycle_dead_lettered_total{replays}` label, so an event that has
been replayed three times and failed again is visibly different from one failing for
the first time. That difference is the signal to escalate rather than repeat.

The handler is **idempotent** — purging an already-purged document succeeds — so
replaying a message that was in fact applied is safe.

## Failure modes worth knowing

**A transfer that is not confirmed.** The consumer publishes with `mandatory=true`
and requires *both* a publisher confirm and the absence of a return. A confirm alone
is not enough: on the default exchange, publishing to a queue that does not exist is
a silent discard that still confirms. If either answer says no, the consumer leaves
the delivery **unacknowledged** and closes its channel after a backoff, so the
broker redelivers it. It never nacks or rejects — rejecting turns a transient
publish failure into terminal handling, and the dead-letter hop behind a reject is
not itself publisher-confirmed.

**A deleted queue, and the parked message behind it.** With
`x-dead-letter-strategy=at-least-once`, a message whose dead-letter target does not
exist at expiry is **retained in its source queue** — verified on 3.13.2 against an
at-most-once control that loses it. The message is not lost, but it is parked:
neither ready nor unacknowledged, so no consumer and no shovel can reach it, and
`messages_ready` reads 0 while `messages` reads 1. The broker logs `Cannot forward
any dead-letter messages from source quorum queue …` while this is going on.

**The recovery is to declare the missing queue, and nothing else.** Once the target
exists the broker retries the hop on its own and releases the message — measured at
2m58s on 3.13.2, twice, on a fixed cycle of about three minutes. No replay, no
restart, no intervention on the source queue; in fact nothing else releases it, and
attempting to drain the source will find nothing to drain.

In practice the service does this for you: it re-declares all five queues at
startup, on every supervisor re-attach, and on every depth poll. A deleted queue is
back within one poll interval and the parked message follows a few minutes later.
The manual step is needed only if the queue is deleted while the service is down —
in which case starting the service *is* the recovery action.

```bash
# If you need to do it by hand, declare with the frozen arguments.
rabbitmqadmin declare queue name=alkemio-collaboration-lifecycle durable=true \
  arguments='{"x-queue-type":"quorum"}'
# Then wait ~3 minutes and confirm the source drained:
rabbitmqctl list_queues name messages messages_ready
```

**The consumer stops processing.** It supervises its own attachment: any way the
delivery stream ends — broker restart, channel fault, a deliberate recycle — it
re-attaches on a bounded backoff, indefinitely, until shutdown. A consumer silently
doing nothing should not be possible; if `collaboration_lifecycle_queue_ready_depth` on Q1
climbs while `collaboration_lifecycle_transfers_total` stays flat and nothing is
being applied, that is the shape to look for.
