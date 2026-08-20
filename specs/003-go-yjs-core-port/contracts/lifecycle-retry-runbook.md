# Lifecycle retry ladder — contract and operations runbook

The lifecycle consumer applies two events from `server`:

| Pattern | What it does | Why losing it matters |
|---|---|---|
| `document.deleted` | runs the owner-delete cascade (disconnect, release, purge durable state) | the only path that purges a document; losing it orphans content the owner believes is gone |
| `document.access_changed` | re-evaluates authorization for a live room's members | the only revocation path; losing it leaves a revoked member editing |

Neither is recoverable by any other route, so the consumer never drops an event it
could not apply. It moves the event down a delay ladder instead, and finally to a
queue where a person can see it.

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
{ "x-queue-type": "quorum" }
```

Nothing else. An inequivalent redeclaration on either side fails
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
| `collaboration_lifecycle_queue_ready_depth{queue}` | gauge | is there unattended work |
| `collaboration_lifecycle_dead_lettered_total{pattern,replays}` | counter | has a person already tried to fix this |

Suggested alerts:

```promql
# Page-worthy: an event has exhausted the ladder. A deletion or revocation is
# NOT applied and will not be retried without a person.
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

It is **ready** depth, and the name says so because the distinction is real. AMQP's
`queue.declare-ok` reports only the ready count, and a message the broker is holding
for a *pending dead-letter hop* is neither ready nor unacknowledged. It reads as
zero here while being very much present.

That state has exactly one cause: an expired retry whose dead-letter target is
missing. It is bounded rather than open-ended — the consumer re-declares every
queue in the topology on each re-attach and on each depth poll, so a deleted queue
comes back within one poll interval and the broker then releases the parked message
on its own (see below). While such a window is being examined, read the total from
the broker rather than from this metric:

```bash
rabbitmqctl list_queues name messages messages_ready
#   <queue>.retry.30s   1   0     <- one message parked, invisible to ready depth
```

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

Both handlers are **idempotent** — purging an already-purged document succeeds, and
re-evaluating access is a recomputation — so replaying a message that was in fact
applied is safe.

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
