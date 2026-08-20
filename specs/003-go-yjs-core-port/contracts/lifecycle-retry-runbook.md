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
| `collaboration_lifecycle_queue_depth{queue}` | gauge | is there unattended work |

Suggested alerts:

```promql
# Page-worthy: an event has exhausted the ladder. A deletion or revocation is
# NOT applied and will not be retried without a person.
collaboration_lifecycle_queue_depth{queue=~".+\\.dlq"} > 0

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
a half minutes. AMQP exposes no message-age reading short of the management API, so
the ladder's own quantization is the age signal.

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

Each message carries `x-collab-attempt`, the count of transfers it has been
through. **A replay must clear it.** A message arriving on Q1 still carrying
`x-collab-attempt: 3` goes straight to the DLQ on its first failure, which looks
exactly like a replay that worked.

```bash
# 1. See what is there, and why. The header tells you how far it got.
rabbitmqadmin get queue=alkemio-collaboration-lifecycle.dlq count=50 ackmode=reject_requeue_true

# 2. Fix the underlying failure. Confirm with
#    increase(collaboration_lifecycle_transfers_total{queue=~".+\\.retry\\..+"}[5m])
#    that new events are no longer entering the ladder.

# 3. Move the DLQ back to the main queue.
rabbitmqctl set_parameter shovel lifecycle-replay '{
  "src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": "alkemio-collaboration-lifecycle.dlq",
  "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": "alkemio-collaboration-lifecycle",
  "src-delete-after": "queue-length",
  "dest-add-forward-headers": false,
  "dest-publish-properties": {"delivery_mode": 2}
}'

# 4. Remove the shovel once the DLQ is empty.
rabbitmqctl clear_parameter shovel lifecycle-replay
```

**Verify the header on one message before shovelling the rest.** A shovel preserves
message headers, and `dest-add-forward-headers: false` only suppresses the shovel's
*own* headers — it does not strip `x-collab-attempt`. If your shovel carries it
through, replay by republishing without it instead:

```bash
# Per-message replay that drops the attempt header.
rabbitmqadmin get queue=alkemio-collaboration-lifecycle.dlq count=1 ackmode=ack_requeue_false \
  | ... extract payload ...
rabbitmqadmin publish exchange=amq.default routing_key=alkemio-collaboration-lifecycle \
  payload="<the envelope>" properties='{"delivery_mode":2}'
```

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

**A deleted tier queue.** With `x-dead-letter-strategy=at-least-once`, a message
whose dead-letter target does not exist at expiry is **retained in its source
queue** — verified on 3.13.2, and verified against an at-most-once control that
loses it. The message is not lost. But it does **not** resume on its own when the
queue is recreated (measured over two minutes; it stays put). Recreate the queue
*and* replay the source tier. Note the count: the retained message is neither ready
nor unacknowledged, so `rabbitmqctl list_queues messages_ready` reads 0 for it —
use the total `messages` column. The broker logs `Cannot forward any dead-letter
messages from source quorum queue …` while this is happening.

**The consumer stops processing.** It supervises its own attachment: any way the
delivery stream ends — broker restart, channel fault, a deliberate recycle — it
re-attaches on a bounded backoff, indefinitely, until shutdown. A consumer silently
doing nothing should not be possible; if `collaboration_lifecycle_queue_depth` on Q1
climbs while `collaboration_lifecycle_transfers_total` stays flat and nothing is
being applied, that is the shape to look for.
