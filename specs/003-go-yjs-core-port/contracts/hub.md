# Contract: `hub.Hub` (cross-pod fan-out)

**Implemented by**: this service, for multi-pod (Redis). Single-process uses the core's
shipped `InProcess` default **as shipped** — no reimplementation (§X, §XI).
**Conformance gate**: `conformance.Hub`, which deliberately injects reordering,
duplication, and redelivery.

## Mapping from the superseded port

The existing `ClusterBroadcaster` is near-isomorphic, which is exactly why the
temptation to wrap rather than rewrite is strongest here:

| `ClusterBroadcaster` | `hub.Hub` |
|---|---|
| `Publish(ctx, id, payload, ephemeral bool)` | `Publish(ctx, Message{DocumentID, SourceID, Kind, Payload})` |
| `Subscribe(ctx, id, handler) (cancel, err)` | `Subscribe(ctx, DocumentID, SourceID, Handler) (Subscription, err)` |
| per-pod source id, drops own frames | `SourceID` echo suppression — a contract obligation |
| `ephemeral bool` | `MessageKind` |

**`ClusterBroadcaster` is deleted, not wrapped** (FR-007, SC-008a).

## Obligations

- **Echo suppression MUST be honoured**: a message must not be delivered back to its own
  logical source. `SourceID` is a logical source, never a connection or network address.
- **Backpressure, not silent loss**: an implementation "must not silently discard a
  message merely because an active local subscriber queue is full" — it applies
  backpressure or reports an error. `Handler` returns an `error` for this purpose.
  *This is a real delta*: the superseded handler had no error path and Redis pub/sub is
  fire-and-forget, so the Redis implementation must have an answer here, not a `nil`.
- **Kind separation**: ephemeral awareness MUST NEVER be written to durable storage
  (FR-009).
- **Payload ownership**: borrowed until `Publish` returns; owned by the receiving
  handler invocation and may be retained.

## What the contract does NOT promise

Neither ordering nor single delivery. Publish success means the hub accepted the
message, not that every remote subscriber received it. Disconnected subscribers may
miss messages entirely.

**Therefore**: completeness comes from persistence and state-vector catch-up, never from
assuming fan-out delivered everything. Any place the current implementation leans on
pub/sub ordering must stop. A redelivered update MUST be a harmless no-op.

## Deployment posture

Ships behind the hub-mode configuration key, but **not load-bearing on day one** — the
initial deployment is single-pod.

**Durable multi-pod is NOT supported** until ownership leases land (FR-022a). Only the
originating pod persists on the flush timer, but edits originating on different pods
make several pods writers of one whole-document blob; the later write supersedes the
earlier and self-heals only if a live pod flushes again. Multi-pod fan-out with a durable
store and no ownership mechanism MUST therefore be REJECTED at startup — the
configuration fails to load, before any backend is built or served, with an error
naming both configuration keys, the consequence, and the supported single-pod
alternative (FR-022b). There is no override.
