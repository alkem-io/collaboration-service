# Phase 1 — Data Model: go-yjs Core Port

**Feature**: `003-go-yjs-core-port` | **Date**: 2026-08-18

Two distinct data planes meet in this service and MUST NOT be conflated:

| Plane | Owner | Carries | Reached via |
|---|---|---|---|
| **Content** | this service | the encoded document bytes | `persistence.Store` → file-service |
| **Index** | Alkemio `server` | content type, authz policy, owner, bucket, pointer, version | `MetadataStore` → RabbitMQ RPC |

Collapsing the index into the content store is the shim FR-007 forbids, and is the most
natural-looking wrong turn in this feature.

---

## Content plane

### Document

One collaborative document's authoritative CRDT state.

| Field | Type | Notes |
|---|---|---|
| id | document identifier | backend-neutral; one namespace across memos and whiteboards |
| doc | CRDT document | authoritative, held **plaintext** on the server (FR-004) |
| convention | memo \| whiteboard | memo = `Y.XmlFragment`; whiteboard = id-keyed `Y.Map` scene (FR-003) |

**Invariant**: single writer. No path may mutate concurrently with the document's own
processing path (`002` FR-004, preserved).

### Checkpoint

The whole encoded document state written by one flush. **Under the checkpoint-only
design this is the only durable record kept per document.**

| Field | Type | Notes |
|---|---|---|
| revision | opaque monotonic position | greater than every previously acknowledged revision for that document |
| update | bytes | a raw Yjs-V2 state snapshot — no envelope, no compression |
| stateVector | bytes | equivalent coverage; derivable from `update` without constructing a document |

**Format note**: the payload is byte-identical in kind to what `server` writes with JS
Yjs (`markdownToYjsV2State`, `populateYDoc`). Compatibility is a design guarantee of the
core and is **not** re-verified here.

### Recovery view

What a load returns: exactly one checkpoint, no trailing records, explicitly complete.

| Field | Value under this design |
|---|---|
| checkpoint | present |
| updates | empty |
| through | the checkpoint's revision |
| next | **empty** — the only valid signal of completeness |

**Validation rule (FR-014)**: a view that presents partial history as complete is a
contract violation and MUST be surfaced as an error, never accepted as a document.

### Fence

Ownership epoch carried by every durable mutation.

| Value | Meaning |
|---|---|
| zero | clustering not in use — the normal mode for every current deployment |
| non-zero | a clustered write, rejected by a fenced store if an older epoch than the newest seen |

**Fixed at construction**, never inferred per write, so one omitted fence cannot
silently disable stale-owner protection. The store is built capable of both modes and
its fenced path is tested (FR-008a) even though no deployment enables it.

---

## Index plane

### Document metadata

Owned by `server`; this service reads and writes it over RPC. Unchanged by this feature
except in name (FR-009a).

| Field | Purpose |
|---|---|
| id | document identity, shared with the content plane |
| contentType | memo \| whiteboard — drives convention selection |
| version | the room's own save counter |
| contentPointer | locates the blob in the content plane |
| authorizationPolicyId | drives authz evaluation |
| ownerRef | drives the delete cascade |
| storageBucketId | the document's own bucket |

**Why it survives**: none of this is expressible in a contract "expressed only in bytes
and revisions". It is a different concern, not a redundant one.

---

## Runtime plane

### Registry & handle

The single lifecycle authority for a document in-process.

| Concept | Responsibility |
|---|---|
| acquire | coalesces concurrent cache misses into one open; a caller abandoning its wait does not cancel the shared initializer |
| open function | **restores the document**: load from the store; if nothing is stored, seed from the content delivered by the metadata fetch (FR-004a) |
| handle | keeps one acquisition alive; exposes an invalidation signal |
| evict | releases a document; never invalidates an outstanding handle |
| invalidate | poisons the generation, closes handles, forces reload on next acquisition |

**Exactly-once seeding (FR-004b)**: because seeding happens inside the coalesced open,
concurrent first-opens produce one seed by construction — not by an emptiness check
performed after acquisition, which races.

**Policy stays here**: the registry starts no goroutines and has no eviction policy of
its own. The `002` idle-release policy remains this service's and must drive `evict`.

### Room (collaboration session)

Members, presence, limits, authz state, and flush policy around one acquired document.
Holds a handle; does **not** own document identity or teardown ordering.

**Lifecycle — teardown flush matrix (FR-011a)**:

| Trigger | Flush? | Why |
|---|---|---|
| graceful shutdown | **yes** | FR-001 — persist before durable backends close |
| idle release (dirty) | **yes** | document is believed good; idling out must not cost a window |
| generation invalidation | **no** | may have diverged; writing it would overwrite good content |
| escalation after repeated write failure | **no** | integrity in doubt, and the store is unreachable anyway |
| panic on the processing path | **no** | `002` precedent — never persist a mid-panic document |

A path that is neither MUST NOT default to flushing.

### Durability state machine

| State | Meaning | Transitions |
|---|---|---|
| clean | no unsaved changes | → dirty on mutation |
| dirty | unsaved changes; flush armed | → clean on successful flush; → undurable on failure |
| undurable | flush failing, still serving | → clean on success; → escalated at the consecutive-failure threshold |
| escalated | invalidated, members disconnected | terminal for this generation; reload on next acquisition |

**Observability (FR-026)**: `undurable` MUST be visible via metrics — flush outcome,
consecutive-failure count, and time-in-state — *before* escalation. Otherwise the first
signal an operator gets is users being disconnected.

**Loss accounting (FR-028)**: entering `escalated` discards unsaved edits, so it MUST
emit a distinct counter, a log entry naming the document and its undurable duration, and
a disconnect reason distinguishable from an ordinary disconnect.

### Fan-out message

| Field | Purpose |
|---|---|
| documentId | routing |
| sourceId | echo suppression — a logical source, never a connection or address |
| kind | durable update \| ephemeral awareness — **never conflated** (FR-009) |
| payload | borrowed until publish returns; owned by the receiving handler |

**Delivery contract**: duplication and reordering are permitted; completeness comes from
persistence and state-vector catch-up, never from assuming fan-out delivered everything.
A redelivered update MUST be a harmless no-op.

### Ownership lease *(deferred — not implemented)*

Named because the store is built fence-capable and its fencing path is tested. No
deployment enables it; durable multi-pod is unsupported until it lands (FR-022a).
