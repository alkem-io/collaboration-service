# Implementation status — `003-go-yjs-core-port`

**As of**: 2026-08-18, end of the autonomous implementation session
**Branch**: `feat/003-go-yjs-core-port` · **Tasks marked done**: 23 / 72
**Gates at every commit**: `go build`, `go vet`, `go vet -tags integration`,
`golangci-lint` **0 issues**, `go test -race ./...` green (20 packages)

Everything below is committed and signed. Nothing is pushed.

---

## What is actually working

**The core swap is complete and the service runs on `go-yjs` v0.0.1.** `y-crdt` is
gone from the code and from the module graph. The full test suite — including the
`002` invariant suite, the e2e harness, and JS-interop — passes.

| Area | State |
|---|---|
| CRDT core | ✅ `go-yjs` v0.0.1 pinned; `y-crdt` and its fork `replace` removed |
| Transport dispatch | ✅ rebuilt on `protocol.InspectMessage` + `EncodeSyncStep2` |
| Awareness framing | ✅ workaround deleted; delegates to the core |
| Document registry | ✅ `memory.Registry` owns identity, acquisition, eviction, invalidation |
| Teardown-flush matrix | ✅ implemented **and** ratcheted (both halves RED-on-revert) |
| `persistence.Store` | ⚠️ **in-process only**; file-service implementation NOT started |
| Flush batching / escalation | ❌ not started |
| `hub.Hub` (Redis) | ❌ not started |
| Conformance | ✅ persistence ×3, memory, hub — 18 subtests, verified executing |

---

## Two corrections to the spec, made during implementation

### 1. Checkpoint-only cannot conform (research.md **D1a**) — a decision was overturned

Clarification **Q1** chose a checkpoint-only `Store`, and **FR-008b** excused the
compaction suite on the grounds that `Compactor` would not be implemented.

That premise is wrong. `conformance.Persistence` appends **opaque byte records**
(`"first"`, `"second"`) and requires them back verbatim and in order, through a
paginated view whose `Through` is fixed by the first page. There is nothing to
merge into a covering checkpoint when records are not CRDT updates.

**Resolved as**: a genuine append log **with** compaction (`CompactingStore`),
used the way Q1 intended — one whole-document update per flush, compacted into
the checkpoint immediately, so steady state is still one checkpoint and one read
on load. FR-012 is now satisfied *by compaction*, which is stronger than Q1's "no
history to replay".

**This is the one thing worth a human review**, because it changes a decision
that was explicitly taken.

### 2. `Invalidate` blocks until handles release

Not in the spec anywhere, found by a test that hung for nine minutes. The wait is
bounded **only by the caller's context**. Any future caller — notably the FR-013
escalation path — must pass a bounded context, or one wedged holder blocks
recovery indefinitely. Poisoning happens *before* the wait, so a non-cooperative
holder can delay destruction but cannot prevent invalidation.

---

## The one architectural fork I deliberately did NOT decide

**How does a file-service `persistence.Store` resolve `DocumentID` → bytes?**

`Store.Load(ctx, DocumentID, opts)` must find its own bytes. file-service assigns
its own blob id, so there is no deterministic addressing from a document id — the
pointer has to be persisted somewhere, and the only durable place is the metadata
index. That forces the file-service `Store` to compose **file-service (bytes) +
`MetadataStore` (pointer + revision)**.

It is a *forced* conclusion rather than a free choice, but it is still a real
architectural decision that:

- makes the `Store` depend on the index, and
- rewrites `persist()` / `loadSnapshot()` — exactly the code paths `002` hardened
  (delete-after-commit, no stranded pointer, bounded backend calls).

I stopped here rather than commit that shape unreviewed at the end of a long
autonomous run. Doing it badly would be worse than not doing it, and it is the
last decision in the feature that a human should see before it sets.

---

## Recommended next steps, in order

1. **Confirm or reject D1a.** Everything downstream in persistence assumes it.
2. **Decide the file-service `Store` composition** (the fork above), then
   T025/T026.
3. **Then** T028–T035: flush batching above the store, the durability state
   machine, escalation, observability, and deleting `BlobStore`.
4. **Then** T050–T058 (Redis hub) — the shipped in-process hub is already
   conformance-gated, so the Redis one inherits the same suite.
5. **Leave T060–T062 (config-key rename) for last and do it in one atomic
   change** across this repo, the manifests on `feat/003-migration`, and
   `server`'s 006 quickstart. FR-022e calls a partial rollout a data-loss risk,
   and it is: the renamed keys have silent defaults, so a stale key would send
   blobs to memory while the service reported healthy.

## Not done, and why

- **T036/T037** (restructure `002` invariants + ledger): the `002` invariant
  suite passed **unchanged** throughout, so there was nothing to restructure
  there. Three older `002`-era tests *did* need restructuring (awareness framing,
  `doc.Share` assertions, awareness seeding) and were restructured without
  weakening — see the core-swap commit. New ratchets added this session were each
  proven non-vacuous.
- **T004** (record the pinned version in research.md sequencing): superseded —
  the version is pinned in `go.mod` and D1a records the substantive finding.
