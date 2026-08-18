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
| file-service adapter | ✅ **defect fixed** (`fb32d05`): rewrites in place, stable pointer |
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

## The remaining persistence decision — much smaller than first described

> **Superseded.** An earlier version of this section claimed the file-service
> `Store` was blocked on a significant architectural fork. That framing rested on
> a misreading of the file-service contract, corrected in `fb32d05`.

**What was wrong**: I read "file-service assigns the id on create" as meaning a
new id per save, and concluded the `Store` would need per-save pointer
bookkeeping through `MetadataStore`.

**What is actually true**: a file id is a **stable identifier — a filename, not a
version**. `PUT /internal/file/{id}/content` ("store-and-link") rewrites the
content behind the same id; file-service swaps the underlying blob and its
content-hash `externalID`, which is its own business. The adapter was creating a
new file per save, which was a **defect**, now fixed with regression tests
(`fb32d05`).

**What that leaves**, and it is modest:

- `Load(DocumentID)` → resolve the document's stable pointer **once** → one GET.
- `Append` → one PUT. No pointer churn, no predecessor, no delete-after-commit.
- The only residual coupling is the `DocumentID → file id` mapping, which
  file-service cannot supply (`CreateDocumentInput` has no ID field, so the id
  cannot be caller-chosen). It is written **once** at first save and read once at
  load — not per flush.

So the `Store` still reads the index, but as a one-time resolution rather than a
per-save write path. That is an implementation detail, not an architecture fork,
and it no longer needs adjudicating before work continues.

**Optional simplification, still open**: expanding file-service to accept a
caller-supplied id is pre-authorized by the constitution ("expanding it is
pre-authorized if the store needs a capability it does not yet expose"). If the
snapshot's file id could simply BE the collaboration document id, the `Store`
becomes fully self-sufficient with no index read at all. Worth doing only if
file-service is being touched anyway — `006` already modifies it.

**Two file-service behaviours any implementation must respect** (both now covered
by tests in the adapter):

- **409 is content dedup**, not a transient fault:
  `unique(externalID, storageBucketID)` refuses bytes already stored under
  another file in the same bucket. It must not be retried as if it were transient.
- **Only a 404 may fall back to creating.** A 500 on rewrite must surface —
  creating a second file forks the document, leaving the row on the old pointer
  while new content lands somewhere unread.

**Also changed**: `MAX_DOC_BYTES` default 32 MiB → **30 MiB**. file-service caps
the rewrite body at exactly 32 MiB, so a document on a 32 MiB budget encodes past
it once v2 framing is added — it would clear our own budget check and then be
refused by the transport, leaving the document accepted and permanently
unsaveable.

## Recommended next steps, in order

1. **Confirm or reject D1a.** Everything downstream in persistence assumes it.
   This is now the ONLY item needing a human decision before implementation.
2. **T025/T026** — the file-service `Store`, on the corrected stable-pointer
   model: resolve the pointer once, `Append` = one PUT, `Load` = one GET.
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
