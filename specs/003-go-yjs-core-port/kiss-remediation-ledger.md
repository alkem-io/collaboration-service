# KISS remediation ledger — MOVED

The ledger is **canonical in the workspace repo**, not here:

> **[alkem-io/agents-hq](https://github.com/alkem-io/agents-hq)** →
> `specs/006-collab-content-unification/kiss-remediation-ledger.md`
>
> (sibling checkout: `../agents-hq/specs/006-collab-content-unification/kiss-remediation-ledger.md`)

This file previously carried a full duplicate of the rows. It is a pointer now
because the duplicate had diverged: it recorded BASIC-006, KISS-010 and KISS-021
as IMPLEMENTED while the canonical ledger had all three OPEN, and it still showed
E2E-006 as PENDING after the canonical copy had recorded a proven failure. Two
readable copies of one register is the second-source-of-truth problem this very
slice removed `MetadataStore.Delete` to avoid, so the rows are deleted rather
than annotated — a superseded copy that still lists 63 rows is exactly what a
later reader mistakes for current.

Cite rows from this repo as **repo → path → row id**, e.g.
`alkem-io/agents-hq → specs/006-collab-content-unification/kiss-remediation-ledger.md → BASIC-004`.
The row ids and that path are both stable by agreement with the ledger's owner:
ids are anchors and only their status text changes beneath them.

Do **not** substitute an `FR-` id for a row id when citing from this repo. The
`FR-` namespace is ambiguous here — `specs/001`, `specs/002` and `specs/003` each
define `FR-001`..`FR-014` with unrelated meanings, so a bare `FR-` id resolves
confidently to whichever generation the reader opens first.
