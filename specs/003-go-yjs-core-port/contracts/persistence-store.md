# Contract: `persistence.CheckpointStore` (content plane)

**Implemented by**: this service. The core ships **no** persistence implementation.
**Backends**: file-service (real); in-process (test/dev fixture).
**Conformance gate**: `conformance.CheckpointPersistence`, plus
`conformance.CheckpointPersistenceDeletion` where the store owns everything that makes
its documents findable — see `conformance-coverage.md` for the suites run and the
reason for each one not run.

## Shape adopted

`CheckpointStore` — one current state per document, replaced on every save. The log
profile (`Store` = `Appender` + `Loader`) is deliberately NOT implemented: the medium's
durable unit is a document-sized blob rewritten in place, and framing a log inside that
blob would break the format other systems read. There is no `Compactor`, no pagination,
and no per-record history; every load is the whole document.

## Obligations

### SaveCheckpoint

- Carries the document's **complete** state. A store replaces rather than merges, so a
  save covering less than a previous one discards the difference permanently and
  silently.
- Returning nil means the state crossed the implementation's durability boundary.
  Reporting durability the backend has not given is the one failure this contract
  cannot detect for the caller.
- The update and state-vector slices are **borrowed only for the call**. An
  implementation that retains or asynchronously writes either MUST copy first.
- **Encoding is required and never inferred.** `EncodingUnspecified` is rejected with
  `ErrEncodingRequired`; a codec the store cannot record is rejected with
  `ErrUnsupportedEncoding` rather than decoded anyway. The wrong decoder does not fail
  — it returns an EMPTY state vector with a nil error — so guessing produces a
  confident wrong answer.

### LoadCheckpoint

- Returns the whole document in one read, with the encoding it was saved under.
- `ErrNotFound` means nothing is stored — the caller initialises an empty document.
- `ErrCorrupt` means state is referenced but cannot be produced: a pointer whose target
  is gone. It MUST NOT be reported as `ErrNotFound`, or the caller initialises a
  document that HAS content as new and the next save overwrites the last good state.

### FenceMode

- Both backends report `Unfenced` and MUST reject a non-zero `Fence` with
  `ErrUnexpectedFence` **before** mutating or erasing anything, rather than accepting an
  epoch they cannot honour (FR-008a).

### ContentPointer ownership

- Where a store addresses content by pointer, that store is the pointer's **sole
  writer**. No other component may send it on a metadata write; a metadata save that
  omits it MUST leave it unchanged.

## Prohibited

- **Delegating to a superseded port.** A store whose body calls a previous
  blob/pointer port is the shim FR-007 forbids: superseded ports are deleted, not
  wrapped.
- **Misreporting durability.** See SaveCheckpoint.
- **Modelling blob history.** The contract with the blob backend is exactly *store
  blob, read blob*. Retention, expiry, and reclamation are the backend's business; this
  service MUST NOT track, expose, or reason about them.

## Failure behaviour

| Condition | Required response |
|---|---|
| transient write failure | keep serving, stay dirty, retry with backoff, tell collaborators edits are not yet durable |
| consecutive failures past the configured threshold | invalidate, signal holders, disconnect with a reason meaning *recent edits could not be saved*; count and log the discarded edits and the undurable duration |
| known divergence from durable state | invalidate immediately, without waiting for the threshold |
| backend unreachable during escalation | still tear down — never leave a session serving unbacked state |

**No secondary storage fallback** (FR-029). Escalation fires precisely because the store
is unreachable; a fallback would reintroduce the adapters being deleted.

## Write-volume envelope

Each flush rewrites the whole document, so sustained volume ≈
`document size ÷ flush interval × actively-edited documents`. This relationship MUST be
documented wherever the interval is configured, and the shipped default MUST be
justified against the configured document-size limit (FR-010a). At the configured document-size
limit and a 500ms interval, one document alone is ~64 MiB/s.
