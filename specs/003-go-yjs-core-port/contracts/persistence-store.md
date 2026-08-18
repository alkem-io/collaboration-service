# Contract: `persistence.Store` (content plane)

**Implemented by**: this service. The core ships **no** persistence implementation.
**Backends**: file-service (real); in-process (test/dev fixture).
**Conformance gate**: `conformance.Persistence` + `conformance.PersistenceFencing`.
`conformance.PersistenceCompaction` does **not** apply — see Shape below (FR-008b).

## Shape adopted

`Store` = `Appender` + `Loader` + `FenceMode`. **`Compactor` is deliberately not
implemented.** This is a permitted point in the contract's design space, not a
shortcut: `Compactor` is optional and a `Loader` may return a checkpoint covering all
history.

## Obligations

### Append

- One call per flush window, carrying the **whole encoded document**.
- Returning nil means the bytes **are durable**. Returning nil before that is
  non-conforming (FR-007a) — this is why batching lives above the store, not inside it.
- The returned revision belongs to this document and is greater than every previously
  acknowledged revision.
- The update slice is **borrowed only until `Append` returns**. An implementation that
  retains it or writes asynchronously MUST copy first.
- `Fence` zero is the ordinary non-clustered mode.

### Load

- Returns one `Checkpoint`, no trailing records, `Next` **empty**.
- An empty `Next` is the *only* valid signal of completeness. Returning partial history
  with an empty `Next` is a contract violation and MUST be surfaced as an error
  (FR-014), never accepted as a document.
- Returned byte slices are caller-owned.

### FenceMode

- Fixed at construction, never inferred per write.
- The implementation MUST be constructible in **both** modes, and the fenced path MUST
  pass its conformance suite in CI even though deployments run unfenced (FR-008a).

## Prohibited

- **Delegating to a superseded port.** A `Store` whose body calls the old
  blob/pointer ports is the shim FR-007 forbids. `BlobStore` is deleted, not wrapped.
- **Misreporting durability.** See Append.
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
justified against the configured document-size limit (FR-010a). At the current 32 MiB
limit and a 500ms interval, one document alone is ~64 MiB/s.
