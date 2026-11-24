# Data model: Saved collaboration derivatives

No database, migrations or new source document fields. These are views of existing metadata/checkpoints, file-service output rows and bounded in-memory work.

## Saved source

| Field | Meaning and validation |
|---|---|
| Document ID and declared type | Existing metadata lookup; the stored type, not a URL parameter or a root guess, selects a memo or whiteboard. |
| Content pointer and encoding | Existing checkpoint locator and declared core codec. Stable pointer does not imply unchanged bytes. Require the saved source to exist; invalid or unreadable data is not empty. |
| Saved fingerprint | Digest of the complete checkpoint bytes, including deletes. Identifies the successful saved state eligible to publish; a Yjs state vector or process-local counter is not sufficient. |
| Source bucket | Trusted existing metadata, required for asset and output ownership. A missing bucket or deleted source prevents publication. |

Each generator decodes into an isolated, short-lived document. The whiteboard view reads `elements`, `files`, and saved `appState`; exclude deleted elements, preserve the binding's stacking order and fit scene bounds, not the viewport. Use only referenced, verified source-owned images. The memo view reads ordered `default` XML nodes/attributes and formatted text runs. Destroy both temporary projections after use. Neither generator creates a room or writes a checkpoint.

## Document outputs and refresh

- **Output association:** `(document ID, variant)` → trusted source bucket, reserved external reference, stable file ID. A whiteboard has separate preview and card PNGs, each showing the entire board. A memo has one Markdown file. Each slot is distinct from the checkpoint and existing uploaded assets. The mapping is the unique scoped reference in file-service; collab creates missing rows.
- **Publication candidate:** document ID, declared type and fingerprint of the latest changed, successfully saved checkpoint in that room, plus an in-process receipt for full publication. The queue retains no live document, content buffer or editing actor.
- **Transitions:** first changed save opens a short window; later saves update its candidate without extending it. Window expiry or a clean normal release submits the latest unpublished revision to the bounded queue. The worker generates from a current saved candidate and creates absent private rows or replaces existing bytes. It marks completion only when all output variants publish. No change, owner invalidation (including draft expiry cleanup) and failed final save do not create new work. Failed/superseded work cannot assert a newer result. One pending slot per document and one running job process-wide suffice.
- **Empty:** a valid saved clear publishes blank images or visually empty Markdown. File-service rejects zero-byte replacements, so the persisted memo representation retains one invisible Markdown reference definition. Conversion and asset failures are never treated as a valid clear.
- **Publication:** use the saved fingerprint for correctness and diagnostics. Readers may see the previous successful file until replacement; one variant's replaced bytes cannot assert the other variant succeeded. Per-file replacement is the existing file-service transaction, not an atomic pair. Owner bucket deletion removes the derived files with the source. No new history, retention job or live browser access is implied.

## Memo Markdown result

One transient UTF-8 body for the saved memo checkpoint at the time of the read. A valid empty memo returns a successful zero-length body, with no storage prefix. An unknown/wrong-type ID, unreadable snapshot, invalid tree, or dependency error is not represented by an empty success. The read creates no artifact/cache and needs no actor identity or application authorization record.
