# Whiteboard migration cross-language seam

This directory is the **one cross-language seam** of the WS-E one-time migration
(`cmd/migrate`). Memos migrate entirely in Go (the vendored y-crdt core decodes
the legacy Yjs update and re-encodes a v2 snapshot). **Whiteboards cannot**: the
Excalidraw-JSON → id-keyed `Y.Map` transform is owned by the TypeScript binding
[`@alkemio/excalidraw-yjs-binding`](https://www.npmjs.com/package/@alkemio/excalidraw-yjs-binding)
(`populateYDoc`) — the *same* binding the client uses, so a migrated scene is
guaranteed structurally identical to what a live editor produces.

`whiteboard-to-ydoc.mjs` is the thin Node step the Go tool shells out to, once per
legacy whiteboard:

```
stdin  : legacy Excalidraw scene JSON  { elements, appState?, files? }
stdout : "BASE64 <b64>"   where <b64> = base64(Y.encodeStateAsUpdate(doc))   # Yjs v1
stderr : diagnostics; non-zero exit ⇒ the Go driver FLAGS this document (never drops)
```

The binding emits **Yjs v1** (it depends on `yjs ^13.6`). Go decodes that v1
update and re-encodes the canonical **v2** snapshot, so whiteboards land on the
exact same persistence path (v2 snapshot → `BlobStore.Put` → `MetadataStore.Save`)
as memos.

This mirrors the repo's existing Go→Node y-protocols interop precedent
(`test/e2e/jsinterop`).

## Decision: why shell out, not re-implement in Go

The migration could (a) shell out to this Node step or (b) be a Node-side
whiteboard tool. **Option (a) is implemented.** Rationale:

- The driver, batch loop, idempotency, resumability, dry-run, validation, and the
  persistence ports already live in Go (`internal/migrate`). Re-doing them in TS
  (option b) duplicates non-trivial logic across two languages — the constitution
  forbids duplicated logic.
- Re-implementing `populateYDoc` in Go (per-property element maps, fractional-index
  repair, `boundElements` sub-maps, JSON-leaf handling) would fork a transform that
  must stay byte-identical to the client's. Shelling out to the published binding
  guarantees it never drifts.
- The seam is confined to this one small script behind the `migrate.NodeRunner`
  interface, so it is swappable (e.g. a future WASM build of the binding) and
  unit-tested in Go without Node (a fake runner) plus exec-tested with Node.

## STUB / PENDING — install the binding

> **This step is the single thing stubbed pending the published binding.** Until
> it is installed, the Go driver flags every whiteboard with
> `ErrWhiteboardSeamUnavailable` (and counts them in the report) rather than
> producing a wrong snapshot. The Go side is fully built, tested, and dry-runnable
> today regardless.

`@alkemio/excalidraw-yjs-binding@0.18.0` is **published** to npm
(`publishConfig.access: public`), BUT as of this build a plain
`npm install @alkemio/excalidraw-yjs-binding` **fails**: its transitive workspace
dependency `@excalidraw/element@0.18.0` is not independently published, and the
built artifact (`dist/prod/index.js`) is **not bundled** — it `import`s the sibling
`@excalidraw/*` workspace packages at runtime. So the binding currently resolves
only inside a built `excalidraw-fork` workspace.

**Two install paths, in order of preference:**

1. **Once the binding ships self-contained** (bundled, or its `@excalidraw/*`
   deps published) — the intended end state:
   ```bash
   cd scripts/migrate && npm ci          # uses package.json here
   ```

2. **Today (build-ahead): from the local excalidraw-fork workspace build:**
   ```bash
   # Build the whole fork workspace so the binding's sibling deps resolve.
   cd ../../excalidraw-fork && yarn && yarn build
   # Point Node's module resolution at that workspace when running the step
   # (e.g. run the step from within the fork, or npm-link the built packages).
   ```
   The exact link recipe depends on the fork's package manager; the invariant the
   Go tool relies on is only the stdin/stdout contract above.

## Running

The Go tool drives this; you rarely call it directly. To enable whiteboards, pass
the script path to `cmd/migrate`:

```bash
go run ./cmd/migrate --source legacy.jsonl --dry-run \
  --wb-script "$(pwd)/scripts/migrate/whiteboard-to-ydoc.mjs"
```

Without `--wb-script`, whiteboards are flagged and memos still migrate.

Smoke-test the script alone:

```bash
echo '{"elements":[{"id":"a","type":"rectangle"}],"appState":{},"files":{}}' \
  | node whiteboard-to-ydoc.mjs
# → BASE64 <b64>
```
