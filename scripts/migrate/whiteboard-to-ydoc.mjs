// whiteboard-to-ydoc.mjs — the cross-language seam of the WS-E migration.
//
// The Go migration tool (cmd/migrate) shells out to THIS script once per legacy
// whiteboard. It is the single place the Excalidraw-JSON → id-keyed Y.Map
// transform runs, and it runs the SAME published binding the client uses
// (@alkemio/excalidraw-yjs-binding's populateYDoc), so a migrated scene is
// structurally identical to what a live editor produces — no Go re-implementation
// to drift.
//
//   stdin :  the legacy Excalidraw scene JSON  ({ elements, appState?, files? })
//   stdout:  "BASE64 <b64>"  where <b64> = base64(Y.encodeStateAsUpdate(doc))
//            — a Yjs *v1* update (the binding depends on yjs ^13.6, which emits
//            v1). The Go side decodes this v1 update and re-encodes the canonical
//            v2 snapshot, so the whiteboard path lands on the same persistence
//            format as memos.
//   stderr:  diagnostics; a non-zero exit aborts conversion for THIS document,
//            which the Go driver records as a flag (never a drop).
//
// ── STUB / PENDING (build-ahead) ─────────────────────────────────────────────
// This script needs `@alkemio/excalidraw-yjs-binding` + `yjs` installed (see
// package.json in this dir + the runbook). The binding's package.json declares it
// public (name @alkemio/excalidraw-yjs-binding, version 0.18.0,
// publishConfig.access=public) but may not yet be PUBLISHED to the registry. Until
// it is, install it from a local build of the excalidraw-fork workspace:
//     (cd ../../excalidraw-fork && yarn && yarn workspace @alkemio/excalidraw-yjs-binding build)
//     npm install ../../excalidraw-fork/packages/yjs-binding
// The import below is the ONLY line that depends on the published package; if it
// is missing, `npm install` here fails fast and the Go driver flags every
// whiteboard with ErrWhiteboardSeamUnavailable rather than producing a wrong
// snapshot. (No lockfile is committed for this one-shot tool — the binding is
// pinned to an exact version in package.json instead.)

import process from 'node:process'
import * as Y from 'yjs'
import { populateYDoc } from '@alkemio/excalidraw-yjs-binding'

function readStdin() {
  return new Promise((resolve, reject) => {
    const chunks = []
    process.stdin.on('data', (c) => chunks.push(c))
    process.stdin.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    process.stdin.on('error', reject)
  })
}

async function main() {
  const raw = await readStdin()
  if (!raw.trim()) {
    process.stderr.write('empty stdin: no Excalidraw JSON to convert\n')
    process.exit(3)
  }

  let scene
  try {
    scene = JSON.parse(raw)
  } catch (e) {
    process.stderr.write(`invalid Excalidraw JSON: ${e.message}\n`)
    process.exit(4)
  }

  // The binding expects an object { elements: [...], appState?, files? }. Reject a
  // JSON value that is not a non-null object (e.g. null, a number, or a bare
  // array) with a clear validation error rather than letting `scene.elements`
  // throw a TypeError reported as an "unexpected error".
  if (scene === null || typeof scene !== 'object' || Array.isArray(scene)) {
    process.stderr.write('invalid Excalidraw scene: expected a JSON object\n')
    process.exit(4)
  }
  // A legacy scene that lacks `elements` is normalized to an empty scene (which
  // yields an empty doc; the Go side treats an empty result as nothing-to-migrate).
  if (!Array.isArray(scene.elements)) {
    scene.elements = []
  }

  const doc = new Y.Doc()
  try {
    populateYDoc(scene, doc)
  } catch (e) {
    process.stderr.write(`populateYDoc failed: ${e.stack || e.message}\n`)
    process.exit(5)
  }

  // v1 update bytes (the binding's encoding); Go re-encodes v2.
  const update = Y.encodeStateAsUpdate(doc)
  process.stdout.write('BASE64 ' + Buffer.from(update).toString('base64') + '\n')
}

main().catch((e) => {
  process.stderr.write(`unexpected error: ${e.stack || e.message}\n`)
  process.exit(1)
})
