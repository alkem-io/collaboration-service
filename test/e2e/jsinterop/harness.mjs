// JS-client interop harness for the Go collaboration-service.
//
// This is the real proof the Go server speaks canonical y-protocols: it uses the
// ACTUAL yjs + y-protocols (sync + awareness) npm packages — the same libraries
// the Alkemio client-web/whiteboard clients use — over a raw `ws` WebSocket, with
// NO custom framing. The wire format is exactly y-protocols' [messageType
// varUint][payload], one message per binary frame, which is what y-websocket
// itself speaks. If the Go server's framing diverged from canonical y-protocols
// in ANY way, this handshake would not converge — that is the signal T017 is
// chartered to surface.
//
// Protocol (mirrors y-websocket's client):
//   - messageType 0 (sync):     y-protocols/sync  readSyncMessage / writeSyncStep1 / writeUpdate
//   - messageType 1 (awareness):y-protocols/awareness encode/applyAwarenessUpdate
//   - messageType 3 (control):  the server's custom JSON control channel; observed, not required
//
// On connect the client sends SyncStep1; the server replies SyncStep2 (+ its own
// SyncStep1 and an awareness snapshot). The doc 'update' observer streams local
// edits to the server as sync Update messages. This is byte-for-byte the
// y-websocket provider's exchange, implemented inline so the test owns the timing.
//
// Modes:
//   --mode edit     insert MARKER text into the memo + broadcast awareness, then
//                   wait until the doc also contains EXPECT (the peer's text),
//                   proving bidirectional convergence; emits a JSON result line.
//   --mode observe  wait until the doc contains EXPECT (the peer's text) and a
//                   peer awareness state is seen; emits a JSON result line.
//
// Output: a single line `RESULT <json>` on success-or-failure, plus a non-zero
// exit code on failure, so the Go test can parse and assert.

import WebSocket from 'ws'
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync'
import * as awarenessProtocol from 'y-protocols/awareness'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

// Wire message types (contracts/ws-protocol.md / y-protocols).
const MESSAGE_SYNC = 0
const MESSAGE_AWARENESS = 1
const MESSAGE_CONTROL = 3

function parseArgs(argv) {
  const args = {}
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a.startsWith('--')) {
      args[a.slice(2)] = argv[i + 1]
      i++
    }
  }
  return args
}

const args = parseArgs(process.argv.slice(2))
const url = args.url
const mode = args.mode || 'observe'
const marker = args.marker || '' // text this client inserts (edit mode)
const expect = args.expect || '' // text this client waits to see (peer's marker)
const timeoutMs = parseInt(args.timeout || '15000', 10)

if (!url) {
  console.error('missing --url')
  process.exit(2)
}

function memoText(doc) {
  // Memo convention: a Y.XmlFragment rooted at "default" (T010). Its toString()
  // is the canonical serialization the Go server's GetXmlFragment("default")
  // mirrors.
  return doc.getXmlFragment('default').toString()
}

function insertMemo(doc, text) {
  const frag = doc.getXmlFragment('default')
  const xt = new Y.XmlText()
  xt.insert(0, text)
  frag.push([xt])
}

const doc = new Y.Doc()
const awareness = new awarenessProtocol.Awareness(doc)

const ws = new WebSocket(url, { perMessageDeflate: false })
ws.binaryType = 'arraybuffer'

let synced = false
let peerAwarenessSeen = false
let finished = false

function send(bytes) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(bytes)
  }
}

function sendSyncStep1() {
  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, MESSAGE_SYNC)
  syncProtocol.writeSyncStep1(enc, doc)
  send(encoding.toUint8Array(enc))
}

function broadcastAwareness() {
  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, MESSAGE_AWARENESS)
  encoding.writeVarUint8Array(
    enc,
    awarenessProtocol.encodeAwarenessUpdate(awareness, [doc.clientID])
  )
  send(encoding.toUint8Array(enc))
}

// Stream local document edits to the server as canonical sync Update messages.
doc.on('update', (update, origin) => {
  // Skip echoing updates we applied FROM the server (origin === ws) back to it.
  if (origin === ws) return
  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, MESSAGE_SYNC)
  syncProtocol.writeUpdate(enc, update)
  send(encoding.toUint8Array(enc))
})

// Broadcast awareness whenever the local state changes (cursor/presence).
awareness.on('update', ({ added, updated, removed }, origin) => {
  if (origin === ws) {
    // A peer (server-fanned) awareness change: note that we saw a remote client.
    const others = awareness.getStates().size > 1
    if (others) peerAwarenessSeen = true
    return
  }
})

ws.on('open', () => {
  sendSyncStep1()
})

// decodeErrors records any frame that failed canonical y-protocols decoding.
// A canonical client decodes EVERY frame the server sends; a single failure is a
// y-protocols framing mismatch — the highest-value signal this harness exists to
// catch — so it fails the run immediately (and is echoed in the result for
// triage).
const decodeErrors = []

ws.on('message', (data) => {
  try {
    handleMessage(data)
  } catch (err) {
    const bytes = new Uint8Array(data)
    decodeErrors.push({
      error: String(err),
      frameType: bytes.length > 0 ? bytes[0] : -1,
      frameLen: bytes.length,
      frameHex: Buffer.from(bytes).toString('hex').slice(0, 120)
    })
    result(false, { reason: 'decode-error' })
  }
})

function handleMessage(data) {
  const bytes = new Uint8Array(data)
  const dec = decoding.createDecoder(bytes)
  const messageType = decoding.readVarUint(dec)
  switch (messageType) {
    case MESSAGE_SYNC: {
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MESSAGE_SYNC)
      // readSyncMessage drives the canonical state machine: a SyncStep1 from the
      // server yields a SyncStep2 reply; SyncStep2 / Update are applied to the
      // doc with `ws` as the origin so our update observer does not echo them.
      const replyType = syncProtocol.readSyncMessage(dec, enc, doc, ws)
      if (encoding.length(enc) > 1) {
        send(encoding.toUint8Array(enc))
      }
      // The server's first SyncStep2 means our initial state has been received.
      if (replyType === syncProtocol.messageYjsSyncStep2 && !synced) {
        synced = true
        onSynced()
      }
      break
    }
    case MESSAGE_AWARENESS: {
      // Canonical y-websocket/y-protocols reads the awareness payload as a
      // length-prefixed array (writeVarUint8Array on the wire). The temporary
      // DIAG below records whether that canonical read succeeds, to isolate the
      // awareness-framing defect from sync convergence.
      const payload = decoding.readVarUint8Array(dec)
      awarenessProtocol.applyAwarenessUpdate(awareness, payload, ws)
      if (awareness.getStates().size > 1) peerAwarenessSeen = true
      break
    }
    case MESSAGE_CONTROL:
      // Server->client JSON control (saved / room-user-change / ...). Not needed
      // for the interop proof; ignored.
      break
    default:
      // Unknown type: y-protocols leniency — ignore.
      break
  }
}

function onSynced() {
  if (mode === 'edit') {
    if (marker) insertMemo(doc, marker)
    awareness.setLocalState({ user: { name: 'js-' + mode }, marker })
    broadcastAwareness()
  } else {
    // Observer announces presence too, so the editor can confirm peer awareness.
    awareness.setLocalState({ user: { name: 'js-observer' } })
    broadcastAwareness()
  }
}

function result(ok, extra) {
  if (finished) return
  finished = true
  const text = memoText(doc)
  const out = {
    ok,
    mode,
    synced,
    peerAwarenessSeen,
    text,
    states: awareness.getStates().size,
    decodeErrors,
    ...extra
  }
  console.log('RESULT ' + JSON.stringify(out))
  try {
    ws.close()
  } catch {
    /* noop */
  }
  process.exit(ok ? 0 : 1)
}

// Success condition: we are synced, we observed at least one peer awareness
// state, and the doc converged to contain the expected peer marker text.
const poll = setInterval(() => {
  if (!synced) return
  const text = memoText(doc)
  const textOk = expect === '' || text.includes(expect)
  if (textOk && peerAwarenessSeen) {
    clearInterval(poll)
    result(true, {})
  }
}, 50)

setTimeout(() => {
  clearInterval(poll)
  const text = memoText(doc)
  const textOk = expect === '' || text.includes(expect)
  result(textOk && peerAwarenessSeen, {
    reason: 'timeout',
    textOk,
    expect
  })
}, timeoutMs)

ws.on('error', (err) => {
  result(false, { reason: 'ws-error', error: String(err) })
})

ws.on('close', () => {
  if (!finished) result(false, { reason: 'ws-closed-early' })
})
