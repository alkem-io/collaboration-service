// Exercise deletion on an actual image through the owner-driven broker path.
import assert from 'node:assert/strict'
import readline from 'node:readline'
import WebSocket from 'ws'
import * as Y from 'yjs'
import * as sync from 'y-protocols/sync'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

const url = process.argv[2]
const doc = new Y.Doc()
const ws = new WebSocket(url, { perMessageDeflate: false })
const timeout = setTimeout(() => { throw new Error('lifecycle smoke timed out') }, 20000)
let synced = false
let ended = false
ws.on('error', err => { throw err })
ws.on('open', () => {
 const enc = encoding.createEncoder()
 encoding.writeVarUint(enc, 0)
 sync.writeSyncStep1(enc, doc)
 ws.send(encoding.toUint8Array(enc))
})
ws.on('message', data => {
 const dec = decoding.createDecoder(new Uint8Array(data))
 const type = decoding.readVarUint(dec)
 if (type === 0) {
  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, 0)
  const reply = sync.readSyncMessage(dec, enc, doc, ws)
  if (encoding.length(enc) > 1) ws.send(encoding.toUint8Array(enc))
  if (reply === sync.messageYjsSyncStep2 && !synced) { synced = true; console.log('READY') }
 } else if (type === 3) {
  const control = JSON.parse(decoding.readVarString(dec))
  if (control.type === 'session-end' || control.kind === 'session-end') {
   assert.equal(control.code, 'document-deleted')
   assert.equal(control.disposition, 'terminal')
   ended = true
  }
 }
})
ws.on('close', () => {
 assert.ok(synced, 'session must join before deletion')
 assert.ok(ended, 'document-deleted control must precede close')
 const reconnect = new WebSocket(url, { perMessageDeflate: false })
 reconnect.on('error', err => { throw err })
 reconnect.on('close', code => {
  assert.equal(code, 1008, 'deleted document must refuse reconnect')
  clearTimeout(timeout)
  doc.destroy()
  console.log('RESULT '+JSON.stringify({ok:true,deleted:true,reconnectDenied:true}))
  process.exit(0)
 })
})
// Keep stdin open so the controlling fixture can await READY before deleting.
readline.createInterface({input:process.stdin})
