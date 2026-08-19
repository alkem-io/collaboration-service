# The in-process path serves all three roles (T064, §III)

Constitution v3.0.0 withdrew the standalone *product* promise but kept the
in-process path for three specific purposes. This is the evidence each still
works after the port, verified 2026-08-19 against the committed tree.

## 1. The test suite

The in-process `persistence.CheckpointStore` and the core's `hub.NewInProcess()`
back the entire unit lane: 18 packages, `go test -race ./...` green.

It matters that the in-process store MIRRORS the file-service store's shape — one
blob per document, replaced on save, state vector derived on read — rather than
being a convenient in-memory log. A fixture with a different shape from production
would mean every test exercising a persistence model the deployed service does not
use, and the checkpoint-vs-log distinction is exactly where this port found its
first contract gap.

## 2. Local development with real editors

Verified end to end, not by inspection:

```
$ env -i PATH=... PORT=4106 ./collab        # no DB, bus, blob store, or auth service
{"msg":"collaboration core wired","fanout":"inmemory","metadata_store":"inmemory",
 "blob_store":"inline","auth_mode":"open","authz_mode":"open"}
```

Two REAL yjs clients (the `y-protocols` interop harness, not a Go stand-in) then
joined the same document over `ws://localhost:4106/collab/...`:

```
edit     {"ok":true,"synced":true,"peerAwarenessSeen":true,"text":"SMOKE-OK","decodeErrors":[]}
observe  {"ok":true,"synced":true,"peerAwarenessSeen":true,"text":"SMOKE-OK","decodeErrors":[]}
```

Both completed the y-protocols handshake, converged on the document, saw each
other's awareness, and reported **zero decode errors** — so the framing a browser
would receive is canonical, from a binary with nothing else running.

## 3. The zero-dependency smoke test

Same boot as above with an EMPTY environment (`env -i`), only `PORT` set:

- `GET /healthz` → `{"status":"ok"}`
- `GET /metrics` → the Prometheus surface, `collaboration_*` series present

No database connection is opened, no bus is dialled, no blob store is contacted.

## What this path is NOT

It is not a deployment option and must never be presented as one. The in-process
store carries **no durability guarantee across a restart** — its package
documentation says so, and `CHECKPOINT_STORE=inline` is documented in `.env.example` as
the non-durable test/development value. The startup log naming
`blob_store=inline` is the operator-visible signal that a process is running on it.
