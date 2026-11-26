# Collaboration derivative contracts

These are stage-one contracts. They change no existing client, GraphQL, file-service or WOPI endpoint. See the [plan](../plan.md) for boundaries and [research](../research.md) for the pinned sources.

## Memo: `GET /internal/memos/{memoId}/markdown`

- Single existing memo UUID; no request body, actor, bearer token, session or application privilege check. The listener starts on every boot, separate from the public HTTP/WebSocket port; it defaults to loopback. No public proxy or frontend API may forward it. A later rollout requires trusted-service-only reachability and TLS; `/internal` is a path, not protection.
- `200 OK`: saved UTF-8 Markdown with `Content-Type: text/markdown; charset=utf-8` and `Cache-Control: no-store`. A valid saved empty memo has an empty body. Generate and bound the result before replying; do not stream a partial success. Do not dereference any persisted links, images or embeds.

| Status | Meaning |
|---|---|
| `400` | Malformed or nil UUID. |
| `404` | No matching memo, including a known non-memo ID. |
| `502` | Known memo without a usable saved source, or corrupt/unsupported saved content. |
| `503` | At capacity, or a transient dependency/deadline failure. |

Non-success responses use typed, generic errors with no saved text, blob identifiers, upstream payloads or credentials. The route does not expose WebSocket authorization decisions or a general document enumeration surface outside the trusted listener. The result is a single saved revision, not a cache, batch API or storage mutation.

## Documents: automatic publication, no new HTTP read endpoint

1. The first changed save opens a five-second window. At its end, submit the latest successfully saved revision, even if the room remains open; later saves do not reset the window. Normal release after the existing idle grace can submit a final unpublished revision without waiting for that window. If the same revision is still running, the final submission uses the same bounded queue and is skipped if the running attempt fully publishes it. Artifact reads do not trigger publication, and a failed/deleted teardown cannot submit unsaved content.
2. One bounded worker verifies the saved fingerprint and source ownership. Whiteboards produce one 1800×720 full-scene PNG without distortion, user crop or fixed mode. Only eligible source-owned assets may be resolved; a missing asset is a render failure. Memos produce one UTF-8 Markdown file; the conversion does not resolve links.
3. Resolve the dedicated slot by its bucket-scoped reference. On a genuine missing-slot lookup, collab creates the artifact with `POST /internal/file`. Once provisioned, `PUT /internal/file/{id}/content` replaces bytes under the existing ID. A failed or conflicting overwrite never falls back to allocation or relinking. Collab does not look up, publish or remove separate card files.

| Document | Suffix on `collab-preview-v1:{documentId}:` | Stored artifact |
|---|---|---|
| Whiteboard | `whiteboard` | `image/png`, full scene fitted to 1800×720. |
| Memo | `markdown` | `.md`, stored as `text/plain` (UTF-8); starts with the reserved, invisible Markdown reference definition `[//alkemio-preview-v1//]: # (Generated memo preview)` and two newlines. The prefix keeps a cleared memo nonzero and prevents HTML sniffing when the body starts with an iframe wrapper. Consumers needing the byte-exact body can strip that prefix or use the independent internal memo endpoint. |

### Provisioning and lifecycle constraints

- Collab derives the reference and bucket from verified document metadata, not a visual URI, caller-supplied destination or a filename. The existing unique `(externalReference, storageBucketId)` index supplies the mapping, including across process restart, without a database of our own. A scoped `GET /internal/file/by-reference?ref=…&bucketId=…` verifies ID/ref/bucket/MIME; targets are private, non-temporary and distinct from the checkpoint. Creation is deliberately policy-less. A duplicate reference returns the existing row without changing its bytes; that attempt does not claim it published a new revision.
- For an asset, verify a real document UUID from the scene with `GET /internal/file/{id}/meta`. Require an eligible asset in the trusted board bucket with the correct ownership/access rules, not an arbitrary internal file. Use the validated metadata's immutable content hash with file-service's internal blob read, never content hashes or destination URLs from a scene. Keep byte, dimension, decode and time limits across both calls.
- Suppress pending work on owner invalidation, including canonical draft expiry cleanup, and let the existing bucket delete cascade remove the files. Recheck authoritative existence, ownership and checkpoint fingerprint before either mutation. The earlier draft expiry timestamp is not available and is not a separate cutoff. Dedicated files must not be moved or shared with another writer; the supported topology has one preview writer. Do not expose new public reads. A later save window or normal release may try publication again; there is no retry schedule. Only a genuine absent-slot lookup permits allocation.
- For later live cutover, the server's existing profile visuals continue to be the public references. The current visual URI does not identify a collab-owned private slot and cannot grant overwrite authority. Server integration must adopt published slots through a trusted association and enforce source-document read access; it is not part of this feature. Server memo GraphQL still converts Yjs on demand, unchanged here.
