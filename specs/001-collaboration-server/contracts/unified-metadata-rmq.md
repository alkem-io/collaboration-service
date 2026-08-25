# Unified Collaboration Metadata Contract (RabbitMQ) — `server` hand-off

**Status:** collab-service side **implemented** (Wave 2, T005.1). **`server`-side
consumer DOES NOT EXIST YET** — this is the cross-repo follow-up (OPEN-3).

This is the new **unified** metadata/index contract that replaces the two legacy
dialects:

- `collaborative-document-service` — `collaboration-document-save` /
  `collaboration-document-fetch`, payload `{documentId, binaryStateInBase64}`
  (Yjs v2 base64).
- `whiteboard-collaboration-service` — `save` / `fetch`, payload
  `{whiteboardId, content}` (Excalidraw JSON).

The unified collaboration-service holds **one** representation (a v2 `Y.Doc`
snapshot) for **both** content types and applies the **metadata/blob split**
(`persistence-ports.md`): the **blob goes to the BlobStore** (inline /
file-service) and **only the index** crosses this bus. The legacy
`binaryStateInBase64` / `content` blob fields are therefore **gone**.

## Transport

NestJS `Transport.RMQ` request/reply (and one fire-and-forget event), identical
framing to the legacy services so the `server` side is a normal
`@MessagePattern` / `@EventPattern` consumer:

- Durable server queue (the existing Alkemio collaboration queue;
  `queueOptions: { durable: true }`, `noAck: true`).
- Request envelope on the wire: `{ "pattern": "<pattern>", "data": <payload>, "id": "<correlationId>" }`,
  published to the server queue with AMQP `correlationId` + `replyTo` (an
  exclusive per-connection reply queue on the collab side).
- Reply envelope: NestJS standard `{ "response": <reply>, "isDisposed": true, "id": "<correlationId>" }`
  (an error is carried in NestJS's `err` field or as the typed `error` field in
  the reply payloads below).

## Patterns & payloads

### `collaboration-save` — request/reply

Upsert the index row (called on first save and every debounced snapshot). The
collab service has already written the blob to its BlobStore; this records where
it lives.

Request `data`:

```jsonc
{
  "id": "<documentId>",          // single id namespace (memo + whiteboard)
  "contentType": "memo" | "whiteboard",
  "version": 4,                   // bumped per persisted snapshot
  "contentPointer": "<locator>", // inline row key | file-service UUID
  "authorizationPolicyId": "<uuid>", // OPEN-1; may be "" in open/standalone
  "ownerRef": "<parent entity id>"   // delete-cascade key (FR-023); optional
}
```

Reply: `{ "success": true }` or `{ "success": false, "error": "<reason>" }`.

> **`server` must persist `contentPointer`** so a fetch returns it — the collab
> service rehydrates the snapshot from file-service using `contentPointer`.
>
> There is deliberately NO store selector on this contract. file-service is the
> storage abstraction for the whole Alkemio stack, so a `contentPointer` is always
> a file-service id and there is no "which store" question at this boundary. The
> collab service's own `CHECKPOINT_STORE` (`inline` | `file-service`) selects its
> INTERNAL adapter for standalone and test runs; `inline` is non-durable and must
> never appear in a server entity, column, or wire payload.

### `collaboration-fetch` — request/reply

Return the index row for a document (the collab service materializes a room and
needs to know where the blob is + the content type + the policy id + the
document's own storage bucket). The reply carries NO document bytes: the collab
service reads the snapshot from file-service itself via `contentPointer`.

Request `data`: `{ "id": "<documentId>" }`

Reply:

```jsonc
{
  "found": true,
  "contentType": "memo" | "whiteboard",
  "version": 4,
  "contentPointer": "<locator>",
  "migrated": true,                   // temporary rollout gate; false is refused before room materialization
  "authorizationPolicyId": "<uuid>",
  "storageBucketId": "<uuid>",        // the document's OWN storage bucket; snapshots persist into it (per-doc bucket)
  "ownerRef": "<parent entity id>"
}
```

Not found: `{ "found": false }`. Error: `{ "found": false, "error": "<reason>" }`.

### `collaboration-delete` — request/reply  *(**RETIRED** — this service no longer issues it)*

> **RETIRED.** `collaboration-service` speaks no delete pattern: `port.MetadataStore`
> is read/upsert only ("There is no Delete: `server` owns the row and removes it").
>
> OWNERSHIP MOVED, and the ORDER is why. Before `server` mutates the row, profile,
> bucket or blob, it confirms a persistent `document.deleted` publish. This service
> temporarily tombstones the id, closes the room and evicts it — it "deletes nothing
> and does not flush" — while `server` completes the owner cascade. A delete call
> from here would duplicate and race that single owner.
>
> The shape below is preserved rather than deleted so a reader who meets the pattern
> name in an older branch, a broker trace, or a `server` consumer learns why it went.
>
> A CONSUMER MAY OR MAY NOT EXIST ON ANY GIVEN `server` BRANCH, and that is
> deliberately not asserted here: Release-A-era branches carry a handler for this
> pattern, its removal is `server`'s own cleanup on `server`'s own schedule, and a
> statement about which branches have it today would be wrong the moment either
> side moves. What is durable is the CONTRACT: this pattern is not part of it,
> nothing here produces it, and any surviving handler is independently retireable
> without reference to this service.

Request `data`: `{ "id": "<documentId>" }`
Reply: `{ "success": true }` or `{ "success": false, "error": "<reason>" }`.

### `collaboration-info` — request/reply  *(**RETIRED** — this service no longer issues it)*

> **RETIRED under KISS-018.** The requester side is deleted: `PatternInfo`,
> `InfoData` and `InfoReply` are gone from `contract.go`.
>
> WHY, and it is not merely disuse. The capability this pattern would carry —
> whether an actor may read or update, and how many collaborators a document
> allows — is ALREADY OWNED by the authorization-evaluation-service, which decides
> per actor, per document, once per session, before the room is materialized.
> Wiring this in would install a SECOND authorization-shaped decision point beside
> it: two sources deciding who may participate, disagreeing whenever the metadata
> row and the policy drift.
>
> The per-document connection cap it also carried is not a security boundary
> either. A private document refuses other actors at the authorization gate and
> never reaches a count check; a document others may legitimately read would have
> them refused on arrival order instead. So `MAX_CONNS_PER_ROOM` remains a global
> resource limit and nothing is lost.
>
> As with `collaboration-delete`, the shape below is preserved rather than deleted.
> Whether a consumer exists on any given `server` branch is deliberately not
> asserted: this pattern is not part of the contract, nothing here produces it, and
> any surviving handler is independently retireable on `server`'s own schedule.

Collaborator-mode inputs, carried forward from both legacy `info` patterns and
**unified**: whiteboard's `{read, update, maxCollaborators}` plus the optional
`isMultiUser` memos added. `actorId`, never `userId` (constitution §III).

Request `data`: `{ "actorId": "<uuid>", "id": "<documentId>" }`

Reply:

```jsonc
{
  "read": true,
  "update": true,
  "maxCollaborators": 12,   // optional — omit when unknown (whiteboard number|undefined)
  "isMultiUser": true       // optional — memos only
}
```

### `collaboration-contribution` — fire-and-forget event  *(emitted in Wave 3, T013)*

The north-star contribution event: the per-window set of contributing actor ids
(carried forward from `collaboration-memo-contribution {memoId, users}` and
`contribution {whiteboardId, users}`, unified under one `id`).

`data`: `{ "id": "<documentId>", "users": [ { "id": "<actorId>" }, ... ] }`

## Source of truth on the collab side

- Contract types: `internal/adapter/outbound/metastore/rabbitmq/contract.go`.
- Adapter (publishes these patterns): `internal/adapter/outbound/metastore/rabbitmq/{store,conn}.go`.
- Selected by `METADATA_STORE=rabbitmq` (+ `RABBITMQ_*` / `RABBITMQ_QUEUE`).

## Open items for the `server` owner

1. Implement the four `@MessagePattern` handlers + the `@EventPattern`
   contribution handler on the existing collaboration queue.
2. Decide the index storage on the `server` side (the collab service is
   agnostic — it only needs the documented replies).
3. Confirm the queue name / env var the collab service should target
   (`RABBITMQ_QUEUE`); the collab side currently requires it explicitly.
