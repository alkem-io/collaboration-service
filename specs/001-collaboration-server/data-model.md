# Data Model — collaboration-server (WS-C, Phase 1)

Two layers, same split as the epic `data-model.md`
(`../agents-hq/specs/003-unify-collab-yjs/data-model.md`, authoritative): the
**CRDT document conventions** (the in-`Y.Doc` shape, owned by the clients + the
`y-crdt` core) and the **service storage schema** (metadata + snapshot, owned by
this server + `server`). This file documents the *server view* and anchors each
shape to the Wave-1 code.

## CRDT document conventions (inside the `Y.Doc`)

The server is otherwise content-agnostic — the clients own the inner shape — but
`convention.go` (`applyConvention`) materializes the root shared type with the
correct kind on a brand-new doc so the first client binds against the expected
structure rather than creating it racily.

### Memo (rich text)
- Root: `Y.XmlFragment` named **`default`** (ProseMirror/TipTap ↔ `y-prosemirror`).
- Server: `doc.GetXmlFragment("default")` on materialization. Merge is
  character-level, intention-preserving (native Yjs). No schema change vs today.

### Whiteboard (Excalidraw scene)
- Root `Y.Map` **`elements`**: key = element id → per-element `Y.Map`:
  - geometry/style props (x, y, width, height, angle, strokeColor, …) — **each a Map key ⇒ per-property merge** (US1).
  - `index`: fractional index string (conflict-free ordering).
  - `isDeleted`/tombstone, `version`, `versionNonce` (retained for Excalidraw interop).
- Root `Y.Map` **`files`**: key = fileId → file descriptor (url/dataURL/mimeType).
- Root `Y.Map` **`appState`**: shared view-state subset (not per-cursor).
- Server: `doc.GetMap("elements")`, `doc.GetMap("files")`, `doc.GetMap("appState")`.
- **Ephemeral (NOT in the doc — awareness/ephemeral channel):** cursor, selection,
  idle, emoji reaction, countdown timer. Fanned out, never applied, never
  persisted (FR-008; `handleMessage` `WireAwareness`/`WireEphemeral`).

### Doc construction
- `newRoomDoc(guid)` → `ycrdt.NewDoc(guid, true, ycrdt.DefaultGCFilter, nil, false)`
  — GC enabled for size; the *configurable* GC policy (FR-025 forward-compat) is
  refined in the `y-crdt` fork, not here.

## Service entities (server-owned)

### Document metadata / index — `model.Metadata` (`model/document.go`)
The small, queryable index row. Owned by `server` (RabbitMQ) or the standalone
metastore; **never** holds the blob bytes.

| field | Go type | notes |
|---|---|---|
| `ID` | `DocumentID` (string) | single id namespace (memo + whiteboard) |
| `ContentType` | `ContentType` (`memo`\|`whiteboard`) | drives the convention/binding; not baked into id |
| `Version` | int | bumped per persisted snapshot; room for a future version timeline (FR-025) |
| `ContentPointer` | string | locator into the blob store (inline row key / file-service object id / S3 key) |
| `BlobStore` | `BlobStoreKind` (`inline`\|`file-service`\|`s3`\|`local`) | which adapter holds the blob — persisted so a doc rehydrates from the right backend regardless of running config |
| `OwnerRef` | string | parent Alkemio entity (lifecycle owner, FR-023); the delete cascade keys off it |
| `CreatedAt`/`UpdatedAt` | time.Time | |

- **Forward (Wave 2, OPEN-1/OPEN-3):** add an **`authorizationPolicyId`** the
  `authzeval` adapter evaluates against, and align the field set with the unified
  RabbitMQ `save`/`fetch` contract. `MetadataStore.Load` returns this row.
- Identity: `ID` unique. Lifecycle: created on first save (or `document.created`
  pre-register); **purged on owner-delete cascade** (FR-023) — no orphans.

### Snapshot — `model.Snapshot` (`model/document.go`)
The encoded full `Y.Doc` state handed to `BlobStore.Put`.

| field | Go type | notes |
|---|---|---|
| `ID` | `DocumentID` | the document |
| `Version` | int | matches the `Metadata.Version` the bytes were persisted at |
| `Data` | []byte | **v2** `EncodeStateAsUpdateV2(doc, nil)` (v1 also readable) |

- Written **debounced/throttled** per room (R7; `Room.persist`). Latest-only in
  v1 (history out of scope, FR-025-forward-compatible). The inline content pointer
  == document id.

### Room (live, in-memory; never persisted) — `service.Room` (`service/room.go`)
The materialized session: the authoritative plaintext `Y.Doc` (FR-021), the
`*ycrdt.Awareness`, the member registry (`map[connID]roomMember`), the `dirty`
flag, `version`, `pointer`, and the run-loop goroutine + debounce/idle timers.
Lazily materialized on first connect (loads the snapshot via `loadSnapshot`),
released on idle/empty or owner-delete. `model.Room` is the lightweight identity/
bookkeeping projection (`model/room.go`).

### Identity / Authorization — `model.{Identity,AuthDecision,Privilege}` (`model/auth.go`)
- `Identity{ActorID string}` — the authN principal (empty = anonymous/open mode; **`actorId`, never `userId`**, constitution §III).
- `Privilege` — `read` (viewer) \| `update-content` (collaborator).
- `AuthDecision{Allowed bool, Reason string}` — the per-doc authZ result; a port
  **error** means "could not answer" → callers **fail closed** (never treat an
  error as a clean denial). `Reason` carries no secrets.

### Awareness / Presence (ephemeral) — `model.{Awareness,CollaboratorMode}` (`model/room.go`)
Per-participant: `ClientID` (y awareness client id), `ActorID`, `Mode`
(`viewer`\|`collaborator`). Broadcast via awareness (type 1) + the ephemeral
channel (type 2), fanned out on `awareness:{id}`; **never persisted**. Inactivity
may downgrade a collaborator to viewer (FR-014, Wave 3).

### Control messages — `model.ControlMessage` (`model/control.go`)
Server→client type-3 events, JSON body in the `[type][payload]` envelope:
`saved` (carries `Version`), `save-error` (carries `Error`, no secrets),
`read-only-state` (carries `ReadOnly`), `room-user-change` (carries `Users`
count), `room-closed`.

### Wire message types — `model.WireMessageType` (`model/control.go`)
`0` `WireSync` · `1` `WireAwareness` · `2` `WireEphemeral` · `3` `WireControl`.
Types 0/1 are y-protocols-owned; 2/3 are this service's channels framed with the
same envelope.

## Validation / rules
- Server holds **plaintext** (FR-021); malformed/hostile frames are dropped, not
  applied (no divergence, no panic — `handleMessage`/`dispatchSync` error paths).
- Whiteboard: different properties merge concurrently; same property → deterministic
  CRDT tiebreak (the core's job).
- Snapshot persist is idempotent on `dirty`: a no-op when nothing changed since the
  last save; on failure the room keeps serving from memory and retries next
  debounce tick (crash-loss window = one debounce interval).
- Enforced limits (Wave 3): max doc size, max conns/room, per-conn update rate
  (FR-024) — breach → control + disconnect, others unaffected.

## References
- Epic conventions + schema: `../agents-hq/specs/003-unify-collab-yjs/data-model.md`
- Wave-1 convention code: `internal/domain/service/convention.go`
- Wave-1 domain types: `internal/domain/model/{document,room,control,auth,errors}.go`
- Persistence contract: `../agents-hq/specs/003-unify-collab-yjs/contracts/persistence-ports.md`
