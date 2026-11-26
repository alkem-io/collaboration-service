# Feature Specification: Native Go Collaboration Previews

**Feature Branch**: `004-native-collab-previews`\
**Created**: 2026-11-23\
**Status**: Draft\
**Input**: Collaboration-service-only native Go whiteboard previews and an internal single-memo Markdown endpoint.

## Clarifications

### Session 2026-11-26

- Q: Does a whiteboard need a separate card image? → A: No. Generate and publish one 1800×720 full-board PNG. Keep the existing full-preview reference and leave any legacy card files untouched.

### Session 2026-11-24

- Q: Can the internal memo Markdown endpoint be disabled? → A: No. It starts on every boot, only on the separate private listener. The bind address may be configured; that does not change the routing or trust boundary.
- Q: Who owns preview-file provisioning, and does memo publication differ from whiteboard publication? → A: Collaboration-service owns both, as WOPI does for its previews. The same worker creates missing private artifacts and overwrites existing ones in place. Whiteboard previews are PNGs; a memo preview is a Markdown file. The independent internal Markdown read remains available. Server integration comes later.
- Q: Do previews refresh only when the room closes? → A: No. Coalesce successful saves in a short, fixed window while the room is open. Normal close submits a final saved revision if it has not already been published.

### Session 2026-11-23

- Q: When a saved whiteboard is cleared, what should happen to its existing automatic and custom preview images? → A: This stage renders only the full saved board; custom crops and frozen previews are deferred. Render a successfully saved empty scene as a blank PNG, keeping the existing output ID. Missing private files follow the November 24 provisioning ruling; genuinely unchanged sessions never publish.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Read saved memo Markdown (Priority: P1)

A trusted cluster service can get a memo's saved Markdown by ID without opening the editor.

**Why this priority**: This is the smallest independently useful delivery for memo previews.

**Independent Test**: Request a saved memo by ID from the internal service network with no live room and no credentials. Compare the returned body against the current converter's output.

**Acceptance Scenarios**:

1. **Given** a memo saved before its editor closed or its browser exited, **When** a trusted internal caller requests it, **Then** the response is the saved Markdown, and no room or additional stored file is created.
2. **Given** a valid empty memo, an unknown or non-memo ID, and a memo whose referenced snapshot cannot be read or decoded, **When** each is requested, **Then** only the empty memo returns a successful empty body; an absent/wrong-type entity and a source failure are distinguishable.
3. **Given** the private listener, public listener and public gateway in an isolated deployment, **When** the same memo request is made without credentials at each boundary, **Then** the private listener works with no application auth calls, and neither public path reaches the handler or returns Markdown.

---

### User Story 2 - Publish document previews from saved edits (Priority: P1)

A consumer can read one 1800×720 full-board PNG for a whiteboard or one Markdown file for a memo while editing continues or after the room closes. The service refreshes the artifact automatically, with no editor callback, browser upload or explicit generation request.

**Why this priority**: Preview freshness must come from saved content, including changes made outside the browser.

**Independent Test**: Save several edits to each document type and leave the connection open through the refresh window. Resolve each private output by its reserved reference and verify the most recent saved content. Save another change and disconnect before the next window ends. Verify normal idle release submits the final save at stable IDs.

**Acceptance Scenarios**:

1. **Given** a whiteboard or memo edited by a browser or MCP client, including a template or draft supported by existing metadata, **When** the first changed save reaches the end of its refresh window or normal room release occurs first, **Then** collab-service offers the latest saved content for publication. The whiteboard PNG fits the whole scene. A browser exiting after a save needs no close/upload call.
2. **Given** a document's output file already exists, **When** new saved content is rendered, **Then** the same ID and URI yield the updated artifact. No file is allocated or visual relinked on that refresh.
3. **Given** several saves in one window, **When** the window ends, **Then** the latest successful save is offered once, without waiting for editing to stop. Normal room release may offer a final saved revision but does not repeat a completed publication. Opening and closing without a content change, and reading stored artifacts, do not render.
4. **Given** a document with an existing artifact, **When** its content is cleared and saved, **Then** the next refresh replaces that file with a blank PNG or visually empty Markdown at the same ID.

### Edge Cases

- A valid saved clear publishes blank output at the same file IDs or creates a previously absent slot. An unreadable or failed render is not empty and must not replace a previous output.
- Missing, invalid or foreign asset locators: surface a generation failure without fetching arbitrary URLs, exposing unrelated files, or publishing a partial image as current.
- A board reopens while a render is running: older work must not overwrite a newer published preview or be labelled as representing a newer revision. A failed final save does not authorize a preview of unsaved content. Owner invalidation, including that issued by draft expiry cleanup, suppresses pending work.
- Failed write, slow conversion, malformed or over-limit input, or exhausted capacity: return an explicit outcome without relinking to a substitute, changing source content or interrupting editing.
- Unknown or deleted memo ID and known memo with a missing or corrupt referenced snapshot are not interchangeable. The whiteboard facility exposes no new read route.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Both facilities MUST derive from the saved authoritative source without joining an editing room or modifying the source, presence, entitlements, contribution accounting, or durable-close behaviour. Memo reads and reads of existing output files work without an active room.
- **FR-002**: The service MUST provide one cluster-internal memo read endpoint, `GET /internal/memos/{memoId}/markdown`, which returns the saved Markdown body as `text/markdown; charset=utf-8`. It MUST distinguish a valid empty memo, an absent or wrong-type ID, and an upstream or conversion failure.
- **FR-003**: The internal memo route MUST start with the service on a dedicated private listener, with no disable switch. It MUST require no credentials or actor identity and make no authentication or authorization calls. It MUST NOT be reachable through public ingress, the WebSocket route or the frontend API. Access is limited by the trusted cluster boundary, not by the `/internal` path name; a future consumer making the result available to users owns the end-user access check.
- **FR-004**: Memo output MUST preserve the current representation of paragraphs and blank lines, headings, inline formatting, links, nested and ordered lists, blockquotes, code, separators, images, tables and embed/iframe content. Use the current converter as the compatibility reference. Conversion MUST NOT resolve links or embeds. The internal read MUST NOT write an artifact or depend on close publication. No batch, ETag or caching is part of this endpoint.
- **FR-005**: Whiteboard output MUST preserve supported saved geometry, text layout and visual semantics. The parity corpus MUST cover shapes, arrows and arrowheads, bound/free text and wrapping, hand-drawn strokes, images, rotation, stacking, frames/clipping, colours/backgrounds, opacity, fills/strokes and non-executing embed placeholders. Inventory the supported set and fonts from the pinned editor; do not silently substitute a simplified renderer.
- **FR-006**: A whiteboard render MUST resolve persisted image references using the supported storage boundary and only assets the source may include. It MUST NOT fetch arbitrary network destinations, execute embedded content or expose unrelated private files. Missing or unauthorized required assets MUST NOT produce a successful partial preview.
- **FR-007**: Generate exactly one 1800×720 PNG of the entire saved whiteboard. Fit the full-board view without cropping the scene to fit or distorting its geometry. Do not generate a separate card image or look up, replace or delete legacy card files. Do not read or implement preview modes, user crop settings or freeze operations in this stage.
- **FR-008**: Collaboration-service MUST write both formats directly to file-service. On a genuine miss of a reserved reference in the document's own bucket, create its private artifact. For an existing eligible row, overwrite its bytes without changing its file ID. A caller-chosen destination, failed lookup other than absence, or unsuccessful overwrite MUST NOT cause allocation or relinking.
- **FR-009**: A changed memo or whiteboard MUST refresh automatically based on successful saves. The first changed save starts a bounded, non-sliding window; at its end the service submits the latest successful save. Normal room release also submits a final saved revision if not already published, without waiting for the window or generation. Both document types share one bounded worker. Skip unchanged sessions; failed saves and artifact reads do not trigger rendering. The independent private memo endpoint still converts on demand. Do not add a retry schedule or another producer.
- **FR-010**: Artifacts MUST remain private and tied to the source's ownership and lifetime. Stop publication when the source is invalidated by the owner or no longer exists, and let the owner cascade remove its files. Draft expiry takes effect here at the existing owner's expiry cleanup; enforcing the stored timestamp before cleanup is deferred. Any future browser-facing delivery MUST check current source READ access before returning bytes or a not-modified result, preserve entitled guest/anonymous reads, and treat preview URLs as references rather than grants. This does not impose application auth on the internal memo route.
- **FR-011**: Both facilities MUST bound input, processing and in-flight work, and respect cancellation. Whiteboard generation MUST bound decoded image and render dimensions. Slow or failed work MUST NOT block source saving, session teardown or durability acknowledgements. Record generation failures without replacing the last successful image with a failed render. Establish initial budgets for each lane from representative qualification measurements before its production work; verify them under combined load before completion. Do not invent service targets.
- **FR-012**: The service MUST document how consumers request memo Markdown and where the whiteboard outputs are written, which saved revision they represent, and how failures are reported. Existing profile visuals and their URIs remain the eventual whiteboard read contract; no new preview read endpoint, GraphQL field or recurring visual relink is required. Demonstrate the facility directly without consumer changes.

### Key Entities *(include if feature involves data)*

- **Saved document**: Existing document ID and declared type, with saved content identity and ownership. The source remains authoritative.
- **Document preview artifact**: Private 1800×720 full-board PNG or a memo Markdown file. Its file identity is stable across saved revisions.
- **Memo Markdown result**: Transient Markdown body derived from a single saved memo, not a stored file or new content entity.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In every memo compatibility fixture, the result preserves the reference's supported structure and content. An internal cold read needs zero credentials, room joins or derivative writes; every tested public path returns zero memo content.
- **SC-002**: The whiteboard PNG is 1800×720 and passes the reviewed visual parity corpus without omitted, misplaced or incorrectly clipped content. The corpus covers every supported element family and representative supported writing systems. Define comparison tolerances against current exports rather than requiring universal pixel identity.
- **SC-003**: A changed, saved room creates its missing private preview files exactly once. A later refresh, including a saved clear, overwrites those IDs with no new rows or relinks. This holds across process restart for both document types.
- **SC-004**: A burst of successful saves produces one candidate per window; release can submit newer work before the next window ends. An unchanged room and repeated reads produce zero. An injected failure does not erase a valid preview; older work cannot replace an already published newer revision.
- **SC-005**: Under measured representative preview load and injected renderer/storage failure, saves and durability acknowledgements continue to succeed within the existing collaboration bounds. Tests observe zero source changes caused by derivation and zero derivative disclosures through exercised unauthorized paths.

## Assumptions

- **Stage boundary**: Only collaboration-service code, build, tests and facility documentation are in scope. Server/client-web, WOPI, file-service and deployment manifests are unchanged. User-selected crops and frozen previews are deferred with consumer integration. No GraphQL/DataLoader, frontend close/upload, preview-settings mutation, template/form, signing, PDF, or export integration; no production/acceptance operation. Existing producers and their preview modes continue unchanged until later integration.
- **Mandated production constraint**: Implement both generators natively in Go in collaboration-service using the existing Go Yjs decoder and saved document conventions (Yjs-V2; whiteboard scene roots and memo `default` rich-text fragment). No second CRDT implementation, Node worker, browser process, embedded JavaScript or server TypeScript conversion call in production. The current browser renderer and server converter may be used as test oracles.
- **Storage boundary**: WOPI is the reference for stable private output files and in-place refresh, not for the trigger: WOPI renders lazily on reads; this facility refreshes after coalesced saves and on normal room release. No new service-owned database/schema, writes to server-owned rows, new deployment topology or new document lifecycle. Plan how to use existing metadata and file-service contracts for trusted output provisioning, saved-source freshness and artifact retention. Verify capabilities on the implementation baseline; if the existing contracts cannot support a requirement, surface the gap rather than widening scope. No competing producer may write a test output; do not activate this facility against live client-written visuals as part of the spec.
- **Planning evidence**: Verify the private service-only route before exposing unauthenticated memo reads in any deployed environment. Plan a bounded native-Go parity proof, fixture comparisons and representative load measurements before implementation. Library and font choices are deferred; do not treat the fork's headless scene API as a renderer or copy WOPI's resource limits.
- **Source baseline**: The supplied investigation used collaboration-service `1fb52e1`, server `f24c16cc5`, client-web `5436d107d` and wopi-service `9dad29c`. Recheck the selected implementation baseline and existing contracts during planning. Server#6430 is background for the later consumer cutover; it does not add a second generation trigger.
