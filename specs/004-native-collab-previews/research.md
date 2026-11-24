# Research: Native Go Collaboration Previews

## R1 — Baselines and evidence

**Decision:** Plan against these sources, checked on 2026-11-23. Refresh again before implementation. No other repository's working files were changed.

| Source | Reviewed baseline |
|---|---|
| collaboration-service `develop` | `1fb52e1ba48cd887794964300efa93f240b695dd` |
| server `develop` | `f24c16cc5a69daa30035c4075268219ab11ccf6d` |
| client-web `develop` | `2eb1b9b117cbfc66814adb7cf89ea6baa311d28a` (subsequent changes from `bc18d709d` only touch the Matrix session controller; editor pin and preview paths unchanged) |
| wopi-service `develop` | `9dad29c38adacbe015fbb4a58654d29dc8394258` |
| file-service `develop` | `259dda0d4ac517184d03bd67779b593f4c09518d` |
| client editor fork tag | `0.5.6`, `90b57f27faa35b570844c2f277e277cf450a7e73` |

**Rationale:** The shipped collab path is RabbitMQ metadata plus file-service checkpoints. Constitution §§II/III describe the later direct admission projection; do not implement it here, broaden its field set, or use a new database query for previews. This is a consumer of the existing read capability, not a replacement persistence/metadata adapter. Baseline `go test` passed the room/service, HTTP, metadata, checkpoint and app packages. The earlier WOPI run passed 41 focused preview/admission tests; neither run tested this unbuilt feature.

**Unit-only baseline:** The independent command in [quickstart §2](quickstart.md#2-run-the-repository-gates) passed the untagged race-enabled test suite on `1fb52e1` and measured **92.69% (2,219/2,394 statements)**. The ≥95% threshold failed before feature code. Add meaningful tests for existing behavior during development and require repository-wide ≥95% at final feature verification. Record the final result separately from this baseline; do not substitute combined coverage or claim §XII has passed before the unit-only gate is green.

## R2 — One whiteboard trigger and a bounded publish path

**Decision:** In `internal/domain/service/room.go`, a changed successful checkpoint save starts a five-second coalescing window shared by both document types. This is an admission interval, not an output SLA. Further saves update the digest without postponing the offer. Normal empty-room release (`releaseIfEmpty`, default idle grace 30s) bypasses the window for the final saved digest; already completed publication is skipped, and the queue coalesces identical in-flight work. The worker retains a compact token, not the room. It reads, decodes and writes off the room loop.

**Rationale:** `Room.persist` clears `dirty` only after `SaveCheckpoint` and metadata save, then resolves durability barriers. At room release a clean board can have been saved earlier, so checking only `dirty` would miss successful edits. Compare the accepted save against the room's initial identity, using a digest of complete checkpoint bytes (no second encoding). The bare file ID is stable, file-service `LoadCheckpoint` supplies a process-local revision (`internal/adapter/outbound/persistence/fileservice/store.go`), and the state vector does not distinguish delete-only updates (`internal/domain/service/durability_barrier_test.go`, `TestADeleteOnlyMutationIsCoveredByTheBarrier`). Carry the fingerprint, not live state or a large buffer. Reread and match it before deriving; if a newer save is already there, skip the superseded job. Single-worker ordering plus at most one pending job per document prevents an old job finishing after a newer publication on the supported single-writer topology. A full queue rejects new work without blocking durability. No replay after a process crash or job failure is promised; a subsequent save window or normal release is an ordinary opportunity, not a retry schedule.

**Alternatives:** WOPI's `PreviewService.Resolve` (at pinned `internal/domain/service/preview_service.go:70–119`) generates lazily on a preview read, comparing the saved source update timestamp; it does not have a separate on-save generation timer. Use its private, stable-file pattern, not its read trigger or DB. Rendering at every checkpoint, watching socket closes individually or making the client call on close would cause redundant work or browser reliance. Host shutdown, forced delete and failed flush are not normal room-close triggers.

## R3 — File identity, privacy, and the legacy association boundary

**Decision (November 24, supersedes external provisioning):** Collab owns private preview creation and replacement for both document types, as WOPI does. The file-service bucket/reference index takes the place of WOPI's private mapping table. A scoped lookup that reports a genuine miss permits one create; an existing file is replaced by ID. A conflict, authorization mismatch or failed overwrite does not permit creation. One failed whiteboard variant does not block its sibling.

**Rationale:** File-service exposes `POST /internal/file`, bucket-scoped `GET /internal/file/by-reference`, and `PUT /internal/file/{id}/content` in `internal/adapter/inbound/http/router.go:66–83`; `db/schema/document.sql` and server `src/migrations/1782299000000-FileExternalReference.ts` pin unique `(externalReference, storageBucketId)` independent of the content hash. The replacement path `StoreAndLinkStream` reads an existing file, reconciles stored MIME, writes content and uses compare-and-set on the observed file row; failed or deleted rows are not recreated. `internal/adapter/inbound/http/public_handler.go` denies policy-less files before serving bytes or a conditional response. Isolated artifacts have no public file authorization, as in WOPI `internal/adapter/outbound/fileservice/file_client.go:155`; never reuse the whiteboard's authorization row for two files (file ACL link is unique). Use the same ownership/ACL for an already-authorized file when eventually provisioned; no ACL updates from the renderer. Validate what is actually read back after file-service processes the PNG, as it need not be byte-for-byte the upload.

**Existing visuals vs owned files:** `FetchReply` and server `whiteboard.service.ts` do not carry visual file IDs. `visual.resolver.mutations.ts` accepts platform storage URIs, which do not grant overwrite authority. This matters for later adoption, not for generating new private slots. Ordinary uploaders cannot set an external reference; Matrix media IDs do not contain colons. The reserved prefix stays private. File-service CAS guards replacement after its row load; dedicated, never-moved output rows and one writer are prerequisites. Server baseline migration `1764590884532` gives the file storage-bucket relation ON DELETE CASCADE; `1776778800000` renames the table without dropping that FK. The manager tombstone cancels active/pending work and the existing owner cascade handles stored rows. No pre-sweep expiry or public read is promised.

**Fresh server memo trace, November 24:** Fetched server `origin/develop` at `f24c16cc5a69daa30035c4075268219ab11ccf6d`. `memo.content.loader.creator.ts:20–63` batch-reads saved Yjs bytes from file-service and calls `yjsStateToMarkdown` on demand. `memo.service.ts:106–167` re-homes linked media, encodes the Markdown input to Yjs V2, then writes that initial snapshot, not an MD file. Collab's stored Markdown is new; the server read path is not switched over here. File-service `259dda0d4ac517184d03bd67779b593f4c09518d` refuses zero-byte PUT and can sniff iframe-wrapper HTML as `text/html`. One invisible Markdown definition makes the private artifact a nonzero, text/plain-classified Markdown document, including after a clear. The private read endpoint returns the exact unwrapped output.

**Alternatives:** Using a per-service SQL cache (WOPI's topology), scanning filenames, consuming the server's GraphQL as an actor, or attaching authorization policies from collab would breach scope or ownership. Refuse replacement and keep the last good artifact on an unavailable association, MIME mismatch, 404/409 or other failure; no destructive repair. File-service lifecycle owns abandoned blobs and deleted buckets.

## R4 — Asset boundary

**Decision:** The pinned client and server persist file document UUIDs as locators. Accept only that form; do not treat any scene string as an address. Read canonical file-service metadata first; require a durable, authorized image belonging to the whiteboard's authoritative bucket and allowed by its asset contract. Read immutable bytes via `/internal/blob/{hash}/content` using only the hash returned by that trusted metadata, not a hash supplied in the scene. This avoids re-resolving a file ID to replacement bytes between verification and read (`file-service/internal/adapter/inbound/http/document_handler.go:171–182`). Disable redirects and bound encoding, bytes, decoded dimensions and total image work. Read only files used by live image elements. Do not fetch an embed or other URL.

**Rationale:** The client `src/domain/common/whiteboard/excalidraw/assetAdapter/useWhiteboardAssetAdapter.ts:42–49,95–144` uploads durable assets to the board bucket and persists document locators; the server `whiteboard.service.ts:734–852` tries to re-home authorized references but can preserve broken or foreign legacy locators. The normal asset policy inherits through whiteboard → profile → bucket → file. The checkpoint's `files` map provides no authorization grant. The renderer cannot borrow an editor's personal access while writing an image for board readers. Reject files that do not meet the ownership contract, including policy-less internal blobs, missing MIME/ownership evidence and unsupported, foreign or temporary files; no partial successful output. Bound malformed or nonfinite scene coordinates before transforms and raster calls. If a supported product locator cannot be verified under existing APIs, surface the gap rather than relax the check.

**Alternatives:** Resolving stored URLs, accepting any UUID in JSON, drawing a missing-image placeholder as current, or copying files at render time are unsafe or out of scope. Editor-defined embed placeholders are representational, not image-load failures.

**Format gate:** Fork `packages/excalidraw/constants.ts` lists SVG and raster formats including GIF, WebP and AVIF; server `src/common/enums/mime.file.type.visual.ts` permits several of these. Determine the effective format set from actual stored outputs of normal/editor import paths and file-service normalization. Match the browser's static animation frame behavior. Decode SVG only with a bounded, inert Go path with no external resources, scripts or foreign objects; fail closed on unsupported paint operations. Canvas's SVG parser can silently omit unsupported nodes, so parse success is not parity. If a supported format cannot be rendered safely, stop and surface the coverage gap before choosing dependencies.

## R5 — Whiteboard raster and font proof

**Decision:** Use the existing `go-yjs` core to project the saved scene, preserving the fork's stacking order, saved element geometry, text lines and style values. The first renderer candidates are [tdewolff/canvas](https://github.com/tdewolff/canvas/tree/891200a8cf2b) for transforms, text/paths and Go rasterization, and [d2lang/rough-go](https://github.com/d2lang/rough-go/blob/v0.2.0/README.md) for deterministic rough paths. Treat them as candidates until the full fixture gate, not as dependencies already approved to ship. Use one full-scene composition, fit it into each bounded output without scene-wide intermediate allocation. Use the editor's licensed fonts and explicit script fallback, never host/browser downloads.

**Rationale and measured proof:** In an external scratch module using Go 1.26.10 with `CGO_ENABLED=0`, MIT candidates `canvas` `v0.0.0-20261118111642-891200a8cf2b` and `rough-go` `v0.2.0` compiled and emitted 500×200 and 410×256 shape+Latin-text PNGs. Canvas parsed all seven sampled Excalifont subsets and all 209 Xiaolai subsets, including verified sample Han glyphs. At seed 177, seven ordered path-operation comparisons against the fork's `roughjs@4.6.4` were equal within `1e-6`: solid/hachure/cross-hatch/zigzag dashed rectangles, curve, curved path and cross-hatch ellipse. The port documents Rough.js 4.6.6 output parity for the D2 profile, not a guarantee for all editor options. The Latin PNG/clip-free path test neither verifies shaping and fallback layout across scripts nor qualifies scene parity. The fork also pins `perfect-freehand@1.2.0`; neither candidate replaces its saved-point-to-outline behaviour. The headless editor API is not a renderer.

**Pre-implementation gate:** Compare bounded Go fixture output to the exact editor's browser export at matched full-board framing. Exercise saved order/rotations, all supported fill/stroke options and dash/seed values, arrowheads/bound text, freehand with pressure, image scaling and clipping, embeddable placeholders, frames, glyph fallback and wrapped text. Test both output sizes. Gate on content/anchor/clip correctness, with reviewed tolerance only for rasterization differences. Confirm pinned font/transitive licenses and `CGO_ENABLED=0` Linux build; verify geometry, image and text before choosing backend or reporting parity. An uncovered material gap stops the renderer lane for a source-compatible design; do not ship a simplified fallback.

**Oracle details:** Fork `packages/element/src/{types.ts,shape.ts,fractionalIndex.ts,Scene.ts}` defines the type set, Rough/arrow and pressure-stroke paths, fractional ordering and ID tie-breaks. Verify legacy missing-index fixtures; never use Go map iteration as stacking order. `packages/excalidraw/scene/export.ts` computes padded world bounds independently of the viewport. `renderer/staticScene.ts` layers bound text and inert embed placeholders and clips frame content. `packages/element/src/renderElement.ts:890–982` derives arrow-label paint anchors from saved points; preserve paint rules without rerouting arrows. Canvas's `Clip` resizes the canvas, not a scoped clip stack: prototype an output-sized rounded mask and preserve element × frame alpha. Use the pinned `fonts/bundledFontFaces.ts` manifest and audit the face licenses and script fallbacks; the Xiaolai WOFF2 subsets alone are about 13 MiB, so don't decode every subset eagerly. The scratch module also cross-built to static Linux/amd64; that does not prove a release-sized font bundle or deployed performance.

**T009 asset qualification:** [The isolated probe](../../test/e2e/previeworacle/README.md) used the published fork 0.5.6 browser exporter with a 64×48 image asset at a negative scene origin. Real Chromium rendered a plain SVG rectangle, an SVG-local `<use>`, and a locally referenced green SVG pattern with center RGBA `33,166,95,255` in both normalized sizes (500×200 and 410×256). The plain control agreed in both initial native candidates. The pinned canvas parser silently dropped `<use>` (transparent center); [oksvg](https://github.com/srwiley/oksvg/tree/be6e8873101c), a BSD-3-Clause alternative, restored that case. **Both decoders silently painted the local pattern black** (RGBA `0,0,0,255`) and ignored a local mask that should hide the green center, even with oksvg's strict mode. The pinned file-service's `internal/imaging/processor.go:85–88` explicitly preserves SVG bytes, so the difference is not normalized away on storage.

**Next candidate, not a full renderer qualification:** [Aster v0.2.0](https://github.com/mgilbir/aster/tree/v0.2.0) (BSD-3-Clause) matched all four center pixels using its Go SVG-to-PNG path. It refused an external image without connecting to the loopback test upstream and returned no bytes after cancellation. The probe built statically on Linux/amd64 with CGO off under the pinned Go 1.26.10. Source trace: `aster.go:646–706` invokes its native rasterizer and PNG writer; the transitive [goecma262](https://github.com/mgilbir/goecma262/tree/v0.3.2) is a regex engine, not a general JavaScript interpreter. The later v0.3.0 SVG probe also matched the four pixels, but [requires Go 1.27](https://raw.githubusercontent.com/mgilbir/aster/v0.3.0/go.mod); upgrading the service is unnecessary for this candidate's initial test. Keep Aster as an asset-rendering candidate only. This does not prove whole-board projection, stacked/rotated/clipped elements, pressure paths, font/script parity, all stored codecs, licensing of any eventual font bundle or render-burst performance. T009 has not passed; no production dependency was added.

**T009 direct saved-state slice, 2026-11-24:** [The reproducible oracle](../../test/e2e/previeworacle/README.md) creates an actual editor `Scene`, then uses `encodeStateAsUpdate("v2")`. The Go probe reads the resulting nested `elements` map, sorts live elements by saved fractional index and ID, ignores the persisted tombstone, reads the saved background and uses only out-of-band fixture bytes for the saved asset locator. Go computes bounds and creates the paint paths itself with Rough-Go; no browser SVG is an input to this run. The SVG asset is separately rasterized to PNG before embedding, because Aster 0.2.0 refuses nested SVG image data in a composed SVG. This is one rasterizer used for both operations, not a format-specific rendering fallback.

The probe matched editor source bounds and paint order for a four-element board (hatched rectangle, rotated translucent ellipse, dashed diamond, and rotated patterned SVG image), the same board plus centered two-line accented Latin text using the pinned Excalifont WOFF2 face, and a 256-element repeat grid. The decode→paint→two-PNG path built as a static Linux binary with CGO off. On Apple M1 Max, observed cold in-process times were approximately 22 ms, 25 ms and 76 ms respectively, excluding oracle reading and pixel comparison. A separate 256-element run peaked at 56 MB RSS; it used a single repeated asset, not an image-heavy load. This is a smoke measurement, not a capacity limit.

| Fixture | Painted pixels differing by ≥32 in any premultiplied RGBA channel versus fitted editor Canvas, 500×200 / 410×256 |
|---|---|
| Four live elements | 3.9% / 3.9% |
| With saved Latin text | 1.8% / 2.7% |
| 256-element grid | 20.1% / 19.4% |
| Named frame with overflowing child | 1.9% / 1.1% |
| Triangle/label and dotted elbow with circle/open arrowhead | 1.3% / 2.4% |
| Saved stylus pressures and simulated mouse pressure | 0.9% / 2.2% |
| Latin, Greek, Cyrillic, and common CJK fixture | 4.6% / 4.1% |
| Rotated, mirrored, square crop of an SVG asset | 2.7% / 2.1% |
| Separate SVG, PNG, JPEG, WebP, GIF, BMP and AVIF sources | 0.6% / 1.1% |
| Rounded rectangle with clipped cross-hatch and label, dotted rounded diamond | 3.9% / 4.7% |
| Inert web and iframe placeholders layered above normal geometry | 0.6% / 1.2% |
| Accented Latin sample across all nine current/legacy family IDs | 2.6% / 2.9% |
| All saved arrowhead variants, including cardinality markers | 0.6% / 0.9% |
| Cleared scene persisted as tombstones | 0% / 0% |

For the grid, comparing target-scale Canvas rendering to the editor's own 1×-then-resample path gave 22.8% / 22.8% by the same metric. Native output versus the target-scale editor gave 8.0% / 18.2%; the grid still needs tight per-feature checks. In visual inspection at 500×200 the objects and placement were present and the native path drew smoother small-scale strokes. These observations do not establish an acceptance threshold.

The new frame fixture verifies that an overflowing orange child is visible inside the frame but is clipped outside; cropping verifies the green/blue interiors after saved flip and rotation at both dimensions. The pressure probe compared all 56 points in each stroke to exact perfect-freehand v1.2.0, with absolute tolerance `1e-9` before rasterization. The codec fixture checks actual pixels from independently encoded SVG, PNG, JPEG, retained WebP and GIF (first frame only); file-service's size-guard fallback for WebP makes this a real persisted format, not just an upload format. The individual small paths took roughly 15–25 ms cold on the same host, not including final raster comparison.

The rounded fixture found an actionable candidate mismatch: Rough-Go exposes a
patterned curve as a clip and a separate sketch; treating it as a plain path
painted the shape solid black. Drawing the sketch in element-local coordinates
within the clip fixes it; the probe asserts both colored lines and clear gaps.
The placeholder fixture verifies that the network is never asked to load embed
content and both placeholders are on top of ordinary geometry. Its captions
are short and do not qualify long wrapping. The Latin family fixture was also
inspected visually: glyphs from all nine fixture families are present; local-only
Helvetica is represented deterministically by Liberation Sans.

File-service's `canonicalizeRaster` keeps canonical BMP and AVIF in their
original format, not necessarily PNG. The saved codec matrix additionally
checks an opaque BMP and a baseline AVIF against Chromium. The candidate uses
`x/image/bmp` and [gav1d v0.2.10](https://github.com/gen2brain/gav1d/tree/v0.2.10/avif)
(BSD-2-Clause; see its COPYING and PATENTS notices). This AVIF probe passes a
codec frame-size limit as well as checking the container dimensions before
decode. An opaque AVIF fixture is not evidence for wide-gamut/HDR or auxiliary
alpha; those still need format-level qualification before dependency selection.

**T009 selected path:** Use Aster 0.2.0 + Rough-Go 0.2.0 for the bounded Go
render, the standard Go raster codecs and `x/image` for BMP/WebP, with the tested
gav1d pin for AVIF. Keep rendering and memory budgets in Go and treat unknown
or unsafe asset constructs as explicit failures, not partial successful images.
The final scratch run asserted placement, bounds, clipping, surviving underlay
color under an arrow label, true shape/line styles, and rejection of untrusted
SVG content. Round-trip fractional-index ties matched the saved editor view.
The server's `whiteboard.scene.to.yjs.v2.state.ts:108–123` normalizes legacy
indices before saving, and the fork normalizes editor-created indices before
persistence. The deliberately malformed no-index checkpoint was refused
instead of inventing order from a Go map.

Compared against the editor 1×-then-fit path, the small corpus allows at most
8% of painted pixels to differ by 32/255 in any premultiplied RGBA channel and
an average channel error of at most 6/255. Use the target-scale Canvas control
for densely packed scenes (at most 20% and 12/255, respectively); the two
browser paths already disagree materially there. Those are fixture tolerances,
not a substitute for the independent placement/clip/order/character checks.
No output dimensions or published source semantics are relaxed.

The CJK fixture selected 15 of 209 Xiaolai subsets by their pinned font-face
ranges. It required an additional pinned [Noto Sans CJK JP
Sans2.004](https://github.com/notofonts/noto-cjk/releases/tag/Sans2.004) fallback
for U+20BB7, which appeared as a missing glyph without the supplement. This
run does not prove universal character coverage. Licensing for packaging:
the fork declares OFL-1.1 for Excalifont/Xiaolai and MIT for Comic Shanns;
[Virgil](https://github.com/excalidraw/virgil/blob/main/LICENSE.md),
[Cascadia](https://github.com/microsoft/cascadia-code/blob/main/LICENSE),
[Nunito](https://github.com/google/fonts/blob/main/ofl/nunito/OFL.txt),
[Lilita One](https://github.com/google/fonts/blob/main/ofl/lilitaone/OFL.txt),
[Assistant](https://github.com/google/fonts/blob/main/ofl/assistant/OFL.txt),
and [Noto CJK](https://github.com/notofonts/noto-cjk/blob/Sans2.004/LICENSE)
have OFL notices; use the actual Aster-supplied Liberation Sans font/notice,
not an unrelated legacy distribution. Retain the notices of exact embedded
faces, Aster and its linked dependencies, Rough-Go and the perfect-freehand
algorithm port. Production font packaging and actual assembled artifact
regressions are T014/T019, after this standalone qualification.

**Comparison with govips:** File-service [develop at `259dda0d`](https://github.com/alkem-io/file-service/blob/259dda0d4ac517184d03bd67779b593f4c09518d/go.mod) pins upstream govips **v2.19.0** with no replacement. Against local libvips 8.18.4 and librsvg 2.62.3, it gave the same observed pixel comparisons as the older file-service fork on six geometry/frame/text SVG specimens. It rasterized nested SVG assets, but the pinned editor's own SVG and Canvas outputs already differ materially on those specimens. Govips therefore does not remove the need for a paint projection or justify changing the CGO-free build; use the native Aster/Rough-Go slice for further qualification. Neither dependency is selected for production.

**Alternatives:** [Go `x/image/vector`](https://pkg.go.dev/golang.org/x/image/vector) plus [go-text shaping](https://pkg.go.dev/github.com/go-text/typesetting/shaping) is a fallback to evaluate if the candidate package has a material gap, not a second runtime renderer. Generic rasterizer alone cannot implement Excalidraw semantics. Browser workers, JS execution and external render services fail the runtime constraint.

## R6 — Memo fidelity and routing

**Decision:** Read metadata first by ID and type, require an existing usable checkpoint, load using the existing `CheckpointStore`, then decode a private throwaway doc with the declared codec. Traverse the `default` fragment's ordered nodes, attributes and formatted text deltas. Serialize only the current memo schema; use exact oracle behavior for Markdown escaping and whitespace. Route `GET /internal/memos/{memoId}/markdown` on a separate always-on private listener with a loopback default. Leave the existing HTTP/WS router and gateway path unchanged. Protect the listener at the network boundary (TLS and restricted trusted-service connectivity are preconditions to cross-service reachability), not with document or actor authentication.

**Rationale and measured proof:** The server `src/domain/common/memo/conversion/{yjs.state.to.markdown,markdown.schema}.ts` is the compatibility authority. `memo.service.ts:166–199` stores a real checkpoint even for empty Markdown; an absent pointer or bad snapshot is not a valid empty memo. Scratch run using actual JS Yjs `13.6.27` + exact pinned server converter and TipTap dependencies versus Go 1.26.10 + `go-yjs` `v0.1.1` matched 468 Markdown bytes from a 746-byte V2 fixture. It included Unicode, heading, nested bold/italic, link/image attributes, blank-line NBSP, mixed nested lists, table blank cell and iframe HTML. Empty matched 0 bytes; invalid nine-byte input was refused. Established structured traversal, not full or hardened serializer parity. Preserve the reference's `1.` ordered markers, table-specific hard breaks/empty cells, and inert embed representation; do not request those URLs. The current collab `internal/adapter/inbound/http/router.go` mounts the public WS/operational surface on one listener; a `/internal` prefix on the same mux is not isolation. The server `.build/traefik/http.yml:385–406` is supporting quickstart evidence, not an authorization control or a production route audit.

**Alternatives:** Reusing the server request-time converter, flattening XML with `ToString`, adding a cache or a batch API, or mounting on the public server behind an unverified rewrite all violate scope. No app-level credentials or actor lookup on the private endpoint. Full fixtures, cancellation, output bounds and dual-listener tests still required.

## R7 — Bounded runtime and qualification

**Decision:** Before production work in either lane, measure representative saved memo reads or isolated whiteboard render bursts on the same host as the baseline, and record budgets justified by the results. Share existing transport/backend limits where applicable. Set memo input, traversal, output and concurrency bounds in T001; set whiteboard image, render and queue bounds in T009. One process-local whiteboard worker is the starting topology. Check limits before allocation, between processing steps and on outbound I/O; hold no room/doc handle while rendering. T020 repeats the measurements on the combined facility. The scratch probes in R5–R6 show viability, not completed resource qualification.

**Rationale:** Go work is not interruptible just by expiring a context. Bounds on snapshot bytes, node count/depth, image decode output, raster dimensions and work units are needed even with a deadline. Reject or drop bounded work visibly, preserve the last good preview, and record stage/outcome counters and duration through existing logging/metrics; never log document contents, tokens or private source URLs. No invented production SLA or borrowed MCP capacity. The feature is not rollout-ready until fixture and collaboration-responsiveness evidence passes.

**T001 memo qualification:** On Go 1.26.10 (darwin/arm64), scratch reads of real Yjs V2 fixtures included local checkpoint I/O, decode and Markdown traversal. Five paired reads of the 642,931-byte (2,000-paragraph) fixture took 15.4 ms mean per pair, allocated 28.0 MB/read, produced 554,886 Markdown bytes, and peaked at 43.5 MB RSS. The 3,490,937-byte (6,000-paragraph) fixture took 64.1 ms per pair, allocated 122.7 MB/read and peaked at 151.8 MB RSS. A 15-level nested-list fixture took 126 µs per pair. Set initial memo-only bounds at 1 MiB encoded, 2 MiB rendered, 16,384 visited nodes/text runs, depth 32, and two concurrent reads; cap the read before parsing, and use a 5-second handler deadline for I/O/cancellation, not a latency promise. These fixture measurements do not include a remote backend; T008 remeasures through the assembled app. The re-run baseline race suite passed and the unit-only check reproduced 92.69%; D2 remains the final coverage gate.

**T008 assembled memo measurement:** The same 642,931-byte fixture, through the private HTTP handler, the real file-service adapter against a loopback blob fixture, and the Go serializer, took 27.7–28.7 ms per pair across three ten-pair runs on Apple M1 Max. Allocation was 84.7 MB per pair (42.4 MB/read); the compiled test process peaked at 61.8 MB RSS. The stage-one bounds remain unchanged. The earlier numbers used a prototype traversal; guarded serializer output length was 556,885 bytes per read. This is a local adapter measurement, not an RMQ or deployed load test.

**T009 image-heavy slice:** 32 distinctly encoded 256×256 PNGs, one SVG
and three other elements (18.6 KB saved checkpoint, 629×383 world bounds)
compiled into two outputs in 0.93 s, at 1800×720 and 410×256. Peak RSS
was 90.2 MiB, including browser fixture JSON/PNG comparison. Pixels differing
by at least 32 were 2.4% / 1.9% of painted pixels against the editor Canvas.
This covered the largest target output and separate assets, not a whole
queue under contention or the maximum accepted encoded input.

**Initial implementation bounds:** Use a 1 MiB saved-render input, 2,600
scene records, up to 160,000 saved point/pressure values and an 8 MiB
serialized projection cap, with a shared deadline through both rasters.
The 2,500-element/945,125-byte fixture took 0.50–0.51 s with a 1,800×720
output and a 410×256 card after quantizing paint-path serialization to
0.01 scene units. All 32 distinct fixture PNGs plus one SVG total about
2.1 megapixels decoded; cap decoded pixel work cumulatively and cap
individual sides/encodings separately. The isolated serial burst (large,
image-heavy, CJK, large) finished four projections in about 2.06 s of
generation time, peaked at 130.5 MiB RSS including fixture comparison,
and did not retain outputs across boards. Start with one renderer and four
pending board IDs rather than a parallel worker pool; the small queue
bounds stale demand and rejects excess offers. These are first test
bounds, not a production latency/SLA claim. Recheck memory and the chosen
asset-size ceiling under assembled load before enabling a deployment.

**Alternatives:** No unbounded goroutine launches, recurring queue, process-distributed scheduler, isolated subprocess renderer or persisted retry queue. Reduce contention by admission and bounded geometry instead of changing collaboration save/close semantics.
