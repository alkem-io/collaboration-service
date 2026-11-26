# Quickstart: Validate native Go previews

Use an isolated test stack only. Artifact publication runs for both supported document types. No public consumer or deployment is changed.

## 1. Qualify against the pinned oracles

- Check the source heads, fork tag, toolchain and two scratch results in [research.md](research.md). Reproduce on the exact chosen library bytes. Verify license notices and a static Linux `CGO_ENABLED=0` build before selecting the dependency set.
- Export a small, representative native editor scene: mixed shapes and rough fills, elbow arrows and labels, pressure strokes, ordered overlaps, clipped frames, rotated/cropped images, Unicode/fallback text, inert embeds and wide/tall/negative bounds. Compare the Go images with the pinned browser export normalized to the same full-board framing at both target sizes. Exercise the saved image format matrix, including the static behavior of GIF and the supported inert SVG cases. Compare geometry, glyphs/line breaks, masks and clipping before defining tolerance for pixel edges. Do not accept parity on a visually incomplete or unsafe result.
- Exercise the full pinned memo fixture matrix plus real saved XML snapshots. Compare the complete Markdown to the pinned server converter byte-for-byte where the oracle specifies formatting. Include valid empty, malformed, oversized and canceled reads. Neither oracle is part of the production process or image.
- Before writing production code for a lane, use its isolated fixture proof and representative saved inputs to record initial budgets in [research R7](research.md#r7--bounded-runtime-and-qualification). For memo, measure saved-read/decode/conversion latency, memory and output across input size, tree depth and concurrent reads. For whiteboard, measure image-heavy and large scenes plus a representative burst of queued closes to set image, render, work and queue bounds. Use a matched feature-off baseline and report measured limits, not guessed defaults. §5 repeats the experiment with both lanes wired.

## 2. Run the repository gates

From the collaboration-service root, record the unit-only baseline before implementation. At final feature verification, require **≥95% repository-wide unit-only statement coverage**. The memo MVP can be verified independently before this final gate. Do not include tagged integration or E2E results. Saved profiles merge repeated blocks by source range before the threshold is checked:

```sh
preview_unit_dir=$(mktemp -d) &&
  go test -race -count=1 -tags='' -covermode=atomic -coverpkg=./... \
    -coverprofile="$preview_unit_dir/unit.cov" ./... &&
  awk 'NR > 1 { n[$1] = $2; if ($3 > 0) hit[$1] = 1 }
    END {
      for (block in n) { total += n[block]; if (hit[block]) covered += n[block] }
      if (!total) exit 1
      printf "Unit-only statement coverage: %.2f%% (%d/%d)\n", 100 * covered / total, covered, total
      exit (100 * covered < 95 * total)
    }' "$preview_unit_dir/unit.cov"
```

Record a red baseline and use its unit profile to pick existing behaviors that lack meaningful regression tests. Close the shortfall during development; the baseline itself does not block starting. Do not substitute merged coverage or pad tests to clear the floor. After implementation also run the existing combined coverage and repository gates:

```sh
CGO_ENABLED=0 go build ./...
go vet ./...
go vet -tags integration ./...
make lint
(cd test/e2e/jsinterop && npm ci) # test fixture and compatibility tools only
make e2e
make integration
make coverage-gate
```

Use a disposable, local RabbitMQ `RABBITMQ_TEST_URL` for the real adapter lane; ensure relevant tests did not skip. The existing in-process test ports are sufficient for negative and wiring tests, but mocks alone do not prove a stable write through real file-service.

Memo-only increment (historical): race/unit coverage was **92.54%** (2,569/2,776) and the scoped combined gate measured **93.0%**. The unmodified base intermittently fails `TestHandshakeBatchIsNotShedOnASmallSendBuffer` (8/100 on a clean archive of `1fb52e1`); no failing run is counted as a coverage pass.

Shared artifact run (historical): build (static CGO-free Linux/amd64 binary), vet, lint,
WebSocket E2E, real loopback file-service and the public gateway isolation
check passed. The packaged renderer also passed the full 24-case browser
corpus, including the pressure and rounded-label geometry checks and both
large stress fixtures. Live RabbitMQ tests require a disposable broker and
were not exercised on this host. At that increment both 95% coverage gates were red:
**93.08% unit-only (3,673/3,946 statements)** and **93.4% scoped combined**
with Traefik enabled. The real file-service path was run separately. No
threshold, filter or production code was changed simply to inflate coverage.

Save-window update (T023): build, vet, lint, race-enabled room tests, full
WebSocket E2E, and the dedicated file-service and Traefik checks passed. Unit-only
coverage was **93.08% (3,701/3,976 statements)** and scoped combined coverage was
**93.5%** with the real file-service and Traefik checks included. Live RabbitMQ
was not configured in that run.

Initial PR verification (T021): **95.19% unit-only (3,800/3,992 statements)**,
with the race detector and no tagged tests, and **95.4% scoped combined**,
with the full WebSocket suite, real loopback file-service, and Traefik. Build,
vet (both tag sets), lint, and the race-enabled WebSocket suite passed. New
regressions pin bounded memo traversal without partial output, visible minimal
strokes and placeholders, verified image bytes, safe storage failures, and
broker/room teardown. No threshold or scope filter was changed.

The one-slot WebSocket handshake could previously shed its own joining client:
activation sent the first participant count while the last initial frame still
occupied the queue. The handler now waits for one slot under the existing
handshake deadline before activating. Wire order and ordinary backpressure are
unchanged. The original failure reproduced four times in 80 attempts; the
corrected path and its deterministic boundary tests passed 100 race-enabled
repeats.

The Linux/amd64 release binary is statically linked and reports `CGO_ENABLED=0`.
The pinned Alpine runtime layer was inspected, `ca-certificates` has only shell
and system-library dependencies, and the Dockerfile copies only the binary and
license notices from the builder. No browser, JavaScript runtime or test tool
is required. A disposable local Docker engine failed to initialize its guest
networking; the actual image build and RabbitMQ-backed integration tests were
not run locally. The existing PR image and integration workflows exercise
those checks. No deployment or consumer cutover was performed.

Review follow-up: **95.31% race-enabled unit-only (3,844/4,033 statements)**.
The real file-service and Traefik walk still verifies stable output IDs, and
now compares the stored memo marker and body exactly. A controlled-clock
test observes no publication offer on unchanged reopen, independent of the
background worker. The pinned browser's [canvas framing](https://github.com/alkem-io/excalidraw-yjs/blob/90b57f27faa35b570844c2f277e277cf450a7e73/packages/excalidraw/scene/export.ts#L565-L574)
uses common geometry bounds plus padding; the renderer intentionally matches
its clipping for ordinary shapes with strokes wider than the padding.

Single-PNG update (T024): **95.33% race-enabled unit-only (3,818/4,005 statements)**
and **95.6% scoped combined**. Build, lint, tagged vet and a race-enabled
file-service walk passed. The walk verified the single 1800×720 file, stable ID,
unchanged legacy card bytes and unchanged memo publication. The combined run
included the full WebSocket suite, disposable file-service and Traefik; live
RabbitMQ was not configured locally.

## 3. Verify the whiteboard artifact, not just worker completion

**Backend contract preflight (not the T019 walk):** Archive file-service at
`259dda0d4ac517184d03bd67779b593f4c09518d` in a temporary directory and
start a disposable loopback Postgres with its guarded schema fixture
(`db/schema/{document,outbox}.sql` and `uuid-ossp`). In the pinned HTTP
router, provision a PNG with `POST /internal/file`: multipart fields
`displayName`, `storageBucketId` and `externalReference` precede `file`;
leave `authorizationId` unset. The preflight received 201. Scoped reference
lookup returned 200, the same lookup in a different bucket returned 404, and
`GET /rest/storage/file/{id}` with the anonymous sentinel returned 403.
A raw PNG `PUT /internal/file/{id}/content` returned 200, changed the content
hash, and preserved the ID, bucket, reference and missing policy. Both
`GET /internal/file/{id}/content` and `GET /internal/blob/{hash}/content`
decoded the new green pixels; only the one provisioned row was added. The
local database was stopped. This verified the existing file-service behavior
without using the publication worker or touching deployed environments.

1. Use real, disposable file-service. Seed a board and a memo checkpoint through its existing API with no preview files. Start collaboration-service in its supported single-writer, file-service-backed topology; the worker needs no enable flag.
2. Send saved edits and leave each connection open through the five-second window. Look up the outputs read-only by scoped reference: one 1800×720 PNG and one Markdown file, no card. Seed a legacy card file and verify its bytes stay unchanged. For both document types, repeat with a saved clear and close before the next window; compare the actual bytes at the same IDs. Reopen and close unchanged after restart: no output writes or allocations.
3. Verify private IDs cannot be fetched through the public file route. See the unit cases for stale candidates, cancellation, conflicting or failed overwrites, and an unavailable/foreign asset. Test the existing owner deletion event and bucket cascade, not the earlier unobservable draft timestamp.

Run the loopback-only WS and real file-service path:

```sh
COLLAB_TEST_PREVIEW_FILE_SERVICE=http://127.0.0.1:57353 \
  go test -race -tags e2e ./test/e2e -run '^TestDocumentPreviewsPublish' -v
```

The test checks that the open whiteboard and memo publish one file each, only two POSTs from collab. A saved clear followed by normal release replaces the PNG with white pixels and the Markdown with a visually empty body, preserving both IDs. The independent memo endpoint returns its exact iframe wrapper without the file marker. Public file requests for the generated artifacts return 403. Deterministic room tests pin multiple save windows, the final submission ahead of the next window, and zero offers on an unchanged reopen of a saved board or memo.

## 4. Verify the memo route boundary

Start the service's public and always-on private listeners in separate local test acceptors. The private listener defaults to `127.0.0.1:4016`; only the bind address is configurable. Also run the isolated public gateway with the pinned server quickstart collaboration route, which targets only the public listener. Confirm normal collaboration through that gateway as a positive control. Seed a valid nonempty memo, valid empty memo, non-memo ID and unreadable checkpoint. Have the harness provide the test addresses and opaque IDs:

```sh
curl -isS "$PRIVATE_URL/internal/memos/$MEMO_ID/markdown"
curl -isS "$PUBLIC_URL/internal/memos/$MEMO_ID/markdown"
curl -isS "$PUBLIC_GATEWAY_URL/internal/memos/$MEMO_ID/markdown"
```

The first is raw Markdown with `text/markdown; charset=utf-8`. Neither public request may reach the private handler, call its dependencies or return saved Markdown. Record the gateway and direct-listener results separately; an unrelated gateway fallback is not a memo success. Repeat the private check for empty and failure cases and assert the [contract statuses](contracts/facility.md#memo-get-internalmemosmemoidmarkdown). Verify zero auth, writing and room calls, and that cancellation reaches the backend. Do not alter deployed routing or enable the private listener on a public port to qualify it.

Reproduce the assembled-app test with the checksum-verified Traefik 2.11.20 binary pinned by the server quickstart:

```sh
COLLAB_TEST_TRAEFIK=/absolute/path/to/traefik \
  go test -race -tags e2e ./test/e2e -run '^TestMemoMarkdown' -v
```

T008 result: private cold nonempty Markdown matched the pinned TS oracle; saved clear returned 200 with an empty body; wrong type returned 404 and a known memo without a checkpoint returned 502. Both direct public and Traefik requests returned 404 without reaching the private handler. The same Traefik process successfully upgraded a normal WS session and returned its saved content. This used the pinned quickstart `/collab` routing rule with a loopback upstream; it did not deploy or test identity middleware. Contract tests separately verify cancellation, read saturation and public/private listener isolation.

## 5. Record measured impact

Recheck the initial bounds recorded in §1 under combined load. On the same host, compare save/barrier responsiveness and process CPU/memory with the feature off and under representative whiteboard render bursts plus memo reads. Include image-heavy and large supported saved scenes, full bounded queues and injected storage failure. Adjust only from measured evidence, confirm no effect on source bytes or close semantics, and record what was actually measured. Report service-only proof separately from the future live identity handoff and protected routing; no public/client cutover is part of this run.

The in-process pressure test starts a close-triggered preview, fills its four pending slots, rejects the fifth, and holds two cold memo readers inside checkpoint reads across the timed save. The closing room has already released while the preview slot is held. The optional mode also checks that the renderer produces both outputs for a 2,300-element fixture; that mode does not establish whether rasterization overlapped the save sample. Interpret its barrier duration only as a queue-pressure measurement, not concurrent raster latency or a deployment SLA. [Research R7](research.md#r7--bounded-runtime-and-qualification) separately records the large/asset-heavy oracle burst.

```sh
go test -race ./internal/domain/service -run '^TestBlockedPreviewAndFullQueue' -count=5
COLLAB_MEASURE_PREVIEWS=1 go test ./internal/domain/service -run '^TestBlockedPreviewAndFullQueue' -v -count=5
```
