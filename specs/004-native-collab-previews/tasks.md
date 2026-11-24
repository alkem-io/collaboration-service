---
description: "Implementation tasks for native Go collaboration previews"
---

# Tasks: Native Go Collaboration Previews

**Input**: [Spec](spec.md), [plan](plan.md), [research](research.md), [data model](data-model.md), [contracts](contracts/facility.md), [quickstart](quickstart.md).
**Scope**: Collaboration-service and isolated tests only. No changes to other services, deployment manifests, existing producers, or collaboration wire semantics.
**Format**: `[P]` marks tasks that can run together after their stated prerequisites, on separate files. Story labels refer to `spec.md`. Paths are relative to the collaboration-service root. Write the tests before product code and show that they fail for the intended reason.

## Phase 1: Setup (shared)

- [X] T001 Refresh the source and toolchain pins and record the existing room, HTTP, metadata, checkpoint, app and separate unit-only coverage baselines from `specs/004-native-collab-previews/quickstart.md` §2 in `specs/004-native-collab-previews/research.md`. Before T003, measure representative saved memo read/decode/conversion, including concurrent reads, and record initial memo budgets in research R7. Report any pre-existing coverage shortfall separately. Keep reference checkouts and deployed environments unchanged (FR-011).

## Phase 2: Foundational (shared)

- [X] T002 Write failing saved-read tests in `internal/domain/service/saved_projection_test.go` for typed metadata, absent and wrong-type IDs, valid empty versus missing/corrupt checkpoints, cancellation, and no room or source write calls (FR-001).
- [X] T003 Reuse the typed metadata lookup and existing `CheckpointStore` in `internal/domain/service/saved_projection.go`; load bounded saved bytes and open and release an isolated Y.Doc with the declared codec. Implement [data-model.md — Saved source.Document ID and declared type; Saved source.Content pointer and encoding](data-model.md#saved-source). Keep room restore and all save paths unchanged.

**Checkpoint**: The saved-read path passes without a live room or a second decoder. Either story can proceed from here.

## Phase 3: User Story 1 — Read saved memo Markdown (P1; MVP)

**Goal**: A trusted internal caller reads one saved memo as raw Markdown without application credentials or an editor.

**Independent test**: Seed saved memo checkpoints; compare the private endpoint's body with the pinned server oracle. Check valid empty and failure statuses and confirm the same path never reaches the handler on the public listener.

### Tests first

- [X] T004 [P] [US1] Add failing byte-parity and bounds tests in `internal/domain/service/memo_markdown_test.go` with pinned oracle goldens in `internal/domain/service/testdata/memo-markdown/`, covering the supported schema in FR-004, ordered/nested structures, Unicode/escaping, line breaks, empty input, malformed nodes, excessive work and canceled traversal.
- [X] T005 [P] [US1] Specify the [memo HTTP contract](contracts/facility.md#memo-get-internalmemosmemoidmarkdown) in `internal/adapter/inbound/http/memo_markdown_test.go`; use `cmd/server/private_http_test.go` to prove the dual-listener boundary, including no metadata lookup through public routing and no AuthN/AuthZ calls on the private route. Cover the declared statuses, `no-store`, safe errors and no partial success (FR-002/003, SC-001).

### Implementation

- [X] T006 [US1] Implement bounded, cancellable Go traversal and Markdown serialization in `internal/domain/service/memo_markdown.go` on top of T003; implement [data-model.md — Saved source.default](data-model.md#saved-source) and [data-model.md — Memo Markdown result.UTF-8 body](data-model.md#memo-markdown-result). Match T004 without fetching referenced content, storing Markdown or adding a cache.
- [X] T007 [US1] Map memo outcomes to the contract in `internal/adapter/inbound/http/memo_markdown.go`; wire the private mux and always-on separate listener through `internal/app/app.go`, `internal/config/config.go`, and `cmd/server/private_http.go` and `cmd/server/main.go`. Bound private read admission and request lifetime independently of the public listener. Do not mount the memo handler on `internal/adapter/inbound/http/router.go` or add credential checks.
- [X] T008 [US1] Run the memo-only cold-read, parity, cancellation and listener-isolation checks in `specs/004-native-collab-previews/quickstart.md` §4. Verify neither public path reaches the memo handler or its dependencies; record each result and the gateway's normal WS positive control. Recheck memo budgets without waiting for the renderer or changing deployed routing. The repository-wide coverage gate is checked at T021 (FR-003/011, SC-001).

**Checkpoint**: The internal memo endpoint is independently demonstrable without the whiteboard worker or any renderer dependency.

## Phase 4: User Story 2 — Generate whiteboard previews from saved edits (P1)

**Goal**: Keep both full-board PNGs and the memo Markdown artifact current with coalesced successful saves, including a final refresh on normal close without delaying close.

**Independent test**: Save several browser- and MCP-shaped edits and leave the client open. Verify one update for the window at the same private IDs. Save another change and disconnect; verify publication after normal release without waiting for the next window, no render for an unchanged reopen/close, and blank output in place after a saved clear.

### Qualification and tests first

- [X] T009 [P] [US2] Build the bounded test-only Go/browser oracle proof in `test/e2e/previeworacle/`; compare both full-board sizes against the pinned fork corpus, stored asset codecs and font/script matrix. Record accepted tolerances, CGO-free Linux build and license/pin evidence in `specs/004-native-collab-previews/research.md` R4–R5; measure representative scenes and render bursts to set initial image, render and queue budgets in R7. **Gate:** if semantics, supported formats, bounded work or safety cannot be demonstrated, stop before production renderer code or `go.mod` changes (FR-005/006/007/011, SC-002).
  - The bounded renderer probe passes the saved fixture corpus and static Linux build. [Research R5/R7](research.md#r5--whiteboard-raster-and-font-proof) records the selected dependencies, tolerances, refusal cases and four-render serial burst. This is not an assertion that production wiring, font packaging, or the combined load gate has passed.
- [X] T010 [P] [US2] Test close triggers: a save while open versus release, delete-only clear, unchanged close, idle grace, memo and whiteboard close, owner deletion/shutdown, failed save and unchanged durability barrier behavior.
- [X] T011 [P] [US2] Test asset and output contracts: scoped ownership, trusted hash reads, first create with the unique reserved reference, in-place PUT, valid sibling refresh, and no allocation after failed/conflicting replacement.
- [X] T012 [P] [US2] Test shared worker bounds, staleness, cancellation, invalidation, failure and stable output IDs across edit/clear/reopen for both types. Assert close is not held by generation. Cover configuration boundaries and worker shutdown. Canonical expiry's upstream caller stays outside this repository; test the owner event it uses.

### Implementation

- [X] T013 [P] [US2] Project saved element order, geometry, assets and background in `internal/domain/service/whiteboardrender/scene.go`. Do not mutate the saved scene (FR-005).
- [X] T014 [US2] Implement bounded full-board rendering in `internal/domain/service/whiteboardrender/`, with reviewed embedded fonts, pinned modules and packaged notices. Compare both sizes against the browser fixture set (FR-005/007, SC-002).
- [X] T015 [P] [US2] Implement typed bucket-scoped asset resolution and derivative-specific file-service operations in `internal/domain/service/preview_files.go` and `internal/adapter/outbound/persistence/fileservice/preview_files.go`. Use the trusted hash and reserved [document slot](data-model.md#document-outputs-and-refresh); no changes to `CheckpointStore` (FR-006/008/010).
- [X] T016 [US2] Implement the shared bounded worker in `internal/domain/service/document_preview.go`. Check fingerprint and ownership before each write. Choose PNG or Markdown depending on the declared type. Report queue, failure and publication outcomes without leaking data (FR-008/009/010/011).
- [X] T017 [US2] Track initial and successful saved fingerprints without encoding again. Use the normal room-release and owner-invalidation boundaries without changing save, barrier or idle semantics (FR-001/009).
- [X] T018 [US2] Wire automatic publication in the existing single-writer file-service topology and share bounded reads with the always-on independent memo endpoint (FR-010/011).
- [X] T019 [US2] Run `test/e2e/document_preview_test.go` on disposable file-service: real stable rows, actual image lookup, memo storage, clear/restart, no extra writes and public-access refusals. Use owner event tests and the actual server FK contract as lifetime evidence, not a claim that server cleanup/UI was deployed (SC-003/004/005).

**Checkpoint**: The automatic whiteboard refresh passes independently, with the old producers untouched and no public image endpoint.

## Phase 5: Polish and cross-cutting verification

- [X] T020 Recheck bounded load against a matched control using `internal/domain/service/preview_load_test.go` and quickstart §5. Measure local RSS/CPU and a separate board's save/barrier under dense rendering, concurrent memo reads, full queues and a blocked publication. Actual file-service and asset-heavy bursts are exercised separately (FR-011, SC-005).
- [X] T021 Use the T001 profile during development to add meaningful regression tests for existing behavior in `internal/` and `cmd/server/` (D2); do not pad tests or change unrelated product behavior. At final verification, require ≥95% repository-wide unit-only coverage per `specs/004-native-collab-previews/quickstart.md` §2. Also run build, vet, lint, race, integration, WS e2e and combined coverage from `Makefile` and `.scripts/coverage-gate.sh`. Record both coverage results and any skipped backend checks. Verify the CGO-free production image has no test tool or browser/JS runtime dependency.
  - **95.19% race-enabled unit-only; 95.4% scoped combined.** The final [quickstart evidence](quickstart.md#2-run-the-repository-gates) records the transport regression caught by the gate, the shipped runtime dependency check, and the live RabbitMQ/image-build checks still to be run by PR CI.
- [X] T022 Update `README.md` and `.env.example` with the private memo bind, automatic artifact publication, failure behavior, verification and protected consumer adoption. No server/client/deployment changes (FR-012).
- [X] T023 Replace close-only scheduling with one shared five-second successful-save window and final release submission. Pin the latest saved revision per window, repeat windows while open, skip a fully published revision on release, and prove both artifact types update while connected against disposable file-service (FR-009, SC-004).

## Dependencies & execution order

```text
T001 → T002 → T003
               ├─ US1: T004 + T005 → T006 → T007 → T008
               └─ US2: T009 + T010 + T011 + T012
                         ├─ T013 → T014 ─┐
                         └─ T015 ────────┴─ T016 → T017 → T018 → T019
Both verified stories → T020 → T022 → T023 → T021
```

T009 is a hard gate for the whiteboard production lane only. T018 and T007 share composition/config files; serialize those edits, not the two stories. If the renderer proof fails, US1 can still be verified as an independent increment; do not mark US2 or the combined gates complete.

### Parallel examples

- **US1:** T004 writes serializer fixtures and domain tests while T005 writes contract and listener tests; they do not modify the same files.
- **US2:** T009 qualifies the renderer while T010, T011 and T012 pin trigger, file-boundary and worker/e2e behavior in separate files. After all four pass their prerequisites (red tests as appropriate), T013 scene projection and T015 file-service adapter work can proceed independently.

## Implementation strategy

1. **MVP:** Finish the shared saved-read path and prove US1 on local private/public listeners, without any whiteboard dependency.
2. **Second increment:** Qualify then build and verify US2 during active editing and normal room release with isolated file-service. No deployment or live producer handoff is authorized.
3. **Finish:** Run combined load and repository gates; record what is proven and what still requires future integration.
