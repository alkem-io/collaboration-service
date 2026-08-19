# Tasks: go-yjs Core Port & Backend-Contract Adoption

**Input**: Design documents from `specs/003-go-yjs-core-port/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md
**Constitution**: v3.0.1

**Tests**: **REQUIRED**, not optional. §VI mandates test-first; FR-008/008a require the
core's conformance suites in CI; FR-018a requires every restructured `002` invariant to
be re-proven non-vacuous. Test tasks are therefore first-class throughout.

**Organization**: grouped by user story. Story independence is real but not perfect here
— this is a rebuild of a coordination layer, so the Dependencies section states honestly
where a story leans on an earlier one.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable — different files, no dependency on incomplete work
- **[Story]**: US1–US5, mapping to spec.md user stories
- **T069–T072** were added by `/speckit-analyze` remediation and sit at their correct execution position **within their phase**, so IDs above T068 are not in global numeric order. Existing tasks were deliberately not renumbered, because the Dependencies and Parallel Opportunities sections cite them by number and renumbering would invalidate every such reference. The new tasks are themselves cited in those sections where relevant (T071, T072 under Parallel Opportunities).

## Path Conventions

Existing hexagonal layout (see plan.md Structure Decision). Domain core in
`internal/domain/`, adapters in `internal/adapter/{inbound,outbound}/`.

---

## Phase 1: Setup

**Purpose**: dependency and scaffolding, nothing behavioural.

- [X] T001 Add `github.com/antst/go-yjs` pinned to an explicit version in `go.mod`, and remove the `replace` directive redirecting `github.com/skyterra/y-crdt` (§XIV, FR-024)
- [X] T002 [P] Create the adapter package skeletons `internal/adapter/outbound/persistence/` and `internal/adapter/outbound/hub/` per plan.md Structure Decision
- [X] T003 [P] Wire the core's `backend/conformance` suites into the CI workflow so they run per package — **satisfied without a CI change**: the suites are ordinary Go tests beside each implementation, so the existing `go test ./...` runs them; the workflow comment now records that
- [X] T004 Record the pinned version and the oracle-reverification rule in `specs/003-go-yjs-core-port/research.md` sequencing notes

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: the core swap and document lifetime. **No user story can proceed until this
phase completes** — every story needs documents that exist on the new core.

- [X] T005 Re-point the CRDT core across the four non-test domain files: `internal/domain/service/{room.go,sync.go,awareness_wire.go,convention.go}` — imports and types only, no behavioural change yet (FR-001)
- [X] T006 Re-point the CRDT core across the test/e2e surface: `internal/domain/service/*_test.go`, `internal/adapter/inbound/ws/handler_test.go`, `internal/app/app_integration_test.go`, `test/e2e/*.go`
- [X] T007 Adopt `memory.Registry` as the owner of document identity and lifetime in `internal/domain/service/manager.go`, replacing the hand-built registry and singleflight acquire (FR-005, D3)
- [X] T008 Implement the registry open function in `internal/domain/service/manager.go`: load from the store, else seed from the metadata-delivered content; a session must never observe a partially initialised document (FR-004a)
- [X] T009 Rebuild `internal/domain/service/room.go` to hold a `memory.Handle` and observe its invalidation signal, ceasing to own document identity or teardown ordering (D3)
- [X] T010 Retire the parts of `internal/domain/service/lifecycle_state.go` that duplicate registry semantics; keep only what the registry does not absorb (D3)
- [X] T011 Keep the `002` idle-release policy driving `Evict` in `internal/domain/service/room.go` — the registry starts no goroutines and has no eviction policy of its own (contracts/registry-session.md)
- [X] T012 Delete `y-crdt` from the module graph and verify zero references remain (`grep -rn "y-crdt" --include="*.go" .`, `go list -m all`) (SC-008)

**Checkpoint**: `go build ./...` clean, `go test -race ./...` green, no `y-crdt` references.

---

## Phase 3: User Story 1 — Collaboration works over the new core (P1) 🎯 MVP

**Goal**: browsers converge over the new core with no client-side change.

**Independent test**: run the existing e2e harness (single-pod and two-pod) and the
JS-interop suite; assert convergence for both document conventions.

### Tests for User Story 1

- [X] T013 [P] [US1] Add a malformed/truncated-frame fuzz test in `internal/adapter/inbound/ws/` asserting offender-only failure — zero room teardowns, zero effect on other members, zero process crashes (SC-019, FR-009c)
- [X] T014 [P] [US1] Extend `test/e2e/jsinterop_test.go` coverage to assert handshake and awareness interop on the new dispatch path (SC-001)

- [X] T071 [P] [US1] Assert the convergence **bound** in `test/e2e/singlepod_test.go` and `test/e2e/twopod_test.go`: connected clients reach identical state within 1s after edits settle, in both modes (SC-002)

### Implementation for User Story 1

- [X] T015 [US1] Rebuild `internal/domain/service/sync.go` on the core's `protocol.SyncHandler`, registering a sync-type override so the byte budget and view-only write rejection still run **before** anything is applied (FR-009b, D4)
- [X] T016 [US1] Rebuild `internal/domain/service/awareness_wire.go` on the core's awareness dispatch, keeping the custom ephemeral channel distinct from y-awareness (FR-009b)
- [X] T017 [US1] Use allocation-free frame inspection for the byte-budget pre-check in `internal/domain/service/limits.go`, so the check works from frame length without decoding (D4)
- [X] T018 [US1] Delete the parallel sync state machine left behind in `internal/domain/service/`, retaining only policy (FR-007, §VIII)
- [X] T019 [US1] Verify both conventions in `internal/domain/service/convention.go`: memo `Y.XmlFragment` and whiteboard id-keyed `Y.Map` per-property merge (FR-003)

**Checkpoint**: e2e single-pod, two-pod, and JS-interop suites green; a malformed frame kills only its own connection.

---

## Phase 4: User Story 2 — Crash loss is bounded and operator-controlled (P1)

**Goal**: edits survive abrupt termination up to a configured window; recovery is fast
regardless of document age.

**Independent test**: drive edits, `kill -9`, restart, assert recovery to the last
completed flush.

### Tests for User Story 2

- [X] T020 [P] [US2] Add a kill/restart durability test in `internal/domain/service/durability_crash_test.go` asserting recovery to the last completed flush across repeated cycles (SC-004)
- [X] T021 [P] [US2] Add a cold-load cost test in `internal/domain/service/durability_coldload_test.go` asserting cost tracks document size, not accumulated edit count (SC-012)
- [X] T022 [P] [US2] Add a concurrent first-open test in `internal/domain/service/seed_exactly_once_test.go` asserting seeding happens **exactly once**, with content identical to a single-session open (SC-015, FR-004b)
- [X] T023 [P] [US2] Add a degraded-durability test in `internal/domain/service/durability_degraded_test.go`: with the backend failing, assert the session keeps serving, retries, and surfaces the not-yet-durable state **via metrics before anyone is disconnected** (SC-013)
- [X] T024 [P] [US2] Add an escalation test in `internal/domain/service/durability_escalation_test.go` asserting a distinct counter, a log entry naming document and undurable duration, and a non-generic disconnect reason (SC-016, FR-028)

### Implementation for User Story 2

- [X] T025 [US2] Implement the checkpoint-only `persistence.Store` over file-service in `internal/adapter/outbound/persistence/fileservice/` — `Appender` + `Loader` + `FenceMode`, deliberately no `Compactor` (D1, contracts/persistence-store.md)
- [X] T026 [US2] Make the store in `internal/adapter/outbound/persistence/fileservice/` constructible in **both** fence modes, threading the epoch through the write path though it is always zero in deployment (FR-008a, D6)
- [X] T027 [P] [US2] Implement the in-process `persistence.Store` fixture in `internal/adapter/outbound/persistence/inprocess/` for the test/dev/smoke path (§III)
- [X] T028 [US2] Implement flush batching **above** the store in a new `internal/domain/service/flush.go`: merge a window, call `Append` once, so `Append` never overstates durability (D2, FR-007a)
- [X] T029 [US2] Make the flush interval operator-configurable in `internal/config/config.go` with a documented default, armed only when the document changed; shutdown flush unconditional (FR-010)
- [X] T030 [US2] Implement the durability state machine (clean → dirty → undurable → escalated) in `internal/domain/service/flush.go` with retry/backoff and a configurable consecutive-failure threshold (FR-013, D10)
- [X] T031 [US2] Implement escalation in `internal/domain/service/flush.go` and the reason code in `internal/domain/model/control.go`: invalidate, signal holders, disconnect with a reason meaning *recent edits could not be saved*; tear down even when the backend is unreachable (FR-013, FR-028). **Both halves of FR-027**: notify collaborators when their edits become not-yet-durable **and again when durability is restored** — a one-way notification leaves clients believing their work is still at risk after recovery
- [X] T032 [US2] Add the observability signals in `internal/adapter/inbound/http/metrics.go` — flush outcome, consecutive-failure count, escalation events, generation invalidation, time-in-undurable — as **metrics**, not only logs (FR-026)
- [X] T069 [US2] Inventory every persistence-related signal the service emits today (metrics and logs) in `internal/adapter/inbound/http/metrics.go`, and assert each has a post-rebuild equivalent — a signal silently lost in the rebuild is invisible until an alert fails to fire during an incident (FR-025, SC-014)
- [X] T033 [US2] Detect and error in `internal/adapter/outbound/persistence/fileservice/` on a recovery view that presents partial history as complete (FR-014)
- [X] T034 [US2] Delete `internal/adapter/outbound/blobstore/` and the `BlobStore` port from `internal/domain/port/ports.go` — removed, not wrapped (FR-007, SC-008a)
- [X] T035 [US2] Document the write-volume envelope (`document size ÷ flush interval × active documents`) beside the interval setting, with the default justified against `MAX_DOC_BYTES` (FR-010a, SC-020)

**Checkpoint**: durability tests green; no `BlobStore` remains; degraded state visible in metrics before disconnects.

---

## Phase 5: User Story 3 — The `002` lifecycle guarantees still hold (P1)

**Goal**: every safety and liveness property from the lifecycle redesign survives the
rebuild.

**Independent test**: the `002` invariant suite runs green **and non-vacuous** against
the rebuilt service.

### Tests for User Story 3

- [X] T036 [US3] Restructure the `002` invariant tests that reach into removed structures, across `internal/domain/service/invariant_*_test.go` — same or stronger property, never weakened (FR-018a)
- [X] T037 [US3] Build the non-vacuity ledger over `internal/domain/service/invariant_*_test.go`, recording it in `specs/003-go-yjs-core-port/non-vacuity-ledger.md`: revert each restructured guarantee in isolation, confirm its test goes RED, restore, record the proof (FR-018a, SC-005)
- [X] T038 [P] [US3] Add teardown-flush matrix tests in `internal/domain/service/invariant_teardown_flush_test.go` proving graceful shutdown and idle release **persist**, while invalidation, escalation, and post-panic teardown **do not** (SC-018, FR-011a)
- [X] T039 [P] [US3] Add a test in `internal/domain/service/invariant_uncooperative_holder_test.go` asserting correctness does not depend on cooperative handle holders (contracts/registry-session.md)

### Implementation for User Story 3

- [X] T040 [US3] Implement the teardown-flush matrix in `internal/domain/service/room.go`; a path that is neither known-good nor poisoned MUST NOT default to flushing (FR-011a/b, D9)
- [X] T041 [US3] Preserve the shutdown drain ordering in `internal/domain/service/manager.go` that persists dirty documents before durable backends close, now expressed over handles (FR-001)
- [X] T042 [US3] Preserve the single mutation chokepoint in `internal/domain/service/room.go` so limits apply on every entry point, local and cross-pod alike (FR-019)
- [X] T043 [US3] Confirm auth and authz behaviour is unchanged across `internal/adapter/outbound/auth/` and `internal/adapter/inbound/ws/handler.go`, including fail-closed evaluation and handshake authentication (FR-020)
- [X] T072 [P] [US3] Assert in `internal/domain/service/invariant_plaintext_test.go` that authoritative documents are held **plaintext** on the server — a preserved invariant from `001` with no current guard (FR-004)
- [X] T044 [US3] Justify in writing any `002` invariant deleted rather than restructured, on the grounds that its property became unreachable by construction (SC-005a)

**Checkpoint**: `002` suite green and every restructured test proven RED-on-revert.

---

## Phase 6: User Story 4 — Port implementations are contract-validated (P2)

**Goal**: every implementation is validated by the core's adversarial suites, not only
by this repo's tests.

**Independent test**: conformance suites run in CI and pass.

- [X] T045 [P] [US4] Wire `conformance.Persistence` against both store implementations (SC-006)
- [X] T046 [US4] Wire `conformance.PersistenceFencing` against a **fenced** instance, though no deployment enables fencing (FR-008a, SC-017)
- [X] T047 [P] [US4] Wire `conformance.Memory` against the registry usage (SC-006)
- [X] T048 [US4] Record which suites apply and **why each non-applicable suite is not run** — notably compaction, since the store implements no `Compactor` (FR-008b)
- [X] T049 [P] [US4] Assert in `internal/app/app.go` that the core's shipped single-process defaults are used **as shipped** in the in-process path, with no bespoke reimplementation (§X, §XI)

**Checkpoint**: all applicable conformance suites green in CI; non-applicable ones documented as decisions.

---

## Phase 7: User Story 5 — Multi-pod survives hostile fan-out (P2)

**Goal**: pods converge although the contract promises neither ordering nor single
delivery.

**Independent test**: two-pod e2e with duplication, reordering, and redelivery injected.

### Tests for User Story 5

- [X] T050 [P] [US5] Wire `conformance.Hub` against the Redis implementation — it injects reordering, duplication, and redelivery (SC-007)
- [X] T051 [P] [US5] Extend `test/e2e/twopod_test.go` to assert convergence and correct echo suppression under hostile delivery (SC-007)
- [X] T052 [P] [US5] Add a redelivery test in `internal/domain/service/fanout_redelivery_test.go` asserting an already-applied update is a harmless no-op

### Implementation for User Story 5

- [X] T053 [US5] Implement `hub.Hub` over Redis in `internal/adapter/outbound/hub/redis/`, natively — **not** wrapping `ClusterBroadcaster` (FR-007, contracts/hub.md)
- [X] T054 [US5] Answer the backpressure delta: `Handler` returns an error and an implementation must not silently discard on a full queue, while Redis pub/sub is fire-and-forget (contracts/hub.md)
- [X] T055 [US5] Remove any reliance on pub/sub ordering in `internal/domain/service/room.go`; completeness comes from persistence and state-vector catch-up (contracts/hub.md)
- [X] T056 [US5] Keep durable and ephemeral traffic separated in `internal/domain/service/awareness_wire.go` — awareness must never reach durable storage (FR-009)
- [X] T057 [US5] Emit the unsupported-combination startup warning in `internal/app/app.go` when multi-pod fan-out is configured with a durable store and no ownership mechanism: **logged at WARN or above, during startup and before the service begins serving, naming both configuration keys and the unsupported combination in the message** (FR-022b)
- [X] T070 [US5] Document durable multi-pod as **unsupported** wherever multi-pod configuration is described — `.env.example`, `README.md`, `CLAUDE.md`, and `deploy/k8s/base/configmap.yaml` on `feat/003-migration`. The runtime warning (T057) is not sufficient on its own: FR-022a requires the precondition be documented, so an operator learns it before enabling multi-pod rather than after (FR-022a)
- [X] T058 [US5] Delete `internal/adapter/outbound/fanout/` and the `ClusterBroadcaster` port — removed, not wrapped (FR-007, SC-008a)

**Checkpoint**: hub conformance green; two-pod convergence under hostile delivery; no `ClusterBroadcaster` remains.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [X] T059 Rename `MetadataStore` to the canonical spelling everywhere — port, config identifiers (`MetadataStore*`), and package path `metastore/` → `metadatastore/` (FR-009a, SC-008b)
- [X] T060 Rename the backend-selection configuration keys in `internal/config/config.go` to match the adopted contracts, leaving the metadata key unchanged (FR-022c)
- [X] T061 Make a removed or renamed configuration key **fail startup with an error naming its replacement** in `internal/config/config.go` — never silently ignored, because these keys have silent defaults that would send blobs to memory (FR-022d, SC-021)
- [X] T062 Coordinate the rename across every consumer: this repo's `.env.example`, `README.md`, `CLAUDE.md`; `deploy/k8s/base/configmap.yaml` on the **unmerged** branch `feat/003-migration`; `server`'s 006 `quickstart-services.yml` (FR-022e)
- [X] T063 [P] Verify zero translation shims by inspection across `internal/adapter/outbound/` — each adopted contract has exactly one implementation per backend, reaching infrastructure directly (SC-008a)
- [X] T064 [P] Confirm the in-process path still serves all three roles per `specs/003-go-yjs-core-port/quickstart.md` §3: test suite, local development with real editors, and the zero-dependency smoke test (§III)
- [X] T065 [P] Verify unit coverage ≥95% across `internal/...`, with no coverage-padding tests (SC-011, §XII)
- [X] T066 Run the full gate set — `go build`, `go vet`, `go vet -tags integration`, `golangci-lint run` at zero, `go test -race ./...` (§IX)
- [ ] T067 Run a full adversarial review of the rebuilt service (`internal/...`, `cmd/...`) and drive it to zero findings (SC-010)
- [X] T068 Update `specs/001-collaboration-server/quickstart.md`, which is stale on the metadata-store default and predates the standalone withdrawal

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (P1)** → **Foundational (P2)** → all user stories
- **Foundational is genuinely blocking**: every story needs documents existing on the new core, so T005–T012 must complete first
- **Polish** last, except T059–T062 which must land atomically with each other

### User story dependencies — stated honestly

This is a rebuild, so independence is partial:

- **US1** depends only on Foundational. It is the true MVP.
- **US2** depends on Foundational. Independent of US1.
- **US3** depends on Foundational, and **materially on US2** — the teardown-flush matrix cannot be tested without a working store, and the invariant restructure touches code US2 changes.
- **US4** depends on US2 and US5 existing to validate; T047 can run right after Foundational.
- **US5** depends on Foundational only. Independent of US1–US3.

### Within each story

Tests before implementation (§VI). Within implementation, follow the listed order — the
sequencing in research.md (core → persistence → registry/session → transport → hub) is
reflected in phase order.

### Parallel opportunities

- **T002, T003** in Setup
- **T013, T014, T071** (US1 tests); **T020–T024, T027** (US2 tests and the in-process store fixture, all different files)
- **T038, T039, T072** (US3 tests); **T045, T047, T049** (US4); **T050–T052** (US5 tests)
- **US1, US2, and US5 can proceed in parallel** once Foundational lands — they touch
  transport, persistence, and fan-out respectively
- **T063–T065** in Polish

**Not parallelizable**: T036/T037 (the invariant restructure and its ledger are one
sequential act); T059–T062 (the rename is atomic across repos).

---

## Implementation Strategy

### MVP: User Story 1 only

Setup → Foundational → US1 gives collaboration working over the new core, provable with
the existing e2e and JS-interop suites. That is a genuine, demonstrable increment: the
core is swapped, browsers converge, and nothing else has changed.

### Incremental delivery

1. **Foundational + US1** — the core swap is real and provable
2. **+ US2** — durability moves to the new contract; `BlobStore` disappears
3. **+ US3** — the `002` net is re-established with its ledger, the highest-risk step
4. **+ US4** — conformance makes the implementations externally validated
5. **+ US5** — multi-pod, not load-bearing on day one
6. **+ Polish** — naming, config rename, coverage, adversarial review

### Deliberately out of this task list

Authorised by constitution v3.0.0 §III but tracked separately, so a foundational port
does not also carry a multi-adapter deletion: removing the `postgres` metadata adapter
(with `pgx`/`sqlc`/`golang-migrate` and the CI Postgres service), the non-file-service content
adapters, and the standalone create/delete HTTP API.

---

## Notes

- **The failure mode that compiles**: implementing a core contract by delegating to the
  port it supersedes. Every deletion task (T034, T058) exists to prevent it, and T063
  verifies it by inspection.
- **The failure mode that stays green**: weakening a `002` invariant while restructuring
  it. T037's ledger is the only defence — a test that passes with its guarantee reverted
  proves nothing.
- **Do not re-verify the CRDT core.** Encoding and merge semantics are gated by the
  core's own differential oracle (§IV). Tests here cover *this service's* usage.
