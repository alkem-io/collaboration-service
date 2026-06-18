# Requirements Quality Checklist: Unified Real-Time Collaboration Server

**Purpose**: Validate that `spec.md` is complete, unambiguous, testable, and ready
to drive Waves 2–4 (Wave 1 already implemented against it retroactively).
**Created**: 2026-06-18
**Feature**: [spec.md](../spec.md) · Workspace epic: `../../agents-hq/specs/003-unify-collab-yjs/`

## Content Quality

- [x] CHK001 No premature implementation detail leaks into FRs — FRs state *what* the server must do; *how* (run-loop, framing, adapters) lives in plan.md/research.md.
- [x] CHK002 Each user story is independently testable and maps to a concrete test or test-to-be (US1/US2/US5 → named Wave-1 tests; US3 → Wave-1 partial + Wave-3 tests).
- [x] CHK003 Written for the server boundary, not re-specifying the CRDT core (`y-crdt`) or the client binding (`excalidraw-fork`/`client-web`) — scope boundary explicit in the header note and Assumptions.
- [x] CHK004 Conforms to the SpecKit spec-template shape (User Scenarios, Requirements, Success Criteria, Assumptions, Clarifications).
- [x] CHK005 Terminology is consistent with the constitution and epic (`actorId` not `userId`; "room", "snapshot", "ports"; memo/whiteboard conventions).

## Requirement Completeness

- [x] CHK006 Every epic FR the server is responsible for (FR-001/002/005/007/008/009/010/012/014/017/018/019/020/021/022/023/024/025) maps to ≥1 server FR — traced inline (e.g. server FR-006 → epic FR-010; FR-016 → epic FR-012/FR-023).
- [x] CHK007 Every server FR carries a **wave tag** so "done vs forward" is unambiguous; Wave-1 FRs are marked ✅.
- [x] CHK008 Every Success Criterion is measurable and testable **in this repo** or in the e2e harness — none is a vanity/external-business metric (constitution §XIII).
- [x] CHK009 Edge cases enumerated and each tied to a wave + (where done) a test: malformed frame, slow consumer, join-race, delete-while-connected, limit breach, late joiner, cross-pod, persist failure.
- [x] CHK010 Out-of-scope is explicit: migration/cutover (WS-E), CRDT internals + fuzz (WS-A), client bindings (WS-B/D), cross-session version history (forward-compat only).
- [x] CHK011 Key entities listed with their owning layer (Room/Manager runtime; Metadata/Snapshot persisted; ports incl. the two Wave-1 additive).

## Requirement Clarity & Consistency

- [x] CHK012 No conflicting requirements between spec.md, plan.md, and tasks.md (cross-checked in the self-analyze pass; see analysis report appended to the agent result).
- [x] CHK013 Each FR is singular and verifiable (no compound "and also" hiding a second untested obligation).
- [x] CHK014 Wave-1 retro-coverage is honest — claims marked ✅ are backed by named, existing tests; nothing aspirational is marked done.
- [x] CHK015 The fail-closed authZ rule (constitution §V, anti-pattern 13) is stated as a requirement (FR-013/FR-015) and reflected in the data-model (`AuthDecision` error semantics).

## Ambiguities & Open Decisions

- [x] CHK016 Genuinely unknown integration contracts are surfaced as **OPEN-1..4**, each grounded by reading the actual sibling service (not guessed — constitution §XV), each with a recommendation.
- [x] CHK017 Each OPEN names the wave/task it blocks (OPEN-1→T006, OPEN-2→T005 blob, OPEN-3→T005 metastore, OPEN-4→T013/T014) so it is resolved before the dependent sub-task, not before Wave 1.
- [x] CHK018 No `[NEEDS CLARIFICATION]` placeholders remain in the FRs — every server FR is decided; only the four cross-service *integration details* are OPEN, and they are isolated to Wave 2+.
- [x] CHK019 OPENs are scoped as implementation detail *inside* an already-frozen workspace contract, not as proposed contract changes — so they do not destabilize the epic.

## Feature Readiness

- [x] CHK020 Constitution Check table in plan.md passes all §I–XV with notes; the one departure (two added ports) is justified as additive/coupling-reducing.
- [x] CHK021 The wave map in spec.md and the phase structure in tasks.md agree (Wave 1 done = T001–T003+T007–T012; Wave 2 = T004–T006; Wave 3 = T013–T016; Wave 4 = T017).
- [x] CHK022 Standalone (zero-dep) and Alkemio modes are both first-class and exercised by quickstart.md + the e2e plan (SC-012/SC-007/SC-011).
- [x] CHK023 The ≥95% coverage + e2e CI gate is captured as an explicit deliverable (FR-018/SC-011/T017), not assumed.
- [x] CHK024 `.specify/feature.json` points at this spec dir so SpecKit tooling resolves the active feature.

## Notes

- Wave-1 FRs/SCs are retro-covered: marked ✅ with the file anchor or named test that proves them (see tasks.md Phase 1/3).
- The four OPENs are the real residual risk for Wave 2 — all are *cross-service contract details*; OPEN-3 (the RabbitMQ dialect with `server`) is the one with a consumer the collab repo cannot see, so it is flagged as needing the `server` owner.
- This checklist is filled as part of the sub-spec authoring; re-run `/speckit-analyze` after each wave lands to keep coverage honest.
