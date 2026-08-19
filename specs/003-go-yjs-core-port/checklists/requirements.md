# Specification Quality Checklist: go-yjs Core Port & Backend-Contract Adoption

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-08-17
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — *see Note 1: deliberate, justified deviation*
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders — *see Note 2*
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain — all resolved, see Note 3
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification — *see Note 1*

## Notes

**Note 1 — named technology is the feature, not a leak.** This checklist item assumes a spec whose technology is a free implementation choice. Here the user's requirement *is* "replace y-crdt with `github.com/antst/go-yjs`, adopting its contracts properly." A version of this spec that avoided naming the library and its contracts could not express the central requirement (FR-005/FR-007: adopt the contracts in substance rather than wrapping the library behind the old port shapes). Technology naming is therefore confined to: identifying the target dependency, describing the contracts being adopted, and the constitutional-impact section. Requirements and success criteria remain stated as observable behavioural outcomes — recovery guarantees, convergence, bounded crash loss — not as API call sequences.

**Note 2 — audience.** The stakeholders for this feature are the operator and the engineering team; there is no end-user-facing surface change (User Story 1's success condition is simply that collaboration works). Stories are written as outcomes rather than internals, but full non-technical accessibility is not achievable for foundational infrastructure work and is not the right bar here.

**Note 3 — all clarifications resolved.** Recorded in the spec's Clarifications section rather than as inline `[NEEDS CLARIFICATION]` markers, since each affects broad swathes of the spec rather than one sentence.

**Resolved this session:**

- **Q1 (durability model)** — ✅ adopt the contract with flushing batched *above* the store; one whole-document blob per flush, one blob read on load; the `CheckpointStore` profile, not the log profile; flush interval is operator configuration, not specification. Governs FR-010/011/012, User Story 3, SC-004, SC-012.
- **Q3 (cluster ownership)** — ✅ deferred; single-pod first makes `Fence` zero the normal mode, and unfenced data can be reopened as fenced with no history migration. No fenced path is built: every store reports `Unfenced` and rejects a non-zero `Fence` (FR-008a). Unfenced data reopens as fenced without a history migration, so fencing arrives with the coordinator that needs it.
- **Redis fan-out** — ✅ ported in this feature (near-isomorphic to `hub.Hub`; §II requires it; §X forbids leaving it dead), but not load-bearing on day one.
- **Blob semantics** — ✅ the contract is **store blob, read blob**. Retention, expiry, and reclamation are the backend's business; no history or restore surface exists or is pending.

- **Q2 (coordination boundary)** — ✅ **full adoption**. `memory.Registry` owns document identity, acquisition, eviction, invalidation and handle lifetime; `Room` is rebuilt around `Handle`/`Done` and the `002` state machine retires wherever it duplicates those semantics. Shutdown-drain ordering, flush policy, presence, limits, authz, and lifecycle-event handling remain this service's own.

**Resolved in Session 2026-08-18 (`/speckit-clarify`):**

- **Backend-unavailable behaviour** — ✅ a transient durable-write failure is *not* divergence. Keep serving, retry with backoff, escalate to invalidate + disconnect after a bounded, configurable number of consecutive failures. FR-013 rewritten; three edge cases replace the single blunt one.
- **Standalone** — ✅ withdrawn as a product configuration; retained only as an in-process test capability. User Story 4 deleted (stories renumbered 1–5), FR-021/SC-009 rewritten, new Non-Goal added. Requires — and received — a §III amendment.
- **`MetadataStore` naming** — ✅ canonical everywhere; `MetadataStore*` identifiers and the `metastore/` package path are renamed (FR-009a, SC-008b).

**Resolved in Session 2026-08-18 (second `/speckit-clarify` pass):**

- **First-open materialization** — ✅ happens inside the registry's open path (restore the checkpoint, or initialise empty when nothing is stored), making it exactly-once by construction rather than by a racing emptiness check.
- **Multi-pod durability** — ✅ declared **not supported** until `cluster.Coordinator` lands: fan-out is delivered, concurrent durable writers are not. Only the originating pod persists, so edits originating on several pods make several pods writers of one whole-document blob. FR-022a/b, plus a required startup warning.
- **Escalation data loss** — ✅ accepted but never silent: distinct counter, log with document id and undurable duration, and a disconnect reason meaning *recent edits could not be saved*. No secondary storage fallback. FR-028/029, SC-016.
- **Fencing** — ✅ not implemented; every store reports `Unfenced` and rejects a non-zero `Fence` rather than accepting one it cannot honour (FR-008a).
- **Eviction policy** — recorded as an assumption, not asked: `InProcessRegistry` starts no goroutines and has no policy of its own, so the `002` idle-release policy remains this service's job. The library forces the answer.

**Resolved in Session 2026-08-18 (third `/speckit-clarify` pass):**

- **Teardown flush matrix** — ✅ flush only when the document is believed good. Graceful shutdown and idle release flush; invalidation, escalation, and post-panic teardown do **not**. Resolves an apparent conflict between "shutdown flush is unconditional" and the handle contract's stop-using-a-poisoned-document rule: *unconditional* scopes to the graceful path. FR-011a/b, SC-018.
- **Transport dispatch** — ✅ rebuilt on the core's message-dispatch handler; domain checks preserved via type overrides. Gains core-level malformed-frame recovery, so a hostile frame fails **one connection** instead of relying on `002`'s run-loop recover, which saves the pod by destroying the room. FR-009b/c, SC-019.
- **Write-volume envelope** — ✅ documented, not engineered around. `MAX_DOC_BYTES` (32 MiB) against a 500ms–10s interval means a limit-sized document can cost ~64 MiB/s; the relationship must be documented and the default justified. Pre-existing, not introduced here. Adaptive flushing explicitly not built (§XI). FR-010a, SC-020.

**Correction (2026-08-18, fourth pass).** The Outstanding item *"the `server`-side RMQ consumer does not exist"* was **wrong and is withdrawn**. It repeated a stale Wave-2 code comment and a check against `server`'s `develop` branch. On `feat/006-collab-content-unification` the consumer is implemented (`collaboration-integration.controller.ts`, `@MessagePattern` SAVE/FETCH/DELETE/INFO), the service is deployed in the 006 quickstart, and whiteboards in real Alkemio spaces run against it via `METADATA_STORE=rabbitmq`. Verified reality is now recorded in the spec's Dependencies section, including that k8s manifests already exist on `feat/003-migration` and are not in this branch's history.

**Resolved in Session 2026-08-18 (fifth pass, opened by the deployment-reality correction):**

- **Configuration-key naming** — ✅ renamed to match the adopted contracts; `METADATA_STORE` unaffected. Recorded as a **coordinated cross-repo change** spanning this repo, the k8s manifests on `feat/003-migration`, and `server`'s 006 quickstart. FR-022c/e/f, SC-021.
- **Silent-default hazard** — a backend selector with a default means a deployment that says nothing, or misspells the key, runs on the non-durable in-process store and loses every document on restart while reporting healthy. `HUB_MODE` and `CHECKPOINT_STORE` are therefore **mandatory**, failing startup with an error naming the missing key (FR-022f).

**No clarifications remain.** Governance gates are cleared — see Note 4.

**Note 4 — governance gates: CLEARED (two amendments).** FR-023 required a constitutional amendment before implementation.

- **v2.0.0 (2026-08-18)** — §II, §IV, §XIV, Technology Stack Constraints, Anti-Patterns, and the file-service integration note: adopt `go-yjs` and its backend contracts; contracts *are* the ports; implementations MUST be native and conformance-validated.
- **v3.0.0 (2026-08-18)** — §III: withdraw the zero-dependency standalone product promise, retain the in-process path explicitly as a test capability, and mark adapters serving only the withdrawn promise as legacy under §X.

`/speckit-plan` is no longer blocked.

**Note 5 — scope reduced after the greenfield clarification.** The service has never been deployed and holds no production data; it is still being written for the first time. The following were removed as inapplicable, and their absence is deliberate rather than an oversight:

- **User Story 2 (existing documents survive the migration)** — deleted. Later, User Story 4 (standalone boot) was deleted too; stories now run 1–5.
- **FR-015/016/017** (data preservation, no-downtime deployment, defined rollback) — deleted. FR IDs are **not** reused, so numbering runs FR-001…FR-014, FR-018…FR-024 plus FR-007a.
- **SC-003** (100% of pre-existing documents open post-migration) — deleted.
- The format-migration edge case, and the compaction-race and paged-view edge cases made unreachable by the checkpoint-only decision.

This also reframes the dependency: `go-yjs` is this team's own product, so a contract that does not fit can be **changed upstream** rather than worked around locally — the two are designed together.
