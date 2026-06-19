# Migration & big-bang cutover runbook — 003-unify-collab-yjs (WS-E)

> **HUMAN-GATED. NOTHING IN THIS RUNBOOK IS AUTOMATED OR EXECUTED BY THIS PR.**
> This document is the build-ahead artifact: the migration tool (`cmd/migrate`)
> and the deploy manifests (`deploy/`) are built and tested *up to* the gate. The
> one-time migration run, the traffic flip, and the legacy decommission are each a
> deliberate, human-initiated, human-approved step. Do not run any of the
> "EXECUTE" blocks below until the corresponding gate is signed off.

Covers the WS-E plan (`specs/003-unify-collab-yjs/plan.md` §Rollout step 6) and
infra-ops tasks T004–T007.

---

## Preconditions (all must be green before any cutover step)

- [ ] WS-A..WS-D merged; **full-stack e2e green** (the WS-F two-pod convergence +
      both content types) — this is the `tasks.md` T007 gate the infra-ops phase
      depends on.
- [ ] `collaboration-service` deployed to the target environment (manifests from
      `deploy/`, moved into `infrastructure-operations`) and healthy (`/healthz`,
      `/metrics` scraped, the PrometheusRule loaded).
- [ ] The **server migration read-path** (`server` task S-T005) is reachable by
      the migration tool. **As of build-ahead this is STUBBED**: the server's
      `CollaborationMigrationService` is in-process NestJS only — it has **no
      HTTP/RMQ/CLI surface**. Before migration, S-T005 must grow an export
      (e.g. `pnpm cli collab:migration-export > legacy.jsonl`, or an HTTP/RMQ
      streamer) producing `LegacyContentRecord` JSONL. The migration tool consumes
      that JSONL unchanged.
- [ ] The **whiteboard cross-language seam** (`scripts/migrate`) is installed —
      the `@alkemio/excalidraw-yjs-binding` Node step resolves (see
      `scripts/migrate/README.md`). Until then whiteboards are *flagged*, not
      migrated.
- [ ] A **rollback window** is agreed: the two legacy services
      (`collaborative-document-service`, `whiteboard-collaboration-service`) stay
      **warm** (deployed, traffic-removed) for its duration.

---

## Step 0 — Capture the size baseline (infra-ops T004, SC-007)

Record current per-content-type blob sizes (memo `content` bytea, whiteboard
compressed `content`) so the post-migration snapshot sizes can be compared. This
is the SC-007 input; the migration tool also reports `legacyBytes`/`snapshotBytes`
per document.

> Read-only. Capture from the DB (or the server export) and store with the run
> record.

---

## Step 1 — Dry-run the migration (NO writes)

**EXECUTE (safe — writes nothing):**

```bash
# Backends come from the env (BLOB_STORE / METADATA_STORE / FILE_SERVICE_* / ...),
# same config the service uses. --dry-run converts + validates every document but
# persists nothing.
go run ./cmd/migrate \
  --source legacy.jsonl \
  --dry-run \
  --wb-script "$PWD/scripts/migrate/whiteboard-to-ydoc.mjs" \
  --report dryrun-report.json
```

Acceptance:
- [ ] `flagged == 0` (or every flagged doc is understood and accepted — corrupt
      legacy blobs are *flagged, not dropped*; triage them).
- [ ] Size baseline sane: no document's `snapshotBytes` is a pathological multiple
      of `legacyBytes` (the tool flags ratio regressions; tune `--max-size-ratio`
      against the Step-0 baseline).
- [ ] Round-trip validation passed for every migrated doc (SC-003/SC-009 — the
      tool rehydrates each snapshot and checks convergence).

**GATE 1 — engineering sign-off on the dry-run report.** Human.

---

## Step 2 — Run the one-time, in-place migration (infra-ops T004)

> The tool is **idempotent + resumable**: re-running skips documents already at the
> target version, so an interrupted run is safe to restart. It **never drops** a
> document it cannot migrate — it flags it.

**EXECUTE (writes snapshots through the service's BlobStore + MetadataStore):**

```bash
go run ./cmd/migrate \
  --source legacy.jsonl \
  --wb-script "$PWD/scripts/migrate/whiteboard-to-ydoc.mjs" \
  --report migration-report.json
# exit 0 = all migrated/skipped; exit 2 = completed with flagged docs (triage);
# exit 1 = aborted (resume by re-running — idempotent).
```

Acceptance:
- [ ] `migrated + skipped == total`, `flagged` triaged.
- [ ] Spot-check: open a sample of migrated memos and whiteboards in the new
      service (read path) and confirm content + structure.
- [ ] SC-012: if blobs are offloaded to file-service, a reload is byte-identical.

**GATE 2 — migration completeness sign-off.** Human.

---

## Step 3 — Big-bang flip (infra-ops T005, FR-006/012/013)

Flip client traffic for **both** memos and whiteboards to `collaboration-service`
in one cutover (the clarified big-bang, not a gradual ramp). Keep the two legacy
deployments **warm** (scaled up, traffic removed) for the rollback window.

> The exact flip is environment-specific (ingress/route swap, client config flag,
> or feature switch). It is a human-performed infra-ops change, reviewed and
> applied via GitOps — NOT scripted here.

Acceptance:
- [ ] New memo + whiteboard sessions land on `collaboration-service`.
- [ ] No spike in `collaboration_snapshots_total{outcome="error"}` /
      `collaboration_fanout_total{outcome="error"}`.
- [ ] Legacy services receiving no new traffic but still warm.

**GATE 3 — QA acceptance sign-off + the production-deploy decision.** Human. This
is the release-train production gate; never auto-checked.

---

## Rollback (infra-ops T006)

Roll back **before** new-stack edits diverge from the legacy snapshots (i.e.
within the rollback window, before significant new collaboration has accumulated
only in the new service).

**Procedure (human, GitOps revert):**
1. Revert the Step-3 flip (point traffic back at the warm legacy services).
2. Because legacy was kept warm and the migration was *in-place / additive* (it
   wrote new-format snapshots without destroying the legacy stores), the legacy
   services resume from their own untouched stores.
3. Triage why (snapshot errors, convergence regressions, fan-out failures via the
   PrometheusRule alerts), fix forward, re-attempt from Step 1.

> The divergence risk grows with time post-flip: edits made only in the new stack
> after the flip are not in the legacy stores. The rollback window length is the
> agreed bound on acceptable divergence; past it, roll-forward-fix is the path, not
> rollback.

---

## Step 4 — Decommission (infra-ops T007, Phase 3 — LAST)

**Only after the confidence period**, with the new stack proven:
- [ ] Remove the `collaborative-document-service` + `whiteboard-collaboration-service`
      deployments.
- [ ] Archive the repos (status `to-retire`).

**GATE 4 — explicit decommission approval.** Human. Irreversible; out of scope for
this build-ahead PR.

---

## Human gates summary

| Gate | What | Owner |
|---|---|---|
| 1 | Dry-run report accepted | Engineering |
| 2 | Migration completeness accepted | Engineering + QA |
| 3 | QA acceptance + production-deploy decision (the flip) | Release Lead / Quality Lead |
| 4 | Legacy decommission approval | Release Lead |

Everything above gate 1's "EXECUTE" blocks is *prepared* by this PR and *run by a
human*. This PR runs none of it.
