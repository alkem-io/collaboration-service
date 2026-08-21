# KISS remediation ledger

This is the cross-repository correction ledger for the collaboration-content
unification work. It exists because the implementation and its reviews spent too
much effort on speculative failure handling while basic browser workflows were
not exercised early enough.

## Rules

1. A production change needs a reachable producer, a reproduced user/operations
   symptom, and a traced owning boundary. No patch is justified by a type member,
   hypothetical frame, or mocked-only state.
2. Prefer restoration of proven legacy behaviour or deletion of machinery over a
   new state machine. KISS is the default disposition.
3. Browser-visible claims require real browser evidence. Persistence claims also
   require cold-load or service-restart evidence. Concurrency claims require at
   least two independent connections.
4. A failure stops the test lane for read-only RCA. Do not patch until the RCA
   identifies the producer, violated assumption, smallest owning-boundary fix,
   counterfactual RED, and blast radius.
5. UUIDs are `uuid.UUID` inside the Go domain/runtime. Parse and format them only
   at external boundaries. Do not preserve stringly typed internals with aliases
   or compatibility shims.
6. `FIXED` means the code landed. `VERIFIED` additionally means the relevant real
   workflow passed. Unit tests alone do not upgrade browser-visible work to
   `VERIFIED`.

## Proven basic defects

| ID | Status | Finding | KISS disposition | Required close evidence |
|---|---|---|---|---|
| BASIC-001 | VERIFIED | Pointer movement emitted two unthrottled awareness frames per event, reached 65–71 frames/s, triggered server close `1013 update-rate-exceeded`, and caused the reconnect popup loop. The legacy client throttled cursor presence. | Client commit `871e30275`: one coalesced pointer+button awareness update at 33 ms / about 30 fps. | Sustained real-browser hold completed: zero closes/popups, peak awareness 30/s, reload persistence. |
| BASIC-002 | VERIFIED | Checkpoint default was changed from the legacy 2 s cadence to 500 ms without workload evidence, causing up to four times the full-document blob churn. | Service commit `b3a5785`: restore 2 s default; retain the existing clean→dirty trailing throttle, do not invent another scheduler. | Sustained dirty browser edit measured 34 PUTs over 79.3 s, mean 2.40 s including persistence latency; awareness-only phase wrote nothing. |
| BASIC-003 | FIXED | All-frame update-rate disconnect and collaborator inactivity downgrade were enabled by speculative defaults. The inactivity path did not even count cursor activity. | Service commit `01ed4f0`: defaults off; keep only as operator opt-ins until separately justified. Contribution window restored to legacy 600 s. | Re-run full basic matrix and 20×20 load with zero implicit disconnects. |
| BASIC-004 | OPEN — PROVEN | Contribution actors are cleared after a failed send and are not flushed on idle teardown. A user can edit, disconnect, and lose the contribution record before the 10-minute window fires. | Maintain one in-room UUID set. On cadence: detach/swap, send the detached set, merge it back only on failure. On teardown: flush the current set once. No worker, queue, or state machine. | REDs for quick edit→leave→idle teardown, swap-before-send, failed-send retention, and no timer/teardown duplicate; then a real short-session workflow. |
| BASIC-005 | OPEN — PROVEN | Existing whiteboard content-set paths can replace storage behind a live room. The next room save can overwrite the new snapshot with stale in-memory content. The memo twin has been moved to the ordinary collaboration session; whiteboard remains open. | Route existing-document replacement through the already existing authorized collaboration WebSocket session. Delete the direct snapshot/pointer rewrite path. Do not add a new RPC or reload control. | Live room: replace content, observe ordinary Yjs update, edit, cold-reopen and prove replacement+edit persist. Also prove cold/no-room replacement. |
| BASIC-006 | OPEN — RCA REQUIRED | The server lifecycle publisher was observed looping with an invalid/empty message pattern on a fresh stack, despite an empty outbox. Delete workflow was not exercised. | Trace the actual producer and Rabbit call before editing. Fix only the owning seam; do not add retry machinery around an invalid message. | Create and delete through the UI; prove the collaboration document is purged and no publisher loop remains. |
| BASIC-007 | OPEN — PROVEN | Quickstart configuration was ahead of the actual service contract (`AUTH_MODE=authzeval`; unsafe Redis+file-service combination), requiring local uncommitted corrections to boot the canonical stack. | Make canonical quickstart use the service's real split auth/authz and supported single-pod topology. Remove misleading combinations rather than translating them. | Clean Docker reset and canonical bring-up with no local compose edits. |
| BASIC-008 | OPEN — PROVEN | `publishedBy` still has an empty-string UUID sink on a sibling path (`publisher?.id || ''`). The bootstrap sink was fixed after a real fresh-DB failure; the sibling remains. | Convert absent actor to `undefined` at the sink; cover the reachable producer before changing it. This should ultimately disappear into the UUID boundary cleanup. | Focused producer RED and fresh-DB bootstrap. |
| BASIC-009 | REJECTED — DRIVER ARTIFACT | An initial two-context probe reported both client sockets clean-closing with code 1000. Instrumentation proved the probe itself pressed Escape after every draw; Escape correctly closed the whiteboard dialog, which called `UnifiedCollabProvider.destroy` → `teardownSocket` → `ws.close(1000)`. A discriminating no-Escape hold kept both same-actor contexts mounted and connected for 27 s with zero closes. | No product change. Remove the dialog-level Escape shortcut from the draw helper and use a canvas action that does not close the editor. | Two-context convergence rerun without Escape. Evidence: `/tmp/006-e2e-canonical/rca009/` and `/tmp/006-e2e-canonical/esc/`. |

## Stringly typed UUID remediation

| ID | Status | Finding | KISS disposition | Required close evidence |
|---|---|---|---|---|
| UUID-001 | OPEN — CENSUS | Actor IDs are strings through `model.Identity`, room membership, contributor collection, and contribution ports. Empty string is used as anonymous identity. | First coherent slice: `uuid.UUID` from auth boundary through room and contribution delivery; `uuid.Nil` for absence where the contract allows it; serialize only in HTTP/Rabbit/client adapters. Fold BASIC-004 into this slice. | Compile-time type proof, boundary parsing tests, contribution tests, basic authenticated/open-mode browser checks. |
| UUID-002 | OPEN — CENSUS | Document IDs are strings across routing, manager, room, stores, and lifecycle messages. | Convert domain/manager/store keys to `uuid.UUID`; parse once at HTTP/WS/Rabbit ingress and format once at outbound URLs/messages. | Invalid-boundary UUID rejection plus create/open/restart/delete E2E. |
| UUID-003 | OPEN — CENSUS | Content pointers, policy IDs, and bucket IDs are represented as strings inside domain/runtime code. | Convert fields that are semantically UUIDs to `uuid.UUID`. Keep true opaque tokens/URLs/locator schemes as strings. Do not mass-replace by name. | Boundary census reviewed; persistence/authz/file-service integration tests and cold-load E2E. |

## Speculative hardening to remove or simplify

| ID | Status | Finding | KISS disposition | Required close evidence |
|---|---|---|---|---|
| KISS-001 | OPEN — DELETE | Client has two reconnect owners: provider scheduling and wrapper `useAutoReconnect`. This creates duplicate timers and state coupling. | Keep the provider as the sole automatic reconnect owner. Keep one explicit user-triggered restart path. Delete the wrapper scheduler. | Real transient service restart: one reconnect sequence, no duplicate popup/timer; clean close stays quiet. |
| KISS-002 | OPEN — DELETE | `connectionError/hasError=true` is not produced by the native-Yjs runtime and is exercised only by mocks. | Delete the fabricated state and its UI plumbing. Do not create a producer merely to preserve it. | Residue search zero; real terminal and transient workflows still render their actual states. |
| KISS-003 | OPEN — SIMPLIFY | `update-rejected` client handling automatically remounts a fresh editor generation and introduced invalidation/generation race machinery. Valid current clients have no reachable producer for malformed document updates. | Keep server candidate-apply integrity. On rejection: stop editing, destroy provider, show Reload. No automatic remount/reconnect. Delete race machinery once the size-limit path no longer shares it. | Inject the actual server control at the integration seam: editor stops, no hidden reconnect, explicit reload restores server state. |
| KISS-004 | OPEN — SIMPLIFY | Document-size-limit recovery has a multi-state “manual fresh generation” flow. | Treat as terminal for the current page: explain and require a full reload. Do not build a second in-page lifecycle. | Real or protocol-level size rejection produces one explanation, zero reconnect loop, explicit reload only. |
| KISS-005 | OPEN — REMOVE DEFAULT | Durability escalation disconnects a room and deliberately discards valid in-memory edits after five backend failures. The legacy system did not do this. | Default threshold `0` means disabled. Keep retry/backoff and visible save-error/recovery. A positive operator value may retain escalation, but it must not be the default. | Real file-service outage: editors stay connected, error is visible, recovery persists edits after service return. |
| KISS-006 | OPEN — SIMPLIFY | `session-end` duplicates disposition/scope in a control frame and then sends a WebSocket close, creating roughly two protocol sources of truth plus extensive close machinery. | Retain writer-queued close so the handshake never blocks the room loop. Use one stable close reason/code as the client contract; remove duplicate scope/disposition control where the close can carry the result. | Delete, shutdown, rate/manual limit, and save-failure workflows map once to the expected UI/retry action. |
| KISS-007 | OPEN — REASSESS | A global connection cap of 50 rejects new sockets, while legacy max-collaborator behaviour downgraded excess writers to viewers. | Re-establish the actual product rule from legacy/requirements before changing code. Prefer one admission rule, not cap plus downgrade. | 20-user board and over-limit browser/protocol test with the agreed behaviour. |
| KISS-008 | OPEN — MEASURE | Per-connection outbound buffer `64` and slow-consumer shedding were not validated for 20-person whiteboards. | Do not tune speculatively. Measure under the agreed 20×20 workload; change only if the buffer demonstrably sheds healthy clients. | Load evidence with queue depth, sheds, latency, CPU and RSS. |
| KISS-009 | OPEN — REASSESS | Backend operations can hold the single room loop for up to 30 seconds. More retry/state machinery was added around this without measuring the basic stall. | Measure first. If user-visible stalls exist, isolate only the blocking backend operation; do not create another room state machine. | File-service latency/failure browser test correlated with room-loop latency. |
| KISS-010 | OPEN — SIMPLIFY | Lifecycle delivery grew into three retry tiers, DLQ and replay machinery before the ordinary delete path was proven end to end. | After BASIC-006 RCA, reduce to main delivery + one delayed retry + DLQ unless production requirements prove more tiers necessary. Keep confirms/manual ack. | UI delete and one broker-outage recovery test; no tight loop or duplicate purge. |
| KISS-011 | OPEN — MEASURE | Auth/existence admission performs repeated metadata loads per join. | Do not add a cache yet. Measure at 400 connections; remove a redundant load only if the same authority is queried twice and correctness remains obvious. | Load-test I/O counts and an authorization denial/non-enumeration test. |

## Verification gates still missing

| ID | Status | Gate |
|---|---|---|
| E2E-001 | VERIFIED | One-user UI create/open/draw, stable long hold with pointer movement, close, full reload, and file-service checkpoint persistence. |
| E2E-002 | RUNNING | Two independent contexts on one whiteboard, now using a corrected driver that never presses the dialog-level Escape shortcut: edit from each, live convergence, stable sockets, and full reload persistence. Repeat with two distinct canonical identities when available. |
| E2E-003 | PENDING | Image upload through UI, peer visibility, reload/cold-load, and stored Yjs inspection proving locators only—no `data:` bytes. |
| E2E-004 | PENDING | Memo two-context live convergence and reload/cold-load persistence. |
| E2E-005 | PENDING | Whiteboard content replacement through template/framing while a room is live (BASIC-005). |
| E2E-006 | PENDING | UI delete through lifecycle delivery and collaboration purge (BASIC-006). |
| LOAD-001 | PENDING | 20 boards × 20 connections for 10 minutes: 396 protocol clients plus 4 Chromium controls; pointer-only then mixed document edits. Assert zero unexpected closes/sheds/errors, awareness cardinality 20, pointer p95 ≤100 ms/p99 ≤250 ms, zero pointer-only checkpoints, mixed checkpoint ceiling about 10/s total at 2 s, and report actual CPU/RSS. Repeat unchanged once. |

## Deferred feature work (frozen until basics close)

These are tracked so they are not lost, but they must not pre-empt the gates above:

- Complete the whiteboard existing-document replacement path in BASIC-005 and
  remove the unsafe direct-write method.
- Finish the read-only template snapshot/codegen integration only after the
  merged repositories and basic matrix remain green.
- Re-pin the fork only when a consuming change genuinely requires the new build;
  keep both consumers on exactly one shared build identifier.

## Evidence currently retained

- Sustained pointer/save gate: `/tmp/006-e2e-canonical/gate/`
- Pointer reconnect RCA: `/tmp/006-e2e-canonical/ratelimit/`
- Canonical create/reload run: `/tmp/006-e2e-canonical/r1/`
- Same-actor two-context failure: `/tmp/006-e2e-canonical/2ctx/`
- Driver-artifact RCA: `/tmp/006-e2e-canonical/rca009/` and `/tmp/006-e2e-canonical/esc/`
- Checkpoint timestamps: `/tmp/gate-puts.txt`
