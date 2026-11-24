# Implementation Plan: Native Go Collaboration Previews

**Branch**: `004-native-collab-previews` | **Date**: 2026-11-23 | **Spec**: [spec.md](spec.md)

## Summary

Use successfully persisted edits to publish document previews during editing and on normal room release. One bounded worker reads the latest checkpoint and creates missing private slots or refreshes them at stable file IDs. Whiteboards generate two full-scene PNGs; memos generate one Markdown file. An unchanged session does nothing. An independent internal-only GET returns raw saved memo Markdown, using the same serializer without triggering publication. Neither path owns editing, saves or business settings.

## Technical Context

| Area | Design |
|---|---|
| Language/runtime | Go 1.26.10 baseline, CGO-free Linux image; existing `go-yjs` `v0.1.1` V2 decoder. No production browser, JavaScript, extra worker process or CRDT changes. |
| Dependencies | Existing chi, metadata and checkpoint ports. The whiteboard renderer uses the native Go libraries qualified in [research R5](research.md#r5--whiteboard-raster-and-font-proof), with pinned embedded fonts. Memo uses typed XML/delta APIs and a Go serializer. |
| Storage | Existing checkpoints remain authoritative. Collab creates private PNG and Markdown files in the source bucket and replaces their bytes. File-service's unique scoped reference is the mapping; no collaboration database/schema. The direct memo response is transient. |
| Topology | One artifact writer on the existing supported file-service topology, with no feature toggle. The private memo router starts on a separate listener, absent from the public handler. |
| Performance | A five-second, non-sliding successful-save window coalesces changes before one bounded worker for both output types. Recheck qualification bounds under combined load. The window is not a delivery SLA. |
| Testing | Test-first domain, adapter and app wiring tests; pinned fixture oracles; real isolated file-service and WS-to-preview tests; cold memo checks through both listeners and the isolated public gateway. Measure unit-only statement coverage separately from the combined gate. |
| Scope | `collaboration-service` only. No modes/crops, HTTP image generation or delivery, new GraphQL fields, separate render timer, server/client/WOPI/file-service/deployment changes, or live visual cutover. |

## Constitution Check

**Before research:** Pass for the specified scope. Check §§I–V (existing ports and one CRDT, no DB or widened access), §§VI/IX/XII (test-first, lint and meaningful coverage), §§VIII/X/XI (no parallel save/room logic), §§XIII/XIV (measured limits and pinned, licensed dependencies).

**After design:** No amendment or exception required. On the pinned baseline, use only the existing metadata read and checkpoint contracts; the constitution's newer direct-admission projection is not implemented or expanded here. The unauthenticated memo listener is always started on a separate address, loopback by default; restricted routing and TLS are mandatory prerequisites to making it reachable by other cluster services. The current single writer makes a local queue sufficient; deployment on multiple durable writers is not granted. New integration scope is not authorized to close any discovered gap.

**Gates:** Verify §XII with a separate ≥95% unit-only statement coverage measurement in addition to the existing combined gate. Adopt no renderer dependency before the full-scene proof in [R5](research.md#r5--whiteboard-raster-and-font-proof) passes. Before enabling beyond a fixture environment, prove exclusive, trusted output associations and network isolation. No arbitrary destination or allocation after an overwrite failure.

**D2 sequencing:** The pre-existing unit coverage shortfall does not block starting development or verifying the memo MVP. Add meaningful regression tests for existing behavior alongside feature work. The repository-wide ≥95% unit-only gate must pass at final feature verification.

## Design and delivery order

1. **Qualify each lane independently before its production work.** The recorded scratch probes establish Go usability, not feature completion. Expand the whiteboard proof to the exact fork's saved scene and full-board oracle before pinning dependencies. Finish the pinned TipTap fixture matrix for the memo. Measure representative memo reads and whiteboard render bursts to set each lane's initial bounds before implementation; the memo lane does not wait for the renderer proof.
2. **Implement the memo lane independently.** Validate the ID and declared memo type, read the checkpoint, decode into a private Y.Doc, traverse `default` and serialize. Use a bounded admission/deadline and return a complete raw body only on success. Add a separate private router and listener through the normal composition root, with no application auth calls.
3. **Implement shared artifact publication.** At the end of an active save window or on normal idle release, enqueue the ID, document type and changed successful checkpoint fingerprint, skipping completed publication. Off the room loop, reread the checkpoint, use the generator for that type, and create a missing reserved slot or replace the existing private file. Reuse owner invalidation and bucket cascade, including draft expiry cleanup. No room retention, retry scheduler or new wire state.
4. **Prove the facility without rewiring consumers.** Drive browser-shaped and MCP-shaped updates through the local WS harness and read back the unchanged file IDs. For memo, exercise both listeners and one real isolated public gateway negative check. Recheck the initial bounds under combined load, verify that failures don't delay close or change durability, and run both coverage gates. Say what isolated proof establishes and what live handoff still needs.

## Project Structure

### Documentation

```text
specs/004-native-collab-previews/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── contracts/facility.md
└── quickstart.md
```

### Source layout (planned)

```text
internal/domain/service/room.go                 # existing idle release / successful save seam
internal/domain/service/                        # saved projection and bounded derivative coordination
internal/domain/port/                           # only derivative-specific file access capabilities
internal/adapter/outbound/persistence/fileservice/ # derivative operations, not another checkpoint store
internal/adapter/inbound/http/                  # separate private memo router and handler
internal/{app,config}/ and cmd/server/           # composition, optional private listener and feature enablement
test/e2e/                                       # isolated WS and file-service proof; fixture oracles only in tests
```

**Structure decision:** Extend the existing Go application and file-service adapter boundary. Share checkpoint reads; don't widen `CheckpointStore`, route through a second metadata producer or add another service. A one-worker design and measured budgets replace any speculative distributed scheduling design. No tasks file or product code is generated by this plan.

## Complexity Tracking

No architecture exception is requested. The only new HTTP surface is the separate private memo endpoint. The later rollout must protect internal routing, connect consumers to the trusted slots, and only then remove superseded generation paths. This plan does not enable that rollout.
