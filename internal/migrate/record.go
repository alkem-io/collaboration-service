// Package migrate is the one-time, in-place legacy→unified content migration
// (WS-E of 003-unify-collab-yjs). For each legacy document it pulls the legacy
// content (memo = a Yjs v1/v2 update, base64-encoded; whiteboard = Excalidraw
// JSON), converts it to the new authoritative Y.Doc v2 snapshot, and writes it
// through the collaboration-service's own persistence ports (BlobStore snapshot +
// MetadataStore index) — exactly the path a live room uses on save
// (data-model.md Snapshot; persistence-ports.md §Migration).
//
// It is a batch driver, NOT part of the running service. The cutover is
// HUMAN-gated (plan.md §Rollout step 6): this package only BUILDS and validates
// the converted snapshots; running it against production is a deliberate,
// human-initiated step (see docs/migration-cutover-runbook.md). The tool is
// idempotent, resumable, and supports a dry-run that converts + validates without
// writing.
//
// # Cross-language seam (whiteboards)
//
// Memos are pure CRDT: the Go vendored y-crdt core decodes the legacy update and
// re-encodes a v2 snapshot entirely in-process. Whiteboards are NOT: the
// Excalidraw-JSON → id-keyed Y.Map transform is owned by the TypeScript binding
// `@alkemio/excalidraw-yjs-binding` (populateYDoc). Rather than re-implement that
// non-trivial transform in Go (and risk it drifting from the binding the clients
// actually use), the whiteboard converter shells out to a small Node step
// (scripts/migrate/whiteboard-to-ydoc.mjs) that runs the published binding and
// emits Yjs *v1* update bytes; Go then decodes those (v1) and re-encodes the
// canonical v2 snapshot, so both content types converge on one persistence path.
// This mirrors the repo's existing Go→Node y-protocols interop precedent
// (test/e2e/jsinterop). See WhiteboardConverter for the seam + the stub note.
package migrate

import (
	"fmt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// LegacyRecord is one legacy document as exposed by the server migration
// read-path (server tasks S-T005). Its shape mirrors the server's
// LegacyContentRecord VERBATIM so the JSONL produced by the server read API
// decodes straight into it:
//
//		{ id, contentType, content?, authorizationPolicyId?, flagged?, flagReason? }
//
//	  - Memo: Content is the base64 of the legacy Yjs update (v1 OR v2 — the
//	    converter probes both). Absent (never-edited memo) ⇒ Content == "".
//	  - Whiteboard: Content is the decompressed Excalidraw JSON string. Empty
//	    whiteboard ⇒ Content == "".
//	  - Flagged: the server could not read the legacy blob (e.g. corrupt
//	    compression). A flagged record is carried through and re-flagged, never
//	    dropped (SC-009 corrupt-blob = flag-not-drop).
type LegacyRecord struct {
	ID                    string `json:"id"`
	ContentType           string `json:"contentType"`
	Content               string `json:"content,omitempty"`
	AuthorizationPolicyID string `json:"authorizationPolicyId,omitempty"`
	Flagged               bool   `json:"flagged,omitempty"`
	FlagReason            string `json:"flagReason,omitempty"`
}

// contentType maps the legacy record's content type string onto the domain enum,
// rejecting anything unrecognised (a corrupt/unknown row is flagged, not guessed).
func (r LegacyRecord) contentType() (model.ContentType, error) {
	switch model.ContentType(r.ContentType) {
	case model.ContentTypeMemo:
		return model.ContentTypeMemo, nil
	case model.ContentTypeWhiteboard:
		return model.ContentTypeWhiteboard, nil
	default:
		return "", fmt.Errorf("unknown content type %q", r.ContentType)
	}
}

// Outcome is the terminal disposition of a single document in a migration run.
type Outcome string

const (
	// OutcomeMigrated marks a document converted and persisted (or, in dry-run,
	// converted and validated) successfully.
	OutcomeMigrated Outcome = "migrated"
	// OutcomeSkipped marks a document already migrated at the target version
	// (idempotent re-run) or with empty/never-edited content (nothing to convert).
	OutcomeSkipped Outcome = "skipped"
	// OutcomeFlagged marks a document that could not be converted/validated
	// (corrupt source, failed round-trip, size regression). It is recorded for
	// human follow-up and the legacy content is left untouched — never dropped.
	OutcomeFlagged Outcome = "flagged"
)

// Result is the per-document record of a migration run, accumulated into the
// Report. Flagged documents carry the reason so the operator can triage them.
type Result struct {
	ID          string  `json:"id"`
	ContentType string  `json:"contentType"`
	Outcome     Outcome `json:"outcome"`
	// Reason explains a Flagged or Skipped outcome (empty for a clean migrate).
	Reason string `json:"reason,omitempty"`
	// LegacyBytes / SnapshotBytes are the size-baseline inputs for SC-007: the
	// approximate legacy on-the-wire size vs. the new v2 snapshot size.
	LegacyBytes   int `json:"legacyBytes,omitempty"`
	SnapshotBytes int `json:"snapshotBytes,omitempty"`
}
