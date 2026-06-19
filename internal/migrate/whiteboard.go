package migrate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// WhiteboardConverter converts a legacy whiteboard. This is the CROSS-LANGUAGE
// SEAM of the migration (the only one).
//
// The Excalidraw-JSON → id-keyed Y.Map transform is owned by the TypeScript
// binding `@alkemio/excalidraw-yjs-binding` (populateYDoc) — the SAME binding
// the client uses, so the migrated scene is guaranteed structurally identical to
// what a live editor would produce. Re-implementing that transform in Go would
// duplicate non-trivial logic (per-property element maps, fractional-index repair,
// boundElements sub-maps, JSON-leaf handling) and risk drift; the constitution
// forbids duplicated logic. So Go shells out to a small Node step
// (scripts/migrate/whiteboard-to-ydoc.mjs) that:
//
//	reads the Excalidraw JSON on stdin → populateYDoc(scene, new Y.Doc())
//	→ prints base64(Y.encodeStateAsUpdate(doc)) on stdout   [Yjs *v1* update]
//
// The binding emits v1 (it depends on yjs ^13.6 and uses encodeStateAsUpdate). Go
// then decodes that v1 update and re-encodes the canonical v2 snapshot, so the
// whiteboard path lands on the SAME persistence path as memos — one snapshot
// format (v2), one BlobStore.Put. This mirrors the repo's existing Go→Node
// y-protocols interop (test/e2e/jsinterop).
//
// ── STUB / PENDING ───────────────────────────────────────────────────────────
// The Node step requires `@alkemio/excalidraw-yjs-binding` to be installed
// (scripts/migrate must `npm ci`). The binding's package.json declares it public
// (version 0.18.0, publishConfig.access=public) but it may not yet be PUBLISHED
// to the registry at migration time. Until it is, the Node step is run from a
// local build of the excalidraw-fork workspace (documented in the runbook +
// scripts/migrate/README.md). If NodeRunner is nil OR the Node step is
// unavailable, WhiteboardConverter.Convert returns ErrWhiteboardSeamUnavailable
// and the driver FLAGS the document (never drops it) — the whiteboard corpus is
// then re-run once the binding is installed. This keeps the Go side fully built,
// tested, and dry-runnable today, with exactly one clearly-marked external
// dependency.
type WhiteboardConverter struct {
	// Runner executes the Node Excalidraw→YDoc step. Nil ⇒ the seam is
	// unavailable and every whiteboard is flagged (build-ahead default).
	Runner NodeRunner
}

// ErrWhiteboardSeamUnavailable is returned when the cross-language Node step is
// not wired (the published binding is pending). The driver maps it to a flag, so
// whiteboards are deferred — not lost — until the seam is installed.
var ErrWhiteboardSeamUnavailable = errors.New("whiteboard migration seam unavailable: the @alkemio/excalidraw-yjs-binding Node step is not installed (build-ahead stub — see scripts/migrate/README.md)")

// NodeRunner runs the Excalidraw-JSON → Yjs-v1-update Node step: given the legacy
// Excalidraw JSON, it returns the v1-encoded Y.Doc update bytes. Abstracted so
// the converter is unit-testable without Node (the tests inject a fake that runs
// the equivalent transform in-process), and so the concrete invocation
// (exec node script vs. a future in-process WASM build of the binding) is swappable.
type NodeRunner interface {
	// ToYUpdateV1 converts the Excalidraw scene JSON to a Yjs v1 update.
	ToYUpdateV1(ctx context.Context, excalidrawJSON []byte) ([]byte, error)
}

// Convert runs the legacy Excalidraw JSON through the Node binding step and
// re-encodes a v2 snapshot. An empty whiteboard (Content == "") yields Empty=true.
// A nil Runner (build-ahead) returns ErrWhiteboardSeamUnavailable.
func (c WhiteboardConverter) Convert(ctx context.Context, rec LegacyRecord) (Conversion, error) {
	if strings.TrimSpace(rec.Content) == "" {
		return Conversion{Empty: true}, nil
	}
	if c.Runner == nil {
		return Conversion{}, ErrWhiteboardSeamUnavailable
	}

	// Sanity-check the legacy content is JSON before paying for a subprocess.
	if !json.Valid([]byte(rec.Content)) {
		return Conversion{}, fmt.Errorf("legacy whiteboard content is not valid JSON (%d bytes)", len(rec.Content))
	}

	// Derive the per-record timeout from the caller's context so a run-level
	// cancellation (SIGINT/SIGTERM, deadline) aborts the Node subprocess promptly
	// rather than waiting out the full 60s.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	v1, err := c.Runner.ToYUpdateV1(ctx, []byte(rec.Content))
	if err != nil {
		return Conversion{}, fmt.Errorf("excalidraw→ydoc node step: %w", err)
	}

	// Decode the v1 update from the binding, then re-encode the canonical v2
	// snapshot (uniform persistence with memos). Apply the whiteboard convention
	// first so an empty/structurally-minimal scene still gets the root maps.
	doc := service.NewMigrationDoc(rec.ID)
	service.ApplyWhiteboardConvention(doc)
	if !tryApply(func() { ycrdt.ApplyUpdate(doc, v1, migrationOrigin) }) {
		return Conversion{}, fmt.Errorf("node step produced an undecodable Yjs v1 update (%d bytes)", len(v1))
	}

	return Conversion{
		Snapshot:    ycrdt.EncodeStateAsUpdateV2(doc, nil),
		LegacyBytes: len(rec.Content),
	}, nil
}

// ExecNodeRunner is the production NodeRunner: it execs `node <script>` and pipes
// the Excalidraw JSON on stdin, reading base64(v1-update) from stdout. The script
// path and node binary are configurable so the runbook can point at a built
// excalidraw-fork workspace before the binding is published.
type ExecNodeRunner struct {
	// NodeBin is the node executable (default "node").
	NodeBin string
	// ScriptPath is the absolute path to scripts/migrate/whiteboard-to-ydoc.mjs.
	ScriptPath string
}

// ToYUpdateV1 execs the Node step. stdout is `BASE64 <b64>` on success (any other
// diagnostics go to stderr, which is surfaced on error).
func (r ExecNodeRunner) ToYUpdateV1(ctx context.Context, excalidrawJSON []byte) ([]byte, error) {
	nodeBin := r.NodeBin
	if nodeBin == "" {
		nodeBin = "node"
	}
	if r.ScriptPath == "" {
		return nil, errors.New("ExecNodeRunner.ScriptPath is empty")
	}

	// The node binary and script path are operator-supplied migration config
	// (--node-bin / --wb-script), not attacker-controlled input — this is a CLI
	// tool a human runs deliberately, per the runbook. #nosec G204
	cmd := exec.CommandContext(ctx, nodeBin, r.ScriptPath) //nolint:gosec // operator-supplied migration config
	cmd.Stdin = bytes.NewReader(excalidrawJSON)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("node step failed: %w (stderr: %s)", err, truncate(strings.TrimSpace(stderr.String()), 500))
	}

	line := strings.TrimSpace(stdout.String())
	const prefix = "BASE64 "
	if !strings.HasPrefix(line, prefix) {
		return nil, fmt.Errorf("node step: unexpected stdout %q (stderr: %s)", truncate(line, 120), truncate(strings.TrimSpace(stderr.String()), 500))
	}
	b64 := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	update, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("node step: decode base64 update: %w", err)
	}
	if len(update) == 0 {
		return nil, errors.New("node step produced an empty update")
	}
	return update, nil
}

// truncate bounds a diagnostic string.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
