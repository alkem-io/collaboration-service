package migrate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// nodeAvailable reports whether a `node` binary is on PATH; the exec tests skip
// without it (CI installs node for the e2e harness, so they run there).
func nodeAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; skipping ExecNodeRunner test")
	}
}

// writeScript drops a throwaway .mjs script in a temp dir and returns its path.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "step.mjs")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestExecNodeRunner_EmitsBase64(t *testing.T) {
	nodeAvailable(t)
	// A stand-in step that echoes a fixed base64 update (the contract is just the
	// "BASE64 <b64>" stdout line — the bytes' meaning is the binding's concern).
	script := writeScript(t, `
import process from 'node:process'
let raw=''; process.stdin.on('data',c=>raw+=c)
process.stdin.on('end',()=>{ process.stdout.write('BASE64 '+Buffer.from([1,2,3,4]).toString('base64')+'\n') })
`)
	out, err := ExecNodeRunner{ScriptPath: script}.ToYUpdateV1(context.Background(), []byte(`{"elements":[]}`))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out) != 4 || out[0] != 1 || out[3] != 4 {
		t.Fatalf("unexpected decoded update: %v", out)
	}
}

func TestExecNodeRunner_NonZeroExitErrors(t *testing.T) {
	nodeAvailable(t)
	script := writeScript(t, `
import process from 'node:process'
process.stderr.write('boom from the step\n'); process.exit(5)
`)
	_, err := ExecNodeRunner{ScriptPath: script}.ToYUpdateV1(context.Background(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "boom from the step") {
		t.Fatalf("want stderr surfaced on non-zero exit, got %v", err)
	}
}

func TestExecNodeRunner_UnexpectedStdoutErrors(t *testing.T) {
	nodeAvailable(t)
	script := writeScript(t, `process.stdout.write('not the prefix\n')`)
	_, err := ExecNodeRunner{ScriptPath: script}.ToYUpdateV1(context.Background(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "unexpected stdout") {
		t.Fatalf("want unexpected-stdout error, got %v", err)
	}
}

func TestExecNodeRunner_BadBase64Errors(t *testing.T) {
	nodeAvailable(t)
	script := writeScript(t, `process.stdout.write('BASE64 @@@not-base64@@@\n')`)
	_, err := ExecNodeRunner{ScriptPath: script}.ToYUpdateV1(context.Background(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "decode base64") {
		t.Fatalf("want base64 decode error, got %v", err)
	}
}

func TestExecNodeRunner_EmptyScriptPathErrors(t *testing.T) {
	_, err := ExecNodeRunner{}.ToYUpdateV1(context.Background(), []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "ScriptPath is empty") {
		t.Fatalf("want empty-script-path error, got %v", err)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("short string should pass through, got %q", got)
	}
	if got := truncate("abcdefghij", 3); got != "abc…" {
		t.Fatalf("want truncated, got %q", got)
	}
}
