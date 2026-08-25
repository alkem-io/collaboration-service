//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
)

// jsResult is the JSON the harness emits on its `RESULT <json>` line.
type jsResult struct {
	OK                bool             `json:"ok"`
	Mode              string           `json:"mode"`
	Synced            bool             `json:"synced"`
	PeerAwarenessSeen bool             `json:"peerAwarenessSeen"`
	Text              string           `json:"text"`
	States            int              `json:"states"`
	DecodeErrors      []map[string]any `json:"decodeErrors"`
	Reason            string           `json:"reason"`
}

// jsInteropDir locates test/e2e/jsinterop relative to this test file.
func jsInteropDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd() // .../test/e2e
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := filepath.Join(wd, "jsinterop")
	if _, err := os.Stat(filepath.Join(dir, "harness.mjs")); err != nil {
		t.Fatalf("jsinterop harness not found at %s: %v", dir, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "node_modules")); err != nil {
		t.Skipf("jsinterop node_modules missing — run `npm ci` in %s (CI installs it): %v", dir, err)
	}
	return dir
}

// runHarness runs the JS harness in the given mode and returns its parsed result.
// It returns an error (rather than calling t.Fatalf) so it is safe to call from a
// spawned goroutine: t.Fatal/FailNow from a non-test goroutine is undefined. The
// caller decides how to fail. A non-zero exit (e.g. a decode error or timeout) is
// reported through the returned result's OK=false plus the raw output for triage.
func runHarness(ctx context.Context, dir, wsBase, docID, mode, marker, expect string) (jsResult, string, error) {
	url := wsBase + "/collab/" + docID + "?type=memo"
	args := []string{
		"harness.mjs",
		"--url", url,
		"--mode", mode,
		"--marker", marker,
		"--expect", expect,
		"--timeout", "20000",
	}
	// The harness path and args are test-controlled constants, not user input.
	cmd := exec.CommandContext(ctx, "node", args...) //nolint:gosec // G204: fixed test harness invocation
	cmd.Dir = dir
	out, _ := cmd.CombinedOutput()
	raw := string(out)

	var res jsResult
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, "RESULT ") {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT ")), &res); err != nil {
				return res, raw, fmt.Errorf("parse harness result %q: %w", line, err)
			}
			return res, raw, nil
		}
	}
	return res, raw, fmt.Errorf("harness (%s) produced no RESULT line; output:\n%s", mode, raw)
}

// TestJSInteropTwoJSClients is the headline interop proof: two ACTUAL yjs +
// y-protocols JS clients (the same libraries client-web/whiteboard use) connect
// to the Go server over a raw WebSocket and converge on both the document and
// awareness — using NO custom framing, only canonical y-protocols. If the Go
// server's framing diverged from canonical y-protocols in ANY way (sync OR
// awareness), the harness would report a decode error or fail to converge. This
// is the real proof the Go server speaks canonical y-protocols (the compat the
// framing revert restored), and the highest-value signal in T017.
func TestJSInteropTwoJSClients(t *testing.T) {
	dir := jsInteropDir(t)
	base := testApp(t, standaloneConfig())

	const docID = "e2e-js-interop"
	// The JS harness opens its own socket, so the Go dial path never runs for
	// this document — create it here or the server refuses an id it has never
	// heard of.
	ensureDocument(t, base, docID, "memo")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type out struct {
		res jsResult
		raw string
		err error
	}
	obsCh := make(chan out, 1)
	go func() {
		// runHarness returns an error rather than calling t.Fatal so it is safe
		// here in a spawned goroutine; the main goroutine asserts on out.err.
		res, raw, err := runHarness(ctx, dir, base, docID, "observe", "", "JS-EDIT")
		obsCh <- out{res, raw, err}
	}()

	// Give the observer a moment to connect and sync first.
	time.Sleep(700 * time.Millisecond)
	edit, editRaw, err := runHarness(ctx, dir, base, docID, "edit", "JS-EDIT", "")
	if err != nil {
		t.Fatalf("edit harness: %v", err)
	}
	observed := <-obsCh
	if observed.err != nil {
		t.Fatalf("observe harness: %v", observed.err)
	}

	for _, c := range []struct {
		name string
		res  jsResult
		raw  string
	}{
		{"editor", edit, editRaw},
		{"observer", observed.res, observed.raw},
	} {
		if len(c.res.DecodeErrors) > 0 {
			t.Errorf("JS %s reported y-protocols DECODE ERRORS (framing mismatch): %+v\n%s",
				c.name, c.res.DecodeErrors, c.raw)
		}
		if !c.res.Synced {
			t.Errorf("JS %s never completed the y-protocols sync handshake\n%s", c.name, c.raw)
		}
		if !c.res.OK {
			t.Errorf("JS %s did not converge (reason=%q)\n%s", c.name, c.res.Reason, c.raw)
		}
		if !c.res.PeerAwarenessSeen {
			t.Errorf("JS %s never observed peer awareness (awareness framing)\n%s", c.name, c.raw)
		}
	}

	// The observer's document must contain the editor's marker — proof the JS
	// edit propagated through the Go server to the other JS client.
	if !contains(observed.res.Text, "JS-EDIT") {
		t.Errorf("JS observer doc did not converge to the editor's marker: %q", observed.res.Text)
	}
}

// TestJSInteropJSEditorGoObserver proves cross-implementation convergence: a real
// JS yjs client edits, and a Go wsClient (a second, independent y-protocols
// implementation) observes — the JS edit converges on the Go side. This is the
// strongest cross-impl signal (two different y-protocols implementations agreeing
// over the Go server's canonical framing).
func TestJSInteropJSEditorGoObserver(t *testing.T) {
	dir := jsInteropDir(t)
	base := testApp(t, standaloneConfig())

	const docID = "e2e-js-go-interop"
	// The JS harness opens its own socket, so the Go dial path never runs for
	// this document — create it here or the server refuses an id it has never
	// heard of.
	ensureDocument(t, base, docID, "memo")

	// Go observer connects first and stays connected.
	goObs := dial(t, base, docID, "memo")
	goObs.setAwareness(ycrdt.MakeObject("user", "go-observer")) // so the JS editor sees a peer
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	edit, raw, err := runHarness(ctx, dir, base, docID, "edit", "JS-TO-GO", "")
	if err != nil {
		t.Fatalf("edit harness: %v", err)
	}

	if len(edit.DecodeErrors) > 0 {
		t.Errorf("JS editor reported decode errors against the Go server: %+v\n%s", edit.DecodeErrors, raw)
	}
	if !edit.Synced {
		t.Fatalf("JS editor never synced with the Go server\n%s", raw)
	}

	// The Go observer's doc must converge to the JS editor's text.
	if !eventually(func() bool { return contains(goObs.memoText(), "JS-TO-GO") }) {
		t.Fatalf("Go observer never converged to the JS editor's edit; got %q", goObs.memoText())
	}
}

// TestJSInteropGoEditorJSObserver is T014 / SC-001: the REVERSE direction, which
// the other interop tests do not cover.
//
// The existing pair both have a JS client producing the edit, so they exercise
// the server's INBOUND path — the JS framing that Go must decode. This one
// inverts it: a Go client edits and announces awareness, and a real yjs client
// must decode what the server sends. That is the outbound half of the dispatch
// rebuilt on the core's protocol package, and it is the half a JS client would
// reject silently by simply never converging rather than by erroring.
//
// Both signals are asserted separately because they travel different framing
// paths: the document update rides the sync protocol, awareness rides
// EncodeAwarenessUpdateMessage, and a regression in one leaves the other working.
func TestJSInteropGoEditorJSObserver(t *testing.T) {
	dir := jsInteropDir(t)
	base := testApp(t, standaloneConfig())

	const docID = "e2e-go-to-js-interop"
	// The JS harness opens its own socket, so the Go dial path never runs for
	// this document — create it here or the server refuses an id it has never
	// heard of.
	ensureDocument(t, base, docID, "memo")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The JS observer runs in the background, waiting for the Go client's marker.
	type out struct {
		res jsResult
		raw string
		err error
	}
	obsCh := make(chan out, 1)
	go func() {
		res, raw, err := runHarness(ctx, dir, base, docID, "observe", "", "GO-TO-JS")
		obsCh <- out{res, raw, err}
	}()

	// Give the JS observer time to connect and complete its handshake before the
	// Go client edits, so the assertion is about fan-out rather than about the
	// initial state transfer.
	time.Sleep(700 * time.Millisecond)

	goEditor := dial(t, base, docID, "memo")
	goEditor.setAwareness(ycrdt.MakeObject("user", "go-editor"))
	time.Sleep(100 * time.Millisecond)
	goEditor.insertMemo("GO-TO-JS ")

	observed := <-obsCh
	if observed.err != nil {
		t.Fatalf("observe harness: %v", observed.err)
	}
	if len(observed.res.DecodeErrors) > 0 {
		t.Errorf("the JS client could not decode what the Go server sent: %+v\n%s", observed.res.DecodeErrors, observed.raw)
	}
	if !observed.res.Synced {
		t.Fatalf("the JS client never completed the handshake against the Go server\n%s", observed.raw)
	}
	if !contains(observed.res.Text, "GO-TO-JS") {
		t.Errorf("a real yjs client did not converge to a Go client's edit: %q\n%s", observed.res.Text, observed.raw)
	}
	if !observed.res.PeerAwarenessSeen {
		t.Errorf("a real yjs client never saw the Go client's awareness; the outbound awareness framing does not match y-protocols\n%s", observed.raw)
	}
}
