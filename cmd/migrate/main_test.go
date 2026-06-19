package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
	"github.com/alkem-io/collaboration-service/internal/migrate"
)

// cleanMemoRecordLine builds a single legacy-memo JSONL line whose content is a
// real base64 v2 Yjs update (built by the same y-crdt core the service runs), so
// the memo converter migrates it cleanly — yielding a run with no flagged docs.
func cleanMemoRecordLine(t *testing.T) string {
	t.Helper()
	doc := service.NewMigrationDoc("clean-memo")
	frag := doc.GetXmlFragment("default").(*ycrdt.YXmlFragment)
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, "clean memo content", nil)
	content := base64.StdEncoding.EncodeToString(ycrdt.EncodeStateAsUpdateV2(doc, nil))
	line, err := json.Marshal(migrate.LegacyRecord{ID: "clean-memo", ContentType: "memo", Content: content})
	if err != nil {
		t.Fatalf("marshal legacy record: %v", err)
	}
	return string(line)
}

// withArgs runs fn with os.Args and the default flag.CommandLine reset to a fresh
// FlagSet, so a test can exercise parseFlags/run without the go-test harness's own
// flags leaking in. It restores both afterwards.
func withArgs(t *testing.T, args []string, fn func()) {
	t.Helper()
	origArgs := os.Args
	origFlags := flag.CommandLine
	defer func() {
		os.Args = origArgs
		flag.CommandLine = origFlags
	}()
	os.Args = append([]string{"migrate"}, args...)
	flag.CommandLine = flag.NewFlagSet("migrate", flag.ContinueOnError)
	fn()
}

// TestParseFlagsDefaults asserts the flag defaults match the documented behaviour
// (empty source, stdout report, no dry-run, node default, the validation ratio).
func TestParseFlagsDefaults(t *testing.T) {
	withArgs(t, nil, func() {
		f := parseFlags()
		if f.source != "" || f.report != "" || f.dryRun || f.seed {
			t.Fatalf("unexpected non-zero defaults: %+v", f)
		}
		if f.nodeBin != "node" {
			t.Errorf("nodeBin default = %q, want node", f.nodeBin)
		}
		if f.maxRatio != migrate.DefaultValidationConfig().MaxSizeRatio {
			t.Errorf("maxRatio default = %v, want %v", f.maxRatio, migrate.DefaultValidationConfig().MaxSizeRatio)
		}
	})
}

// TestParseFlagsParsesValues asserts the flags bind the provided values.
func TestParseFlagsParsesValues(t *testing.T) {
	withArgs(t, []string{"--source", "x.jsonl", "--report", "out.json", "--dry-run", "--seed", "--node-bin", "nodejs", "--max-size-ratio", "4.5"}, func() {
		f := parseFlags()
		if f.source != "x.jsonl" || f.report != "out.json" || !f.dryRun || !f.seed || f.nodeBin != "nodejs" || f.maxRatio != 4.5 {
			t.Fatalf("flags not bound: %+v", f)
		}
	})
}

// TestOpenSourceSeed asserts --seed yields the built-in corpus source.
func TestOpenSourceSeed(t *testing.T) {
	src, closeFn, err := openSource(flags{seed: true})
	if err != nil {
		t.Fatalf("openSource(seed): %v", err)
	}
	defer closeFn()
	if src == nil {
		t.Fatal("seed source is nil")
	}
	// The seed corpus is non-empty: at least one record must read out.
	if _, ok, err := src.Next(); err != nil || !ok {
		t.Fatalf("seed source produced no records (ok=%v err=%v)", ok, err)
	}
}

// TestOpenSourceMissingSourceErrors asserts an empty --source (and no --seed) is a
// configuration error.
func TestOpenSourceMissingSourceErrors(t *testing.T) {
	if _, _, err := openSource(flags{}); err == nil {
		t.Fatal("openSource with no source and no seed must error")
	}
}

// TestOpenSourceFile asserts a real JSONL file is opened and read.
func TestOpenSourceFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.jsonl")
	// One minimal legacy memo record.
	line := `{"id":"doc-1","contentType":"memo","content":""}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	src, closeFn, err := openSource(flags{source: path})
	if err != nil {
		t.Fatalf("openSource(file): %v", err)
	}
	defer closeFn()
	rec, ok, err := src.Next()
	if err != nil || !ok || rec.ID != "doc-1" {
		t.Fatalf("file source read = (%+v, ok=%v, err=%v)", rec, ok, err)
	}
}

// TestOpenSourceFileMissingErrors asserts a non-existent --source path errors.
func TestOpenSourceFileMissingErrors(t *testing.T) {
	if _, _, err := openSource(flags{source: filepath.Join(t.TempDir(), "nope.jsonl")}); err == nil {
		t.Fatal("openSource of a missing file must error")
	}
}

// TestOpenSourceStdin asserts "-" reads from stdin.
func TestOpenSourceStdin(t *testing.T) {
	origStdin := os.Stdin
	defer func() { os.Stdin = origStdin }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdin = r
	go func() {
		_, _ = w.WriteString(`{"id":"piped","contentType":"memo","content":""}` + "\n")
		_ = w.Close()
	}()

	src, closeFn, err := openSource(flags{source: "-"})
	if err != nil {
		t.Fatalf("openSource(-): %v", err)
	}
	defer closeFn()
	rec, ok, err := src.Next()
	if err != nil || !ok || rec.ID != "piped" {
		t.Fatalf("stdin source read = (%+v, ok=%v, err=%v)", rec, ok, err)
	}
}

// TestWriteReportToStdout asserts an empty report path writes JSON to stdout.
func TestWriteReportToStdout(t *testing.T) {
	origStdout := os.Stdout
	defer func() { os.Stdout = origStdout }()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	rep := migrate.Report{Migrated: 2, Total: 3, Flagged: 1}
	writeErr := writeReport(flags{}, rep)
	_ = w.Close()

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	out := string(buf[:n])
	if writeErr != nil {
		t.Fatalf("writeReport(stdout): %v", writeErr)
	}
	if !strings.Contains(out, `"migrated": 2`) || !strings.Contains(out, `"total": 3`) {
		t.Fatalf("stdout report missing fields: %s", out)
	}
}

// TestWriteReportToFile asserts a report path writes the JSON file (0600).
func TestWriteReportToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	rep := migrate.Report{Migrated: 5, Total: 5}
	if err := writeReport(flags{report: path}, rep); err != nil {
		t.Fatalf("writeReport(file): %v", err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // test reads a file it just wrote under t.TempDir()
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var got migrate.Report
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}
	if got.Migrated != 5 || got.Total != 5 {
		t.Fatalf("round-tripped report mismatch: %+v", got)
	}
}

// TestRunRejectsSeedWithoutDryRun asserts the --seed safety guard: running the
// synthetic seed corpus without --dry-run is refused (exit 1) so seed data is
// never persisted into a real store.
func TestRunRejectsSeedWithoutDryRun(t *testing.T) {
	withArgs(t, []string{"--seed"}, func() {
		if code := run(); code != 1 {
			t.Fatalf("run(--seed without --dry-run) = %d, want 1 (guard must refuse)", code)
		}
	})
}

// TestRunSeedDryRunStandalone drives the full run() happy path against the
// standalone (inmemory metadata + inline blob) backends with the built-in seed
// corpus and --dry-run. This exercises buildDriver (config.Load +
// BuildMigrationBackends), the driver Run, and writeReport end to end without any
// external dependency or persisted state. The seed corpus converts cleanly, so the
// expected exit is 0 (no flagged documents).
func TestRunSeedDryRunStandalone(t *testing.T) {
	// Force the zero-dependency selection so config.Load needs nothing external.
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("BLOB_STORE", "inline")
	t.Setenv("AUTH_MODE", "open")
	t.Setenv("FANOUT_MODE", "inmemory")

	// Report to a temp file so the run does not spew JSON to the test's stdout.
	reportPath := filepath.Join(t.TempDir(), "seed-report.json")

	withArgs(t, []string{"--seed", "--dry-run", "--report", reportPath}, func() {
		code := run()
		// 0 = all migrated/skipped cleanly; 2 = some flagged (still a valid run). The
		// seed corpus is designed to convert, so 0 is expected; tolerate 2 so a
		// seed-corpus tweak that flags a doc does not make this brittle. 1 (abort)
		// must NOT happen.
		if code == 1 {
			t.Fatalf("run(--seed --dry-run, standalone) aborted (exit 1)")
		}
	})

	// The report artifact was written and is valid JSON reflecting a dry run.
	data, err := os.ReadFile(reportPath) //nolint:gosec // test reads a file run() wrote under t.TempDir()
	if err != nil {
		t.Fatalf("read seed report: %v", err)
	}
	var rep migrate.Report
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("seed report is not valid JSON: %v", err)
	}
	if !rep.DryRun {
		t.Error("seed run report should be marked dryRun")
	}
	if rep.Total == 0 {
		t.Error("seed run processed no documents")
	}
}

// TestRunFailsOnMissingSource asserts run() exits 1 when the source file does not
// exist (the openSource error branch).
func TestRunFailsOnMissingSource(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.jsonl")
	withArgs(t, []string{"--source", missing}, func() {
		if code := run(); code != 1 {
			t.Fatalf("run(missing source) = %d, want 1", code)
		}
	})
}

// TestRunCleanSourceExitsZero drives run() over a source that converts cleanly
// (a single real memo, no whiteboards to flag) so the run returns 0 — the
// all-migrated success path.
func TestRunCleanSourceExitsZero(t *testing.T) {
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("BLOB_STORE", "inline")
	t.Setenv("AUTH_MODE", "open")

	// A real v2 memo update, base64-encoded, that the Go memo converter migrates.
	memo := cleanMemoRecordLine(t)
	srcPath := filepath.Join(t.TempDir(), "clean.jsonl")
	if err := os.WriteFile(srcPath, []byte(memo+"\n"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	reportPath := filepath.Join(t.TempDir(), "report.json")

	withArgs(t, []string{"--source", srcPath, "--dry-run", "--report", reportPath}, func() {
		if code := run(); code != 0 {
			t.Fatalf("run(clean memo source) = %d, want 0", code)
		}
	})
}

// TestRunFailsWhenReportUnwritable asserts run() exits 1 when the run completes but
// the report cannot be written (an unwritable report path) — the report is the
// primary artifact, so a missing report must not look like a clean run.
func TestRunFailsWhenReportUnwritable(t *testing.T) {
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("BLOB_STORE", "inline")
	t.Setenv("AUTH_MODE", "open")

	// A directory path that is not a writable file target: point --report at a path
	// whose parent does not exist, so os.WriteFile fails.
	badReport := filepath.Join(t.TempDir(), "no-such-dir", "report.json")

	withArgs(t, []string{"--seed", "--dry-run", "--report", badReport}, func() {
		if code := run(); code != 1 {
			t.Fatalf("run(unwritable report) = %d, want 1", code)
		}
	})
}

// TestRunFailsOnBadBackendConfig asserts run() exits 1 when buildDriver cannot
// wire the backends — here BLOB_STORE=local with no LOCAL_BLOB_ROOT, which
// buildBlob rejects (the buildDriver error branch).
func TestRunFailsOnBadBackendConfig(t *testing.T) {
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("BLOB_STORE", "local") // requires LOCAL_BLOB_ROOT, which we leave unset
	t.Setenv("LOCAL_BLOB_ROOT", "")
	t.Setenv("AUTH_MODE", "open")

	withArgs(t, []string{"--seed", "--dry-run"}, func() {
		if code := run(); code != 1 {
			t.Fatalf("run(bad backend config) = %d, want 1", code)
		}
	})
}
