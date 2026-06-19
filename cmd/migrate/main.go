// Command migrate is the one-time, in-place legacy→unified content migration
// tool (WS-E of 003-unify-collab-yjs). It reads legacy memo/whiteboard content
// (as LegacyContentRecord JSONL — the server migration read-path's export
// format), converts each document to the new authoritative Y.Doc v2 snapshot, and
// writes it through the collaboration-service's own BlobStore + MetadataStore —
// the exact persistence path a live room uses.
//
// THE CUTOVER IS HUMAN-GATED. This tool BUILDS migrated snapshots; it does not
// flip traffic, decommission anything, or run unattended in production. Run it
// deliberately, per the runbook (docs/migration-cutover-runbook.md), with
// --dry-run first.
//
// Usage:
//
//	migrate --source legacy.jsonl                 # convert+persist (uses env config)
//	migrate --source legacy.jsonl --dry-run       # convert+validate, write nothing
//	migrate --source - < legacy.jsonl             # read JSONL from stdin
//	migrate --seed --dry-run                       # run the built-in seed corpus
//	migrate --source legacy.jsonl --report out.json
//
// Backend selection (BLOB_STORE / METADATA_STORE / FILE_SERVICE_* / RABBITMQ_* /
// ALKEMIO_DATABASE_*) is read from the environment via the SAME config loader the
// service uses (internal/config), so the migration writes to whichever blob/meta
// store the deployment is configured for. The whiteboard cross-language step
// needs the Node binding (see --wb-script); without it whiteboards are flagged,
// not dropped.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/migrate"
)

type flags struct {
	source   string
	report   string
	dryRun   bool
	seed     bool
	wbScript string
	nodeBin  string
	maxRatio float64
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.source, "source", "", "path to legacy-record JSONL (\"-\" for stdin); omit with --seed")
	flag.StringVar(&f.report, "report", "", "write the JSON run report to this path (default: stdout)")
	flag.BoolVar(&f.dryRun, "dry-run", false, "convert + validate every document but persist nothing")
	flag.BoolVar(&f.seed, "seed", false, "run the built-in seed corpus instead of --source (smoke test / dry-run)")
	flag.StringVar(&f.wbScript, "wb-script", "", "path to scripts/migrate/whiteboard-to-ydoc.mjs (enables the whiteboard cross-language step; without it whiteboards are flagged)")
	flag.StringVar(&f.nodeBin, "node-bin", "node", "node executable for the whiteboard step")
	flag.Float64Var(&f.maxRatio, "max-size-ratio", migrate.DefaultValidationConfig().MaxSizeRatio, "flag a document whose v2 snapshot exceeds this multiple of its legacy size (SC-007); 0 disables")
	flag.Parse()
	return f
}

func run() int {
	f := parseFlags()

	logger, err := config.NewLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		return 1
	}
	defer func() { _ = logger.Sync() }()

	source, closeSource, err := openSource(f)
	if err != nil {
		logger.Error("open source", zap.Error(err))
		return 1
	}
	defer closeSource()

	driver, cleanup, err := buildDriver(f, source, logger)
	if err != nil {
		logger.Error("build migration backends", zap.Error(err))
		return 1
	}
	defer cleanup()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rep, err := driver.Run(ctx)
	// Always emit the (possibly partial) report — a resumable run wants to know
	// what got done before an abort.
	writeErr := writeReport(f, rep)
	if writeErr != nil {
		logger.Error("write report", zap.Error(writeErr))
	}
	if err != nil {
		logger.Error("migration aborted", zap.Error(err))
		return 1
	}
	// The report is the primary artifact for resumability/triage; if it could not
	// be written, fail the run (exit 1) even though the migration itself completed,
	// so an operator/CI does not mistake a missing report for a clean run.
	if writeErr != nil {
		return 1
	}
	// A run that flagged documents is a non-fatal partial success: exit 2 so a
	// CI/operator notices, but the report distinguishes flagged from failed.
	if rep.Flagged > 0 {
		logger.Warn("migration completed with flagged documents", zap.Int("flagged", rep.Flagged))
		return 2
	}
	return 0
}

// openSource resolves the legacy-record stream: the built-in seed corpus, stdin,
// or a JSONL file.
func openSource(f flags) (migrate.Source, func(), error) {
	if f.seed {
		return migrate.NewSliceSource(migrate.SeedCorpus()), func() {}, nil
	}
	if f.source == "" {
		return nil, nil, fmt.Errorf("--source is required (or use --seed)")
	}
	if f.source == "-" {
		return migrate.NewJSONLSource(os.Stdin), func() {}, nil
	}
	file, err := os.Open(f.source)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", f.source, err)
	}
	return migrate.NewJSONLSource(file), func() { _ = file.Close() }, nil
}

// buildDriver assembles the migration driver: the persistence backends (from env
// config, reusing the service's adapter selection), the converters, and the
// validation config. A seed/dry-run still wires the configured backends so the
// dry-run exercises the real selection (but writes nothing).
func buildDriver(f flags, source migrate.Source, logger *zap.Logger) (*migrate.Driver, func(), error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	backends, cleanup, err := app.BuildMigrationBackends(cfg, logger)
	if err != nil {
		return nil, nil, err
	}

	var wbRunner migrate.NodeRunner
	if f.wbScript != "" {
		wbRunner = migrate.ExecNodeRunner{NodeBin: f.nodeBin, ScriptPath: f.wbScript}
	}

	valCfg := migrate.DefaultValidationConfig()
	valCfg.MaxSizeRatio = f.maxRatio

	d := &migrate.Driver{
		Source:     source,
		Memo:       migrate.MemoConverter{},
		Whitebrd:   migrate.WhiteboardConverter{Runner: wbRunner},
		Blob:       backends.Blob,
		Metadata:   backends.Metadata,
		BlobKind:   backends.BlobKind,
		Validation: valCfg,
		DryRun:     f.dryRun,
		Logger:     logger.Named("migrate"),
	}
	return d, cleanup, nil
}

// writeReport serializes the run report as indented JSON to the report path or
// stdout.
func writeReport(f flags, rep migrate.Report) error {
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if f.report == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(f.report, data, 0o600)
}

func main() {
	os.Exit(run())
}
