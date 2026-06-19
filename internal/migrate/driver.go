package migrate

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// TargetVersion is the metadata version stamped on a migrated document. The
// migration is the document's first unified-service snapshot, so version 1 is the
// natural baseline; a re-run that finds a row already at >= this version skips it
// (idempotency). It is deliberately distinct from any live-room version counter —
// once the service serves the document, the room owns and bumps the version from
// here.
const TargetVersion = 1

// Driver runs the migration: it pulls legacy documents from a Source, converts
// each via the content-type-appropriate Converter, validates the result, and —
// unless DryRun — persists it through the service's own BlobStore + MetadataStore
// (the same ports a live room writes through). It is idempotent (skips documents
// already migrated), resumable (re-running over the same source re-processes in
// order and short-circuits done work), and never drops a document it cannot
// migrate — it flags it for human follow-up.
type Driver struct {
	Source   Source
	Memo     Converter
	Whitebrd Converter
	Blob     port.BlobStore
	Metadata port.MetadataStore
	// BlobKind names the backend Blob writes to, recorded in each metadata row so
	// the document rehydrates from the right place (parity with Room.persist).
	BlobKind model.BlobStoreKind
	// Validation tunes the post-conversion checks.
	Validation ValidationConfig
	// DryRun converts + validates every document but writes nothing (no Blob.Put,
	// no Metadata.Save). The Report still reflects what WOULD happen, including the
	// size baseline and any flags — the human-gate dress rehearsal.
	DryRun bool
	// Logger is structured; nil ⇒ no-op.
	Logger *zap.Logger
}

// Report is the accumulated outcome of a run: per-document results plus rollups.
type Report struct {
	Results  []Result `json:"results"`
	Migrated int      `json:"migrated"`
	Skipped  int      `json:"skipped"`
	Flagged  int      `json:"flagged"`
	Total    int      `json:"total"`
	DryRun   bool     `json:"dryRun"`
}

// Run drives the migration to completion (or until the Source errors). A Source
// error is returned with the partial Report so the caller can see what was done
// before the abort — the run is resumable from where it stopped.
func (d *Driver) Run(ctx context.Context) (Report, error) {
	rep := Report{DryRun: d.DryRun}
	log := d.Logger
	if log == nil {
		log = zap.NewNop()
	}

	for {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		rec, ok, err := d.Source.Next()
		if err != nil {
			return rep, fmt.Errorf("read legacy source: %w", err)
		}
		if !ok {
			break
		}
		res := d.migrateOne(ctx, rec, log)
		rep.Results = append(rep.Results, res)
		rep.Total++
		switch res.Outcome {
		case OutcomeMigrated:
			rep.Migrated++
		case OutcomeSkipped:
			rep.Skipped++
		case OutcomeFlagged:
			rep.Flagged++
		}
	}
	log.Info("migration run complete",
		zap.Int("total", rep.Total), zap.Int("migrated", rep.Migrated),
		zap.Int("skipped", rep.Skipped), zap.Int("flagged", rep.Flagged),
		zap.Bool("dryRun", rep.DryRun))
	return rep, nil
}

// migrateOne processes a single legacy document. It never returns an error: every
// failure mode is captured as an OutcomeFlagged Result so one bad document cannot
// abort the batch (flag-not-drop, SC-009).
func (d *Driver) migrateOne(ctx context.Context, rec LegacyRecord, log *zap.Logger) Result {
	res := Result{ID: rec.ID, ContentType: rec.ContentType}

	// A source-flagged record (server could not read the legacy blob) is carried
	// through as flagged — never silently converted from absent content.
	if rec.Flagged {
		return flag(res, "source-flagged: "+rec.FlagReason)
	}

	ct, err := rec.contentType()
	if err != nil {
		return flag(res, err.Error())
	}

	// Idempotency / resumability: a row already at the target version was migrated
	// on a previous run — skip it (no re-convert, no re-write).
	if !d.DryRun && d.alreadyMigrated(ctx, rec.ID) {
		res.Outcome = OutcomeSkipped
		res.Reason = "already migrated"
		return res
	}

	conv, err := d.convert(ctx, rec, ct)
	if err != nil {
		if errors.Is(err, ErrWhiteboardSeamUnavailable) {
			return flag(res, "whiteboard seam unavailable (build-ahead): "+err.Error())
		}
		return flag(res, "convert: "+err.Error())
	}
	res.LegacyBytes = conv.LegacyBytes
	res.SnapshotBytes = len(conv.Snapshot)

	if conv.Empty {
		res.Outcome = OutcomeSkipped
		res.Reason = "empty legacy content"
		return res
	}

	if err := Validate(conv, d.Validation); err != nil {
		return flag(res, "validate: "+err.Error())
	}

	if d.DryRun {
		res.Outcome = OutcomeMigrated // would-migrate; nothing written.
		res.Reason = "dry-run (not persisted)"
		return res
	}

	if err := d.persist(ctx, rec, ct, conv); err != nil {
		return flag(res, "persist: "+err.Error())
	}
	res.Outcome = OutcomeMigrated
	log.Debug("migrated document", zap.String("id", rec.ID), zap.String("type", rec.ContentType),
		zap.Int("legacyBytes", res.LegacyBytes), zap.Int("snapshotBytes", res.SnapshotBytes))
	return res
}

// convert dispatches to the content-type converter, passing the run context so a
// converter that shells out aborts on cancellation.
func (d *Driver) convert(ctx context.Context, rec LegacyRecord, ct model.ContentType) (Conversion, error) {
	switch ct {
	case model.ContentTypeMemo:
		return d.Memo.Convert(ctx, rec)
	case model.ContentTypeWhiteboard:
		return d.Whitebrd.Convert(ctx, rec)
	default:
		return Conversion{}, fmt.Errorf("no converter for content type %q", ct)
	}
}

// alreadyMigrated reports whether the document already has a metadata row at (or
// past) the target version — the idempotency short-circuit. A not-found row (or a
// load error, treated conservatively as not-migrated) means proceed: a transient
// load error must not skip a document silently, so we re-attempt the migration
// (persist is itself idempotent — it overwrites the snapshot + upserts the row).
func (d *Driver) alreadyMigrated(ctx context.Context, id string) bool {
	meta, err := d.Metadata.Load(ctx, model.DocumentID(id))
	if err != nil {
		return false
	}
	return meta.Version >= TargetVersion
}

// persist writes the converted snapshot through the service's own ports — the
// SAME path Room.persist uses: Blob.Put the v2 snapshot, then Metadata.Save the
// index row (content_pointer + blob_store + content_type + policy id + version).
// On first migration the pointer hint is the document id (inline pointer == id);
// a re-run reuses the recorded pointer so a re-write lands in place.
func (d *Driver) persist(ctx context.Context, rec LegacyRecord, ct model.ContentType, conv Conversion) error {
	hint := rec.ID
	if existing, err := d.Metadata.Load(ctx, model.DocumentID(rec.ID)); err == nil && existing.ContentPointer != "" {
		hint = existing.ContentPointer
	}

	pointer, err := d.Blob.Put(ctx, hint, conv.Snapshot)
	if err != nil {
		return fmt.Errorf("blob put: %w", err)
	}

	meta := model.Metadata{
		ID:                    model.DocumentID(rec.ID),
		ContentType:           ct,
		Version:               TargetVersion,
		ContentPointer:        pointer,
		BlobStore:             d.BlobKind,
		AuthorizationPolicyID: rec.AuthorizationPolicyID,
	}
	if err := d.Metadata.Save(ctx, meta); err != nil {
		// The blob is already written; the row failed. Leave the orphan blob (it
		// is overwritten on the next attempt) and surface the error so the doc is
		// flagged and re-run — never half-record a migration as done.
		return fmt.Errorf("metadata save: %w", err)
	}
	return nil
}

// flag stamps a Result as flagged with a reason (the single place an OutcomeFlagged
// is constructed, so the reason is always set).
func flag(res Result, reason string) Result {
	res.Outcome = OutcomeFlagged
	res.Reason = reason
	return res
}
