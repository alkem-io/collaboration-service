package migrate

import (
	"context"
	"encoding/base64"
	"fmt"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// Conversion is the output of converting one legacy document to the new
// authoritative form: the v2-encoded snapshot bytes to persist, plus the
// approximate legacy input size for the SC-007 size baseline.
type Conversion struct {
	// Snapshot is the v2-encoded full Y.Doc state — byte-identical in shape to
	// what Room.persist writes (EncodeStateAsUpdateV2), so a migrated document
	// rehydrates through the normal loadSnapshot path (ApplyUpdateV2).
	Snapshot []byte
	// LegacyBytes is the decoded legacy input size (the memo update bytes / the
	// whiteboard JSON bytes), the SC-007 baseline numerator.
	LegacyBytes int
	// Empty is true when the legacy content was empty/never-edited: there is
	// nothing to migrate and the driver records OutcomeSkipped.
	Empty bool
}

// Converter turns one legacy document's content into a v2 snapshot. Implementations
// are content-type specific (memo = pure Go; whiteboard = cross-language).
type Converter interface {
	// Convert produces the v2 snapshot (and size baseline) for one legacy record,
	// or Conversion{Empty:true} when the legacy content was empty/never-edited.
	// ctx carries the run's cancellation/deadline so a converter that shells out
	// (whiteboard → Node) aborts promptly on SIGINT/SIGTERM.
	Convert(ctx context.Context, rec LegacyRecord) (Conversion, error)
}

// MemoConverter converts a legacy memo. Legacy memo content is the base64 of a
// Yjs update that may be v1 (older clients) OR v2 (newer) — the server stores the
// raw bytes and the read-path base64-encodes them. Yjs is already a CRDT, so the
// conversion is a decode→re-encode: apply the legacy update into a fresh GC'd
// Y.Doc (probing v2 then v1), then re-encode the canonical v2 snapshot. The memo
// convention (root Y.XmlFragment "default") is applied so an empty/structurally
// minimal doc still rehydrates with the expected root (parity with a live room's
// applyConvention).
type MemoConverter struct{}

// Convert decodes the legacy memo update and re-encodes a v2 snapshot. An empty
// Content (never-edited memo) yields Empty=true. Garbage that decodes as neither
// v1 nor v2 is an error (the driver flags it — never silently drops).
func (MemoConverter) Convert(_ context.Context, rec LegacyRecord) (Conversion, error) {
	if rec.Content == "" {
		return Conversion{Empty: true}, nil
	}
	raw, err := base64.StdEncoding.DecodeString(rec.Content)
	if err != nil {
		return Conversion{}, fmt.Errorf("base64-decode memo update: %w", err)
	}

	doc, err := decodeLegacyUpdate(rec.ID, raw)
	if err != nil {
		return Conversion{}, err
	}
	service.ApplyMemoConvention(doc)

	return Conversion{
		Snapshot:    ycrdt.EncodeStateAsUpdateV2(doc, nil),
		LegacyBytes: len(raw),
	}, nil
}

// decodeLegacyUpdate applies a legacy Yjs update of unknown wire version into a
// fresh GC'd doc and returns it. It probes v2 first (the format newer clients
// persist and the format we re-emit), then v1 — each into a SEPARATE fresh doc so
// a partial apply from the wrong codec never leaks into the result.
//
// Decoding the wrong wire version (or pure garbage) can either panic the codec OR
// "succeed" while decoding NOTHING (the v2 decoder, fed garbage, may leave the
// doc empty rather than erroring). So each attempt must BOTH not panic (tryApply)
// AND yield a non-empty document — a non-trivial input that decodes to zero
// client structs is a decode miss, not a valid empty doc. Only when neither
// version produces state do we give up and let the driver flag the document
// (never silently persisting an empty snapshot for content that was not empty).
func decodeLegacyUpdate(guid string, update []byte) (*ycrdt.Doc, error) {
	v2 := service.NewMigrationDoc(guid)
	if tryApply(func() { ycrdt.ApplyUpdateV2(v2, update, migrationOrigin) }) && docHasState(v2) {
		return v2, nil
	}
	v1 := service.NewMigrationDoc(guid)
	if tryApply(func() { ycrdt.ApplyUpdate(v1, update, migrationOrigin) }) && docHasState(v1) {
		return v1, nil
	}
	return nil, fmt.Errorf("legacy memo update decodes as neither Yjs v2 nor v1, or decodes to empty (%d bytes)", len(update))
}

// docHasState reports whether the doc decoded any client structs — the signal
// that an apply actually ingested content rather than no-op'ing on a wrong-codec
// or garbage update.
func docHasState(doc *ycrdt.Doc) bool {
	return doc.Store != nil && len(doc.Store.Clients) > 0
}

// tryApply runs fn, recovering a panic from the codec into a boolean so the
// converter can fall back to the other wire version instead of crashing the run.
func tryApply(fn func()) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
		}
	}()
	fn()
	return true
}

// migrationOrigin tags migration-applied updates so they are distinguishable from
// live edits if a doc observer is ever attached (none is, in the batch path).
var migrationOrigin = "migration"
