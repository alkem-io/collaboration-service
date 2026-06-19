package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// errBlob is a BlobStore whose Put fails, to drive the driver's persist blob-error
// branch.
type errBlob struct {
	port.BlobStore
	putErr error
}

func (b errBlob) Put(context.Context, string, []byte) (string, error) {
	return "", b.putErr
}

func (b errBlob) Delete(context.Context, string) error { return nil }

// errMeta wraps a MetadataStore and forces Save to fail (Load delegates so the
// idempotency check still works), driving persist's metadata-error branch.
type errMeta struct {
	port.MetadataStore
	saveErr error
}

func (m errMeta) Save(context.Context, model.Metadata) error { return m.saveErr }

// TestValidateRejectsEmptySnapshot asserts a non-Empty conversion with a
// zero-length snapshot is rejected (the producer-side invariant).
func TestValidateRejectsEmptySnapshot(t *testing.T) {
	err := Validate(Conversion{Empty: false, Snapshot: nil}, DefaultValidationConfig())
	if err == nil {
		t.Fatal("Validate(empty snapshot) must error")
	}
}

// TestValidateEmptyConversionIsOK asserts an Empty conversion short-circuits to no
// error (nothing to validate).
func TestValidateEmptyConversionIsOK(t *testing.T) {
	if err := Validate(Conversion{Empty: true}, DefaultValidationConfig()); err != nil {
		t.Fatalf("Validate(Empty) should be nil, got %v", err)
	}
}

// TestValidateRejectsOversizedSnapshot asserts the SC-007 size-ratio ceiling: a
// snapshot far larger than the legacy input (above the floor) is flagged.
func TestValidateRejectsOversizedSnapshot(t *testing.T) {
	// Build a real, reloadable memo snapshot, then assert it trips the ratio check
	// when the legacy baseline is set tiny and the ratio ceiling + floor are low.
	rec := LegacyRecord{ID: "big", ContentType: "memo", Content: memoUpdate(t, "big", "some content that encodes to a non-trivial snapshot", true)}
	conv, err := MemoConverter{}.Convert(context.Background(), rec)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	conv.LegacyBytes = 1 // tiny baseline so any real snapshot blows the ratio
	cfg := ValidationConfig{MaxSizeRatio: 1.5, SizeFloorBytes: 1}
	if err := Validate(conv, cfg); err == nil {
		t.Fatal("Validate must flag a snapshot that exceeds the size-ratio ceiling")
	}
}

// TestBytesEqual asserts the local equality helper: length mismatch and per-byte
// mismatch both return false; identical slices return true.
func TestBytesEqual(t *testing.T) {
	if bytesEqual([]byte{1, 2}, []byte{1, 2, 3}) {
		t.Error("length mismatch must be unequal")
	}
	if bytesEqual([]byte{1, 2, 3}, []byte{1, 9, 3}) {
		t.Error("byte mismatch must be unequal")
	}
	if !bytesEqual([]byte{1, 2, 3}, []byte{1, 2, 3}) {
		t.Error("identical slices must be equal")
	}
}

// TestConvertUnknownContentTypeErrors asserts the driver's convert dispatch
// returns an error for an unsupported content type (no converter).
func TestConvertUnknownContentTypeErrors(t *testing.T) {
	d, _, _ := newDriver(NewSliceSource(nil), false, WhiteboardConverter{})
	if _, err := d.convert(context.Background(), LegacyRecord{ID: "x"}, model.ContentType("spreadsheet")); err == nil {
		t.Fatal("convert(unknown content type) must error")
	}
}

// TestPersistBlobPutErrorPropagates asserts a Blob.Put failure during persist is
// surfaced (so the document is flagged + re-run, never half-recorded).
func TestPersistBlobPutErrorPropagates(t *testing.T) {
	d, _, _ := newDriver(NewSliceSource(nil), false, WhiteboardConverter{})
	d.Blob = errBlob{putErr: errors.New("blob down")}
	conv := Conversion{Snapshot: []byte("snap")}
	if err := d.persist(context.Background(), LegacyRecord{ID: "doc"}, model.ContentTypeMemo, conv); err == nil {
		t.Fatal("persist must surface a Blob.Put error")
	}
}

// TestPersistMetadataSaveErrorPropagates asserts a Metadata.Save failure during
// persist is surfaced (the blob is written but the row failed — never half-record).
func TestPersistMetadataSaveErrorPropagates(t *testing.T) {
	d, _, meta := newDriver(NewSliceSource(nil), false, WhiteboardConverter{})
	d.Metadata = errMeta{MetadataStore: meta, saveErr: errors.New("index down")}
	conv := Conversion{Snapshot: []byte("snap")}
	if err := d.persist(context.Background(), LegacyRecord{ID: "doc"}, model.ContentTypeMemo, conv); err == nil {
		t.Fatal("persist must surface a Metadata.Save error")
	}
}

// TestPersistReusesExistingPointer asserts a re-run reuses the recorded content
// pointer as the blob hint (so a re-write lands in place rather than creating a
// new object), the idempotent re-persist path.
func TestPersistReusesExistingPointer(t *testing.T) {
	d, _, meta := newDriver(NewSliceSource(nil), false, WhiteboardConverter{})
	ctx := context.Background()
	// Seed an existing row with a content pointer.
	if err := meta.Save(ctx, model.Metadata{
		ID: "doc-reuse", ContentType: model.ContentTypeMemo, Version: 1,
		ContentPointer: "doc-reuse", BlobStore: model.BlobStoreInline,
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	conv := Conversion{Snapshot: []byte("snap-v2")}
	if err := d.persist(ctx, LegacyRecord{ID: "doc-reuse"}, model.ContentTypeMemo, conv); err != nil {
		t.Fatalf("persist (reuse pointer): %v", err)
	}
	// The row is upserted with the reused content pointer (the existing pointer is
	// fed back as the blob hint, so the re-write lands in place).
	got, err := meta.Load(ctx, "doc-reuse")
	if err != nil {
		t.Fatalf("load after persist: %v", err)
	}
	if got.ContentPointer != "doc-reuse" {
		t.Fatalf("re-persist did not reuse the existing content pointer: %+v", got)
	}
}
