package migrate

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// --- helpers ---------------------------------------------------------------

// memoUpdate builds a real memo doc (Y.XmlFragment "default" + text) and returns
// base64(v2-or-v1 update), the legacy on-the-wire shape.
func memoUpdate(t *testing.T, id, text string, v2 bool) string {
	t.Helper()
	doc := service.NewMigrationDoc(id)
	frag := doc.GetXmlFragment("default").(*ycrdt.YXmlFragment)
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, text, nil)
	var upd []byte
	if v2 {
		upd = ycrdt.EncodeStateAsUpdateV2(doc, nil)
	} else {
		upd = ycrdt.EncodeStateAsUpdate(doc, nil)
	}
	return base64.StdEncoding.EncodeToString(upd)
}

// memoTextFromSnapshot rehydrates a v2 snapshot exactly as Room.loadSnapshot does
// and returns the memo's rendered text — the proof the migrated snapshot carries
// the original content (SC-003).
func memoTextFromSnapshot(t *testing.T, snapshot []byte) string {
	t.Helper()
	doc := service.NewMigrationDoc("verify")
	ycrdt.ApplyUpdateV2(doc, snapshot, "verify")
	return doc.GetXmlFragment("default").(*ycrdt.YXmlFragment).ToString()
}

// fakeNodeRunner runs the equivalent of the Node populateYDoc step in-process: it
// builds an id-keyed elements doc from a tiny scene shape and returns a v1 update,
// so the whiteboard converter is exercised without Node.
type fakeNodeRunner struct {
	ids      []string
	failWith error
}

func (f fakeNodeRunner) ToYUpdateV1(_ context.Context, _ []byte) ([]byte, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	doc := service.NewMigrationDoc("wb")
	service.ApplyWhiteboardConvention(doc)
	elements := doc.GetMap("elements").(*ycrdt.YMap)
	for _, id := range f.ids {
		el := ycrdt.NewYMap(nil)
		elements.Set(id, el)
		el.Set("id", id)
		el.Set("type", "rectangle")
	}
	return ycrdt.EncodeStateAsUpdate(doc, nil), nil
}

func newDriver(src Source, dryRun bool, wb Converter) (*Driver, port.BlobStore, port.MetadataStore) {
	blob := inline.New()
	meta := metainmem.New()
	d := &Driver{
		Source:     src,
		Memo:       MemoConverter{},
		Whitebrd:   wb,
		Blob:       blob,
		Metadata:   meta,
		BlobKind:   model.BlobStoreInline,
		Validation: DefaultValidationConfig(),
		DryRun:     dryRun,
	}
	return d, blob, meta
}

// --- memo conversion -------------------------------------------------------

func TestMemoConverter_V2RoundTrips(t *testing.T) {
	rec := LegacyRecord{ID: "m1", ContentType: "memo", Content: memoUpdate(t, "m1", "hello v2", true)}
	conv, err := MemoConverter{}.Convert(rec)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if conv.Empty || len(conv.Snapshot) == 0 {
		t.Fatal("expected a non-empty snapshot")
	}
	if got := memoTextFromSnapshot(t, conv.Snapshot); !strings.Contains(got, "hello v2") {
		t.Fatalf("snapshot lost content: %q", got)
	}
	if err := Validate(conv, DefaultValidationConfig()); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestMemoConverter_V1FallbackRoundTrips(t *testing.T) {
	// A v1-encoded legacy update must decode via the v1 fallback and re-encode v2.
	rec := LegacyRecord{ID: "m2", ContentType: "memo", Content: memoUpdate(t, "m2", "hello v1", false)}
	conv, err := MemoConverter{}.Convert(rec)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if got := memoTextFromSnapshot(t, conv.Snapshot); !strings.Contains(got, "hello v1") {
		t.Fatalf("v1 fallback lost content: %q", got)
	}
}

func TestMemoConverter_EmptyIsSkip(t *testing.T) {
	conv, err := MemoConverter{}.Convert(LegacyRecord{ID: "m3", ContentType: "memo", Content: ""})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if !conv.Empty {
		t.Fatal("empty content should yield Empty=true")
	}
}

func TestMemoConverter_CorruptIsError(t *testing.T) {
	// Valid base64, but not a Yjs update of either version — must error (driver flags).
	garbage := base64.StdEncoding.EncodeToString([]byte("definitely-not-a-yjs-update-payload"))
	_, err := MemoConverter{}.Convert(LegacyRecord{ID: "m4", ContentType: "memo", Content: garbage})
	if err == nil {
		t.Fatal("corrupt memo update should error, not silently succeed")
	}
}

func TestMemoConverter_BadBase64IsError(t *testing.T) {
	_, err := MemoConverter{}.Convert(LegacyRecord{ID: "m5", ContentType: "memo", Content: "!!!not base64!!!"})
	if err == nil {
		t.Fatal("non-base64 content should error")
	}
}

// --- whiteboard conversion (cross-language seam) ---------------------------

func TestWhiteboardConverter_SeamUnavailableWhenNoRunner(t *testing.T) {
	_, err := WhiteboardConverter{Runner: nil}.Convert(
		LegacyRecord{ID: "w1", ContentType: "whiteboard", Content: `{"elements":[]}`})
	if !errors.Is(err, ErrWhiteboardSeamUnavailable) {
		t.Fatalf("want ErrWhiteboardSeamUnavailable, got %v", err)
	}
}

func TestWhiteboardConverter_EmptyIsSkip(t *testing.T) {
	conv, err := WhiteboardConverter{Runner: fakeNodeRunner{}}.Convert(
		LegacyRecord{ID: "w2", ContentType: "whiteboard", Content: ""})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if !conv.Empty {
		t.Fatal("empty whiteboard should yield Empty=true")
	}
}

func TestWhiteboardConverter_InvalidJSONIsError(t *testing.T) {
	_, err := WhiteboardConverter{Runner: fakeNodeRunner{}}.Convert(
		LegacyRecord{ID: "w3", ContentType: "whiteboard", Content: "{not json"})
	if err == nil {
		t.Fatal("invalid JSON whiteboard content should error")
	}
}

func TestWhiteboardConverter_RoundTripsViaRunner(t *testing.T) {
	rec := LegacyRecord{ID: "w4", ContentType: "whiteboard", Content: `{"elements":[{"id":"a"},{"id":"b"}]}`}
	conv, err := WhiteboardConverter{Runner: fakeNodeRunner{ids: []string{"a", "b"}}}.Convert(rec)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if conv.Empty || len(conv.Snapshot) == 0 {
		t.Fatal("expected a non-empty whiteboard snapshot")
	}
	// Rehydrate and confirm the id-keyed elements survived into the v2 snapshot.
	doc := service.NewMigrationDoc("verify")
	ycrdt.ApplyUpdateV2(doc, conv.Snapshot, "verify")
	elements := doc.GetMap("elements").(*ycrdt.YMap)
	if !elements.Has("a") || !elements.Has("b") {
		t.Fatalf("element ids lost: keys=%v", elements.Keys())
	}
	if err := Validate(conv, DefaultValidationConfig()); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestWhiteboardConverter_RunnerErrorPropagates(t *testing.T) {
	_, err := WhiteboardConverter{Runner: fakeNodeRunner{failWith: errors.New("boom")}}.Convert(
		LegacyRecord{ID: "w5", ContentType: "whiteboard", Content: `{"elements":[]}`})
	if err == nil || strings.Contains(err.Error(), "ErrWhiteboardSeamUnavailable") {
		t.Fatalf("runner error should propagate as a convert error, got %v", err)
	}
}

// --- validation ------------------------------------------------------------

func TestValidate_SizeRatioCeiling(t *testing.T) {
	// A tiny legacy input with a snapshot above the floor and over the ratio is flagged.
	rec := LegacyRecord{ID: "m1", ContentType: "memo", Content: memoUpdate(t, "m1", strings.Repeat("x", 2000), true)}
	conv, err := MemoConverter{}.Convert(rec)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	// Force the legacy baseline tiny + the floor below the snapshot so the ratio bites.
	conv.LegacyBytes = 1
	cfg := ValidationConfig{MaxSizeRatio: 2.0, SizeFloorBytes: 1}
	if err := Validate(conv, cfg); err == nil {
		t.Fatal("expected a size-ratio regression flag")
	}
}

func TestValidate_FloorExemptsTinyDocs(t *testing.T) {
	conv := Conversion{Snapshot: []byte{1, 2, 3}, LegacyBytes: 1}
	// Snapshot (3 bytes) is below the default 4 KiB floor — ratio check skipped.
	if err := Validate(Conversion{Snapshot: validV2Snapshot(t), LegacyBytes: 1}, DefaultValidationConfig()); err != nil {
		t.Fatalf("tiny doc under floor should pass: %v", err)
	}
	_ = conv
}

func TestValidate_EmptySnapshotIsError(t *testing.T) {
	if err := Validate(Conversion{Snapshot: nil, LegacyBytes: 10}, DefaultValidationConfig()); err == nil {
		t.Fatal("empty snapshot should fail validation")
	}
}

func TestValidate_UnreloadableSnapshotIsError(t *testing.T) {
	if err := Validate(Conversion{Snapshot: []byte{0xde, 0xad, 0xbe, 0xef}, LegacyBytes: 10}, DefaultValidationConfig()); err == nil {
		t.Fatal("garbage snapshot should fail the round-trip")
	}
}

func validV2Snapshot(t *testing.T) []byte {
	t.Helper()
	doc := service.NewMigrationDoc("v")
	frag := doc.GetXmlFragment("default").(*ycrdt.YXmlFragment)
	xt := ycrdt.NewYXmlText()
	frag.Push(ycrdt.ArrayAny{xt})
	xt.Insert(0, "x", nil)
	return ycrdt.EncodeStateAsUpdateV2(doc, nil)
}

// --- driver: persistence, idempotency, dry-run, flag-not-drop --------------

func TestDriver_PersistsThroughPorts(t *testing.T) {
	src := NewSliceSource([]LegacyRecord{
		{ID: "m1", ContentType: "memo", Content: memoUpdate(t, "m1", "persisted", true), AuthorizationPolicyID: "p1"},
	})
	d, blob, meta := newDriver(src, false, WhiteboardConverter{})
	rep, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Migrated != 1 || rep.Total != 1 {
		t.Fatalf("expected 1 migrated, got %+v", rep)
	}
	// The metadata row is written at the target version with the policy id + kind.
	m, err := meta.Load(context.Background(), "m1")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if m.Version != TargetVersion || m.ContentType != model.ContentTypeMemo ||
		m.AuthorizationPolicyID != "p1" || m.BlobStore != model.BlobStoreInline {
		t.Fatalf("metadata not written correctly: %+v", m)
	}
	// The blob rehydrates to the original content.
	snap, err := blob.Get(context.Background(), m.ContentPointer)
	if err != nil {
		t.Fatalf("get blob: %v", err)
	}
	if got := memoTextFromSnapshot(t, snap); !strings.Contains(got, "persisted") {
		t.Fatalf("persisted snapshot lost content: %q", got)
	}
}

func TestDriver_IdempotentReRunSkips(t *testing.T) {
	recs := []LegacyRecord{{ID: "m1", ContentType: "memo", Content: memoUpdate(t, "m1", "once", true)}}
	d, _, meta := newDriver(NewSliceSource(recs), false, WhiteboardConverter{})
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Second run over the same source (and the same — already-written — meta store).
	d2 := &Driver{Source: NewSliceSource(recs), Memo: MemoConverter{}, Whitebrd: WhiteboardConverter{},
		Blob: inline.New(), Metadata: meta, BlobKind: model.BlobStoreInline, Validation: DefaultValidationConfig()}
	rep, err := d2.Run(context.Background())
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if rep.Skipped != 1 || rep.Migrated != 0 {
		t.Fatalf("re-run should skip the already-migrated doc, got %+v", rep)
	}
}

func TestDriver_DryRunWritesNothing(t *testing.T) {
	src := NewSliceSource([]LegacyRecord{
		{ID: "m1", ContentType: "memo", Content: memoUpdate(t, "m1", "dry", true)},
	})
	d, blob, meta := newDriver(src, true, WhiteboardConverter{})
	rep, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Migrated != 1 || !rep.DryRun {
		t.Fatalf("dry-run should report a would-migrate, got %+v", rep)
	}
	if _, err := meta.Load(context.Background(), "m1"); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("dry-run must not write the metadata row")
	}
	if _, err := blob.Get(context.Background(), "m1"); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("dry-run must not write the blob")
	}
}

func TestDriver_FlagNotDrop(t *testing.T) {
	src := NewSliceSource([]LegacyRecord{
		{ID: "ok", ContentType: "memo", Content: memoUpdate(t, "ok", "fine", true)},
		{ID: "corrupt", ContentType: "memo", Content: base64.StdEncoding.EncodeToString([]byte("nope"))},
		{ID: "srcflag", ContentType: "whiteboard", Flagged: true, FlagReason: "decompress_failed"},
		{ID: "unknown", ContentType: "spreadsheet", Content: "x"},
		{ID: "wbnoseam", ContentType: "whiteboard", Content: `{"elements":[{"id":"a"}]}`},
	})
	d, _, _ := newDriver(src, false, WhiteboardConverter{Runner: nil})
	rep, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Total != 5 || rep.Migrated != 1 || rep.Flagged != 4 {
		t.Fatalf("flag-not-drop: every bad doc should be flagged, none dropped: %+v", rep)
	}
	// Every flagged result carries a reason (no silent flag).
	for _, r := range rep.Results {
		if r.Outcome == OutcomeFlagged && r.Reason == "" {
			t.Fatalf("flagged %s has no reason", r.ID)
		}
	}
}

func TestDriver_ContextCancelAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d, _, _ := newDriver(NewSliceSource(SeedCorpus()), true, WhiteboardConverter{})
	if _, err := d.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// --- source ----------------------------------------------------------------

func TestJSONLSource_ParsesAndSkipsBlanks(t *testing.T) {
	in := `{"id":"a","contentType":"memo","content":"x"}

{"id":"b","contentType":"whiteboard"}
`
	s := NewJSONLSource(strings.NewReader(in))
	var ids []string
	for {
		rec, ok, err := s.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			break
		}
		ids = append(ids, rec.ID)
	}
	if strings.Join(ids, ",") != "a,b" {
		t.Fatalf("want a,b got %v", ids)
	}
}

func TestJSONLSource_MissingIDErrors(t *testing.T) {
	s := NewJSONLSource(strings.NewReader(`{"contentType":"memo"}`))
	if _, _, err := s.Next(); err == nil {
		t.Fatal("a record without an id should error")
	}
}

func TestJSONLSource_BadJSONErrors(t *testing.T) {
	s := NewJSONLSource(strings.NewReader(`{not json}`))
	if _, _, err := s.Next(); err == nil {
		t.Fatal("malformed JSON should error")
	}
}

// TestJSONLSource_LargeRecord guards the scanner buffer size: a record whose line
// exceeds the old 16 MiB cap (but is well within the buffer) must parse rather
// than fail with "token too long". A document at the 32 MiB MaxDocBytes ceiling,
// base64-encoded in the line, lands above 16 MiB, so this is a real corpus case.
func TestJSONLSource_LargeRecord(t *testing.T) {
	const contentLen = 20 << 20 // 20 MiB of content — past the old 16 MiB line cap
	var b strings.Builder
	b.Grow(contentLen + 64)
	b.WriteString(`{"id":"big","contentType":"memo","content":"`)
	b.WriteString(strings.Repeat("A", contentLen))
	b.WriteString(`"}`)

	s := NewJSONLSource(strings.NewReader(b.String()))
	rec, ok, err := s.Next()
	if err != nil {
		t.Fatalf("large record must parse, got: %v", err)
	}
	if !ok || rec.ID != "big" || len(rec.Content) != contentLen {
		t.Fatalf("large record decoded wrong: ok=%v id=%q len=%d", ok, rec.ID, len(rec.Content))
	}
}

// --- seed corpus -----------------------------------------------------------

func TestSeedCorpus_DryRunOutcomes(t *testing.T) {
	d, _, _ := newDriver(NewSliceSource(SeedCorpus()), true, WhiteboardConverter{Runner: nil})
	rep, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("seed dry-run: %v", err)
	}
	// memo-v2 + memo-v1 migrate; empties skip; corrupt/unknown/source-flagged/
	// whiteboard(no-seam) flag.
	if rep.Migrated != 2 || rep.Skipped != 2 || rep.Flagged != 4 {
		t.Fatalf("unexpected seed outcomes: %+v", rep)
	}
}

func TestSeedCorpus_WhiteboardMigratesWithRunner(t *testing.T) {
	// With a runner wired, the seed whiteboard migrates (the empty one still skips).
	d, _, _ := newDriver(NewSliceSource(SeedCorpus()), true, WhiteboardConverter{Runner: fakeNodeRunner{ids: []string{"el-1", "el-2"}}})
	rep, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("seed dry-run: %v", err)
	}
	if rep.Migrated != 3 { // 2 memos + 1 whiteboard
		t.Fatalf("whiteboard should migrate with a runner: %+v", rep)
	}
}
