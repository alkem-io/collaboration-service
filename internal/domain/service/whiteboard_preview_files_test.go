package service

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"slices"
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

type previewFilesFixture struct {
	assets map[uuid.UUID]port.PreviewFile
	slots  map[string]port.PreviewFile
	hashes []string
	blob   []byte
	err    error
}

func (f *previewFilesFixture) LookupPreviewAsset(_ context.Context, id uuid.UUID) (port.PreviewFile, error) {
	if file, ok := f.assets[id]; ok {
		return file, nil
	}
	return port.PreviewFile{}, port.ErrPreviewFileMissing
}

func (f *previewFilesFixture) LookupPreviewSlot(_ context.Context, _ uuid.UUID, ref string) (port.PreviewFile, error) {
	if file, ok := f.slots[ref]; ok {
		return file, nil
	}
	return port.PreviewFile{}, port.ErrPreviewFileMissing
}

func (f *previewFilesFixture) ReadPreviewAsset(_ context.Context, hash string, _ int64) ([]byte, error) {
	f.hashes = append(f.hashes, hash)
	if f.blob != nil || f.err != nil {
		return f.blob, f.err
	}
	return []byte("asset"), nil
}

func TestSavedImageRendersOnlyFromVerifiedStorageBytes(t *testing.T) {
	doc := ycrdt.NewDoc("saved-asset")
	defer doc.Destroy()
	id, bucket := uuid.New(), uuid.New()
	doc.GetMap("files").Set("image-key", id.String())
	e := ycrdt.NewYMap(nil)
	doc.GetMap("elements").Set("image", e)
	for k, v := range map[string]any{
		"id": "image", "index": "a0", "type": "image", "fileId": "image-key",
		"width": 100., "height": 80., "opacity": 100., "scale": ycrdt.ArrayAny{1., 1.},
		"strokeColor": "transparent", "backgroundColor": "transparent",
	} {
		e.Set(k, v)
	}
	pixel := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	red := color.NRGBA{R: 255, A: 255}
	pixel.SetNRGBA(0, 0, red)
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, pixel); err != nil {
		t.Fatal(err)
	}
	hash := strings.Repeat("a", 64)
	fixture := &previewFilesFixture{
		assets: map[uuid.UUID]port.PreviewFile{id: {
			ID: id, BucketID: bucket, AuthorizationID: uuid.New(), ContentHash: hash,
			MIME: "image/png", Size: int64(encoded.Len()),
		}},
		blob: encoded.Bytes(),
	}
	source := previewFileSource{files: fixture, bucket: bucket}
	preview, err := renderBoardPreview(t.Context(), doc, source)
	if err != nil || !slices.Equal(fixture.hashes, []string{hash}) {
		t.Fatalf("verified asset was not read once from storage: %v; reads=%v", err, fixture.hashes)
	}
	for _, variant := range [][]byte{preview.Full, preview.Card} {
		img, err := png.Decode(bytes.NewReader(variant))
		if err != nil {
			t.Fatal(err)
		}
		c := img.Bounds().Max.Div(2)
		if got := color.NRGBAModel.Convert(img.At(c.X, c.Y)); got != red {
			t.Fatalf("stored image did not render at the center: %v", got)
		}
	}
	fixture.blob, fixture.err = nil, errors.New("asset storage unavailable")
	preview, err = renderBoardPreview(t.Context(), doc, source)
	if !errors.Is(err, fixture.err) || preview.Full != nil || preview.Card != nil {
		t.Fatalf("asset outage returned a partial preview or hid its cause: %v", err)
	}
}

func (*previewFilesFixture) ReplacePreview(context.Context, uuid.UUID, string, []byte) error {
	panic("file verification must not write")
}

func (*previewFilesFixture) CreatePreview(context.Context, uuid.UUID, string, string, []byte) (bool, error) {
	panic("file verification must not create")
}

func TestWhiteboardPreviewAssetRequiresTrustedOwnerAndHash(t *testing.T) {
	id, bucket := uuid.New(), uuid.New()
	checkpoint := uuid.New()
	base := port.PreviewFile{ID: id, BucketID: bucket, AuthorizationID: uuid.New(), ContentHash: strings.Repeat("a", 64), MIME: "image/png", Size: 5}
	for _, tc := range []struct {
		name    string
		locator string
		change  func(*port.PreviewFile)
	}{
		{"missing ID", uuid.NewString(), nil},
		{"URL", "https://example.invalid/asset.png", nil},
		{"checkpoint", checkpoint.String(), nil},
		{"wrong owner", id.String(), func(f *port.PreviewFile) { f.BucketID = uuid.New() }},
		{"unpublished", id.String(), func(f *port.PreviewFile) { f.Temporary = true }},
		{"internal only", id.String(), func(f *port.PreviewFile) { f.AuthorizationID = uuid.Nil }},
		{"wrong MIME", id.String(), func(f *port.PreviewFile) { f.MIME = "text/html" }},
		{"claims different ID", id.String(), func(f *port.PreviewFile) { f.ID = uuid.New() }},
		{"oversized asset", id.String(), func(f *port.PreviewFile) { f.Size = 6000 }},
		{"missing hash", id.String(), func(f *port.PreviewFile) { f.ContentHash = "" }},
		{"trusted", id.String(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			if tc.change != nil {
				tc.change(&row)
			}
			files := &previewFilesFixture{assets: map[uuid.UUID]port.PreviewFile{id: row}}
			src := previewFileSource{files: files, bucket: bucket, checkpoint: checkpoint}
			asset, err := src.asset(t.Context(), tc.locator, 100)
			if tc.name == "trusted" {
				if err != nil || asset.mime != "image/png" || string(asset.bytes) != "asset" || !slices.Equal(files.hashes, []string{row.ContentHash}) {
					t.Fatalf("trusted asset = %v, %v; reads %v", asset, err, files.hashes)
				}
			} else if err == nil || len(files.hashes) != 0 {
				t.Fatalf("unsafe asset validated or triggered a hash read: %v, reads %v", err, files.hashes)
			}
		})
	}
}

func TestPreviewAssetMetadataControlsContentRead(t *testing.T) {
	id, bucket := uuid.New(), uuid.New()
	for _, tc := range []struct {
		stored, expected string
		size             int64
	}{
		{"image/x-png", "image/png", 5},
		{"image/jfif", "image/jpeg", 5},
		{"image/png", "", 4},
		{"image/png", "", 6},
	} {
		file := port.PreviewFile{
			ID: id, BucketID: bucket, AuthorizationID: uuid.New(),
			ContentHash: strings.Repeat("a", 64), MIME: tc.stored, Size: tc.size,
		}
		fixture := &previewFilesFixture{assets: map[uuid.UUID]port.PreviewFile{id: file}}
		source := previewFileSource{files: fixture, bucket: bucket, checkpoint: uuid.New()}
		got, err := source.asset(t.Context(), id.String(), 10)
		if tc.expected == "" {
			if err == nil || len(got.bytes) != 0 {
				t.Fatalf("mismatched size returned image bytes: %v, %v", got, err)
			}
		} else if err != nil || got.mime != tc.expected || string(got.bytes) != "asset" {
			t.Fatalf("registered %q did not produce %q: %v, %v", tc.stored, tc.expected, got, err)
		}
	}
}

func TestWhiteboardPreviewSlotMustBeDedicatedPrivatePNG(t *testing.T) {
	board, bucket, checkpoint, id := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ref := previewSlotReference(board, previewCard)
	base := port.PreviewFile{ID: id, BucketID: bucket, ExternalReference: ref, MIME: "image/png"}
	for _, tc := range []struct {
		name   string
		change func(*port.PreviewFile)
	}{
		{"different bucket", func(f *port.PreviewFile) { f.BucketID = uuid.New() }},
		{"unrelated reference", func(f *port.PreviewFile) { f.ExternalReference = "other" }},
		{"public policy", func(f *port.PreviewFile) { f.AuthorizationID = uuid.New() }},
		{"temporary", func(f *port.PreviewFile) { f.Temporary = true }},
		{"checkpoint", func(f *port.PreviewFile) { f.ID = checkpoint }},
		{"wrong MIME", func(f *port.PreviewFile) { f.MIME = "image/jpeg" }},
		{"private PNG", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			if tc.change != nil {
				tc.change(&row)
			}
			files := &previewFilesFixture{slots: map[string]port.PreviewFile{ref: row}}
			src := previewFileSource{files: files, bucket: bucket, checkpoint: checkpoint}
			if _, err := src.slot(t.Context(), board, previewFull); !errors.Is(err, port.ErrPreviewFileMissing) {
				t.Fatalf("missing full slot = %v", err)
			}
			got, err := src.slot(t.Context(), board, previewCard)
			if tc.change == nil && (err != nil || got != id) || tc.change != nil && (err == nil || got != uuid.Nil) {
				t.Fatalf("card slot = %v, %v", got, err)
			}
		})
	}
}
