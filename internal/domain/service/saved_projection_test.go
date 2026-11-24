package service

import (
	"context"
	"errors"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Only the read side is backed: an accidental write fails the test.
type projectionIndex struct {
	port.MetadataStore
	load func(context.Context, model.DocumentID) (model.Metadata, error)
}

func (p projectionIndex) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	return p.load(ctx, id)
}

type projectionCheckpoint struct {
	persistence.CheckpointStore
	load func(context.Context, backend.DocumentID) (persistence.Checkpoint, error)
}

func TestCanceledProjectionDoesNotReturnAUsableDocument(t *testing.T) {
	source := ycrdt.NewDoc("memo")
	defer source.Destroy()
	update, err := ycrdt.EncodeStateAsUpdateV2(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if doc, err := decodeProjection(ctx, "memo", update); doc != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read handed a usable document to the renderer: %v", err)
	}
}

func (p projectionCheckpoint) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	return p.load(ctx, id)
}

func TestSavedProjectionDistinguishesMissingCorruptAndEmpty(t *testing.T) {
	d := ycrdt.NewDoc("fixture")
	d.GetXMLFragment("default").Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
	empty, err := ycrdt.EncodeStateAsUpdateV2(d, nil)
	d.Destroy()
	if err != nil {
		t.Fatal(err)
	}
	upstream := errors.New("unreachable")
	for _, tt := range []struct {
		name       string
		kind       model.ContentType
		indexErr   error
		storeErr   error
		encoding   persistence.CheckpointEncoding
		update     []byte
		wantErr    error
		wantReads  int
		cancelMeta bool
		cancelRead bool
	}{
		{name: "unknown", indexErr: model.ErrNotFound, wantErr: model.ErrNotFound},
		{name: "wrong kind", kind: model.ContentTypeWhiteboard, wantErr: model.ErrNotFound},
		{name: "index transport", indexErr: upstream, wantErr: upstream},
		{name: "missing checkpoint", storeErr: persistence.ErrNotFound, wantErr: ErrSavedContent, wantReads: 1},
		{name: "missing referenced file", storeErr: persistence.ErrCorrupt, wantErr: ErrSavedContent, wantReads: 1},
		{name: "blob transport", storeErr: upstream, wantErr: upstream, wantReads: 1},
		{name: "wrong codec", encoding: persistence.EncodingV1, update: empty, wantErr: ErrSavedContent, wantReads: 1},
		{name: "corrupt payload", update: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, wantErr: ErrSavedContent, wantReads: 1},
		{name: "garbage that yields an empty state vector", update: []byte("not Yjs"), wantErr: ErrSavedContent, wantReads: 1},
		{name: "empty transport body", wantErr: ErrSavedContent, wantReads: 1},
		{name: "snapshot too large", update: make([]byte, 65), wantErr: ErrSavedContent, wantReads: 1},
		{name: "canceled after metadata reply does not load a blob", update: empty, cancelMeta: true, wantErr: context.Canceled},
		{name: "canceled by caller during read", update: empty, cancelRead: true, wantErr: context.Canceled, wantReads: 1},
		{name: "valid saved empty memo", update: empty, wantReads: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			projection := savedProjection{
				metadata: projectionIndex{load: func(_ context.Context, id model.DocumentID) (model.Metadata, error) {
					if tt.cancelMeta {
						cancel()
					}
					kind := tt.kind
					if kind == "" {
						kind = model.ContentTypeMemo
					}
					return model.Metadata{ID: id, ContentType: kind}, tt.indexErr
				}},
				checkpoints: projectionCheckpoint{load: func(_ context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
					reads++
					if id != "fixture" {
						t.Fatalf("read wrong checkpoint: %s", id)
					}
					enc := tt.encoding
					if enc == persistence.EncodingUnspecified {
						enc = persistence.EncodingV2
					}
					if tt.cancelRead {
						cancel()
					}
					return persistence.Checkpoint{Encoding: enc, Update: tt.update}, tt.storeErr
				}},
				maxBytes: 64,
			}
			doc, err := projection.open(ctx, "fixture", model.ContentTypeMemo)
			if doc != nil {
				defer doc.Destroy()
			}
			if !errors.Is(err, tt.wantErr) || reads != tt.wantReads {
				t.Fatalf("open: err=%v, reads=%d; want %v, %d reads", err, reads, tt.wantErr, tt.wantReads)
			}
			if tt.wantErr == nil {
				if doc == nil || len(doc.GetXMLFragment("default").ToArray()) != 1 {
					t.Fatal("did not decode the saved empty paragraph")
				}
			} else if doc != nil {
				t.Fatal("returned a projection on failure")
			}
		})
	}
}

func TestMemoMarkdownReaderCanceledBeforeLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := NewMemoMarkdownReader(projectionIndex{}, projectionCheckpoint{})
	if body, err := reader(ctx, "not-reached"); !errors.Is(err, context.Canceled) || body != "" {
		t.Fatalf("canceled request returned %q, %v", body, err)
	}
}
