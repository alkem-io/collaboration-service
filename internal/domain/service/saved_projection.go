package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// ErrSavedContent means a known document's checkpoint is unavailable or cannot
// be projected. It is not an unknown document or a successfully empty result.
var ErrSavedContent = errors.New("saved content cannot be read")

type savedProjection struct {
	metadata    port.MetadataStore
	checkpoints persistence.CheckpointStore
	maxBytes    int
}

// open returns a caller-owned scratch document, never a live room document.
func (s savedProjection) open(ctx context.Context, id model.DocumentID, kind model.ContentType) (*ycrdt.Doc, error) {
	_, update, err := s.load(ctx, id, kind)
	if err != nil {
		return nil, err
	}
	return decodeProjection(ctx, id, update)
}

func (s savedProjection) load(ctx context.Context, id model.DocumentID, kind model.ContentType) (model.Metadata, []byte, error) {
	if err := ctx.Err(); err != nil {
		return model.Metadata{}, nil, err
	}
	meta, err := s.metadata.Load(ctx, id)
	if err != nil {
		return model.Metadata{}, nil, err
	}
	if meta.ContentType != kind {
		return model.Metadata{}, nil, model.ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return model.Metadata{}, nil, err
	}
	snapshot, err := s.checkpoints.LoadCheckpoint(ctx, backend.DocumentID(id))
	if errors.Is(err, persistence.ErrNotFound) || errors.Is(err, persistence.ErrCorrupt) {
		return model.Metadata{}, nil, fmt.Errorf("%w: %w", ErrSavedContent, err)
	}
	if err != nil {
		return model.Metadata{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return model.Metadata{}, nil, err
	}
	if snapshot.Encoding != persistence.EncodingV2 || len(snapshot.Update) == 0 || len(snapshot.Update) > s.maxBytes {
		return model.Metadata{}, nil, ErrSavedContent
	}
	return meta, snapshot.Update, nil
}

func decodeProjection(ctx context.Context, id model.DocumentID, update []byte) (*ycrdt.Doc, error) {
	doc := ycrdt.NewDoc(string(id))
	if err := ycrdt.ApplyUpdateV2(doc, update, nil); err != nil {
		doc.Destroy()
		return nil, fmt.Errorf("%w: %w", ErrSavedContent, err)
	}
	if err := ctx.Err(); err != nil {
		doc.Destroy()
		return nil, err
	}
	return doc, nil
}
