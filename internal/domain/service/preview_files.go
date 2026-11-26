package service

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// The variant is the reserved per-document slot as well as the output format selector.
type previewVariant string

const (
	previewFull     previewVariant = "whiteboard"
	previewMarkdown previewVariant = "markdown"
)

func (v previewVariant) mediaType() string {
	if v == previewMarkdown {
		return "text/plain"
	}
	return "image/png"
}

func previewSlotReference(id uuid.UUID, variant previewVariant) string {
	return "collab-preview-v1:" + id.String() + ":" + string(variant)
}

type previewAsset struct {
	mime  string
	bytes []byte
}

// One source bucket binds both asset reads and output lookup. Callers do not
// supply network locations, storage references or output IDs from scene data.
type previewFileSource struct {
	files      port.PreviewFiles
	bucket     uuid.UUID
	checkpoint uuid.UUID
}

func (s previewFileSource) asset(ctx context.Context, locator string, maxBytes int64) (previewAsset, error) {
	id, err := uuid.Parse(locator)
	if err != nil || id == uuid.Nil || locator != id.String() || id == s.checkpoint || s.bucket == uuid.Nil {
		return previewAsset{}, errors.New("invalid saved preview asset locator")
	}
	file, err := s.files.LookupPreviewAsset(ctx, id)
	if err != nil {
		return previewAsset{}, err
	}
	if !eligiblePreviewAsset(file, id, s.bucket, maxBytes) {
		return previewAsset{}, errors.New("preview asset is not an eligible owned image")
	}
	mime := file.MIME
	switch mime {
	case "image/png", "image/jpeg", "image/gif", "image/svg+xml", "image/webp", "image/bmp", "image/avif":
	case "image/x-png":
		mime = "image/png"
	case "image/jpg", "image/jfif":
		mime = "image/jpeg"
	default:
		return previewAsset{}, errors.New("unsupported saved preview image format")
	}
	raw, err := s.files.ReadPreviewAsset(ctx, file.ContentHash, file.Size)
	if err != nil {
		return previewAsset{}, err
	}
	if int64(len(raw)) != file.Size {
		return previewAsset{}, errors.New("preview asset size did not match metadata")
	}
	return previewAsset{mime: mime, bytes: raw}, nil
}

func eligiblePreviewAsset(file port.PreviewFile, id, bucket uuid.UUID, maxBytes int64) bool {
	return file.ID == id && file.BucketID == bucket && !file.Temporary &&
		file.AuthorizationID != uuid.Nil && len(file.ContentHash) == 64 &&
		file.Size > 0 && file.Size <= maxBytes
}

func (s previewFileSource) slot(ctx context.Context, board uuid.UUID, variant previewVariant) (uuid.UUID, error) {
	if board == uuid.Nil || s.bucket == uuid.Nil {
		return uuid.Nil, errors.New("preview requires a canonical board and bucket")
	}
	ref := previewSlotReference(board, variant)
	file, err := s.files.LookupPreviewSlot(ctx, s.bucket, ref)
	if err != nil {
		return uuid.Nil, err
	}
	if file.ID == uuid.Nil || file.ID == s.checkpoint || file.BucketID != s.bucket ||
		file.ExternalReference != ref || file.Temporary || file.AuthorizationID != uuid.Nil || file.MIME != variant.mediaType() {
		return uuid.Nil, errors.New("preview output is not a dedicated private artifact")
	}
	return file.ID, nil
}
