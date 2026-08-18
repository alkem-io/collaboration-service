// Package metapointer resolves a document's stable file pointer from the Alkemio
// metadata index.
//
// file-service assigns file ids on create and accepts no caller-supplied id, so
// the DocumentID -> file id mapping has to live somewhere. The index already
// carries it as ContentPointer, alongside the document's storage bucket, so this
// bridges the two rather than introducing a second home for the same fact.
//
// It is READ on load and WRITTEN once, when a document's file is first created.
// The pointer is stable thereafter, so steady-state saves never write it again.
package metapointer

import (
	"context"
	"errors"
	"fmt"

	"github.com/antst/go-yjs/backend"

	fsstore "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/fileservice"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Resolver adapts a MetadataStore to the file-service store's pointer lookup.
type Resolver struct {
	meta port.MetadataStore
}

// New constructs a Resolver over the document index.
func New(meta port.MetadataStore) *Resolver { return &Resolver{meta: meta} }

// Pointer returns the document's stable file pointer and storage bucket.
//
// A row that exists but carries no ContentPointer means the document has an index
// entry and no file yet — the first-save case, reported as ErrNoPointer rather
// than as an error, so the store creates one.
func (r *Resolver) Pointer(ctx context.Context, id backend.DocumentID) (string, string, error) {
	meta, err := r.meta.Load(ctx, model.DocumentID(id))
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			return "", "", fsstore.ErrNoPointer
		}
		return "", "", fmt.Errorf("loading document index: %w", err)
	}
	if meta.ContentPointer == "" {
		return "", meta.StorageBucketID, fsstore.ErrNoPointer
	}
	return meta.ContentPointer, meta.StorageBucketID, nil
}

// Record persists a newly created file pointer onto the document's index row.
//
// Read-modify-write on the row: the index carries content type, authorization
// policy, owner and bucket alongside the pointer, and MetadataStore.Save takes a
// whole row, so the other fields must be carried through rather than blanked. A
// missing row is not synthesised — a pointer with no document to attach it to
// would be unreachable, which is exactly the failure the caller must see.
func (r *Resolver) Record(ctx context.Context, id backend.DocumentID, pointer string) error {
	docID := model.DocumentID(id)
	meta, err := r.meta.Load(ctx, docID)
	if err != nil {
		return fmt.Errorf("loading document index before recording pointer: %w", err)
	}
	meta.ID = docID
	meta.ContentPointer = pointer
	if err := r.meta.Save(ctx, meta); err != nil {
		return fmt.Errorf("recording content pointer: %w", err)
	}
	return nil
}

var _ fsstore.PointerResolver = (*Resolver)(nil)
