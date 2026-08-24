package ws

import (
	"context"
	"errors"

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// anyDocumentExists is a MetadataStore for the transport tests: it behaves
// exactly like the in-memory store except that a document nobody registered
// still reports as EXISTING.
//
// Join refuses an unknown document (Manager.requireDocument), which is right —
// but in this package the subject is framing, limits, close codes and handshake
// ordering, and every one of those tests would otherwise have to pre-register a
// document to say anything about a socket. The gate keeps its own tests, in the
// service package and in admission_close_test.go, where existence IS the subject
// and this double is deliberately not used.
//
// ContentType is left empty on the synthesized row on purpose: loadMetadata only
// overrides the room's content type when the row names one, so a whiteboard test
// still gets a whiteboard.
type anyDocumentExists struct{ port.MetadataStore }

func (a anyDocumentExists) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	meta, err := a.MetadataStore.Load(ctx, id)
	if errors.Is(err, model.ErrNotFound) {
		return model.Metadata{ID: id, Migrated: true}, nil
	}
	return meta, err
}

// openDocs builds the store above over a fresh in-memory index.
func openDocs() port.MetadataStore { return anyDocumentExists{metainmem.New()} }
