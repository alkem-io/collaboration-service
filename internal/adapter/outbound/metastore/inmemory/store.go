// Package inmemory is a standalone, in-process MetadataStore: it keeps the
// document index in a map so the service runs without RabbitMQ/Postgres. The
// rabbitmq adapter (server save/fetch bus, Alkemio default) and the postgres
// adapter (standalone) land with task T005; this skeleton stub keeps the
// metastore layout real and lets the service boot zero-dependency.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Store is the in-process MetadataStore.
type Store struct {
	mu   sync.RWMutex
	rows map[model.DocumentID]model.Metadata
}

// New constructs an empty in-process metadata store.
func New() *Store {
	return &Store{rows: make(map[model.DocumentID]model.Metadata)}
}

// Load returns the index row for id, or model.ErrNotFound.
func (s *Store) Load(_ context.Context, id model.DocumentID) (model.Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.rows[id]
	if !ok {
		return model.Metadata{}, model.ErrNotFound
	}
	return meta, nil
}

// Save upserts the index row, bumping its version and updated-at timestamp.
func (s *Store) Save(_ context.Context, meta model.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := s.rows[meta.ID]; ok {
		meta.CreatedAt = existing.CreatedAt
		meta.Version = existing.Version + 1
	} else {
		meta.CreatedAt = now
		if meta.Version == 0 {
			meta.Version = 1
		}
	}
	meta.UpdatedAt = now
	s.rows[meta.ID] = meta
	return nil
}

// Delete removes the index row for id. Idempotent.
func (s *Store) Delete(_ context.Context, id model.DocumentID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)
	return nil
}

// compile-time assertion that Store satisfies the port.
var _ port.MetadataStore = (*Store)(nil)
