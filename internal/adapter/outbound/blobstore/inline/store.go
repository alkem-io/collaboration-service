// Package inline is the default BlobStore: the encoded Y.Doc snapshot lives
// inline (in the main DB today; here an in-process map for the standalone
// skeleton) keyed by content pointer. The file-service, S3, and local adapters
// (sibling packages, task T005) provide the offload implementations.
package inline

import (
	"context"
	"sync"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Store is the in-process inline BlobStore. The real Alkemio adapter persists
// to the main DB; this standalone implementation keeps the latest snapshot per
// pointer in memory, which is enough to satisfy the port and to run the service
// standalone without a database. It is safe for concurrent use.
type Store struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

// New constructs an empty inline blob store.
func New() *Store {
	return &Store{blobs: make(map[string][]byte)}
}

// Put stores the snapshot bytes under pointer, replacing any previous snapshot,
// and echoes the pointer back (inline blobs are addressed by the stable pointer
// the caller supplies — the document id).
func (s *Store) Put(_ context.Context, pointer string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.blobs[pointer] = cp
	return pointer, nil
}

// Get returns the snapshot bytes for pointer, or model.ErrNotFound.
func (s *Store) Get(_ context.Context, pointer string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.blobs[pointer]
	if !ok {
		return nil, model.ErrNotFound
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

// Delete removes the snapshot for pointer. Idempotent.
func (s *Store) Delete(_ context.Context, pointer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.blobs, pointer)
	return nil
}

// compile-time assertion that Store satisfies the port.
var _ port.BlobStore = (*Store)(nil)
