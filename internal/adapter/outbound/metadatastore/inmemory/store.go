// Package inmemory is an in-process MetadataStore: it keeps the document index
// in a map so the service runs without RabbitMQ. It serves the in-process
// development and test path and is NOT durable — the Alkemio topology uses the
// rabbitmq adapter (server save/fetch bus), which is the system of record.
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
//
// On conflict it preserves the existing value of every column the incoming
// Metadata leaves BLANK — one canonical save behavior
// across backends. This matters because two callers Save partial rows:
//   - a per-snapshot persist (Room.persist) carries content_pointer but
//     historically blank lifecycle fields; and
//   - a pre-register (Manager.PreRegister, reached only from the no-bus
//     document-create HTTP handler) carries owner_ref/content_type but a blank
//     content_pointer.
//
// Without "blank = unchanged", a wholesale row replace would let the first snapshot
// save wipe the pre-registered owner_ref (the delete cascade key, FR-023), and a
// REPEATED pre-register — the same document created twice over the HTTP endpoint,
// or any second Save carrying a blank pointer — wipe the live content_pointer back
// to "" (orphaning the persisted blob and bumping the version). A non-blank value
// still wins (a genuine update).
//
// The repeat is a plain repeated call, not a broker redelivery: no inbound event
// reaches this store. The lifecycle consumer handles `document.deleted` only.
func (s *Store) Save(_ context.Context, meta model.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := s.rows[meta.ID]; ok {
		meta.CreatedAt = existing.CreatedAt
		meta.Version = existing.Version + 1
		meta = coalesceBlank(meta, existing)
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

// coalesceBlank fills the blank fields of an incoming upsert from the existing
// row, so a Save that carries only a subset of the columns does not clobber the
// rest to their zero value. Version,
// CreatedAt, and UpdatedAt are managed by Save and intentionally excluded.
func coalesceBlank(in, existing model.Metadata) model.Metadata {
	if in.ContentType == "" {
		in.ContentType = existing.ContentType
	}
	if in.ContentPointer == "" {
		in.ContentPointer = existing.ContentPointer
	}
	if in.AuthorizationPolicyID == "" {
		in.AuthorizationPolicyID = existing.AuthorizationPolicyID
	}
	if in.OwnerRef == "" {
		in.OwnerRef = existing.OwnerRef
	}
	if in.StorageBucketID == "" {
		in.StorageBucketID = existing.StorageBucketID
	}
	return in
}

// compile-time assertion that Store satisfies the port.
var _ port.MetadataStore = (*Store)(nil)
