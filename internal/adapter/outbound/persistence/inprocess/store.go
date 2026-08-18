// Package inprocess is an in-memory persistence.CheckpointStore: one current
// document state per id, replaced on every save.
//
// It backs the in-process path — the test suite, the local development loop
// (real editors, no Alkemio infrastructure), and the documented zero-dependency
// smoke test (constitution §III). It carries NO durability guarantee across a
// restart and must never be presented as a deployment option.
//
// It deliberately mirrors the SHAPE of the file-service store rather than being
// a convenient in-memory log: one blob per document, no envelope, no stored
// state vector, derived on read. If the fixture had a different shape from
// production, every test would be exercising a persistence model the deployed
// service does not use.
package inprocess

import (
	"context"
	"fmt"
	"sync"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	"github.com/antst/go-yjs/crdt"
)

// Store keeps one current state per document. It is safe for concurrent use.
type Store struct {
	mu        sync.Mutex
	blobs     map[backend.DocumentID][]byte
	revisions map[backend.DocumentID]persistence.Revision
	revision  persistence.Revision
	mode      persistence.FenceMode
	// fences records the highest epoch accepted per document. Only a fenced
	// store consults it; an unfenced one never populates it.
	fences map[backend.DocumentID]backend.Fence
}

// New constructs an empty unfenced store — the ordinary non-clustered mode, and
// the one the file-service store also reports (research.md D6a).
func New() *Store { return newStore(persistence.Unfenced) }

// NewFenced constructs a store requiring a fence on every save. It exists so the
// fenced path is exercised by conformance (FR-008a); no deployment uses it, and
// the file-service store cannot support it — a file row has nowhere to persist
// the epoch.
func NewFenced() *Store { return newStore(persistence.Fenced) }

func newStore(mode persistence.FenceMode) *Store {
	return &Store{
		blobs:     map[backend.DocumentID][]byte{},
		revisions: map[backend.DocumentID]persistence.Revision{},
		fences:    map[backend.DocumentID]backend.Fence{},
		mode:      mode,
	}
}

// FenceMode reports the fixed mutation-authority mode. It is a property of the
// store, never inferred per write, so one omitted fence cannot silently disable
// stale-owner protection.
func (s *Store) FenceMode() persistence.FenceMode { return s.mode }

// checkFence validates a save's epoch against the store's mode. Callers hold mu.
func (s *Store) checkFence(id backend.DocumentID, fence backend.Fence) error {
	switch s.mode {
	case persistence.Unfenced:
		if fence != 0 {
			return persistence.ErrUnexpectedFence
		}
	case persistence.Fenced:
		if fence == 0 {
			return persistence.ErrFenceRequired
		}
		if fence < s.fences[id] {
			return persistence.ErrStaleFence
		}
	}
	return nil
}

// SaveCheckpoint replaces the document's durable state.
//
// Returning nil means the state is durable — there is no buffering, so it never
// reports a durability it does not have. Update is borrowed only for the call
// and is copied before being retained. StateVector is required by the contract
// but deliberately NOT stored: this medium has nowhere to put it, and the
// contract permits deriving it on read.
func (s *Store) SaveCheckpoint(ctx context.Context, req persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFence(req.DocumentID, req.Fence); err != nil {
		return 0, err
	}
	if req.Fence > s.fences[req.DocumentID] {
		s.fences[req.DocumentID] = req.Fence
	}
	s.revision++
	s.blobs[req.DocumentID] = append([]byte(nil), req.Update...)
	s.revisions[req.DocumentID] = s.revision
	return s.revision, nil
}

// LoadCheckpoint returns the document's current state, deriving the state vector
// from the stored bytes. Both returned slices are caller-owned.
func (s *Store) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return persistence.Checkpoint{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	blob, ok := s.blobs[id]
	if !ok {
		return persistence.Checkpoint{}, persistence.ErrNotFound
	}
	vector, err := crdt.EncodeStateVectorFromUpdate(blob)
	if err != nil {
		// Bytes that will not parse cannot form the state a successful load
		// promises, which is precisely ErrCorrupt.
		return persistence.Checkpoint{}, fmt.Errorf("%w: %w", persistence.ErrCorrupt, err)
	}
	return persistence.Checkpoint{
		Revision:    s.revisions[id],
		Update:      append([]byte(nil), blob...),
		StateVector: vector,
	}, nil
}

var _ persistence.CheckpointStore = (*Store)(nil)
