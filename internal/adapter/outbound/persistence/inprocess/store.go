// Package inprocess is an in-memory persistence.CheckpointStore: one current
// document state per id, replaced on every save.
//
// It backs the in-process path — the test suite, the local development loop
// (real editors, no Alkemio infrastructure), and the documented zero-dependency
// smoke test (constitution §III). It carries NO durability guarantee across a
// restart and must never be presented as a deployment option.
//
// It deliberately mirrors the SHAPE of the file-service store rather than being
// a convenient in-memory log: one blob per document, no envelope, replaced on
// every save. If the fixture had a different shape from production, every test
// would be exercising a persistence model the deployed service does not use.
//
// It does differ from the file-service store in one respect, deliberately: this
// medium can record the state vector and the codec alongside the bytes, so it
// accepts either V1 or V2. The file-service blob is a bare Yjs update that other
// systems read, with nowhere to put that metadata, so it supports V2 only.
package inprocess

import (
	"context"
	"fmt"
	"sync"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
)

// Store keeps one current state per document. It is safe for concurrent use.
type Store struct {
	mu        sync.Mutex
	blobs     map[backend.DocumentID][]byte
	vectors   map[backend.DocumentID][]byte
	encodings map[backend.DocumentID]persistence.CheckpointEncoding
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
		vectors:   map[backend.DocumentID][]byte{},
		encodings: map[backend.DocumentID]persistence.CheckpointEncoding{},
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
	// Checked BEFORE the lock and before any mutation: a rejected save must leave
	// the store untouched. This medium records the codec alongside the bytes, so
	// it accepts either — but never an unstated one, because the zero value would
	// otherwise silently become whichever codec this store happens to prefer.
	// That is the confident-wrong-answer the field exists to remove.
	switch req.Encoding {
	case persistence.EncodingV1, persistence.EncodingV2:
	case persistence.EncodingUnspecified:
		return 0, persistence.ErrEncodingRequired
	default:
		return 0, fmt.Errorf("%w: %d", persistence.ErrUnsupportedEncoding, req.Encoding)
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
	s.vectors[req.DocumentID] = append([]byte(nil), req.StateVector...)
	s.encodings[req.DocumentID] = req.Encoding
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
	// The vector is RETURNED AS STORED, never re-derived. Deriving it means
	// choosing a decoder, and picking the wrong one is not an error:
	// EncodeStateVectorFromUpdate on V2 bytes returns a confident, EMPTY vector
	// with err == nil, which reads as "this document has nothing from any client".
	// This store keeps what the writer computed, alongside the codec the writer
	// declared, so the question never arises here.
	// Copied on the way OUT as well as in: the contract says both returned slices
	// are caller-owned, so handing back the stored backing array would let a
	// caller's mutation reach into this store's state.
	vector := append([]byte(nil), s.vectors[id]...)
	return persistence.Checkpoint{
		Revision:    s.revisions[id],
		Encoding:    s.encodings[id],
		Update:      append([]byte(nil), blob...),
		StateVector: vector,
	}, nil
}

var _ persistence.CheckpointStore = (*Store)(nil)

// Delete removes a document's durable state (persistence.Deleter).
//
// Idempotent: deleting an absent document succeeds. The owner-delete cascade
// retries, and the second attempt must not fail the operation it is completing.
//
// A REJECTED delete leaves the state intact. That is the property that stops a
// superseded owner erasing what its replacement is serving, so the fence is
// checked before anything is removed rather than alongside.
func (s *Store) Delete(ctx context.Context, req persistence.DeleteRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFence(req.DocumentID, req.Fence); err != nil {
		return err
	}
	// The fence high-water mark is retained across the delete: a stale owner must
	// not be able to erase, then re-save under its old epoch as if it were current.
	if req.Fence > s.fences[req.DocumentID] {
		s.fences[req.DocumentID] = req.Fence
	}
	delete(s.blobs, req.DocumentID)
	delete(s.vectors, req.DocumentID)
	delete(s.revisions, req.DocumentID)
	return nil
}

var _ persistence.DeletingCheckpointStore = (*Store)(nil)
