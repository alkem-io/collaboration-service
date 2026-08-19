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
// Like the file-service store it accepts V2 only — the codec this service writes
// and reads. It differs in recording the state vector alongside the bytes rather
// than deriving it, which the file-service blob (a bare Yjs update other systems
// read) has nowhere to put.
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
}

// New constructs an empty store. It is UNFENCED, like the file-service store
// (research.md D6a): this service never writes a fence, and the one topology a
// fence would protect — multiple pods owning one document — is explicitly
// unsupported. A non-zero fence is therefore rejected rather than honoured.
func New() *Store { return newStore() }

func newStore() *Store {
	return &Store{
		blobs:     map[backend.DocumentID][]byte{},
		vectors:   map[backend.DocumentID][]byte{},
		encodings: map[backend.DocumentID]persistence.CheckpointEncoding{},
		revisions: map[backend.DocumentID]persistence.Revision{},
	}
}

// FenceMode reports the fixed mutation-authority mode: always Unfenced. It is a
// property of the store, never inferred per write.
func (s *Store) FenceMode() persistence.FenceMode { return persistence.Unfenced }

// checkFence rejects a fence this store cannot honour. An Unfenced store that
// silently accepted an epoch would let a caller believe it had stale-owner
// protection it does not have.
func (s *Store) checkFence(_ backend.DocumentID, fence backend.Fence) error {
	if fence != 0 {
		return persistence.ErrUnexpectedFence
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
	// the store untouched.
	//
	// V2 ONLY, matching the deployed file-service store. Room.persist encodes V2
	// and restoreInto reads V2, so nothing in this service produces anything else;
	// a codec with no producer would be untested flexibility that also makes this
	// fixture a weaker stand-in for production than it should be.
	//
	// EncodingUnspecified is refused separately because the zero value would
	// otherwise silently become whichever codec this store happens to prefer — the
	// confident-wrong-answer the field exists to remove.
	switch req.Encoding {
	case persistence.EncodingV2:
	case persistence.EncodingUnspecified:
		return 0, persistence.ErrEncodingRequired
	default:
		return 0, fmt.Errorf("%w: this store accepts V2 only, got %d", persistence.ErrUnsupportedEncoding, req.Encoding)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkFence(req.DocumentID, req.Fence); err != nil {
		return 0, err
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
	delete(s.blobs, req.DocumentID)
	delete(s.vectors, req.DocumentID)
	delete(s.revisions, req.DocumentID)
	return nil
}

var _ persistence.DeletingCheckpointStore = (*Store)(nil)
