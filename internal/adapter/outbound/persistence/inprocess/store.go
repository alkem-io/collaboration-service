// Package inprocess is an in-memory persistence.Store: the durable-history
// contract implemented over process memory.
//
// It backs the in-process path — the test suite, the local development loop
// (real editors, no Alkemio infrastructure), and the documented zero-dependency
// smoke test (constitution §III). It carries NO durability guarantee across a
// restart and must never be presented as a deployment option.
//
// It is a GENUINE append log with compaction, not a latest-value cache. That is
// not a stylistic choice: conformance.Persistence appends opaque byte records
// ("first", "second") and requires them back verbatim, in order, through a
// paginated recovery view whose Through is fixed by the first page. A store
// that kept only the newest whole-document blob cannot satisfy that — there is
// nothing to merge when records are not CRDT updates. See research.md D1a.
package inprocess

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
)

// Store is an in-memory CompactingStore. It is safe for concurrent use.
type Store struct {
	mu   sync.Mutex
	docs map[backend.DocumentID]*docLog
	mode persistence.FenceMode
}

// docLog is one document's durable history: an optional checkpoint covering
// everything through its revision, plus the records appended after it.
type docLog struct {
	checkpoint *persistence.Checkpoint
	records    []persistence.Record
	nextRev    persistence.Revision
	// lastFence is the highest epoch accepted so far; a fenced store rejects any
	// write bearing an older one (stale-owner rejection).
	lastFence backend.Fence
}

// New constructs an empty unfenced store — the ordinary non-clustered mode.
func New() *Store { return &Store{docs: map[backend.DocumentID]*docLog{}, mode: persistence.Unfenced} }

// NewFenced constructs an empty store that requires a fence on every mutation.
// Deployments run unfenced today; this exists so the fenced path is exercised by
// conformance while it is still cheap to correct (FR-008a).
func NewFenced() *Store {
	return &Store{docs: map[backend.DocumentID]*docLog{}, mode: persistence.Fenced}
}

// FenceMode reports the fixed mutation-authority mode. It is a property of the
// store, never inferred per write, so one omitted fence cannot silently disable
// stale-owner protection.
func (s *Store) FenceMode() persistence.FenceMode { return s.mode }

// checkFence validates a write's epoch against the store's mode. Callers hold s.mu.
func (s *Store) checkFence(log *docLog, fence backend.Fence) error {
	switch s.mode {
	case persistence.Unfenced:
		if fence != 0 {
			return persistence.ErrUnexpectedFence
		}
	case persistence.Fenced:
		if fence == 0 {
			return persistence.ErrFenceRequired
		}
		if log != nil && fence < log.lastFence {
			return persistence.ErrStaleFence
		}
	}
	return nil
}

// Append durably records one transaction update and returns its revision.
//
// Returning nil means the bytes are durable: there is no internal buffering, so
// Append never reports a durability it does not have (FR-007a). Update is
// borrowed only for the call, so it is copied before being retained.
func (s *Store) Append(ctx context.Context, req persistence.AppendRequest) (persistence.Revision, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.docs[req.DocumentID]
	if err := s.checkFence(log, req.Fence); err != nil {
		return 0, err
	}
	if log == nil {
		log = &docLog{nextRev: 1}
		s.docs[req.DocumentID] = log
	}
	if req.Fence > log.lastFence {
		log.lastFence = req.Fence
	}
	rev := log.nextRev
	log.nextRev++
	log.records = append(log.records, persistence.Record{
		Revision: rev,
		Update:   append([]byte(nil), req.Update...),
	})
	return rev, nil
}

// Load returns one page of a self-consistent recovery view.
//
// Through is fixed by the first page and carried in the continuation token, so a
// paged read never observes appends that landed mid-walk — while a fresh Load
// sees them. An empty Next is the ONLY signal that the view is complete.
func (s *Store) Load(ctx context.Context, id backend.DocumentID, opts persistence.LoadOptions) (persistence.Page, error) {
	if err := ctx.Err(); err != nil {
		return persistence.Page{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	log, ok := s.docs[id]
	if !ok {
		return persistence.Page{}, persistence.ErrNotFound
	}

	first := opts.PageToken == ""
	through, start, err := s.resolveCursor(log, opts.PageToken)
	if err != nil {
		return persistence.Page{}, err
	}

	// Only records within the fixed view, from the cursor onward.
	var visible []persistence.Record
	for _, rec := range log.records {
		if rec.Revision > through {
			break
		}
		if rec.Revision >= start {
			visible = append(visible, rec)
		}
	}

	limit := opts.Limit
	next := persistence.PageToken("")
	if limit > 0 && len(visible) > limit {
		// The next page resumes at the first record we are not returning.
		next = encodeToken(through, visible[limit].Revision)
		visible = visible[:limit]
	}

	page := persistence.Page{Through: through, Next: next}
	// Checkpoint is normally present only on the first page.
	if first && log.checkpoint != nil {
		page.Checkpoint = &persistence.Checkpoint{
			Revision:    log.checkpoint.Revision,
			Update:      append([]byte(nil), log.checkpoint.Update...),
			StateVector: append([]byte(nil), log.checkpoint.StateVector...),
		}
	}
	// Both byte slices returned by Load are caller-owned, so copy every record:
	// a caller mutating a returned Update must not reach durable state.
	page.Updates = make([]persistence.Record, 0, len(visible))
	for _, rec := range visible {
		page.Updates = append(page.Updates, persistence.Record{
			Revision: rec.Revision,
			Update:   append([]byte(nil), rec.Update...),
		})
	}
	return page, nil
}

// resolveCursor returns the fixed view bound and the first revision to include.
// Callers hold s.mu.
func (s *Store) resolveCursor(log *docLog, token persistence.PageToken) (through, start persistence.Revision, err error) {
	if token == "" {
		// A fresh view: bound it at the newest revision currently durable.
		through = log.nextRev - 1
		if log.checkpoint != nil && log.checkpoint.Revision > through {
			through = log.checkpoint.Revision
		}
		return through, 0, nil
	}
	return decodeToken(token)
}

// Compact atomically installs a checkpoint covering everything through Basis and
// removes only the records at or before it.
//
// It is a compare-and-swap against Basis: records appended AFTER the basis must
// survive, because a compaction racing an append must never swallow that append.
func (s *Store) Compact(ctx context.Context, req persistence.CompactRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	log, ok := s.docs[req.DocumentID]
	if !ok {
		return persistence.ErrNotFound
	}
	if err := s.checkFence(log, req.Fence); err != nil {
		return err
	}
	// The basis must name a revision this store actually holds, and must not move
	// backwards past an existing checkpoint.
	if req.Basis >= log.nextRev {
		return fmt.Errorf("%w: basis %d is beyond the newest revision %d", persistence.ErrConflict, req.Basis, log.nextRev-1)
	}
	if log.checkpoint != nil && req.Basis < log.checkpoint.Revision {
		return fmt.Errorf("%w: basis %d precedes the installed checkpoint %d", persistence.ErrConflict, req.Basis, log.checkpoint.Revision)
	}
	if req.Fence > log.lastFence {
		log.lastFence = req.Fence
	}

	// CheckpointUpdate and StateVector are borrowed only for the call.
	log.checkpoint = &persistence.Checkpoint{
		Revision:    req.Basis,
		Update:      append([]byte(nil), req.CheckpointUpdate...),
		StateVector: append([]byte(nil), req.StateVector...),
	}
	kept := log.records[:0:0]
	for _, rec := range log.records {
		if rec.Revision > req.Basis {
			kept = append(kept, rec)
		}
	}
	log.records = kept
	return nil
}

// --- page tokens -------------------------------------------------------------
//
// A token carries the fixed view bound and the resume point. Its contents are
// private to this implementation; callers only round-trip it.

func encodeToken(through, start persistence.Revision) persistence.PageToken {
	return persistence.PageToken(strconv.FormatUint(uint64(through), 10) + ":" + strconv.FormatUint(uint64(start), 10))
}

func decodeToken(token persistence.PageToken) (through, start persistence.Revision, err error) {
	parts := strings.SplitN(string(token), ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("%w: malformed page token", persistence.ErrCorrupt)
	}
	t, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: malformed page token bound", persistence.ErrCorrupt)
	}
	sv, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: malformed page token cursor", persistence.ErrCorrupt)
	}
	return persistence.Revision(t), persistence.Revision(sv), nil
}

var (
	_ persistence.Store           = (*Store)(nil)
	_ persistence.CompactingStore = (*Store)(nil)
)
