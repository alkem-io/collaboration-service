package service

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// measuringStore records what a cold load actually costs: how many reads it
// issues and how many bytes come back.
type measuringStore struct {
	*persistinprocess.Store
	reads     atomic.Int64
	bytesRead atomic.Int64
}

func (s *measuringStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	cp, err := s.Store.LoadCheckpoint(ctx, id)
	if err == nil {
		s.reads.Add(1)
		s.bytesRead.Add(int64(len(cp.Update)))
	}
	return cp, err
}

// appendIntoOneNode appends text into the document's SINGLE text node, creating
// it on first use.
//
// The distinction from the shared insertText helper is the whole experiment.
// insertText pushes a NEW XmlText node per call, so N calls build an N-node
// document — genuinely larger in CRDT terms even when the rendered text matches,
// which would make a size comparison across different call counts meaningless.
// Appending into one node keeps the STRUCTURE fixed so only the number of edits
// that produced the text varies, which is the variable SC-012 is about.
func appendIntoOneNode(doc *ycrdt.Doc, s string) {
	f := doc.GetXmlFragment("default")
	xt, _ := f.GetFirstChild().(*ycrdt.YXmlText)
	if xt == nil {
		xt = ycrdt.NewYXmlText()
		f.Push(ycrdt.ArrayAny{xt})
	}
	xt.Insert(xt.GetLength(), s, ycrdt.Object{})
}

// TestColdLoadCostTracksDocumentSizeNotEditCount is T021 / SC-012.
//
// This is the property that made the checkpoint profile the right shape for this
// service, so it is worth defending rather than assuming. A log-shaped store
// replays accumulated records, so a document edited a thousand times costs a
// thousand records to open even if it holds one paragraph — opening a long-lived
// document gets steadily slower for reasons a user cannot see or influence, and
// compaction stops being an optimisation and becomes load-bearing.
//
// The assertion is in READS and BYTES rather than wall time: timing would be
// flaky on a shared CI box and would measure the machine rather than the design.
//
// Non-vacuity: make the store accumulate updates instead of replacing them (a
// log) and the many-edits document reads back proportionally more.
func TestColdLoadCostTracksDocumentSizeNotEditCount(t *testing.T) {
	const (
		fewEdits  = 5
		manyEdits = 60
		total     = 60 // identical final text length for both documents
	)

	// Same final text, same document structure — only the number of edits that
	// produced it differs, by 40x.
	sparse := buildDocument(t, "cold-sparse", fewEdits, strings.Repeat("x", total/fewEdits))
	dense := buildDocument(t, "cold-dense", manyEdits, strings.Repeat("x", total/manyEdits))

	if sparse.text != dense.text {
		t.Fatalf("precondition: both documents must hold identical content, got %d vs %d chars", len(sparse.text), len(dense.text))
	}

	// ONE read each — not one per edit.
	if sparse.reads != 1 || dense.reads != 1 {
		t.Fatalf("cold load issued %d reads for %d edits and %d reads for %d edits; a cold load must be a single whole-document read (SC-012)",
			sparse.reads, fewEdits, dense.reads, manyEdits)
	}

	// And comparable bytes. Yjs merges adjacent insertions from one client into
	// contiguous runs, so a 40x difference in edit count must not produce anything
	// like a 40x difference in stored size. A log would.
	ratio := float64(dense.bytesRead) / float64(sparse.bytesRead)
	if ratio > 2 {
		t.Fatalf("cold load read %d bytes for %d edits vs %d bytes for %d edits (%.1fx) for IDENTICAL content — cost is tracking EDIT COUNT, not document size (SC-012)",
			dense.bytesRead, manyEdits, sparse.bytesRead, fewEdits, ratio)
	}
	t.Logf("cold load: %d edits → %d bytes; %d edits → %d bytes (%.2fx)",
		fewEdits, sparse.bytesRead, manyEdits, dense.bytesRead, ratio)
}

type coldLoadResult struct {
	text      string
	reads     int64
	bytesRead int64
}

// buildDocument writes a document with the given number of edits, FLUSHING AFTER
// EACH ONE, then drops the Manager and measures a cold reopen.
//
// The per-edit flush is what makes this test discriminating. An earlier version
// let the debounce coalesce the writes, so both documents flushed once and a
// log-shaped store was indistinguishable from a checkpoint one — the test passed
// against a deliberately log-shaped store, which means it was not testing what
// its own comment claimed. Flushing per edit is also the honest model of the
// requirement: SC-012 is about a document that has accumulated edit HISTORY, and
// history only accumulates in storage when the edits were actually written.
func buildDocument(t *testing.T, id model.DocumentID, edits int, per string) coldLoadResult {
	t.Helper()
	deps := newTestDeps()
	store := &measuringStore{Store: deps.store}
	deps.Checkpoint = store

	cfg := RoomConfig{SendBuffer: 256, SaveDebounce: time.Millisecond, IdleTimeout: 5 * time.Millisecond}

	write := NewManager(deps.Deps, cfg, nil, zap.NewNop())
	a := newFakeClient(t)
	a.join(write, id, model.ContentTypeMemo)
	a.observeUpdates()

	want := ""
	for i := range edits {
		a.withDoc(func(doc *ycrdt.Doc) { appendIntoOneNode(doc, per) })
		want += per
		waitFor(t, fmt.Sprintf("%s edit %d durable", id, i), func() bool {
			return storedTextContains(t, deps, id, want)
		})
	}
	want = a.text()
	write.Close()

	// Measure only the COLD load: everything above was the write path.
	store.reads.Store(0)
	store.bytesRead.Store(0)

	read := NewManager(deps.Deps, cfg, nil, zap.NewNop())
	t.Cleanup(read.Close)
	b := newFakeClient(t)
	b.join(read, id, model.ContentTypeMemo)
	b.observeUpdates()
	waitFor(t, "cold load converged", func() bool { return b.text() == want })

	return coldLoadResult{text: b.text(), reads: store.reads.Load(), bytesRead: store.bytesRead.Load()}
}
