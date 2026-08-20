package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/memory"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// observingStore counts LoadCheckpoint calls, which is how "restored exactly
// once" is observed: the coalescing happens inside the registry, so the only
// evidence visible from outside is how many times the store was asked.
type observingStore struct {
	*persistinprocess.Store
	loads atomic.Int64
}

func (s *observingStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	s.loads.Add(1)
	return s.Store.LoadCheckpoint(ctx, id)
}

// TestConcurrentFirstOpensRestoreExactlyOnce is T022 / FR-004a / FR-004b /
// SC-015.
//
// The registry's Acquire coalesces concurrent cache misses onto ONE open call and
// publishes nothing until that call returns. Restoring INSIDE it is what makes
// first-open restore exactly-once by CONSTRUCTION, and what stops a session
// observing a document that exists but has not been restored: restoring after
// Acquire publishes an empty document and fills it in afterwards, so an opener
// arriving in that window sees an empty editor for a document that has content —
// and its first flush persists that emptiness over the real state.
//
// It calls newRoom DIRECTLY rather than joining through the Manager, and that is
// the whole point. Manager.acquire already collapses concurrent first-connects
// with a singleflight, so a Manager-level test passes whether the restore is
// inside the open function or after it — it measures the singleflight and reports
// it as evidence about the registry. An earlier version of this test did exactly
// that and stayed green when the restore was moved back out. Driving newRoom
// concurrently is what isolates the registry's guarantee, which is the one the
// requirement is about and the one that still holds if the singleflight is ever
// removed or bypassed.
//
// Non-vacuity: move the restore out of the open function and this fails with one
// LoadCheckpoint per concurrent room.
func TestConcurrentFirstOpensRestoreExactlyOnce(t *testing.T) {
	deps := newTestDeps()
	store := &observingStore{Store: deps.store}
	deps.Checkpoint = store
	// One shared registry, as the Manager supplies, so all rooms contend for the
	// same document identity.
	deps.Registry = memory.NewRegistry()

	const doc model.DocumentID = "concurrent-open"
	seed := newBareRoom(t)
	insertText(seed.doc, "restored content ")
	update, err := ycrdt.EncodeStateAsUpdateV2(seed.doc, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := deps.putState(context.Background(), string(doc), update); err != nil {
		t.Fatalf("seed durable state: %v", err)
	}
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	const openers = 8
	rooms := make([]*Room, openers)
	errs := make([]error, openers)
	var wg sync.WaitGroup
	for i := range openers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rooms[i], errs[i] = newRoom(context.Background(), doc, model.ContentTypeMemo,
				deps.Deps, fastConfig(), NopMetrics{}, zap.NewNop())
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("newRoom %d: %v", i, err)
		}
		t.Cleanup(releaseRoom(rooms[i]))
	}

	// EXACTLY ONE restore, however many rooms raced for the document.
	if got := store.loads.Load(); got != 1 {
		t.Fatalf("LoadCheckpoint called %d times for %d concurrent first-opens; the restore must be coalesced into the registry's single open call (FR-004b)", got, openers)
	}

	// Every room holds the SAME restored document — none observed it pre-restore.
	for i, room := range rooms {
		if !contains(xmlText(room.doc), "restored content") {
			t.Fatalf("room %d holds %q; a session must never observe a document that exists but has not been restored (FR-004a)", i, xmlText(room.doc))
		}
		if room.doc != rooms[0].doc {
			t.Fatalf("room %d holds a DIFFERENT document instance; the registry must hand one live document to every holder", i)
		}
	}
}
