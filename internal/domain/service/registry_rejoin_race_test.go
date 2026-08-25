package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// countingLoadStore counts materializations. A room only calls LoadCheckpoint when
// it is built from nothing, so the count is how many times the document was truly
// re-opened rather than handed to a joiner from a resident registry entry.
type countingLoadStore struct {
	*persistinprocess.Store
	loads atomic.Int64
}

func (c *countingLoadStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	c.loads.Add(1)
	return c.Store.LoadCheckpoint(ctx, id)
}

// TestReleaseAndRematerializeLosesNoWrites drives the document through repeated
// full release → evict → re-materialize cycles and asserts every write survives.
//
// This is the observable form of a registry lifetime bug. If a re-materializing
// room were ever handed a handle to a destroyed entry — or opened a second Y.Doc
// for an id that already had one — the two branches would both flush, and the
// loser's edits would be overwritten with no error raised anywhere. The symptom is
// "my changes vanished", so only the surviving CONTENT can catch it; handle
// identity and call counts cannot.
//
// The rounds are SEQUENTIAL, each waiting for the room to release before the next
// joins. That is deliberate and was learned the hard way: a first version fired 40
// concurrent joiners, and every one of them coalesced onto a single room. It
// materialized the document exactly ONCE, never evicted, and so never exercised
// re-materialization at all — it passed while asserting nothing. Hence the
// materialization assertion below, which fails if this test ever silently decays
// back into that shape.
func TestReleaseAndRematerializeLosesNoWrites(t *testing.T) {
	const (
		id     = model.DocumentID("rejoin-race")
		rounds = 12
		perRnd = 2
	)
	deps := newTestDeps()
	counting := &countingLoadStore{Store: deps.store}
	deps.Checkpoint = counting
	// Without an index row the room skips the restore entirely, so every round
	// would start from an empty document and the accumulation assertion would be
	// measuring nothing.
	if err := deps.meta.Save(context.Background(), model.Metadata{
		ID: id, ContentType: model.ContentTypeMemo, ContentPointer: "ptr",
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	mgr := NewManager(deps.Deps, fastConfig(), nil, nil)

	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < perRnd; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := newFakeClient(t)
				c.join(mgr, id, model.ContentTypeMemo)
				// observeUpdates is what forwards a local edit to the room; without it
				// the write never reaches the server and the round proves nothing.
				c.observeUpdates()
				c.insertText("x")
				c.session.Leave()
			}()
		}
		wg.Wait()
		// Wait for the release, so the NEXT round must rebuild the document from the
		// checkpoint rather than joining a room that is still resident.
		waitFor(t, "the room releases between rounds", func() bool { return mgr.RoomCount() == 0 })
	}

	// Non-vacuity, asserted in-test: every round after the first must have rebuilt
	// the document. If joins ever coalesce onto one resident room again, this drops
	// to 1 and the content assertion below stops meaning anything.
	if got := counting.loads.Load(); got < rounds-1 {
		t.Fatalf("the document materialized %d time(s) across %d rounds; the rounds coalesced onto a resident room and never exercised re-materialization", got, rounds)
	}

	cp, err := deps.store.LoadCheckpoint(context.Background(), backend.DocumentID(id))
	if err != nil {
		t.Fatalf("the rounds stored nothing: %v", err)
	}
	scratch := newRoomDoc(string(id))
	if err := ycrdt.ApplyUpdateV2(scratch, cp.Update, nil); err != nil {
		t.Fatalf("stored bytes do not materialize: %v", err)
	}
	// A lost branch shows up here and nowhere else: the writes that landed on the
	// orphaned document are simply absent from the state that survived.
	if got, want := len(xmlText(scratch)), rounds*perRnd; got != want {
		t.Fatalf("%d of %d writes survived %d release/re-materialize cycles; a branch was overwritten", got, want, rounds)
	}
}
