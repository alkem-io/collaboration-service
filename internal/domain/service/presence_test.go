package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestAwarenessEvictedOnDisconnect is the headline presence guarantee (FR-014,
// closing the Wave-1 D6 deferral): when a connection leaves, the server forces an
// awareness removal for that connection's y-client id so remaining peers stop
// rendering its cursor — without waiting for the 30s y-awareness TTL.
func TestAwarenessEvictedOnDisconnect(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const docID = model.DocumentID("evict-aware")

	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	aClient := a.aware.ClientID
	a.setAwareness(ycrdt.MakeObject("user", "alice"))

	b := newFakeClient(t)
	b.join(mgr, docID, model.ContentTypeMemo)
	b.observeUpdates()

	// B learns A's presence (the join-time snapshot + the live update).
	waitFor(t, "b sees a presence", func() bool {
		return b.awarenessUserOf(aClient) == "alice"
	})

	// A disconnects. The server must fan a forced awareness removal to B, so B's
	// awareness state for A's client id is cleared.
	a.session.Leave()

	waitFor(t, "b no longer renders a cursor", func() bool {
		return b.awarenessUserOf(aClient) == nil
	})
}

// TestCollaboratorDowngradedOnInactivity asserts a collaborator that goes idle
// past CollaboratorInactivity is downgraded to viewer (read-only-state control),
// mirroring the legacy whiteboard collaborator_inactivity behaviour (FR-014).
func TestCollaboratorDowngradedOnInactivity(t *testing.T) {
	cfg := fastConfig()
	cfg.CollaboratorInactivity = 30 * time.Millisecond
	mgr, _ := testManager(t, cfg)

	a := newFakeClient(t)
	a.join(mgr, "downgrade", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("active ") // one mutation, then go idle

	waitFor(t, "read-only-state downgrade control", func() bool {
		return hasReadOnly(a, true)
	})
}

// TestMutationResetsInactivity asserts a collaborator that keeps editing is NOT
// downgraded — each mutation resets the inactivity timer (FR-014).
func TestMutationResetsInactivity(t *testing.T) {
	cfg := fastConfig()
	cfg.CollaboratorInactivity = 80 * time.Millisecond
	mgr, _ := testManager(t, cfg)

	a := newFakeClient(t)
	a.join(mgr, "stay-active", model.ContentTypeMemo)
	a.observeUpdates()

	// Edit repeatedly for longer than the inactivity window; the timer must keep
	// resetting so no downgrade fires.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		a.insertText("x ")
		time.Sleep(20 * time.Millisecond)
	}
	if hasReadOnly(a, true) {
		t.Fatal("an actively-editing collaborator was downgraded to viewer")
	}
}

// TestContributionMetricFlush asserts the north-star contribution metric flushes
// the per-window set of contributing actor ids both to the Prometheus gauge (via
// Metrics.ContributingActors) and the Contributor port (RMQ in Alkemio mode).
func TestContributionMetricFlush(t *testing.T) {
	cfg := fastConfig()
	cfg.ContributionWindow = 30 * time.Millisecond
	metrics := &countingMetrics{}
	contrib := &captureContributor{}
	deps := newTestDeps()
	deps.Contributor = contrib

	mgr := NewManager(deps.Deps, cfg, metrics, nil)

	a := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	a.join(mgr, "contrib", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("hello ")

	waitFor(t, "contribution gauge set", func() bool {
		return metrics.contributors.Load() >= 1
	})
	waitFor(t, "contribution event emitted", func() bool {
		return contrib.lastActorCount() >= 1
	})
	if got := contrib.lastDoc(); got != "contrib" {
		t.Fatalf("contribution doc = %q, want contrib", got)
	}
}

// TestContributionFlushesWhenTheLastEditorLeaves proves the contribution
// window is not the sole delivery trigger. A short-lived edit session can end
// before the first window tick; releasing that room must flush the actor once.
func TestContributionFlushesWhenTheLastEditorLeaves(t *testing.T) {
	cfg := fastConfig()
	cfg.IdleTimeout = 20 * time.Millisecond
	cfg.ContributionWindow = time.Hour
	contrib := &recordingContributor{}
	deps := newTestDeps()
	deps.Contributor = contrib
	mgr := NewManager(deps.Deps, cfg, nil, nil)

	a := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	a.join(mgr, "contribution-on-release", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("one edit")
	a.session.Leave()

	waitFor(t, "contribution flush on idle release", func() bool {
		return contrib.callCount() == 1
	})
	if got := contrib.lastActors(); len(got) != 1 || got[0].String() != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("teardown contribution actors = %v, want the editor once", got)
	}
}

// TestContributionFlushSwapsBeforeCallingThePort pins the room-loop ownership
// boundary: the elapsed window is detached before an external call can observe
// or re-enter the room's current set.
func TestContributionFlushSwapsBeforeCallingThePort(t *testing.T) {
	room := newBareRoom(t)
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}
	room.deps.Contributor = contributorFunc(func(context.Context, model.DocumentID, []uuid.UUID) error {
		if len(room.contributors) != 0 {
			t.Fatalf("current contribution set still contains the detached window: %v", room.contributors)
		}
		return nil
	})

	room.flushContribution(context.Background())
}

func TestContributionDeduplicatesTypedActorID(t *testing.T) {
	room := newBareRoom(t)
	recorded := &recordingContributor{}
	room.deps.Contributor = recorded
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}
	room.contributors[actor] = struct{}{}

	room.flushContribution(context.Background())
	if got := recorded.lastActors(); len(got) != 1 || got[0] != actor {
		t.Fatalf("contribution actors = %v, want one typed UUID", got)
	}
}

func TestContributionIncludesAnonymousButExcludesOpenMode(t *testing.T) {
	room := newBareRoom(t)
	anonymous := uuid.Nil
	room.members[1] = roomMember{id: 1, actorID: nil}
	room.members[2] = roomMember{id: 2, actorID: &anonymous}

	room.recordActivity(1)
	room.recordActivity(2)
	if len(room.contributors) != 1 {
		t.Fatalf("contributors = %v, want only the resolvable anonymous UUID", room.contributors)
	}
	if _, ok := room.contributors[uuid.Nil]; !ok {
		t.Fatal("anonymous uuid.Nil actor was not recorded")
	}
}

// TestFailedContributionFlushIsRetried proves a transient bus error cannot erase
// the elapsed actor set: it is merged back and emitted by the next successful
// flush.
func TestFailedContributionFlushIsRetried(t *testing.T) {
	room := newBareRoom(t)
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}
	calls := 0
	room.deps.Contributor = contributorFunc(func(_ context.Context, _ model.DocumentID, actors []uuid.UUID) error {
		calls++
		if calls == 1 {
			return errors.New("bus unavailable")
		}
		if len(actors) != 1 || actors[0] != actor {
			t.Fatalf("retry actors = %v, want %s", actors, actor)
		}
		return nil
	})

	room.startContributionFlush()
	first := <-room.commands
	room.finishContributionFlush(first.contribution)
	if _, retained := room.contributors[actor]; !retained {
		t.Fatal("failed contribution flush discarded the actor")
	}
	room.startContributionFlush()
	second := <-room.commands
	room.finishContributionFlush(second.contribution)
	if calls != 2 {
		t.Fatalf("contribution calls = %d, want failed attempt plus retry", calls)
	}
	if len(room.contributors) != 0 {
		t.Fatalf("successful retry left actors pending: %v", room.contributors)
	}
}

// TestBlockedContributionDoesNotBlockDocumentUpdates proves the analytics port
// is not on the room's single-writer critical path. A blocked periodic emit must
// not delay an ordinary document update reaching another member.
func TestBlockedContributionDoesNotBlockDocumentUpdates(t *testing.T) {
	cfg := fastConfig()
	cfg.ContributionWindow = 10 * time.Millisecond
	blocked := newBlockingContributor()
	deps := newTestDeps()
	deps.Contributor = blocked
	mgr := NewManager(deps.Deps, cfg, nil, nil)
	t.Cleanup(func() {
		close(blocked.release)
		mgr.Close()
	})

	a := newFakeClientWithIdentity(t, "11111111-1111-1111-1111-111111111111")
	a.join(mgr, "contribution-nonblocking", model.ContentTypeMemo)
	a.observeUpdates()
	b := newFakeClientWithIdentity(t, "22222222-2222-2222-2222-222222222222")
	b.join(mgr, "contribution-nonblocking", model.ContentTypeMemo)
	b.observeUpdates()

	a.insertText("starts cadence ")
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("periodic contribution emit did not start")
	}
	a.insertText("still live ")
	waitFor(t, "update while contribution emit is blocked", func() bool {
		return contains(b.text(), "still live")
	})
}

// TestPeriodicContributionThenTeardownDoesNotDuplicate proves the teardown
// flush consumes only the current set, not a window already emitted by the
// timer.
func TestPeriodicContributionThenTeardownDoesNotDuplicate(t *testing.T) {
	room := newBareRoom(t)
	recorded := &recordingContributor{}
	room.deps.Contributor = recorded
	room.contributors[uuid.MustParse("11111111-1111-1111-1111-111111111111")] = struct{}{}

	room.startContributionFlush()
	completed := <-room.commands
	room.finishContributionFlush(completed.contribution)
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	if got := recorded.callCount(); got != 1 {
		t.Fatalf("contribution calls = %d, want one periodic batch and no teardown duplicate", got)
	}
}

func TestTeardownRetriesKnownFailedInFlightContribution(t *testing.T) {
	room := newBareRoom(t)
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}
	calls := 0
	room.deps.Contributor = contributorFunc(func(_ context.Context, _ model.DocumentID, actors []uuid.UUID) error {
		calls++
		if calls == 1 {
			return errors.New("bus unavailable")
		}
		if len(actors) != 1 || actors[0] != actor {
			t.Fatalf("teardown retry actors = %v, want %s", actors, actor)
		}
		return nil
	})

	room.startContributionFlush()
	<-room.commands // result is ready, but teardown wins before the room dispatches it.
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
	if calls != 2 {
		t.Fatalf("contribution calls = %d, want failed periodic attempt plus teardown retry", calls)
	}
}

// hasReadOnly reports whether the client received a read-only-state control
// marking it read-only (a viewer downgrade).
func hasReadOnly(c *fakeClient, readOnly bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.control {
		// ReadOnly is a *bool on the wire: nil means the frame carries no read-only
		// state (skip it), so match only frames whose explicit value equals the one
		// asked for. This is what lets the test distinguish a regain (false) frame
		// from a frame that never set readOnly at all.
		if m.Kind == model.ControlReadOnlyState && m.ReadOnly != nil && *m.ReadOnly == readOnly {
			return true
		}
	}
	return false
}

// captureContributor records the last contribution event for assertions.
type captureContributor struct {
	doc        atomic.Value // string
	actorCount atomic.Int64
}

func (c *captureContributor) Contribution(_ context.Context, id model.DocumentID, actorIDs []uuid.UUID) error {
	c.doc.Store(string(id))
	c.actorCount.Store(int64(len(actorIDs)))
	return nil
}

func (c *captureContributor) lastDoc() string {
	if v := c.doc.Load(); v != nil {
		return v.(string)
	}
	return ""
}

func (c *captureContributor) lastActorCount() int { return int(c.actorCount.Load()) }

type contributorFunc func(context.Context, model.DocumentID, []uuid.UUID) error

func (f contributorFunc) Contribution(ctx context.Context, id model.DocumentID, actors []uuid.UUID) error {
	return f(ctx, id, actors)
}

type recordingContributor struct {
	mu      sync.Mutex
	batches [][]uuid.UUID
}

type blockingContributor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingContributor() *blockingContributor {
	return &blockingContributor{started: make(chan struct{}), release: make(chan struct{})}
}

func (c *blockingContributor) Contribution(context.Context, model.DocumentID, []uuid.UUID) error {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return nil
}

func (c *recordingContributor) Contribution(_ context.Context, _ model.DocumentID, actors []uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, append([]uuid.UUID(nil), actors...))
	return nil
}

func (c *recordingContributor) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

func (c *recordingContributor) lastActors() []uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches) == 0 {
		return nil
	}
	return append([]uuid.UUID(nil), c.batches[len(c.batches)-1]...)
}

// TestTeardownCompletesCriticalWorkBeforeAnalytics is the ordering guarantee.
//
// Contribution emission is best-effort (FR-014) and talks to the same bus that
// may be why a shutdown is slow. Ordered before the durable flush it can consume
// the whole shutdown budget — Manager.Close allows roughly ONE backend timeout
// for the entire drain — and the drain is then abandoned before persisting,
// losing exactly the edits the shutdown flush exists to save. So the flush and
// every member's terminal control must be reached while analytics is still
// blocked.
//
// Non-vacuity: move settleContributionFlight/flushContributionNow back above the
// flush callback in teardown and this fails — persisted stays false and the
// member is never told, because the blocked Contributor holds the room loop.
func TestTeardownCompletesCriticalWorkBeforeAnalytics(t *testing.T) {
	room := newBareRoom(t)
	blocked := newBlockingContributor()
	room.deps.Contributor = blocked
	// Released exactly once, from this goroutine, and on every exit path so a
	// failed assertion cannot leave teardown parked in the blocked emit.
	releasedOnce := false
	release := func() {
		if !releasedOnce {
			releasedOnce = true
			close(blocked.release)
		}
	}
	defer release()

	// A pending contribution, so teardown has analytics work to do at all.
	room.contributors[uuid.MustParse("11111111-1111-1111-1111-111111111111")] = struct{}{}

	client := newFakeClient(t)
	room.members[1] = roomMember{id: 1, conn: client}

	persisted := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		room.teardown(model.NewSessionEnd(model.CodeServerShutdown), func() {
			close(persisted)
		})
	}()

	// The critical half must complete while the Contributor is still blocked.
	select {
	case <-persisted:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown did not reach the durable flush while analytics was blocked; a wedged bus would cost the final snapshot")
	}
	waitFor(t, "member told before analytics", func() bool {
		end, _ := client.sessionEnd()
		return end != nil
	})

	// Confirm the emit was actually REACHED, so the ordering above was observed
	// rather than analytics having been skipped altogether. blockingContributor
	// closes `started` on entry, so this is a signal, not a timing window.
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown never reached the contribution emit; the ordering assertions above would pass even with analytics removed")
	}

	release()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown did not finish after the contribution was released")
	}
}

// TestDisabledContributionWindowCollectsAndEmitsNothing pins that a zero window
// means OFF, not "every tick".
//
// newOptionalTicker already stops the periodic timer at that setting, so
// recording without it would accumulate actor ids in a map only teardown ever
// drains — an unbounded set on a long-lived room — and then emit to a bus the
// operator switched off.
//
// Non-vacuity: drop the contributionEnabled guard from recordActivity and the
// contributors set is non-empty; drop it from flushContribution and the teardown
// emit fires, so calls becomes 1.
func TestDisabledContributionWindowCollectsAndEmitsNothing(t *testing.T) {
	room := newBareRoom(t)
	room.cfg.ContributionWindow = 0
	recorded := &recordingContributor{}
	room.deps.Contributor = recorded

	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.members[1] = roomMember{id: 1, conn: newFakeClient(t), actorID: &actor}
	room.recordActivity(1)
	if len(room.contributors) != 0 {
		t.Fatalf("contributors = %v, want none collected with the window disabled", room.contributors)
	}

	// Teardown is the only reachable emit path with the window off — the periodic
	// timer is stopped by newOptionalTicker, so its tick never fires. Poking
	// startContributionFlush directly would fabricate a state the config cannot
	// produce, so the assertion goes through teardown instead.
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)
	if got := recorded.callCount(); got != 0 {
		t.Fatalf("contribution calls = %d, want 0 with the window disabled", got)
	}
}

// TestCloseDeletedDoesNotEmitContributionsForADeletedDocument pins that the
// owner-delete path skips analytics entirely.
//
// The row is already gone when this event arrives — `server` removes the entity
// before enqueueing it — and `server`'s contribution consumer resolves the document
// id against the memo and whiteboard rows. A deleted document misses both, so the
// event is discarded and logged as "collaboration-contribution for unknown
// document". Emitting it is a bus round trip whose only outcome is a warn per
// delete. The dropped final window is tracked as BASIC-004 in the canonical
// remediation ledger (alkem-io/agents-hq ->
// specs/006-collab-content-unification/kiss-remediation-ledger.md).
//
// Non-vacuity: drop the `end.Code != model.CodeDocumentDeleted` guard in teardown
// and this emits, failing here.
func TestCloseDeletedDoesNotEmitContributionsForADeletedDocument(t *testing.T) {
	room := newBareRoom(t)
	recorded := &recordingContributor{}
	room.deps.Contributor = recorded
	room.contributors[uuid.MustParse("11111111-1111-1111-1111-111111111111")] = struct{}{}

	room.teardown(model.NewSessionEnd(model.CodeDocumentDeleted), nil)

	if got := recorded.callCount(); got != 0 {
		t.Fatalf("contribution calls = %d on the owner-delete path, want 0: the document is gone and the consumer would discard it", got)
	}
}

// TestShutdownStillEmitsContributionsAfterPurgeSkip is the companion that keeps
// the guard honest: skipping on delete must not silently disable the ordinary
// teardown flush every other path relies on.
func TestShutdownStillEmitsContributionsAfterPurgeSkip(t *testing.T) {
	room := newBareRoom(t)
	recorded := &recordingContributor{}
	room.deps.Contributor = recorded
	actor := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	room.contributors[actor] = struct{}{}

	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	if got := recorded.callCount(); got != 1 {
		t.Fatalf("contribution calls = %d on shutdown, want 1", got)
	}
	if got := recorded.lastActors(); len(got) != 1 || got[0] != actor {
		t.Fatalf("shutdown contribution actors = %v, want %s", got, actor)
	}
}

// TestActorsArrivingDuringAnInFlightEmitLandInTheNextWindow pins the detach/swap
// boundary from the other side.
//
// The batch is detached on the room loop BEFORE the port is called, so an actor
// that contributes while the emit is in flight belongs to the NEXT window — it
// must be neither lost nor smuggled into the batch already on the wire (which the
// port has by value and cannot see grow).
//
// Non-vacuity: drop the map swap in detachContributors and the new actor is
// carried in the in-flight batch, so the next window is empty and this fails.
func TestActorsArrivingDuringAnInFlightEmitLandInTheNextWindow(t *testing.T) {
	room := newBareRoom(t)
	first := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	second := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	room.contributors[first] = struct{}{}

	recorded := &recordingContributor{}
	room.deps.Contributor = recorded

	room.startContributionFlush()
	completion := <-room.commands

	// Arrives after the detach, while the first batch is being emitted.
	room.members[1] = roomMember{id: 1, conn: newFakeClient(t), actorID: &second}
	room.recordActivity(1)

	room.finishContributionFlush(completion.contribution)

	if got := recorded.lastActors(); len(got) != 1 || got[0] != first {
		t.Fatalf("in-flight batch = %v, want only %s", got, first)
	}
	if _, held := room.contributors[second]; !held {
		t.Fatalf("actor arriving mid-emit was lost; contributors = %v", room.contributors)
	}
	if _, duplicated := room.contributors[first]; duplicated {
		t.Fatal("a successfully emitted actor was left pending and would be emitted twice")
	}
}
