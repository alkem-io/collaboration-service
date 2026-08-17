package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	ycrdt "github.com/skyterra/y-crdt"

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

	a := newFakeClientWithIdentity(t, "actor-1")
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

func (c *captureContributor) Contribution(_ context.Context, id model.DocumentID, actorIDs []string) error {
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
