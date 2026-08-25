package service

import (
	"context"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fastConfig keeps the debounce and idle windows short so persistence/release
// tests stay quick and deterministic.
func fastConfig() RoomConfig {
	return RoomConfig{
		SaveDebounce: 20 * time.Millisecond,
		IdleTimeout:  40 * time.Millisecond,
		SendBuffer:   256,
	}
}

// TestTwoClientMemoConvergence is the headline guarantee (SC-002): two clients
// edit a memo (Y.XmlFragment) concurrently; after the server fans the edits out,
// both converge to identical document state.
func TestTwoClientMemoConvergence(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const docID = model.DocumentID("memo-1")

	a := newFakeClient(t)
	b := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	b.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	b.observeUpdates()

	// Concurrent edits into each client's "default" XmlFragment.
	a.insertText("alpha ")
	b.insertText("beta ")

	waitFor(t, "memo convergence", func() bool {
		return a.text() == b.text() && len(a.text()) > 0
	})

	if got := a.text(); got != b.text() {
		t.Fatalf("memo diverged: a=%q b=%q", got, b.text())
	}
	// Both edits survived (convergence, not last-write-wins loss).
	all := a.text()
	if !contains(all, "alpha") || !contains(all, "beta") {
		t.Fatalf("lost an edit on convergence: %q", all)
	}
}

// TestTwoClientWhiteboardConvergence is the same headline guarantee for a
// whiteboard: an id-keyed Y.Map "elements" with per-element maps converges under
// concurrent inserts of distinct elements (per-property merge, data-model.md).
func TestTwoClientWhiteboardConvergence(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const docID = model.DocumentID("wb-1")

	a := newFakeClient(t)
	b := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeWhiteboard)
	b.join(mgr, docID, model.ContentTypeWhiteboard)
	a.observeUpdates()
	b.observeUpdates()

	// Each client adds a distinct element keyed by its id.
	a.addElement("el-a", map[string]interface{}{"x": float64(10), "type": "rectangle"})
	b.addElement("el-b", map[string]interface{}{"x": float64(20), "type": "ellipse"})

	waitFor(t, "whiteboard convergence", func() bool {
		return a.elementsLen() == 2 && b.elementsLen() == 2
	})

	if a.elementsLen() != b.elementsLen() {
		t.Fatalf("whiteboard diverged: a=%d b=%d elements", a.elementsLen(), b.elementsLen())
	}
	if !a.hasElement("el-a") || !a.hasElement("el-b") {
		t.Fatalf("client a missing an element: keys=%v", a.elementKeys())
	}
	if !b.hasElement("el-a") || !b.hasElement("el-b") {
		t.Fatalf("client b missing an element: keys=%v", b.elementKeys())
	}
}

// TestAwarenessFanOutAndNotPersisted asserts that an awareness update from one
// client reaches the other (T009 fan-out) and that awareness/ephemeral state is
// NOT written into the persisted snapshot (FR-008).
func TestAwarenessFanOutAndNotPersisted(t *testing.T) {
	mgr, deps := testManager(t, fastConfig())
	const docID = model.DocumentID("memo-aware")

	a := newFakeClient(t)
	b := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	b.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	b.observeUpdates()

	// One real document edit so a snapshot is actually produced.
	a.insertText("persisted ")

	// A's awareness (cursor) update — should fan out to B but never persist.
	awClientID := a.aware.ClientID
	a.setAwareness(ycrdt.MakeObject("cursor", "10:4", "user", "alice"))

	// An ephemeral cursor/emoji event — fanned out, never applied to the doc.
	a.session.Forward(encodeEphemeral([]byte(`{"type":"EMOJI_REACTION","emoji":"party"}`)))

	// B receives the awareness state.
	waitFor(t, "awareness fan-out", func() bool {
		return b.awarenessUserOf(awClientID) != nil
	})
	if got := b.awarenessUserOf(awClientID); got != "alice" {
		t.Fatalf("awareness not fanned out to B: got %v", got)
	}
	// B received the ephemeral frame.
	waitFor(t, "ephemeral fan-out", func() bool { return b.ephemeralCount() == 1 })

	// Wait for the debounced snapshot, then decode it and assert it has the doc
	// edit but no awareness/ephemeral leakage.
	waitFor(t, "snapshot saved", func() bool {
		_, err := deps.storedState(context.Background(), string(docID))
		return err == nil
	})
	snap, err := deps.storedState(context.Background(), string(docID))
	if err != nil {
		t.Fatalf("blob get: %v", err)
	}
	reloaded := ycrdt.NewDoc("guid")
	_ = ycrdt.ApplyUpdateV2(reloaded, snap, nil)

	if txt := xmlText(reloaded); !contains(txt, "persisted") {
		t.Fatalf("snapshot missing the doc edit: %q", txt)
	}
	// Awareness/ephemeral state never becomes a shared type in the doc, so it
	// must not appear anywhere in the persisted snapshot's JSON (FR-008).
	if docMentions(reloaded, "alice") || docMentions(reloaded, "party") {
		t.Fatalf("ephemeral/awareness state leaked into the persisted snapshot: %s",
			ycrdtJSON(reloaded))
	}
}

// TestPersistenceRoundTrip is the US2 no-regression: edits → debounced snapshot →
// room released → a fresh room lazily reloads the snapshot and matches. Covered
// for both a memo and a whiteboard document.
func TestPersistenceRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		content model.ContentType
		seed    func(c *fakeClient)
		check   func(t *testing.T, c *fakeClient)
	}{
		{
			name:    "memo",
			content: model.ContentTypeMemo,
			seed:    func(c *fakeClient) { c.insertText("durable memo") },
			check: func(t *testing.T, c *fakeClient) {
				if got := c.text(); !contains(got, "durable memo") {
					t.Fatalf("memo not restored: %q", got)
				}
			},
		},
		{
			name:    "whiteboard",
			content: model.ContentTypeWhiteboard,
			seed: func(c *fakeClient) {
				c.addElement("rect-1", map[string]interface{}{"x": float64(5), "type": "rectangle"})
			},
			check: func(t *testing.T, c *fakeClient) {
				if !c.hasElement("rect-1") {
					t.Fatalf("whiteboard element not restored: keys=%v", c.elementKeys())
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, deps := testManager(t, fastConfig())
			docID := model.DocumentID("rt-" + tc.name)

			// Session 1: edit, let it persist, then let the room release.
			a := newFakeClient(t)
			a.join(mgr, docID, tc.content)
			a.observeUpdates()
			tc.seed(a)

			waitFor(t, "snapshot saved", func() bool {
				return hasControlKind(a, model.ControlSaved)
			})
			a.session.Leave()

			// The room releases after the idle timeout; wait for the registry to drop it.
			waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })

			// Metadata index was written.
			meta, err := deps.meta.Load(context.Background(), docID)
			if err != nil {
				t.Fatalf("metadata not persisted: %v", err)
			}
			if meta.ContentType != tc.content {
				t.Fatalf("metadata content type = %q, want %q", meta.ContentType, tc.content)
			}

			// Session 2: a fresh client joins; the new room lazily reloads the
			// snapshot, so the client converges to the persisted state.
			b := newFakeClient(t)
			b.join(mgr, docID, tc.content)
			waitFor(t, "reload converged", func() bool {
				if tc.content == model.ContentTypeMemo {
					return contains(b.text(), "durable")
				}
				return b.hasElement("rect-1")
			})
			tc.check(t, b)
		})
	}
}

// TestOfflineReconnectNoLostEdits is US5 (FR-009): a client edits while
// partitioned from the server, the server is edited concurrently, then the
// client reconnects and drives SyncStep1 — the state-vector diff catches both
// sides up with no lost edits.
func TestOfflineReconnectNoLostEdits(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const docID = model.DocumentID("memo-us5")

	// Online client A establishes the room and a shared baseline.
	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("base ")
	waitFor(t, "baseline applied", func() bool { return contains(a.text(), "base") })

	// Client B connects and syncs the baseline, then "goes offline": it does NOT
	// observe/forward its edits yet (the partition), so its edits buffer locally.
	b := newFakeClient(t)
	b.join(mgr, docID, model.ContentTypeMemo)
	waitFor(t, "B synced baseline", func() bool { return contains(b.text(), "base") })

	// Simulate a network partition: block inbound delivery so the room's fan-out
	// of A's subsequent edits cannot reach B (both inbound and outbound are cut —
	// B has no observeUpdates registered, so it also cannot send).
	b.partition()

	// While B is partitioned, both sides edit concurrently.
	b.insertText("offline-b ") // buffered locally on B (not forwarded)
	a.insertText("online-a ")  // A is online; reaches the server

	// Let A's edit settle into the server (B must not have it yet — it is partitioned).
	waitFor(t, "A edit on server", func() bool { return contains(a.text(), "online-a") })
	if contains(b.text(), "online-a") {
		t.Fatalf("B saw A's edit while partitioned — partition not simulated")
	}

	// B reconnects: restore inbound delivery, then flush its offline buffer and
	// drive SyncStep1 so the server replies with the delta B is missing.
	b.unpartition()
	b.observeUpdates()        // now B forwards local edits
	b.pushBufferedAndResync() // flush offline buffer + SyncStep1

	waitFor(t, "US5 convergence", func() bool {
		return a.text() == b.text() &&
			contains(a.text(), "offline-b") &&
			contains(a.text(), "online-a")
	})

	final := a.text()
	for _, frag := range []string{"base", "offline-b", "online-a"} {
		if !contains(final, frag) {
			t.Fatalf("US5 lost an edit: %q missing %q", final, frag)
		}
	}
	if a.text() != b.text() {
		t.Fatalf("US5 diverged: a=%q b=%q", a.text(), b.text())
	}
}

// TestIdleReleasePersistsFinalSnapshot asserts a room with edits releases on
// idle and persists a final snapshot even if the debounce timer had not yet
// fired (release-time save, T011).
func TestIdleReleasePersistsFinalSnapshot(t *testing.T) {
	// Long debounce so the only save is the release-time one; short idle.
	mgr, deps := testManager(t, RoomConfig{
		SaveDebounce: 10 * time.Second,
		IdleTimeout:  20 * time.Millisecond,
		SendBuffer:   256,
	})
	const docID = model.DocumentID("memo-idle")

	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("final ")
	a.session.Leave()

	waitFor(t, "room released", func() bool { return mgr.RoomCount() == 0 })
	waitFor(t, "final snapshot saved", func() bool {
		_, err := deps.storedState(context.Background(), string(docID))
		return err == nil
	})
}
