package service

import (
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestEscalationIsCountedNamedAndExplained is SC-016 / FR-028.
//
// Escalation discards unsaved edits. That loss is accepted — the store is
// unreachable, so there is nowhere else to put them, and a fallback path would
// reintroduce exactly the adapters this feature removed. What is forbidden is
// losing them QUIETLY, and "quietly" has three specific meanings here, each
// asserted below:
//
//  1. no distinct counter — escalation folded into the generic failure metric,
//     so an operator cannot alert on data loss as distinct from a retryable blip;
//  2. no log naming the document and how long it had been failing — an operator
//     can see that something was discarded but not what, or for how long;
//  3. a generic disconnect — the user is told the connection closed, not that
//     their recent edits could not be saved, so they reasonably assume their work
//     is fine.
//
// Non-vacuity: each assertion targets a different mechanism, so weakening any one
// of them (dropping the counter, the log fields, or the reason code) fails only
// its own check.
func TestEscalationIsCountedNamedAndExplained(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	store := newOutageStore()
	metrics := &durabilityMetrics{}
	open := authopen.New()

	const threshold = 3
	mgr := NewManager(Deps{

		Metadata:   metainmem.New(),
		Checkpoint: store,
		Auth:       open,
		AuthZ:      open,
	}, RoomConfig{
		SaveDebounce: 5 * time.Millisecond,
		IdleTimeout:  10 * time.Second,
		SendBuffer:   256,
		Limits:       Limits{FlushFailureThreshold: threshold},
	}, metrics, zap.New(core))

	const doc model.DocumentID = "escalate-me"
	a := newFakeClient(t)
	a.join(mgr, doc, model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("work that will be discarded ")

	// The retry backoff drives the failure count to the threshold on its own; no
	// further edits are needed, which is the point — a document nobody is still
	// typing into must not sit undurable forever.
	waitFor(t, "escalation after the threshold is crossed", func() bool {
		return len(metrics.escalations()) > 0
	})

	// 1. A DISTINCT counter, not the generic snapshot-failure metric.
	esc := metrics.escalations()
	if len(esc) != 1 {
		t.Fatalf("expected exactly one escalation, got %d", len(esc))
	}

	// 2. A log entry naming the document and the undurable duration.
	entries := logs.FilterMessageSnippet("durability escalation").All()
	if len(entries) == 0 {
		t.Fatal("no escalation log entry; discarded edits must be recorded, not merely counted")
	}
	fields := entries[0].ContextMap()
	if got, ok := fields["doc"]; !ok || got != string(doc) {
		t.Fatalf("escalation log does not name the document (doc=%v); an operator cannot tell WHAT was discarded", fields["doc"])
	}
	if _, ok := fields["undurable_for"]; !ok {
		t.Fatalf("escalation log does not carry undurable_for; an operator cannot tell how long the document had been failing. fields: %v", fields)
	}
	if got, ok := fields["consecutive_failures"]; !ok || got != int64(threshold) {
		t.Fatalf("escalation log should report the threshold crossing, got consecutive_failures=%v want %d", got, threshold)
	}

	// 3. A disconnect reason that says what happened.
	if !hasControlReason(a, model.ControlRoomClosed, model.ReasonEditsNotSaved) {
		t.Fatalf("clients were disconnected without %q; a generic close leaves them assuming their work was saved", model.ReasonEditsNotSaved)
	}

	// The room is torn down, and NOT flushed on the way out — the teardown matrix
	// forbids writing a document whose durability is in doubt, and here the store
	// is the thing that is broken.
	waitFor(t, "room released after escalation", func() bool { return mgr.RoomCount() == 0 })
	if _, err := store.LoadCheckpoint(t.Context(), backend.DocumentID(doc)); err == nil {
		t.Fatal("escalation must not flush on teardown: the store is unreachable, which is why we are here")
	}
}
