package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeManager records the lifecycle calls the consumer makes, to assert routing
// without a live Manager/room.
type fakeManager struct {
	mu          sync.Mutex
	purged      []model.DocumentID
	reEvaluated []model.DocumentID
	registered  []model.Metadata
	purgeErr    error
	registerErr error
}

func (f *fakeManager) Purge(_ context.Context, id model.DocumentID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, id)
	return f.purgeErr
}

func (f *fakeManager) ReEvaluate(_ context.Context, id model.DocumentID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reEvaluated = append(f.reEvaluated, id)
}

func (f *fakeManager) PreRegister(_ context.Context, meta model.Metadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, meta)
	return f.registerErr
}

func (f *fakeManager) purgedIDs() []model.DocumentID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.DocumentID(nil), f.purged...)
}

// eventBody builds the NestJS event envelope { pattern, data, id } the consumer
// decodes (the same shape the metadata-store RPC uses).
func eventBody(t *testing.T, pattern string, data any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	body, err := json.Marshal(map[string]any{"pattern": pattern, "data": json.RawMessage(raw), "id": "corr-1"})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return body
}

func newConsumer(mgr Manager) *Consumer {
	return &Consumer{mgr: mgr, logger: zap.NewNop()}
}

// TestDocumentDeletedCascades asserts a document.deleted event triggers a Manager
// purge for the document id (FR-023, SC-010).
func TestDocumentDeletedCascades(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-1"}))

	got := mgr.purgedIDs()
	if len(got) != 1 || got[0] != "doc-1" {
		t.Fatalf("purged = %v, want [doc-1]", got)
	}
}

// TestDocumentDeletedIdempotentOnError asserts a purge error is swallowed (logged,
// not propagated) so a redelivery or an absent doc never crashes the consumer.
func TestDocumentDeletedIdempotentOnError(t *testing.T) {
	mgr := &fakeManager{purgeErr: errors.New("already gone")}
	c := newConsumer(mgr)
	// Must not panic; the error is logged and dropped.
	c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-2"}))
}

// TestDocumentAccessChangedReEvaluates asserts document.access_changed triggers a
// re-evaluation for the document (T014/T015).
func TestDocumentAccessChangedReEvaluates(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	c.handle(context.Background(), eventBody(t, PatternDocumentAccessChanged, AccessChangedEvent{ID: "doc-4"}))

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.reEvaluated) != 1 || mgr.reEvaluated[0] != "doc-4" {
		t.Fatalf("reEvaluated = %v, want [doc-4]", mgr.reEvaluated)
	}
}

// TestUnknownPatternIgnored asserts an unrelated pattern is ignored without error
// (the consumer shares the bus with the metadata-store RPC replies).
func TestUnknownPatternIgnored(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)
	c.handle(context.Background(), eventBody(t, "some-other-pattern", map[string]string{"x": "y"}))
	c.handle(context.Background(), []byte("not json"))
	if len(mgr.purgedIDs()) != 0 {
		t.Fatal("unknown pattern triggered a cascade")
	}
}

// TestMalformedAndEmptyEventsAreTerminal asserts an event with a malformed or
// empty-id payload drives no cascade and is judged terminal: no amount of
// redelivery makes an unparseable body or a blank id actionable, so it leaves
// the retry schedule for the dead-letter queue instead of cycling forever.
func TestMalformedAndEmptyEventsAreTerminal(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	bodies := [][]byte{
		eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: ""}),
		eventBody(t, PatternDocumentAccessChanged, AccessChangedEvent{ID: ""}),
		// Non-object data for each pattern → unmarshal error.
		eventBody(t, PatternDocumentDeleted, "not-an-object"),
		eventBody(t, PatternDocumentAccessChanged, []int{1, 2}),
	}
	for i, body := range bodies {
		if got := c.handle(context.Background(), body); got != ackTerminal {
			t.Errorf("body %d: handle = %v, want ackTerminal", i, got)
		}
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.purged)+len(mgr.registered)+len(mgr.reEvaluated) != 0 {
		t.Fatalf("empty/malformed events drove a cascade: purged=%v registered=%v reEvaluated=%v",
			mgr.purged, mgr.registered, mgr.reEvaluated)
	}
}

// TestHandleVerdictsSeparateSuccessFromUnactionable asserts the two non-retry
// verdicts stay distinct: work that completed acks and stops, while an envelope
// no redelivery can help is terminal so consume routes it to the DLQ. Collapsing
// them would silently discard events nobody ever sees.
func TestHandleVerdictsSeparateSuccessFromUnactionable(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	for _, tc := range []struct {
		name string
		body []byte
		want ackAction
	}{
		{"deleted", eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "d"}), ackSuccess},
		{"access_changed", eventBody(t, PatternDocumentAccessChanged, AccessChangedEvent{ID: "a"}), ackSuccess},
		{"unknown-pattern", eventBody(t, "other", map[string]string{"x": "y"}), ackTerminal},
		{"not-json", []byte("not json"), ackTerminal},
		{"empty-id-deleted", eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: ""}), ackTerminal},
	} {
		if got := c.handle(context.Background(), tc.body); got != tc.want {
			t.Errorf("%s: handle = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestHandleNacksRequeueOnPurgeFailure asserts a transient purge failure returns
// retryLater so the delete event is redelivered (not at-most-once dropped) — the
// cascade is a correctness requirement (no orphan documents).
func TestHandleNacksRequeueOnPurgeFailure(t *testing.T) {
	mgr := &fakeManager{purgeErr: errors.New("backend down")}
	c := newConsumer(mgr)
	if got := c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-fail"})); got != retryLater {
		t.Fatalf("handle on purge failure = %v, want retryLater", got)
	}
}
