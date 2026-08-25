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
	mu       sync.Mutex
	closed   []model.DocumentID
	closeErr error
}

func (f *fakeManager) CloseDeleted(_ context.Context, id model.DocumentID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = append(f.closed, id)
	return f.closeErr
}

func (f *fakeManager) closedIDs() []model.DocumentID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.DocumentID(nil), f.closed...)
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

// TestDocumentDeletedClosesTheRoom asserts a document.deleted event routes to the
// Manager's close for that document id (FR-023, SC-010).
func TestDocumentDeletedClosesTheRoom(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-1"}))

	got := mgr.closedIDs()
	if len(got) != 1 || got[0] != "doc-1" {
		t.Fatalf("closed = %v, want [doc-1]", got)
	}
}

// TestMalformedAndEmptyEventsAreTerminal asserts an event with a malformed or
// empty-id payload drives no close and is judged terminal: no amount of
// redelivery makes an unparseable body or a blank id actionable, so it leaves
// the retry schedule for the dead-letter queue instead of cycling forever.
func TestMalformedAndEmptyEventsAreTerminal(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	bodies := [][]byte{
		eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: ""}),
		// Non-object data for each pattern → unmarshal error.
		eventBody(t, PatternDocumentDeleted, "not-an-object"),
	}
	for i, body := range bodies {
		if got := c.handle(context.Background(), body); got != rejectPoison {
			t.Errorf("body %d: handle = %v, want rejectPoison", i, got)
		}
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.closed) != 0 {
		t.Fatalf("empty/malformed events drove a close: closed=%v", mgr.closed)
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
		{"unknown-pattern", eventBody(t, "other", map[string]string{"x": "y"}), rejectPoison},
		{"not-json", []byte("not json"), rejectPoison},
		{"empty-id-deleted", eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: ""}), rejectPoison},
	} {
		if got := c.handle(context.Background(), tc.body); got != tc.want {
			t.Errorf("%s: handle = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestATransientCloseFailureIsRetriedNotDropped asserts a transient close failure
// returns requeue, so the broker redelivers it rather than it being
// acked away. document.deleted is the only path that closes a room for a deleted
// document, so dropping one leaves a live room serving content the owner
// believes is gone.
func TestATransientCloseFailureIsRetriedNotDropped(t *testing.T) {
	mgr := &fakeManager{closeErr: errors.New("backend down")}
	c := newConsumer(mgr)
	if got := c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: "doc-fail"})); got != requeue {
		t.Fatalf("handle on a refused close = %v, want requeue", got)
	}
}
