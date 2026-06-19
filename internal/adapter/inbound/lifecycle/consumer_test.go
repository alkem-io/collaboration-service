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

func (f *fakeManager) ReEvaluate(id model.DocumentID) {
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
// decodes (the same shape the metastore RPC uses).
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

// TestDocumentCreatedPreRegisters asserts document.created pre-registers metadata
// with the carried content type + owner ref (optional create path, T015).
func TestDocumentCreatedPreRegisters(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	c.handle(context.Background(), eventBody(t, PatternDocumentCreated, CreatedEvent{
		ID: "doc-3", ContentType: "whiteboard", OwnerRef: "callout-9",
	}))

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.registered) != 1 {
		t.Fatalf("registered count = %d, want 1", len(mgr.registered))
	}
	got := mgr.registered[0]
	if got.ID != "doc-3" || got.ContentType != model.ContentTypeWhiteboard || got.OwnerRef != "callout-9" {
		t.Fatalf("pre-registered metadata mismatch: %+v", got)
	}
}

// TestDocumentCreatedToleratesPreRegisterError asserts a pre-register failure is
// logged and swallowed (a create event must not crash the consumer).
func TestDocumentCreatedToleratesPreRegisterError(t *testing.T) {
	mgr := &fakeManager{registerErr: errors.New("index down")}
	c := newConsumer(mgr)
	c.handle(context.Background(), eventBody(t, PatternDocumentCreated, CreatedEvent{ID: "doc-x", ContentType: "memo"}))
}

// TestDocumentCreatedNormalizesContentType asserts an unknown/empty content type
// on a create event is defaulted to memo rather than persisted verbatim (which
// would write invalid metadata that breaks convention application).
func TestDocumentCreatedNormalizesContentType(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want model.ContentType
	}{
		{"", model.ContentTypeMemo},
		{"bogus", model.ContentTypeMemo},
		{"whiteboard", model.ContentTypeWhiteboard},
	} {
		mgr := &fakeManager{}
		c := newConsumer(mgr)
		c.handle(context.Background(), eventBody(t, PatternDocumentCreated, CreatedEvent{ID: "doc-n", ContentType: tc.raw}))
		mgr.mu.Lock()
		if len(mgr.registered) != 1 || mgr.registered[0].ContentType != tc.want {
			t.Errorf("contentType %q normalized to %v, want %v", tc.raw, mgr.registered, tc.want)
		}
		mgr.mu.Unlock()
	}
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
// (the consumer shares the bus with the metastore RPC replies).
func TestUnknownPatternIgnored(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)
	c.handle(context.Background(), eventBody(t, "some-other-pattern", map[string]string{"x": "y"}))
	c.handle(context.Background(), []byte("not json"))
	if len(mgr.purgedIDs()) != 0 {
		t.Fatal("unknown pattern triggered a cascade")
	}
}

// TestMalformedAndEmptyEventsIgnored asserts an event with a malformed or
// empty-id payload is ignored rather than driving a cascade with a blank id.
func TestMalformedAndEmptyEventsIgnored(t *testing.T) {
	mgr := &fakeManager{}
	c := newConsumer(mgr)

	c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, DeletedEvent{ID: ""}))
	c.handle(context.Background(), eventBody(t, PatternDocumentCreated, CreatedEvent{ID: ""}))
	c.handle(context.Background(), eventBody(t, PatternDocumentAccessChanged, AccessChangedEvent{ID: ""}))
	// Non-object data for each pattern → unmarshal error, ignored.
	c.handle(context.Background(), eventBody(t, PatternDocumentDeleted, "not-an-object"))
	c.handle(context.Background(), eventBody(t, PatternDocumentCreated, 42))
	c.handle(context.Background(), eventBody(t, PatternDocumentAccessChanged, []int{1, 2}))

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.purged)+len(mgr.registered)+len(mgr.reEvaluated) != 0 {
		t.Fatalf("empty/malformed events drove a cascade: purged=%v registered=%v reEvaluated=%v",
			mgr.purged, mgr.registered, mgr.reEvaluated)
	}
}
