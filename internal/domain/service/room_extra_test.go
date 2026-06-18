package service

import (
	"bytes"
	"context"
	"errors"
	"testing"

	ycrdt "github.com/skyterra/y-crdt"
	"github.com/skyterra/y-crdt/protocol"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	blobinline "github.com/alkem-io/collaboration-service/internal/adapter/outbound/blobstore/inline"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// failingMetaSave is a MetadataStore whose Save errors, to drive the
// save-error-on-metadata branch of persist (R7).
type failingMetaSave struct{ port.MetadataStore }

func (failingMetaSave) Save(context.Context, model.Metadata) error { return errors.New("index down") }
func (failingMetaSave) Load(context.Context, model.DocumentID) (model.Metadata, error) {
	return model.Metadata{}, model.ErrNotFound
}

// TestSaveErrorOnMetadataFailure asserts a metadata Save failure (after a
// successful blob Put) still emits save-error and increments the failure metric.
func TestSaveErrorOnMetadataFailure(t *testing.T) {
	open := authopen.New()
	metrics := &countingMetrics{}
	deps := Deps{
		Metadata: failingMetaSave{},
		Blob:     blobinline.New(),
		Auth:     open,
		AuthZ:    open,
	}
	mgr := NewManager(deps, fastConfig(), metrics, nil)

	a := newFakeClient(t)
	a.join(mgr, "meta-fail", model.ContentTypeMemo)
	a.observeUpdates()
	a.insertText("x ")

	waitFor(t, "save-error control", func() bool { return hasControlKind(a, model.ControlSaveError) })
	if metrics.snapsFailed.Load() == 0 {
		t.Errorf("snapshot-failed metric not incremented on metadata failure")
	}
}

// TestLateJoinerReceivesAwareness asserts that awareness applied to the room is
// handed to a client that joins afterwards (the join-time awareness snapshot).
func TestLateJoinerReceivesAwareness(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	const docID = model.DocumentID("late-aware")

	a := newFakeClient(t)
	a.join(mgr, docID, model.ContentTypeMemo)
	a.observeUpdates()
	aClient := a.aware.ClientID
	// A's awareness command and B's join command are serialized on the room's
	// run loop in submission order, so by the time B joins the room has already
	// recorded A's presence and includes it in B's join-time snapshot.
	a.setAwareness(ycrdt.Object{"user": "early"})

	b := newFakeClient(t)
	b.join(mgr, docID, model.ContentTypeMemo)
	waitFor(t, "late joiner sees presence", func() bool {
		return b.awarenessUserOf(aClient) == "early"
	})
}

// TestUnknownWireTypeIgnored asserts a client-sent control/unknown frame is
// ignored (no panic, no mutation) — matches y-protocols leniency.
func TestUnknownWireTypeIgnored(t *testing.T) {
	room := newBareRoom(t)

	var control bytes.Buffer
	protocol.WriteMessage(&control, uint8(model.WireControl), []byte(`{"kind":"saved"}`))
	if room.handleMessage(1, control.Bytes()) {
		t.Error("control frame should not mutate the document")
	}

	var unknown bytes.Buffer
	protocol.WriteMessage(&unknown, 99, []byte("nonsense"))
	if room.handleMessage(1, unknown.Bytes()) {
		t.Error("unknown frame should not mutate the document")
	}

	// A malformed (empty) frame is dropped without mutation.
	if room.handleMessage(1, nil) {
		t.Error("empty frame should not mutate the document")
	}
}

// TestNopMetricsCallable asserts the no-op metrics default is safe to call (it
// is the fallback the manager and room install when no metrics are wired).
func TestNopMetricsCallable(t *testing.T) {
	var m Metrics = NopMetrics{}
	m.RoomOpened()
	m.RoomClosed()
	m.ConnOpened()
	m.ConnClosed()
	m.SnapshotSaved()
	m.SnapshotFailed()
	m.FanoutPublished(0)
	m.FanoutFailed()
	m.ContributingActors(3)

	// The standalone default Contributor drops the event (no bus).
	if err := (noopContributor{}).Contribution(context.Background(), "doc", []string{"a"}); err != nil {
		t.Fatalf("noop contributor returned an error: %v", err)
	}
}

// TestDispatchSyncMalformed asserts a non-sync / malformed framed message is
// handled without error and without a reply.
func TestDispatchSyncMalformed(t *testing.T) {
	room := newBareRoom(t)

	// An awareness-typed frame routed into dispatchSync is a no-op, no reply.
	var aw bytes.Buffer
	protocol.WriteMessage(&aw, protocol.MessageAwareness, []byte{0})
	var reply bytes.Buffer
	if _, err := room.dispatchSync(aw.Bytes(), &reply, 1, true); err != nil {
		t.Fatalf("dispatchSync(non-sync): %v", err)
	}
	if reply.Len() != 0 {
		t.Errorf("non-sync frame produced a reply")
	}

	// An empty buffer is a read error.
	if _, err := room.dispatchSync(nil, &reply, 1, true); err == nil {
		t.Errorf("expected error on empty frame")
	}
}
