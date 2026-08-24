package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"

	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"

	ycrdt "github.com/antst/go-yjs/crdt"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// wedgeRoom builds a room with NO run loop and registers it so the manager's first
// acquire returns it. Because nothing drains its command channel, a command
// enqueued into it is never processed — modelling the join/purge-vs-teardown race
// where enqueue's buffered send wins against a concurrently-closing room. Tearing
// the room down (finish) then closes its done channel and removes it from the
// registry, exactly as a real cmdClose / idle release would.
func wedgeRoom(t *testing.T, m *Manager, deps testDeps, id model.DocumentID) *Room {
	t.Helper()
	room, err := newRoom(context.Background(), id, model.ContentTypeMemo, deps.Deps, m.cfg, NopMetrics{}, zap.NewNop())
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	room.onReleased = func() { m.remove(id, room) }
	m.mu.Lock()
	m.rooms[id] = room
	m.mu.Unlock()
	return room
}

// TestJoinDoesNotHangWhenRoomTearsDownAfterEnqueue is the regression guard for the
// join-vs-teardown race. Manager.Join's enqueue can win the buffered-send race into
// a room whose run loop then exits without processing the join, so the join result
// is never written. A bare `<-res` would block the join goroutine — and leak the
// hijacked WebSocket behind it — forever; the fix selects on the room's done
// channel and retries against a fresh room.
func TestJoinDoesNotHangWhenRoomTearsDownAfterEnqueue(t *testing.T) {
	m, deps := testManager(t, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second})
	id := model.DocumentID("doc-join-teardown-race")
	room := wedgeRoom(t, m, deps, id)

	c := newFakeClient(t)
	joinErr := make(chan error, 1)
	// Join now requires the document to exist; register it so the test exercises
	// the behaviour it is actually about.
	if err := m.PreRegister(context.Background(), model.Metadata{ID: id, ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("pre-register: %v", err)
	}
	go func() {
		_, _, err := m.Join(context.Background(), JoinRequest{ID: id, Content: model.ContentTypeMemo, Identity: c.identity, Conn: c})
		joinErr <- err
	}()

	// Wait until Join has enqueued cmdJoin into the wedged (un-drained) room: at that
	// point Join is committed to waiting for either the result or the teardown.
	waitFor(t, "cmdJoin buffered in the wedged room", func() bool { return len(room.commands) == 1 })

	// Tear the room down with its cmdJoin still unprocessed (mirrors a concurrent
	// cmdClose / idle release): closes room.done and drops it from the registry.
	// Without the fix, Join is parked on <-res forever.
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	select {
	case err := <-joinErr:
		if err != nil {
			t.Fatalf("Join should retry into a fresh room and succeed, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Join hung after the room tore down with its cmdJoin still unprocessed")
	}
}

// TestCloseDeletedDoesNotHangWhenRoomTearsDownAfterEnqueue is the same race on
// the lifecycle path: cmdCloseDeleted can land in a room that tears down without
// running it, so the result channel is never written. CloseDeleted selects on
// room.done and treats the already-evicted room as success instead of blocking.
func TestCloseDeletedDoesNotHangWhenRoomTearsDownAfterEnqueue(t *testing.T) {
	m, deps := testManager(t, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second})
	id := model.DocumentID("doc-purge-teardown-race")
	room := wedgeRoom(t, m, deps, id)

	purgeErr := make(chan error, 1)
	go func() { purgeErr <- m.CloseDeleted(context.Background(), id) }()

	waitFor(t, "cmdCloseDeleted buffered in the wedged room", func() bool { return len(room.commands) == 1 })
	room.teardown(model.NewSessionEnd(model.CodeServerShutdown), nil)

	select {
	case err := <-purgeErr:
		if err != nil {
			t.Fatalf("CloseDeleted should accept an already-evicted room, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseDeleted hung after the room tore down with its command unprocessed")
	}
}

// gateStore wraps a CheckpointStore and holds SaveCheckpoint open until release
// is closed, so a test can observe whether Manager.Close returns before the final
// shutdown save has actually been written.
type gateStore struct {
	inner   *persistinprocess.Store
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (g *gateStore) SaveCheckpoint(ctx context.Context, req persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return g.inner.SaveCheckpoint(ctx, req)
}

func (g *gateStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	return g.inner.LoadCheckpoint(ctx, id)
}

func (g *gateStore) FenceMode() persistence.FenceMode { return g.inner.FenceMode() }

// TestManagerCloseWaitsForFinalSnapshotDrain is the regression guard for the
// shutdown snapshot-loss bug: Manager.Close enqueued cmdClose and returned
// immediately, so App.Close went on to tear down the metadata/blob backends while
// the rooms were still asynchronously persisting their final snapshot — losing the
// last debounce window of edits. The fix makes Close block until every room has
// finished its final persist (r.done). Here we hold the blob Put open and assert
// Close stays blocked until we release it, then that the snapshot landed durably.
func TestManagerCloseWaitsForFinalSnapshotDrain(t *testing.T) {
	deps := newTestDeps()
	gate := &gateStore{inner: deps.store, started: make(chan struct{}), release: make(chan struct{})}
	deps.Checkpoint = gate
	// SaveDebounce/IdleTimeout long so the ONLY persist is the cmdClose one and the
	// room does not release on its own mid-test.
	m := NewManager(deps.Deps, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 30 * time.Second}, nil, zap.NewNop())

	id := model.DocumentID("doc-shutdown-drain")
	c := newFakeClient(t)
	c.join(m, id, model.ContentTypeMemo)
	c.observeUpdates()
	// A real edit dirties the room, so cmdClose has a snapshot to persist (a clean
	// room's persist is a no-op and would never reach the blob).
	c.withDoc(func(doc *ycrdt.Doc) { insertText(doc, "shutdown-edit") })

	closeReturned := make(chan struct{})
	go func() { m.Close(); close(closeReturned) }()

	// The final snapshot persist must start — proving Close enqueued cmdClose and the
	// room is mid-persist...
	select {
	case <-gate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("final snapshot persist never started after Close (room not dirtied or cmdClose never ran)")
	}
	// ...and Close must NOT have returned while that persist is still in flight.
	// Without the fix Close returns here, and App.Close would then close the durable
	// backends out from under the in-flight Put.
	select {
	case <-closeReturned:
		t.Fatal("Manager.Close returned before the final snapshot completed (backends could close mid-save → lost edits)")
	case <-time.After(150 * time.Millisecond):
	}

	close(gate.release) // let the persist finish
	select {
	case <-closeReturned:
	case <-time.After(3 * time.Second):
		t.Fatal("Manager.Close did not return after the final snapshot completed")
	}

	// The final snapshot actually landed durably.
	if _, err := deps.meta.Load(context.Background(), id); err != nil {
		t.Fatalf("metadata index not saved after shutdown drain: %v", err)
	}
}
