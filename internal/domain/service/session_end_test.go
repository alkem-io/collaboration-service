package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// --- A: the typed contract is exhaustive and every emitter populates it ---

// TestEverySessionEndCodeCarriesScopeAndDisposition walks the WHOLE code set and
// requires each to resolve to a complete intent.
//
// It exists because the failure it guards is silent: a code added without a
// table entry would reach a client as an empty disposition, which is not a value
// the client can branch on — it would simply not know whether to reconnect. The
// table is the only place that mapping lives, so this is the only place that can
// notice it is missing.
func TestEverySessionEndCodeCarriesScopeAndDisposition(t *testing.T) {
	want := map[model.SessionEndCode]model.SessionEnd{
		model.CodeUpdateRateExceeded:        {Code: model.CodeUpdateRateExceeded, Scope: model.ScopeMember, Disposition: model.DispositionTransient},
		model.CodeDocumentSizeLimitExceeded: {Code: model.CodeDocumentSizeLimitExceeded, Scope: model.ScopeMember, Disposition: model.DispositionManual},
		model.CodeDocumentDeleted:           {Code: model.CodeDocumentDeleted, Scope: model.ScopeDocument, Disposition: model.DispositionTerminal},
		model.CodeEditsNotSaved:             {Code: model.CodeEditsNotSaved, Scope: model.ScopeDocument, Disposition: model.DispositionTerminal},
		model.CodeServerShutdown:            {Code: model.CodeServerShutdown, Scope: model.ScopeDocument, Disposition: model.DispositionTransient},
	}

	codes := model.SessionEndCodes()
	if len(codes) != len(want) {
		t.Fatalf("the code set changed: %d codes, %d pinned here. A new code needs a client disposition agreed with client-web before it ships", len(codes), len(want))
	}
	for _, code := range codes {
		got := model.NewSessionEnd(code)
		expected, ok := want[code]
		if !ok {
			t.Fatalf("code %q has no pinned scope/disposition", code)
		}
		if got != expected {
			t.Errorf("session end for %q = %+v, want %+v", code, got, expected)
		}
		if got.Scope == "" || got.Disposition == "" {
			t.Errorf("code %q reaches the client with an empty scope/disposition", code)
		}
	}
}

// TestServerShutdownIsAnnouncedAndRetryable pins the case that carried NOTHING
// before: a graceful shutdown used to broadcast a bare room-closed with no
// reason and no error, so the one situation where reconnecting is exactly right
// was indistinguishable from the ones where it is exactly wrong.
func TestServerShutdownIsAnnouncedAndRetryable(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	a := newFakeClient(t)
	a.join(mgr, "shutdown-doc", model.ContentTypeMemo)

	mgr.Close()

	var end *model.ControlMessage
	for _, m := range a.controlMessages() {
		if m.Kind == model.ControlSessionEnd {
			cp := m
			end = &cp
		}
	}
	if end == nil {
		t.Fatal("graceful shutdown told the client nothing")
	}
	if end.Code != model.CodeServerShutdown {
		t.Errorf("shutdown code = %q, want %q", end.Code, model.CodeServerShutdown)
	}
	if end.Disposition != model.DispositionTransient {
		t.Errorf("shutdown disposition = %q, want %q — a client that does not reconnect after a deploy never comes back", end.Disposition, model.DispositionTransient)
	}
	if end.Scope != model.ScopeDocument {
		t.Errorf("shutdown scope = %q, want %q", end.Scope, model.ScopeDocument)
	}
}

// TestPerMemberLimitIsScopedToTheMember pins the distinction the old contract
// could not express: a rate-limited client was told "room-closed", the same
// words the room used when it genuinely ended, even though the room was still
// serving everybody else.
func TestPerMemberLimitIsScopedToTheMember(t *testing.T) {
	cfg := fastConfig()
	// The same budget the rate-limit test uses: loose enough that ordinary
	// handshake and sync traffic is not charged into a disconnect, tight enough
	// that a burst of 30 edits from one member trips it.
	cfg.Limits.UpdateRatePerSec = 5
	cfg.Limits.UpdateBurst = 2
	mgr, _ := testManager(t, cfg)

	a := newFakeClient(t)
	b := newFakeClient(t)
	a.join(mgr, "scoped", model.ContentTypeMemo)
	b.join(mgr, "scoped", model.ContentTypeMemo)
	a.observeUpdates()

	for i := 0; i < 30; i++ {
		a.insertText("x")
	}

	waitFor(t, "rate-limited member ended", func() bool {
		return hasControlCode(a, model.CodeUpdateRateExceeded)
	})
	for _, m := range a.controlMessages() {
		if m.Kind == model.ControlSessionEnd && m.Code == model.CodeUpdateRateExceeded {
			if m.Scope != model.ScopeMember {
				t.Errorf("rate-limit scope = %q, want %q: the room is still serving other members", m.Scope, model.ScopeMember)
			}
			if m.Disposition != model.DispositionTransient {
				t.Errorf("rate-limit disposition = %q, want %q: the bucket refills, so a backoff genuinely resolves it", m.Disposition, model.DispositionTransient)
			}
		}
	}
	// The other member was NOT ended.
	if end, _ := b.sessionEnd(); end != nil {
		t.Fatalf("a per-member limit ended an innocent bystander: %+v", end)
	}
}

// --- B: the reason reaches the client BEFORE the close ---

// TestMembersAreToldBeforeTheyAreClosed is the ordering guarantee, asserted where
// it is decided: the room queues the session-end control and only then the close
// intent, both onto the same per-connection queue.
//
// Without it a client gets a bare disconnect and has to guess whether its work
// was saved, whether the document still exists, and whether to come back —
// which is precisely the state the old contract left it in.
//
// Non-vacuity: move the CloseAfterDrain call in teardown ABOVE the Send and this
// fails, because the end is then recorded with no control frame ahead of it.
func TestMembersAreToldBeforeTheyAreClosed(t *testing.T) {
	mgr, _ := testManager(t, fastConfig())
	a := newFakeClient(t)
	a.join(mgr, "ordered", model.ContentTypeMemo)

	mgr.Close()

	end, toldFirst := a.sessionEnd()
	if end == nil {
		t.Fatal("the connection was never closed")
	}
	if !toldFirst {
		t.Fatal("the close was queued BEFORE the session-end control: a client reading up to the close would have no reason for it")
	}
	if !hasControlCode(a, model.CodeServerShutdown) {
		t.Fatal("the client was closed without a session-end control")
	}
	if end.Code != model.CodeServerShutdown {
		t.Errorf("close intent = %q, want %q; the close must agree with what the client was told", end.Code, model.CodeServerShutdown)
	}
}

// --- C: an unknown document is refused before anything is materialized ---

// countingDeps records the durable calls a join makes, so a refusal can be shown
// to cost nothing rather than merely to return an error.
type countingDeps struct {
	port.MetadataStore
	loads atomic.Int64
	saves atomic.Int64
}

func (c *countingDeps) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	c.loads.Add(1)
	return c.MetadataStore.Load(ctx, id)
}

func (c *countingDeps) Save(ctx context.Context, meta model.Metadata) error {
	c.saves.Add(1)
	return c.MetadataStore.Save(ctx, meta)
}

// countingCheckpoints records restores, which is the expensive half of
// materializing a room.
type countingCheckpoints struct {
	*persistinprocess.Store
	loads atomic.Int64
}

func (c *countingCheckpoints) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	c.loads.Add(1)
	return c.Store.LoadCheckpoint(ctx, id)
}

func countingManager(t *testing.T) (*Manager, *countingDeps, *countingCheckpoints) {
	t.Helper()
	meta := &countingDeps{MetadataStore: metainmem.New()}
	cp := &countingCheckpoints{Store: persistinprocess.New()}
	open := authopen.New()
	mgr := NewManager(Deps{
		Metadata:   meta,
		Checkpoint: cp,
		Auth:       open,
		AuthZ:      open,
	}, fastConfig(), nil, nil)
	t.Cleanup(mgr.Close)
	return mgr, meta, cp
}

// TestUnknownDocumentIsRefusedWithoutMaterializing is the resurrection gate.
//
// The metadata store is the durable existence record, and this is the gate that
// consults it. Without it, connecting to an id that does not exist materializes a
// fresh room, seeds an empty document, and its first flush writes content and an
// index row for a document that was never there — or was deleted. The refusal has
// to happen BEFORE materialization, so this asserts the absence of the work rather
// than just the presence of an error.
//
// Non-vacuity: delete the requireDocument call from Join and the room is
// materialized, LoadCheckpoint runs, and the counts below are non-zero.
func TestUnknownDocumentIsRefusedWithoutMaterializing(t *testing.T) {
	mgr, meta, cp := countingManager(t)
	a := newFakeClient(t)

	_, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: "never-existed", Content: model.ContentTypeMemo, Identity: a.identity, Conn: a,
	})
	if !errors.Is(err, ErrDocumentUnknown) {
		t.Fatalf("join of an unknown document = %v, want ErrDocumentUnknown", err)
	}
	if got := cp.loads.Load(); got != 0 {
		t.Errorf("LoadCheckpoint ran %d times for a document that does not exist; the refusal must precede materialization", got)
	}
	if got := meta.saves.Load(); got != 0 {
		t.Errorf("Metadata.Save ran %d times for a document that does not exist; that is the row that resurrects it", got)
	}
	mgr.mu.Lock()
	rooms := len(mgr.rooms)
	mgr.mu.Unlock()
	if rooms != 0 {
		t.Errorf("%d room(s) left behind for a refused join", rooms)
	}
}

// TestInvalidatedGenerationTellsMembersTheirEditsAreGone covers the teardown path
// that reaches members WITHOUT going through the Manager: the registry poisons
// the document's generation and the room tears down without persisting.
//
// It said nothing at all before — the sockets were simply abandoned — so a
// client whose unsaved work had just been discarded could not tell this from a
// network blip and would reconnect showing an older document as if nothing had
// happened.

// serverOwnedMeta models the production ownership boundary: `server` owns the
// metadata row and removes it itself. Collab has no Delete on the MetadataStore
// port — that method was removed with the owner-delete cascade — so a test that
// needs the row gone has to take it away from the outside, exactly as the real
// owner does.
type serverOwnedMeta struct {
	port.MetadataStore
	mu   sync.Mutex
	gone map[model.DocumentID]bool
}

// serverRemoved is `server` deleting the row.
func (m *serverOwnedMeta) serverRemoved(id model.DocumentID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gone[id] = true
}

func (m *serverOwnedMeta) Load(ctx context.Context, id model.DocumentID) (model.Metadata, error) {
	m.mu.Lock()
	removed := m.gone[id]
	m.mu.Unlock()
	if removed {
		return model.Metadata{}, model.ErrNotFound
	}
	return m.MetadataStore.Load(ctx, id)
}
