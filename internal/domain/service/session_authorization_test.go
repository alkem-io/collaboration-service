package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	"go.uber.org/zap"

	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// admissionStore counts the durable reads a join causes, so "nothing was
// materialized" can be asserted against the backend rather than inferred.
type admissionStore struct {
	persistence.CheckpointStore
	mu    sync.Mutex
	loads int
}

func (a *admissionStore) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	a.mu.Lock()
	a.loads++
	a.mu.Unlock()
	return a.CheckpointStore.LoadCheckpoint(ctx, id)
}

func (a *admissionStore) loadCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.loads
}

// scriptedAuthZ answers a scripted sequence of decisions and records every call,
// so a test can assert HOW MANY evaluations a session costs and when they happen.
type scriptedAuthZ struct {
	mu    sync.Mutex
	calls []model.Privilege
	// decide answers by privilege and call ordinal (1-based across all calls).
	decide func(p model.Privilege, nth int) (model.AuthDecision, error)
}

func (s *scriptedAuthZ) Authenticate(context.Context, string) (model.Identity, error) {
	return model.Identity{}, nil
}

func (s *scriptedAuthZ) Evaluate(_ context.Context, _ model.Identity, _ model.DocumentID, p model.Privilege) (model.AuthDecision, error) {
	s.mu.Lock()
	s.calls = append(s.calls, p)
	nth := len(s.calls)
	s.mu.Unlock()
	return s.decide(p, nth)
}

func (s *scriptedAuthZ) seen() []model.Privilege {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.Privilege(nil), s.calls...)
}

// decideBy answers each privilege with a fixed verdict.
func decideBy(read, write bool) func(model.Privilege, int) (model.AuthDecision, error) {
	return func(p model.Privilege, _ int) (model.AuthDecision, error) {
		if p == model.PrivilegeRead {
			return model.AuthDecision{Allowed: read}, nil
		}
		return model.AuthDecision{Allowed: write}, nil
	}
}

// admissionManager builds a Manager whose document ALREADY EXISTS durably, so a
// materialization would be visible as a checkpoint load.
func admissionManager(t *testing.T, authz *scriptedAuthZ, doc model.DocumentID) (*Manager, *admissionStore) {
	t.Helper()
	inner := persistinprocess.New()
	store := &admissionStore{CheckpointStore: inner}
	meta := metainmem.New()
	ctx := context.Background()

	if _, err := inner.SaveCheckpoint(ctx, persistence.SaveCheckpointRequest{
		DocumentID: backend.DocumentID(doc), Encoding: persistence.EncodingV2,
		Update: seedUpdate(t, string(doc)), StateVector: []byte("sv"),
	}); err != nil {
		t.Fatalf("seed durable state: %v", err)
	}
	if err := meta.Save(ctx, model.Metadata{
		ID: doc, ContentType: model.ContentTypeMemo, ContentPointer: string(doc),
	}); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	mgr := NewManager(Deps{
		Metadata: meta, Checkpoint: store, Auth: authz, AuthZ: authz,
	}, fastConfig(), nil, zap.NewNop())
	t.Cleanup(mgr.Close)
	return mgr, store
}

// TestADeniedReadNeverMaterializesTheDocument is the admission gate.
//
// Authorization used to be decided inside the room, which means the room already
// existed by the time anyone asked: acquire had loaded the document out of
// durable storage, opened a fan-out subscription, and taken a registry slot. The
// refused caller was never SENT anything — that boundary always held — but an
// actor with no read access could make the service fetch and decode any document
// it could name, leave a live room behind for the idle timeout, and learn from
// the latency whether the document existed. Twenty-five denied joins produced
// twenty-five live rooms.
func TestADeniedReadNeverMaterializesTheDocument(t *testing.T) {
	const doc model.DocumentID = "denied-doc"
	authz := &scriptedAuthZ{decide: decideBy(false, false)}
	mgr, store := admissionManager(t, authz, doc)

	client := newFakeClient(t)
	session, frames, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo,
		Identity: testIdentity("intruder"), Conn: client,
	})

	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("join = %v, want ErrForbidden", err)
	}
	if session != nil || len(frames) != 0 {
		t.Fatalf("a denied join returned session=%v frames=%d", session, len(frames))
	}
	if got := store.loadCount(); got != 0 {
		t.Fatalf("a denied join loaded the document %d time(s); an actor with no read access must not be able to make the service fetch and decode it", got)
	}
	if got := mgr.RoomCount(); got != 0 {
		t.Fatalf("a denied join left %d live room(s); each one holds a document in memory, a fan-out subscription, and a registry slot", got)
	}
	if residentInRegistry(t, mgr.registry, doc) {
		t.Fatal("a denied join left the document resident in the registry")
	}
	// It stopped at READ: there is nothing to decide about writing once reading is
	// refused, and asking would be a second call to a backend for no purpose.
	if seen := authz.seen(); len(seen) != 1 || seen[0] != model.PrivilegeRead {
		t.Fatalf("evaluations = %v, want exactly one READ", seen)
	}
}

// TestAnAuthZOutageFailsClosedWithoutMaterializing asserts a backend error is a
// REFUSAL that is not a denial: nothing is materialized, and the error is not
// ErrForbidden, so the handshake closes with a transient status and the client
// keeps retrying instead of treating an outage as "you may not have this".
func TestAnAuthZOutageFailsClosedWithoutMaterializing(t *testing.T) {
	const doc model.DocumentID = "outage-doc"
	boom := errors.New("authz backend unreachable")
	authz := &scriptedAuthZ{decide: func(model.Privilege, int) (model.AuthDecision, error) {
		return model.AuthDecision{}, boom
	}}
	mgr, store := admissionManager(t, authz, doc)

	client := newFakeClient(t)
	_, _, err := mgr.Join(context.Background(), JoinRequest{
		ID: doc, Content: model.ContentTypeMemo,
		Identity: testIdentity("someone"), Conn: client,
	})

	if err == nil {
		t.Fatal("join succeeded while the authorization backend was unreachable")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("join = %v, want it to wrap the backend error", err)
	}
	if errors.Is(err, ErrForbidden) {
		t.Fatal("a backend outage was reported as a denial; the client would stop retrying a transient failure")
	}
	if got := store.loadCount(); got != 0 {
		t.Fatalf("an unresolved authorization loaded the document %d time(s)", got)
	}
	if got := mgr.RoomCount(); got != 0 {
		t.Fatalf("an unresolved authorization left %d live room(s)", got)
	}
}

// TestASessionCostsExactlyTwoEvaluations pins the shape of the contract: both
// privileges are decided ONCE, at establishment, and nothing re-evaluates while
// the session runs.
//
// The count is the assertion. One evaluation would mean write capability was
// guessed; more than two, or any growth while traffic flows, would mean the
// service was re-authorizing per frame — which is a different contract with a
// different cost, and not the one chosen.
func TestASessionCostsExactlyTwoEvaluations(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write bool
		want  model.CollaboratorMode
	}{
		{"collaborator", true, model.ModeCollaborator},
		{"viewer", false, model.ModeViewer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := model.DocumentID("session-" + tc.name)
			authz := &scriptedAuthZ{decide: decideBy(true, tc.write)}
			mgr, _ := admissionManager(t, authz, doc)

			client := newFakeClient(t)
			client.join(mgr, doc, model.ContentTypeMemo)
			client.observeUpdates()

			seen := authz.seen()
			if len(seen) != 2 || seen[0] != model.PrivilegeRead || seen[1] != model.PrivilegeUpdateContent {
				t.Fatalf("establishment evaluations = %v, want exactly [read, update-content]", seen)
			}

			// Traffic must not cost further evaluations.
			client.insertText("hello ")
			waitFor(t, "the edit to be applied", func() bool { return mgr.RoomCount() == 1 })
			if got := authz.seen(); len(got) != 2 {
				t.Fatalf("evaluations grew to %v during document traffic; authorization is established per session, not per frame", got)
			}

			// The capability is asserted where the client can see it: a viewer is
			// told it is read-only up front, a collaborator is not.
			readOnly := hasControlKind(client, model.ControlReadOnlyState)
			if wantReadOnly := tc.want == model.ModeViewer; readOnly != wantReadOnly {
				t.Fatalf("client saw read-only=%v, want %v for a %s session", readOnly, wantReadOnly, tc.want)
			}
		})
	}
}

// TestASessionFailsClosedOnEitherEvaluation asserts an authorization backend that
// errors on EITHER privilege refuses the session rather than degrading it.
//
// The update-content half is the one worth stating: read succeeded, so the caller
// is entitled to the document, and the tempting failure mode is to admit them as
// a viewer and carry on. That silently converts an infrastructure outage into a
// permission decision — a collaborator would find their document read-only with
// no explanation and no error, and the client would not retry because nothing
// looked wrong. Both halves fail closed, and neither is ErrForbidden, so the
// handshake closes transiently.
func TestASessionFailsClosedOnEitherEvaluation(t *testing.T) {
	boom := errors.New("evaluation failed")
	for _, tc := range []struct {
		name string
		fail model.Privilege
	}{
		{"read evaluation errors", model.PrivilegeRead},
		{"update-content evaluation errors", model.PrivilegeUpdateContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := model.DocumentID("failclosed-" + string(tc.fail))
			authz := &scriptedAuthZ{decide: func(p model.Privilege, _ int) (model.AuthDecision, error) {
				if p == tc.fail {
					return model.AuthDecision{}, boom
				}
				return model.AuthDecision{Allowed: true}, nil
			}}
			mgr, store := admissionManager(t, authz, doc)

			client := newFakeClient(t)
			_, _, err := mgr.Join(context.Background(), JoinRequest{
				ID: doc, Content: model.ContentTypeMemo,
				Identity: testIdentity("a"), Conn: client,
			})
			if err == nil {
				t.Fatal("the session was established despite an unresolved authorization")
			}
			if errors.Is(err, ErrForbidden) {
				t.Fatal("an evaluation failure was reported as a denial; the client stops retrying a transient outage")
			}
			if got := store.loadCount(); got != 0 {
				t.Fatalf("an unresolved authorization loaded the document %d time(s)", got)
			}
			if got := mgr.RoomCount(); got != 0 {
				t.Fatalf("an unresolved authorization left %d live room(s)", got)
			}
		})
	}
}
