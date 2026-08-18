package ws

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	authopen "github.com/alkem-io/collaboration-service/internal/adapter/outbound/auth/open"
	metainmem "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metastore/inmemory"
	persistinprocess "github.com/alkem-io/collaboration-service/internal/adapter/outbound/persistence/inprocess"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// TestExplicitlyUnknownContentTypeIsRejectedBeforeTheUpgrade covers the 400 arm,
// which is a correctness gate rather than input hygiene.
//
// An ABSENT ?type= defaults to memo; an EXPLICIT but unknown one is a 400. The
// asymmetry is deliberate: silently defaulting an unknown type would, for a
// brand-new document with no snapshot, seed the WRONG convention root — a memo's
// Y.XmlFragment where the whiteboard roots belong. That root is then durable, and
// the two creation paths (this handshake and the REST create, which already 400s)
// would disagree about what the document is.
//
// Rejecting BEFORE the upgrade matters too: after the upgrade the only way to
// report the problem is a close frame, which clients retry.
func TestExplicitlyUnknownContentTypeIsRejectedBeforeTheUpgrade(t *testing.T) {
	mgr := service.NewManager(service.Deps{
		Metadata:   metainmem.New(),
		Checkpoint: persistinprocess.New(),
		Auth:       authopen.New(),
		AuthZ:      authopen.New(),
	}, service.RoomConfig{SendBuffer: 16}, nil, zap.NewNop())

	h := &Handler{Auth: authopen.New(), Manager: mgr, Logger: zap.NewNop()}
	r := chi.NewRouter()
	r.Method(http.MethodGet, "/collab/{documentId}", h)

	req := httptest.NewRequest(http.MethodGet, "/collab/doc-1?type=spreadsheet", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; an explicit unknown ?type= must be refused before the upgrade, or a new document is seeded with the wrong convention root", rec.Code)
	}
}

// TestAbsentContentTypeDefaultsToMemo is the other half of the asymmetry, so the
// test above cannot pass against a handler that rejects everything.
func TestAbsentContentTypeDefaultsToMemo(t *testing.T) {
	got, err := contentTypeFromRequest(httptest.NewRequest(http.MethodGet, "/collab/doc-1", nil))
	if err != nil {
		t.Fatalf("an absent ?type= must default, not error: %v", err)
	}
	if got != "memo" {
		t.Fatalf("default content type = %q, want memo", got)
	}
}
