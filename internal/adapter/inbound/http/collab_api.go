package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// CollabLifecycle is the slice of the domain Manager the standalone create API
// drives (T016). The concrete service.Manager satisfies it.
//
// There is no delete counterpart. `document.deleted` now only closes and evicts a
// live room — `server` deletes the durable state before publishing — so a
// standalone DELETE would have reported success while removing nothing. An
// endpoint that misreports what it did is worse than no endpoint (§II), and the
// standalone REST surface is accepted for removal regardless.
type CollabLifecycle interface {
	// PreRegister writes an initial metadata row ahead of first connect.
	PreRegister(ctx context.Context, meta model.Metadata) error
}

// CollabAPIHandler serves the standalone document create REST API for the no-bus
// deployment (FR-020). In Alkemio mode `server` owns document creation; this HTTP
// surface is the standalone equivalent.
type CollabAPIHandler struct {
	// Lifecycle is the domain manager the handlers drive.
	Lifecycle CollabLifecycle
	// RequireAuthorizationPolicy rejects a create with an empty authorizationPolicyId
	// (set true in authzeval mode). Without it an empty policy id would register a
	// document that looks valid but fails EVERY later authorization evaluation
	// (the authzeval adapter fails closed on an empty policy), so the document is
	// silently unreachable. In open/standalone mode authZ grants everything, so the
	// policy id is genuinely optional and this stays false.
	RequireAuthorizationPolicy bool
}

// maxCreateBody bounds the create request body (it is a tiny JSON document).
const maxCreateBody = 4 << 10

// CreateDocumentRequest is the POST /collab/{documentId} body: the document's
// content type and optional lifecycle owner. An absent content type defaults to
// memo.
type CreateDocumentRequest struct {
	// ContentType selects the document convention (memo | whiteboard). Optional —
	// an absent/empty value defaults to memo.
	ContentType string `json:"contentType,omitempty"`
	// OwnerRef is the parent Alkemio entity that owns the document's lifecycle.
	// Optional.
	OwnerRef string `json:"ownerRef,omitempty"`
	// AuthorizationPolicyID is the Alkemio authorization policy the per-document
	// authZ adapter evaluates against (OPEN-1). Optional in open/standalone mode
	// (authZ grants everything); in authzeval mode it is required for the policy
	// resolver to evaluate read/update-content. The bus pre-register
	// (collaboration-save) carries the same field, so the standalone REST path
	// keeps parity with it.
	AuthorizationPolicyID string `json:"authorizationPolicyId,omitempty"`
}

// CreateDocumentResponse is returned by POST /collab/{documentId}: the registered
// id and its resolved content type.
type CreateDocumentResponse struct {
	ID          string `json:"id"`
	ContentType string `json:"contentType"`
}

// Render writes the response as JSON with HTTP 201 (a document was registered).
func (r CreateDocumentResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(r)
}

// ErrorResponse is the JSON error body for the collaboration API (a named struct,
// not map[string]any, so the OpenAPI spec stays generatable — anti-pattern 11).
type ErrorResponse struct {
	Error string `json:"error"`
}

// Render writes the error as JSON with the given status code.
func (r ErrorResponse) Render(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(r)
}

// Create pre-registers a document for the standalone (no-bus) deployment: it reads
// the content type from the body (defaulting to memo), writes the metadata row via
// the Manager, and returns 201. The room itself still materializes lazily on first
// WebSocket connect — this only seeds the index ahead of time.
//
// @Summary     Create (pre-register) a collaboration document
// @Description Pre-registers a document's metadata for the standalone (no-bus)
// @Description deployment so it exists in the index ahead of its first connect.
// @Tags        collaboration
// @Accept      json
// @Produce     json
// @Param       documentId path string true "Document id"
// @Param       body body CreateDocumentRequest true "Document content type + owner"
// @Success     201 {object} CreateDocumentResponse
// @Failure     400 {object} ErrorResponse
// @Failure     500 {object} ErrorResponse
// @Router      /collab/{documentId} [post]
func (h *CollabAPIHandler) Create(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "documentId")
	if id == "" {
		ErrorResponse{Error: "missing document id"}.Render(w, http.StatusBadRequest)
		return
	}

	var req CreateDocumentRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, maxCreateBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		ErrorResponse{Error: "malformed request body"}.Render(w, http.StatusBadRequest)
		return
	}

	content, err := resolveContentType(req.ContentType)
	if err != nil {
		ErrorResponse{Error: err.Error()}.Render(w, http.StatusBadRequest)
		return
	}

	// In authzeval mode an empty authorizationPolicyId registers a document that
	// fails every later authorization evaluation (the authzeval adapter fails
	// closed on an empty policy id), making it silently unreachable. Reject it up
	// front rather than persisting an unusable document.
	if h.RequireAuthorizationPolicy && req.AuthorizationPolicyID == "" {
		ErrorResponse{Error: "authorizationPolicyId is required in authzeval mode"}.Render(w, http.StatusBadRequest)
		return
	}

	meta := model.Metadata{
		ID:                    model.DocumentID(id),
		ContentType:           content,
		OwnerRef:              req.OwnerRef,
		AuthorizationPolicyID: req.AuthorizationPolicyID,
	}
	if err := h.Lifecycle.PreRegister(r.Context(), meta); err != nil {
		ErrorResponse{Error: "failed to register document"}.Render(w, http.StatusInternalServerError)
		return
	}

	CreateDocumentResponse{ID: id, ContentType: string(content)}.Render(w)
}

// resolveContentType maps the request's content type to a domain ContentType,
// defaulting an empty value to memo and rejecting an unknown explicit value.
func resolveContentType(v string) (model.ContentType, error) {
	switch model.ContentType(v) {
	case "":
		return model.ContentTypeMemo, nil
	case model.ContentTypeMemo:
		return model.ContentTypeMemo, nil
	case model.ContentTypeWhiteboard:
		return model.ContentTypeWhiteboard, nil
	default:
		return "", errors.New("contentType must be one of memo, whiteboard")
	}
}
