// Package fileservice is the file-service-backed BlobStore (port.BlobStore): the
// encoded Y.Doc v2 snapshot is offloaded to the existing Alkemio file-service
// via its internal API (OPEN-2 — no file-service expansion for v1):
//
//   - Put    → REWRITE the document's existing file in place via
//     PUT /internal/file/{id}/content ("store-and-link"), keeping the content
//     pointer STABLE. Only when no file exists yet (first save) does it create
//     one with multipart POST /internal/file and adopt the returned UUID.
//   - Get    → GET /internal/file/{id}/content
//   - Delete → DELETE /internal/file/{id}
//
// Each snapshot is uploaded into the DOCUMENT'S OWN storage bucket (the bucketID
// passed to Put, resolved from the collaboration-fetch metadata), so blobs
// co-locate with the document's other media; the configured StorageBucketID is
// only a fallback for standalone / no-metadata uploads. The authorizationId
// field is omitted entirely — a snapshot is an internal infra blob whose access
// is governed by the document's own authz and the (unauthenticated) internal
// API, not a per-file authorization_policy row. Omitting it makes file-service
// write a NULL authz column; file's UNIQUE(authorizationId) permits any number
// of NULLs, so every snapshot persists (reusing one fixed id would collide after
// the first row). Uploads are capped at MaxUploadSize (MAX_UPLOAD_SIZE). The
// internal API has no auth (in-cluster trust); transport is plain HTTP inside the
// cluster mesh.
package fileservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// Config carries the per-deployment file-service settings.
type Config struct {
	// BaseURL is the file-service root, e.g. http://file-service:4003.
	BaseURL string
	// StorageBucketID is the FALLBACK bucket for snapshot uploads when the
	// per-document bucket is unknown (standalone / no-metadata). The normal
	// path uploads into the document's own bucket (the bucketID passed to Put).
	StorageBucketID string
	// MaxUploadSize caps a snapshot upload in bytes (MAX_UPLOAD_SIZE); zero
	// falls back to file-service's own 32 MiB default ceiling.
	MaxUploadSize int64
	// HTTPClient overrides the default client (tests); nil uses a 30s client.
	HTTPClient *http.Client
}

// Store offloads snapshots to file-service.
type Store struct {
	cfg    Config
	client *http.Client
}

// createResponse is the subset of file-service's POST /internal/file response
// the blob store needs: the assigned UUID (the content pointer) and the
// content-addressed externalID (logged for dedup observability).
type createResponse struct {
	ID         string `json:"id"`
	ExternalID string `json:"externalID"`
	Size       int64  `json:"size"`
	Reused     bool   `json:"reused"`
}

// New constructs a file-service blob store, validating the required settings.
// AuthorizationID is intentionally NOT a setting: snapshots are uploaded with no
// authorizationId so file-service writes a NULL authz column (UNIQUE permits
// many NULLs). StorageBucketID is only the fallback bucket; the per-document
// bucket comes from Put.
func New(cfg Config) (*Store, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("file-service blob store: BaseURL is required")
	}
	if cfg.StorageBucketID == "" {
		return nil, fmt.Errorf("file-service blob store: StorageBucketID is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Store{cfg: cfg, client: client}, nil
}

// Put uploads the snapshot as a new file-service object and returns its UUID as the
// content pointer. It does NOT delete the previous snapshot — the caller (room
// persist) deletes the superseded pointer AFTER committing the new one to the
// metadata index (delete-after-commit, 002 FR-002), so a failed commit can never
// strand the row on a deleted blob. prevPointer is retained for the BlobStore
// contract but is no longer used here.
//
// bucketID is the document's own storage bucket (from the metadata index); the
// snapshot is uploaded into it so blobs co-locate with the document. An empty
// bucketID (standalone / no-metadata) falls back to the configured bucket.
func (s *Store) Put(ctx context.Context, prevPointer, bucketID string, data []byte) (string, error) {
	if limit := s.cfg.MaxUploadSize; limit > 0 && int64(len(data)) > limit {
		return "", fmt.Errorf("snapshot %d bytes exceeds MAX_UPLOAD_SIZE %d", len(data), limit)
	}

	// A file id is a STABLE identifier — a filename, not a version. The normal
	// path therefore REWRITES the document's existing file, leaving the content
	// pointer unchanged; file-service swaps the underlying blob (and its
	// content-hash externalID) behind that stable id, which is its business, not
	// ours.
	//
	// prevPointer is the caller's current pointer, which on the FIRST save is the
	// document id used as a hint rather than a real file id (see port.BlobStore).
	// Rather than have the adapter guess which it is, try the rewrite and treat a
	// 404 as "no file yet, create one". That also self-heals a pointer whose file
	// was removed out of band.
	if prevPointer != "" {
		id, err := s.replace(ctx, prevPointer, data)
		switch {
		case err == nil:
			return id, nil
		case errors.Is(err, model.ErrNotFound):
			// fall through to create
		default:
			return "", err
		}
	}

	bucket := bucketID
	if bucket == "" {
		bucket = s.cfg.StorageBucketID
	}
	return s.upload(ctx, bucket, data)
}

// replace rewrites an existing file's content in place, returning the unchanged
// pointer. It reports model.ErrNotFound when no such file exists so Put can
// create one instead.
func (s *Store) replace(ctx context.Context, pointer string, data []byte) (string, error) {
	// PathEscape the pointer (see Get): a path-significant byte must not
	// re-target the write at a different file-service object.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer)+"/content", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("file-service replace: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return pointer, nil
	case http.StatusNotFound:
		return "", model.ErrNotFound
	case http.StatusConflict:
		// file-service deduplicates on content hash within a bucket
		// (unique(externalID, storageBucketID)), so an identical snapshot already
		// stored under a DIFFERENT file in this bucket is refused. This is a
		// permanent condition for these bytes, not a transient fault, so it must
		// not be retried as one.
		return "", fmt.Errorf("file-service replace: content already stored under another file in this bucket (dedup conflict): %s", readErrBody(resp.Body))
	default:
		return "", fmt.Errorf("file-service replace: unexpected status %d: %s", resp.StatusCode, readErrBody(resp.Body))
	}
}

func (s *Store) upload(ctx context.Context, bucket string, data []byte) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	part, err := mw.CreateFormFile("file", "snapshot.ybin")
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return "", fmt.Errorf("write snapshot part: %w", err)
	}
	for name, value := range map[string]string{
		"displayName":     "collaboration-snapshot",
		"storageBucketId": bucket,
	} {
		if err := mw.WriteField(name, value); err != nil {
			return "", fmt.Errorf("write field %s: %w", name, err)
		}
	}
	if err := mw.Close(); err != nil {
		return "", fmt.Errorf("close multipart: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.BaseURL+"/internal/file", &body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("file-service upload: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("file-service upload: unexpected status %d: %s", resp.StatusCode, readErrBody(resp.Body))
	}

	var cr createResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&cr); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}
	if cr.ID == "" {
		return "", fmt.Errorf("file-service upload returned an empty id")
	}
	return cr.ID, nil
}

// Get fetches the snapshot bytes for a file-service object id, mapping a 404 to
// model.ErrNotFound.
func (s *Store) Get(ctx context.Context, pointer string) ([]byte, error) {
	// PathEscape the pointer: it is normally a file-service UUID, but escaping it
	// keeps a pointer that ever carried a path-significant byte (/ ? #) from
	// re-targeting the request to a different resource (defense in depth).
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer)+"/content", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("file-service get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, model.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("file-service get: unexpected status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read snapshot body: %w", err)
	}
	return data, nil
}

// Delete removes a file-service object. A 404 is a no-op (idempotent cascade).
func (s *Store) Delete(ctx context.Context, pointer string) error {
	// PathEscape the pointer (see Get): keep a path-significant byte in the pointer
	// from re-targeting the DELETE at an unintended file-service object.
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("file-service delete: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("file-service delete: unexpected status %d", resp.StatusCode)
}

// readErrBody reads a bounded error body for diagnostics.
func readErrBody(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 2<<10))
	return strings.TrimSpace(string(b))
}

// compile-time assertion that Store satisfies the port.
var _ port.BlobStore = (*Store)(nil)
