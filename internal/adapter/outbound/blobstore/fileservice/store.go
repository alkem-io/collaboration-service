// Package fileservice is the file-service-backed BlobStore (port.BlobStore): the
// encoded Y.Doc v2 snapshot is offloaded to the existing Alkemio file-service
// via its internal API (OPEN-2 — no file-service expansion for v1):
//
//   - Put    → multipart POST /internal/file (the returned UUID is the content
//     pointer); the previous snapshot's file is deleted so old versions do not
//     accumulate (latest-only, R7).
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
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
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

// Put uploads the snapshot as a new file-service object and returns its UUID as
// the content pointer. prevPointer is the document's previous pointer (a
// file-service UUID for a re-save, or the document id on the first save): when it
// is a real previous UUID, its file is deleted after a successful upload so old
// snapshots do not accumulate (latest-only). The document id on first save does
// not match any file-service object, so the delete is harmlessly skipped.
//
// bucketID is the document's own storage bucket (from the metadata index); the
// snapshot is uploaded into it so blobs co-locate with the document. An empty
// bucketID (standalone / no-metadata) falls back to the configured bucket.
func (s *Store) Put(ctx context.Context, prevPointer, bucketID string, data []byte) (string, error) {
	if limit := s.cfg.MaxUploadSize; limit > 0 && int64(len(data)) > limit {
		return "", fmt.Errorf("snapshot %d bytes exceeds MAX_UPLOAD_SIZE %d", len(data), limit)
	}

	bucket := bucketID
	if bucket == "" {
		bucket = s.cfg.StorageBucketID
	}

	id, err := s.upload(ctx, bucket, data)
	if err != nil {
		return "", err
	}

	// Drop the superseded snapshot (best-effort: a failed cleanup must not fail
	// the save — the new snapshot is already durable and recorded, and the orphan
	// is reclaimable).
	if prevPointer != "" && prevPointer != id {
		_ = s.Delete(ctx, prevPointer)
	}
	return id, nil
}

// upload performs the multipart POST into bucket and returns the assigned
// file-service UUID. authorizationId is deliberately omitted: file-service then
// stores a NULL authz column, which UNIQUE(authorizationId) permits any number
// of — so every snapshot persists.
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.BaseURL+"/internal/file/"+pointer+"/content", nil)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.cfg.BaseURL+"/internal/file/"+pointer, nil)
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
