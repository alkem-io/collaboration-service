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

	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

type previewFiles struct {
	base   string
	client *http.Client
}

// NewPreviewFiles constructs a private derivative adapter that never follows redirects.
func NewPreviewFiles(cfg Config) (port.PreviewFiles, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("preview file-service needs a trusted HTTP base URL")
	}
	client := http.Client{Timeout: 5 * time.Second}
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &previewFiles{base: base, client: &client}, nil
}

// LookupPreviewAsset reads the provenance associated with an asset UUID.
func (f *previewFiles) LookupPreviewAsset(ctx context.Context, id uuid.UUID) (port.PreviewFile, error) {
	if id == uuid.Nil {
		return port.PreviewFile{}, errors.New("asset ID is required")
	}
	return f.metadata(ctx, "/internal/file/"+id.String()+"/meta")
}

// LookupPreviewSlot scopes the existing reference lookup to the board's bucket.
func (f *previewFiles) LookupPreviewSlot(ctx context.Context, bucket uuid.UUID, reference string) (port.PreviewFile, error) {
	if bucket == uuid.Nil || reference == "" {
		return port.PreviewFile{}, errors.New("preview bucket and reference are required")
	}
	query := url.Values{"ref": {reference}, "bucketId": {bucket.String()}}
	return f.metadata(ctx, "/internal/file/by-reference?"+query.Encode())
}

func (f *previewFiles) metadata(ctx context.Context, path string) (port.PreviewFile, error) {
	raw, err := f.exchange(ctx, http.MethodGet, path, "", nil, 16<<10)
	if err != nil {
		return port.PreviewFile{}, err
	}
	var file port.PreviewFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return port.PreviewFile{}, errors.New("invalid preview file metadata")
	}
	return file, nil
}

// ReadPreviewAsset reads bounded immutable bytes through the internal blob route.
func (f *previewFiles) ReadPreviewAsset(ctx context.Context, hash string, maxBytes int64) ([]byte, error) {
	if len(hash) != 64 || strings.IndexFunc(hash, func(r rune) bool {
		return (r < '0' || r > '9') && (r < 'a' || r > 'f')
	}) >= 0 || maxBytes <= 0 || maxBytes > 32<<20 {
		return nil, errors.New("invalid asset hash or read bound")
	}
	return f.exchange(ctx, http.MethodGet, "/internal/blob/"+hash+"/content", "", nil, maxBytes)
}

// ReplacePreview changes bytes behind the ID without allocating a new row.
func (f *previewFiles) ReplacePreview(ctx context.Context, id uuid.UUID, mediaType string, body []byte) error {
	if id == uuid.Nil || previewFilename(mediaType) == "" || len(body) == 0 || len(body) > 8<<20 {
		return errors.New("invalid preview ID or output size")
	}
	_, err := f.exchange(ctx, http.MethodPut, "/internal/file/"+id.String()+"/content", mediaType, body, 0)
	return err
}

// CreatePreview uses the file-service's bucket+reference identity. It does
// not set authorizationId, request content deduplication or relink a visual.
func (f *previewFiles) CreatePreview(ctx context.Context, bucket uuid.UUID, reference, mediaType string, body []byte) (bool, error) {
	name := previewFilename(mediaType)
	if bucket == uuid.Nil || reference == "" || name == "" || len(body) == 0 || len(body) > 8<<20 {
		return false, errors.New("invalid preview bucket, reference or output size")
	}
	var data bytes.Buffer
	form := multipart.NewWriter(&data)
	for key, value := range map[string]string{
		"storageBucketId": bucket.String(), "externalReference": reference, "displayName": name,
		"skipDedup": "true", "skipImageProcessing": "true",
		"allowedMimeTypes": mediaType,
	} {
		if err := form.WriteField(key, value); err != nil {
			return false, err
		}
	}
	part, err := form.CreateFormFile("file", name)
	if err != nil {
		return false, err
	}
	if _, err := part.Write(body); err != nil {
		return false, err
	}
	if err := form.Close(); err != nil {
		return false, err
	}
	raw, err := f.exchange(ctx, http.MethodPost, "/internal/file", form.FormDataContentType(), data.Bytes(), 16<<10)
	if err != nil {
		return false, err
	}
	var reply struct {
		ID     uuid.UUID `json:"id"`
		Reused bool      `json:"reused"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil || reply.ID == uuid.Nil {
		return false, errors.New("invalid created preview metadata")
	}
	return !reply.Reused, nil
}

func previewFilename(mediaType string) string {
	switch mediaType {
	case "image/png":
		return "collaboration-preview.png"
	case "text/plain":
		return "collaboration-preview.md"
	default:
		return ""
	}
}

func (f *previewFiles) exchange(ctx context.Context, method, path, contentType string, data []byte, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, f.base+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	switch res.StatusCode {
	case http.StatusCreated:
		if method != http.MethodPost {
			return nil, errors.New("unexpected create status for preview read or replacement")
		}
	case http.StatusOK, http.StatusNoContent:
		if method == http.MethodPost {
			return nil, errors.New("missing created-preview response")
		}
	case http.StatusNotFound:
		return nil, port.ErrPreviewFileMissing
	case http.StatusConflict:
		return nil, port.ErrPreviewFileConflict
	default:
		return nil, fmt.Errorf("preview file-service status %d", res.StatusCode)
	}
	if limit == 0 {
		return nil, nil
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, errors.New("preview response exceeded byte bound")
	}
	return raw, nil
}

var _ port.PreviewFiles = (*previewFiles)(nil)
