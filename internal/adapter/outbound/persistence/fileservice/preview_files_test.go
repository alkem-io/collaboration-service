package fileservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

//nolint:gocyclo // One fixture pins the four outbound calls and refuses any fallback call.
func TestPreviewFilesUseScopedExistingSlotsAndHashReads(t *testing.T) {
	bucket := uuid.New()
	id := uuid.New()
	assetID := uuid.New()
	hash := strings.Repeat("1", 64)
	const ref = "collab-preview-v1:test:card"
	var writes atomic.Int32
	png := []byte("\x89PNG\r\n\x1a\nrender payload")
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /internal/file/by-reference":
			if r.URL.Query().Get("ref") != ref || r.URL.Query().Get("bucketId") != bucket.String() {
				t.Error("preview slot lookup not scoped by ref and bucket")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(w, `{"id":%q,"storageBucketId":%q,"externalReference":%q,"mimeType":"image/png"}`, id, bucket, ref)
		case "GET /internal/file/" + assetID.String() + "/meta":
			_, _ = fmt.Fprintf(w, `{"id":%q,"storageBucketId":%q,"externalID":%q,"mimeType":"image/svg+xml","authorizationId":%q}`,
				assetID, bucket, hash, uuid.New())
		case "GET /internal/blob/" + hash + "/content":
			_, _ = io.WriteString(w, "<svg/>")
		case "PUT /internal/file/" + id.String() + "/content":
			writes.Add(1)
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, png) || r.Header.Get("Content-Type") != "image/png" {
				t.Error("replacement was not a raw PNG")
			}
		default:
			t.Errorf("unexpected request (no create or fallback allowed): %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer host.Close()
	files, err := NewPreviewFiles(Config{BaseURL: host.URL})
	if err != nil {
		t.Fatal(err)
	}
	slot, err := files.LookupPreviewSlot(t.Context(), bucket, ref)
	if err != nil || slot.ID != id || slot.ExternalReference != ref || slot.BucketID != bucket || slot.AuthorizationID != uuid.Nil {
		t.Fatalf("slot metadata = %+v, err=%v", slot, err)
	}
	asset, err := files.LookupPreviewAsset(t.Context(), assetID)
	if err != nil || asset.ID != assetID || asset.ContentHash != hash || asset.AuthorizationID == uuid.Nil {
		t.Fatalf("asset metadata = %+v, err=%v", asset, err)
	}
	blob, err := files.ReadPreviewAsset(t.Context(), asset.ContentHash, 12)
	if err != nil || string(blob) != "<svg/>" {
		t.Fatalf("asset bytes = %q, err=%v", blob, err)
	}
	if err := files.ReplacePreview(t.Context(), slot.ID, "image/png", png); err != nil || writes.Load() != 1 {
		t.Fatalf("write count = %d, err=%v", writes.Load(), err)
	}
}

func TestPreviewFilesRefuseRedirectOverflowAndReplacementFailures(t *testing.T) {
	id := uuid.New()
	bucket := uuid.New()
	var redirects atomic.Int32
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/file/by-reference":
			w.WriteHeader(http.StatusNotFound)
		case "/internal/file/" + id.String() + "/content":
			w.WriteHeader(http.StatusConflict)
		case "/internal/blob/" + strings.Repeat("2", 64) + "/content":
			http.Redirect(w, r, "/must-not-follow", http.StatusFound)
		case "/internal/blob/" + strings.Repeat("3", 64) + "/content":
			_, _ = io.WriteString(w, "123456789")
		default:
			redirects.Add(1)
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer host.Close()
	files, err := NewPreviewFiles(Config{BaseURL: host.URL, HTTPClient: host.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.LookupPreviewSlot(t.Context(), bucket, "missing"); !errors.Is(err, port.ErrPreviewFileMissing) {
		t.Fatalf("missing slot disposition: %v", err)
	}
	if err := files.ReplacePreview(t.Context(), id, "image/png", []byte("png")); !errors.Is(err, port.ErrPreviewFileConflict) {
		t.Fatalf("conflicting replacement disposition: %v", err)
	}
	for _, hash := range []string{strings.Repeat("2", 64), strings.Repeat("3", 64), "invalid/hash"} {
		if raw, err := files.ReadPreviewAsset(t.Context(), hash, 8); err == nil || len(raw) > 0 {
			t.Fatalf("asset read must not return bytes for invalid result: %q, %v", raw, err)
		}
	}
	if redirects.Load() != 0 {
		t.Fatalf("recorded %d unexpected calls", redirects.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := files.LookupPreviewAsset(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("asset context not carried to transport: %v", err)
	}
}

func TestInvalidFileServiceRepliesAreNeverMistakenForAnAbsentSlot(t *testing.T) {
	for _, tc := range []struct {
		code     int
		response string
	}{
		{http.StatusOK, "malformed response"},
		{http.StatusCreated, `{"id":"00000000-0000-0000-0000-000000000000"}`},
		{http.StatusBadGateway, "transient failure"},
		{http.StatusForbidden, "forbidden"},
	} {
		t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected write after lookup failure: %s", r.Method)
				}
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.response)
			}))
			defer host.Close()
			files, err := NewPreviewFiles(Config{BaseURL: host.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := files.LookupPreviewSlot(t.Context(), uuid.New(), "fixture")
			if err == nil || errors.Is(err, port.ErrPreviewFileMissing) || got.ID != uuid.Nil {
				t.Fatalf("untrusted response is not a genuine missing row: %+v, %v", got, err)
			}
		})
	}
}

func TestPreviewCreationRejectsUnconfirmedOrTruncatedWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
		len  string
		want error
	}{
		{"missing created status", http.StatusOK, `{"id":"` + uuid.NewString() + `"}`, "", nil},
		{"missing created ID", http.StatusCreated, `{"reused":false}`, "", nil},
		{"truncated reply", http.StatusCreated, `{"id":"`, "150", io.ErrUnexpectedEOF},
		{"backend conflict", http.StatusConflict, "", "", port.ErrPreviewFileConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/internal/file" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				if tc.len != "" {
					w.Header().Set("Content-Length", tc.len)
				}
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer host.Close()
			files, err := NewPreviewFiles(Config{BaseURL: host.URL})
			if err != nil {
				t.Fatal(err)
			}
			created, err := files.CreatePreview(t.Context(), uuid.New(), "preview", "image/png", []byte("png"))
			if created || err == nil || errors.Is(err, port.ErrPreviewFileMissing) || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("unconfirmed write reported a creation or wrong disposition: %v %v", created, err)
			}
		})
	}
}

func TestPreviewFileOperationsRejectInvalidFileIdentitiesBeforeIO(t *testing.T) {
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("local validation allowed an invalid file identity or output: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer host.Close()
	for _, base := range []string{"", "file:///local", host.URL + "?injected=query", host.URL + "#fragment"} {
		if files, err := NewPreviewFiles(Config{BaseURL: base}); files != nil || err == nil {
			t.Fatalf("accepted an invalid file-service base: %q", base)
		}
	}
	files, err := NewPreviewFiles(Config{BaseURL: host.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if _, err := files.LookupPreviewAsset(ctx, uuid.Nil); err == nil {
		t.Fatal("accepted nil asset")
	}
	if _, err := files.LookupPreviewSlot(ctx, uuid.Nil, "preview"); err == nil {
		t.Fatal("accepted bucketless slot")
	}
	if err := files.ReplacePreview(ctx, uuid.New(), "image/png", nil); err == nil {
		t.Fatal("accepted a zero-byte replacement")
	}
	if _, err := files.ReadPreviewAsset(ctx, strings.Repeat("1", 64), 0); err == nil {
		t.Fatal("accepted an unbounded asset read")
	}
}

//nolint:gocyclo // Verify one multipart contract and two dispositions against the same capture server.
func TestPreviewFileProvisionUsesPrivateScopedIdentity(t *testing.T) {
	bucket := uuid.New()
	const ref = "collab-preview-v1:fixture:markdown"
	const body = "# Saved memo"
	var requests atomic.Int32
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/internal/file" {
			t.Errorf("unexpected bootstrap follow-up or fallback: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		reader, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		form, err := reader.ReadForm(1 << 16)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer func() { _ = form.RemoveAll() }()
		for key, want := range map[string]string{
			"displayName": "collaboration-preview.md", "storageBucketId": bucket.String(),
			"externalReference": ref, "skipDedup": "true", "skipImageProcessing": "true",
			"authorizationId": "", "temporaryLocation": "", "allowedMimeTypes": "text/plain",
		} {
			if got := strings.Join(form.Value[key], ""); got != want {
				t.Errorf("metadata %s = %q, want %q", key, got, want)
			}
		}
		hdrs := form.File["file"]
		if len(hdrs) != 1 {
			t.Error("expected one multipart file")
			return
		}
		hdr := hdrs[0]
		file, err := hdr.Open()
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		content, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil || string(content) != body || hdr.Filename != "collaboration-preview.md" {
			t.Errorf("invalid multipart data: %q %v %v", content, hdr, err)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"id":%q,"reused":%t}`, uuid.New(), requests.Add(1) > 1)
	}))
	defer host.Close()
	files, err := NewPreviewFiles(Config{BaseURL: host.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, expectCreated := range []bool{true, false} {
		created, err := files.CreatePreview(t.Context(), bucket, ref, "text/plain", []byte(body))
		if err != nil || created != expectCreated {
			t.Fatalf("created=%v error=%v, want %v", created, err, expectCreated)
		}
	}
	if created, err := files.CreatePreview(t.Context(), bucket, ref, "text/html", []byte(body)); created || err == nil || requests.Load() != 2 {
		t.Fatalf("unsafe format attempted bootstrap (created=%v err=%v requests=%v)", created, err, requests.Load())
	}
}
