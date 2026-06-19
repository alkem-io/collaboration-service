//go:build e2e

package e2e

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// stubFileService is a faithful in-process stand-in for file-service's
// /internal/file API (the contract the fileservice blob adapter targets, OPEN-2):
// POST stores the multipart `file` bytes and returns an assigned UUID + size;
// GET /{id}/content serves them; DELETE removes them. It records how many blobs
// it currently holds so the e2e can prove the snapshot was actually offloaded to
// file-service (not kept inline).
type stubFileService struct {
	mu     sync.Mutex
	byID   map[string][]byte
	nextID int
	puts   int
}

func newStubFileService() *stubFileService {
	return &stubFileService{byID: map[string][]byte{}}
}

func (s *stubFileService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/file", s.create)
	mux.HandleFunc("GET /internal/file/{id}/content", s.content)
	mux.HandleFunc("DELETE /internal/file/{id}", s.delete)
	return mux
}

type fsCreateResponse struct {
	ID         string `json:"id"`
	ExternalID string `json:"externalID"`
	Size       int64  `json:"size"`
	Reused     bool   `json:"reused"`
}

func (s *stubFileService) create(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "bad multipart", http.StatusBadRequest)
		return
	}
	var fileBytes []byte
	fields := map[string]string{}
	for {
		part, perr := mr.NextPart()
		if errors.Is(perr, io.EOF) {
			break
		}
		if perr != nil {
			http.Error(w, "read part", http.StatusBadRequest)
			return
		}
		b, rerr := io.ReadAll(part)
		name := part.FormName()
		_ = part.Close()
		if rerr != nil {
			http.Error(w, "read part body", http.StatusBadRequest)
			return
		}
		if name == "file" {
			fileBytes = b
		} else {
			fields[name] = string(b)
		}
	}
	if fields["storageBucketId"] == "" || fields["authorizationId"] == "" {
		http.Error(w, "missing required field", http.StatusBadRequest)
		return
	}
	if fileBytes == nil {
		http.Error(w, "missing file part", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := idFromInt(s.nextID)
	s.byID[id] = append([]byte(nil), fileBytes...)
	s.puts++
	writeJSON(w, fsCreateResponse{ID: id, ExternalID: "ext-" + id, Size: int64(len(fileBytes))})
}

func (s *stubFileService) content(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	data, ok := s.byID[id]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
}

func (s *stubFileService) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	_, ok := s.byID[id]
	delete(s.byID, id)
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted"})
}

func (s *stubFileService) blobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func idFromInt(n int) string {
	const hex = "0123456789abcdef"
	suffix := make([]byte, 12)
	for i := 11; i >= 0; i-- {
		suffix[i] = hex[n%16]
		n /= 16
	}
	return "00000000-0000-0000-0000-" + string(suffix)
}

// fileServiceConfig is the standalone config with the snapshot blob offloaded to
// a file-service at baseURL (BLOB_STORE=file-service). The metadata index stays
// in-process, so the persisted metadata row carries only a content pointer — the
// blob bytes live in file-service (SC-006/SC-012).
func fileServiceConfig(baseURL string) *config.Config {
	cfg := standaloneConfig()
	cfg.BlobStore = config.BlobStoreFileService
	cfg.FileService = config.FileServiceConfig{
		BaseURL:         baseURL,
		StorageBucketID: "11111111-1111-1111-1111-111111111111",
		AuthorizationID: "22222222-2222-2222-2222-222222222222",
		MaxUploadSize:   32 << 20,
	}
	return cfg
}

// TestFileServiceBlobOffloadRoundTrip persists a memo with BLOB_STORE=file-service
// (blob offloaded, metadata index inline), releases the room, and reconnects: the
// reloaded document is identical, fetched back from file-service via the content
// pointer (SC-006/SC-012). The stub file-service confirms the snapshot bytes
// actually crossed to it rather than being kept inline.
func TestFileServiceBlobOffloadRoundTrip(t *testing.T) {
	stub := newStubFileService()
	fsSrv := httptest.NewServer(stub.handler())
	t.Cleanup(fsSrv.Close)

	base := testApp(t, fileServiceConfig(fsSrv.URL))

	const docID = "e2e-fileservice-memo"
	a := dial(t, base, docID, "memo")
	time.Sleep(80 * time.Millisecond)
	a.insertMemo("offloaded-to-file-service ")

	// Let the debounce persist the snapshot to file-service, then disconnect so
	// the room idle-releases (a final snapshot save).
	time.Sleep(700 * time.Millisecond)
	if stub.blobCount() == 0 {
		t.Fatal("file-service received no snapshot — blob was not offloaded")
	}
	a.close()
	time.Sleep(200 * time.Millisecond)

	// A fresh client reconnects; the room rehydrates by fetching the snapshot from
	// file-service via the stored content pointer.
	b := dial(t, base, docID, "memo")
	if !eventually(func() bool { return contains(b.memoText(), "offloaded-to-file-service") }) {
		t.Fatalf("reloaded-from-file-service content mismatch: %q", b.memoText())
	}
}
