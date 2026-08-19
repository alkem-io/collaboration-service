// Package fileservice is the file-service-backed persistence.CheckpointStore:
// one document, one stable file, rewritten in place.
//
// Shape and why it is this shape (research.md D1a):
//
//   - A file id is a STABLE identifier — a filename, not a version. Saving
//     rewrites the content behind the same id via PUT /internal/file/{id}/content
//     ("store-and-link"); file-service swaps the underlying blob and its
//     content-hash externalID behind that id, which is its business, not ours.
//   - The stored bytes are a BARE Yjs-V2 state snapshot with no envelope. That
//     is not a preference: Alkemio's `server` also writes this blob (document
//     create, and the one-time content migration), so anything we wrapped around
//     it would make the two writers mutually unreadable. It is why the log
//     profile was unusable here and the checkpoint profile exists.
//   - The state vector is DERIVED on read. A file row has nowhere to put it —
//     its only structured metadata is a typed image-specific ContentMetadata —
//     and the contract explicitly permits deriving it.
//
// Fencing is NOT supported and the store reports Unfenced (research.md D6a).
// Rejecting a superseded owner means remembering the highest accepted epoch per
// document, durably; a file row cannot hold it, and keeping it in the Alkemio
// index would make the persistence-level backstop depend on reaching a second
// service — which is exactly what a backstop must not do, since it exists for
// the case where a partitioned holder is still alive. Ownership is the cluster
// lease's job here.
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

	"github.com/antst/go-yjs/backend"
	"github.com/antst/go-yjs/backend/persistence"
	"github.com/antst/go-yjs/crdt"
)

// PointerResolver maps a document to its stable file pointer.
//
// file-service assigns file ids on create and does not accept a caller-supplied
// one, so the DocumentID -> file id mapping has to live somewhere. It is written
// ONCE, when the document's file is first created, and read on load — not per
// save, because the pointer never changes after that.
//
// It is an interface rather than a direct MetadataStore dependency so this store
// reaches its own infrastructure and the mapping's home stays a wiring decision.
type PointerResolver interface {
	// Pointer returns the document's stable file pointer and storage bucket.
	// It MUST report ErrNoPointer when the document has no file yet.
	Pointer(ctx context.Context, id backend.DocumentID) (pointer, bucket string, err error)
	// Record persists a newly created pointer. It is called at most once per
	// document, immediately after the file is created.
	Record(ctx context.Context, id backend.DocumentID, pointer string) error
}

// ErrNoPointer reports that a document has no file yet, so the next save must
// create one. It is the resolver's "not found", kept distinct from
// persistence.ErrNotFound so a resolver failure is never mistaken for an
// empty document.
var ErrNoPointer = errors.New("fileservice: document has no file pointer yet")

// Config carries the per-deployment file-service settings.
type Config struct {
	// BaseURL is the file-service root, e.g. http://file-service:4003.
	BaseURL string
	// FallbackBucketID is used when the resolver reports no per-document bucket.
	FallbackBucketID string
	// MaxUploadSize caps a snapshot in bytes. It MUST stay below file-service's
	// own request-body limit: a document sitting exactly on the limit encodes to
	// slightly more once v2 framing is added, so it would pass our budget check
	// and then be refused by the transport — accepted, and permanently unsaveable.
	MaxUploadSize int64
	// HTTPClient overrides the default client (tests); nil uses a 30s client.
	HTTPClient *http.Client
}

// Store persists one current state per document into file-service.
type Store struct {
	cfg      Config
	client   *http.Client
	pointers PointerResolver
	// revisions is a per-process monotonic counter. file-service assigns its own
	// row Version, but that is its concurrency token, not our revision, and it is
	// not returned by the content-write path — so revisions are process-local and
	// strictly increasing, which is all the contract requires.
	revisions revisionCounter
}

// New constructs a store. resolver supplies the DocumentID -> file pointer map.
func New(cfg Config, resolver PointerResolver) (*Store, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("fileservice: BaseURL is required")
	}
	if resolver == nil {
		return nil, errors.New("fileservice: a PointerResolver is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Store{cfg: cfg, client: client, pointers: resolver}, nil
}

// FenceMode reports Unfenced. See the package doc: this medium cannot hold a
// per-document epoch, so stale-owner rejection is the cluster lease's job.
func (s *Store) FenceMode() persistence.FenceMode { return persistence.Unfenced }

// SaveCheckpoint replaces the document's durable state.
//
// StateVector is required by the contract but deliberately not stored — a file
// row has nowhere to put it, and LoadCheckpoint derives it instead.
func (s *Store) SaveCheckpoint(ctx context.Context, req persistence.SaveCheckpointRequest) (persistence.Revision, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if req.Fence != 0 {
		return 0, persistence.ErrUnexpectedFence
	}
	if limit := s.cfg.MaxUploadSize; limit > 0 && int64(len(req.Update)) > limit {
		return 0, fmt.Errorf("snapshot %d bytes exceeds the configured limit %d", len(req.Update), limit)
	}

	pointer, bucket, err := s.pointers.Pointer(ctx, req.DocumentID)
	switch {
	case err == nil:
		if rerr := s.rewrite(ctx, pointer, req.Update); rerr == nil {
			return s.revisions.next(), nil
		} else if !errors.Is(rerr, persistence.ErrNotFound) {
			return 0, rerr
		}
		// The file vanished out of band; fall through and create a new one so
		// saving is not wedged forever on a pointer that no longer resolves.
	case errors.Is(err, ErrNoPointer):
		// First save for this document.
	default:
		return 0, fmt.Errorf("resolving file pointer: %w", err)
	}

	if bucket == "" {
		bucket = s.cfg.FallbackBucketID
	}
	created, err := s.create(ctx, bucket, req.Update)
	if err != nil {
		return 0, err
	}
	if err := s.pointers.Record(ctx, req.DocumentID, created); err != nil {
		// The bytes are durable but unreachable: nothing maps the document to
		// them. Report failure rather than a revision the caller would treat as
		// a successful save.
		return 0, fmt.Errorf("recording file pointer for a stored snapshot: %w", err)
	}
	return s.revisions.next(), nil
}

// LoadCheckpoint returns the document's current state, deriving the state vector
// from the stored bytes. Both returned slices are caller-owned — the response
// body is read into a fresh buffer, and the derived vector is freshly allocated.
func (s *Store) LoadCheckpoint(ctx context.Context, id backend.DocumentID) (persistence.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return persistence.Checkpoint{}, err
	}
	pointer, _, err := s.pointers.Pointer(ctx, id)
	switch {
	case err == nil:
	case errors.Is(err, ErrNoPointer):
		return persistence.Checkpoint{}, persistence.ErrNotFound
	default:
		return persistence.Checkpoint{}, fmt.Errorf("resolving file pointer: %w", err)
	}

	blob, err := s.fetch(ctx, pointer)
	if err != nil {
		return persistence.Checkpoint{}, err
	}
	vector, err := deriveStateVector(blob)
	if err != nil {
		// Stored bytes that will not parse cannot form the state a successful
		// load promises.
		return persistence.Checkpoint{}, fmt.Errorf("%w: %w", persistence.ErrCorrupt, err)
	}
	return persistence.Checkpoint{
		Revision:    s.revisions.current(),
		Update:      blob,
		StateVector: vector,
	}, nil
}

// rewrite swaps the content behind an existing, stable file id.
func (s *Store) rewrite(ctx context.Context, pointer string, data []byte) error {
	// PathEscape the pointer: a path-significant byte must not re-target the
	// write at a different file-service object.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer)+"/content", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("file-service rewrite: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return persistence.ErrNotFound
	case http.StatusConflict:
		// CAUTION — this is content DEDUPLICATION, not stale-owner protection.
		//
		// file-service enforces unique(externalID, storageBucketID). Writing bytes
		// that already belong to ANOTHER file in the same bucket is refused;
		// writing identical bytes to THIS file succeeds. The coincidence is
		// dangerous: during testing a 409 can look like a concurrency guard, and
		// it provides none — two owners writing DIFFERENT states both succeed.
		// Ownership is the cluster lease's job (this store is Unfenced).
		//
		// It is also permanent for these bytes, not transient, so it must not be
		// retried as a transient fault.
		return fmt.Errorf("file-service rewrite: content already stored under another file in this bucket (dedup conflict): %s", readErrBody(resp.Body))
	default:
		return fmt.Errorf("file-service rewrite: unexpected status %d: %s", resp.StatusCode, readErrBody(resp.Body))
	}
}

// create makes the document's file for the first time and returns its id.
func (s *Store) create(ctx context.Context, bucket string, data []byte) (string, error) {
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

	// authorizationId is omitted deliberately: a snapshot is internal infra whose
	// access is governed by the document's own authz, so file-service writes a
	// NULL authz column. UNIQUE(authorizationId) permits any number of NULLs.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.BaseURL+"/internal/file", &body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("file-service create: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("file-service create: unexpected status %d: %s", resp.StatusCode, readErrBody(resp.Body))
	}
	var cr struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&cr); err != nil {
		return "", fmt.Errorf("decode create response: %w", err)
	}
	if cr.ID == "" {
		return "", errors.New("file-service create returned an empty id")
	}
	return cr.ID, nil
}

// fetch reads a file's content into a caller-owned buffer.
func (s *Store) fetch(ctx context.Context, pointer string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer)+"/content", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("file-service fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		// CORRUPT, not "not found". The index says this document HAS state — it
		// carries a pointer — and the file behind that pointer is gone. That is a
		// different situation from a document that was never saved, and the
		// difference is load-bearing: a caller that treats it as "nothing stored"
		// would seed create-time content over a document that had real content, and
		// the next save would overwrite the last good state with it. ErrNotFound is
		// reserved for a document with no pointer at all.
		return nil, fmt.Errorf("%w: file %q behind the document pointer is missing", persistence.ErrCorrupt, pointer)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("file-service fetch: unexpected status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func readErrBody(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 2<<10))
	return strings.TrimSpace(string(b))
}

// deriveStateVector extracts the state vector from a stored update.
//
// This store CANNOT keep the vector the writer computed: its blob is a bare Yjs
// update by cross-repo contract — `server` writes the same shape on create and in
// the T009 migration — so there is nowhere to put a second value without
// changing a format two other repos read.
//
// Deriving means choosing a decoder, and the update carries no V1/V2
// discriminator. Choosing wrong does not fail: EncodeStateVectorFromUpdate on V2
// bytes returns err == nil and an EMPTY vector, which reads as "this document
// has nothing from any client" — a confident wrong answer, which is worse than a
// decode error because nothing downstream can detect it.
//
// V2 first because that is what this service writes (Room.persist encodes V2).
// The V1 fallback exists for updates this service did not write — the migration
// path, and the conformance suite, whose fixtures are V1-encoded.
//
// "Empty means wrong codec" is safe because of a measured asymmetry in the core,
// verified here in both directions at several document sizes:
//
//	              V1 bytes    V2 bytes
//	V1 decoder    correct     00
//	V2 decoder    00          correct
//
// The wrong decoder ALWAYS returns the empty vector — never an error, and never
// a plausible-but-wrong non-empty value. So a non-empty result can only come
// from the right decoder, and empty from both means the document is genuinely
// empty, where either answer is identical anyway.
//
// Note what this does NOT give us: with nothing declaring the encoding, empty is
// indistinguishable from wrong for a single decode. There is no reference to
// check against, which is why guessing was unfixable rather than merely
// unreliable — and why the declared encoding replaces this rather than
// refining it.
//
// TEMPORARY. The go-yjs owners are making the encoding explicit in the
// persistence contract rather than inferred, and extending the conformance suite
// to cover both codecs so a store handling only one cannot pass. When that
// lands, this sniffing is deleted in favour of the declared encoding.
func deriveStateVector(update []byte) ([]byte, error) {
	if v2, err := crdt.EncodeStateVectorFromUpdateV2(update); err == nil && !isEmptyStateVector(v2) {
		return v2, nil
	}
	v1, err := crdt.EncodeStateVectorFromUpdate(update)
	if err == nil && !isEmptyStateVector(v1) {
		return v1, nil
	}
	// Neither decoder found any client state. Re-run the V2 decoder so a genuinely
	// unparseable update surfaces its error rather than an empty vector.
	return crdt.EncodeStateVectorFromUpdateV2(update)
}

// isEmptyStateVector reports the encoding of "no client state": a single
// zero-valued length prefix.
func isEmptyStateVector(v []byte) bool { return len(v) <= 1 }

var _ persistence.DeletingCheckpointStore = (*Store)(nil)

// Delete removes the document's file (persistence.Deleter).
//
// Idempotent: a document with no pointer, or whose file is already gone,
// deletes successfully — the cascade retries, and a second delete must not fail
// the operation it is completing.
//
// The store is Unfenced, so a non-zero fence is ErrUnexpectedFence and the state
// is left intact. Checked BEFORE the pointer is resolved: a rejected delete must
// not have reached the network at all.
func (s *Store) Delete(ctx context.Context, req persistence.DeleteRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.Fence != 0 {
		return persistence.ErrUnexpectedFence
	}
	pointer, _, err := s.pointers.Pointer(ctx, req.DocumentID)
	switch {
	case err == nil:
	case errors.Is(err, ErrNoPointer):
		return nil // never had a file
	default:
		return fmt.Errorf("resolving file pointer: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		s.cfg.BaseURL+"/internal/file/"+url.PathEscape(pointer), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := s.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("file-service delete: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("file-service delete: unexpected status %d: %s", resp.StatusCode, readErrBody(resp.Body))
	}
}
