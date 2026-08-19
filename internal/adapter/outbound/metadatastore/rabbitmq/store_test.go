package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// fakeRPC captures each Call/Emit and returns scripted replies, so the unified
// contract shape the adapter publishes is asserted without a live RabbitMQ (the
// server consumer does not exist yet — OPEN-3 cross-repo follow-up).
type fakeRPC struct {
	calls []capturedCall
	emits []capturedCall

	// reply is unmarshalled into the Call's reply arg, keyed by pattern.
	replies map[string]any
	callErr error
	emitErr error
}

type capturedCall struct {
	pattern string
	data    any
}

func (f *fakeRPC) Call(_ context.Context, pattern string, data, reply any) error {
	f.calls = append(f.calls, capturedCall{pattern: pattern, data: data})
	if f.callErr != nil {
		return f.callErr
	}
	if r, ok := f.replies[pattern]; ok && reply != nil {
		// Round-trip through JSON so the test scripts the wire reply, not a Go
		// value directly — the same path the real transport takes.
		raw, _ := json.Marshal(r)
		_ = json.Unmarshal(raw, reply)
	}
	return nil
}

func (f *fakeRPC) Emit(_ context.Context, pattern string, data any) error {
	if f.emitErr != nil {
		return f.emitErr
	}
	f.emits = append(f.emits, capturedCall{pattern: pattern, data: data})
	return f.emitErr
}

func TestSavePublishesUnifiedContract(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternSave: SaveReply{Success: true}}}
	store := newWithRPC(f)

	err := store.Save(context.Background(), model.Metadata{
		ID:                    "doc-1",
		ContentType:           model.ContentTypeWhiteboard,
		Version:               4,
		ContentPointer:        "file-uuid",
		BlobStore:             model.BlobStoreFileService,
		AuthorizationPolicyID: "pol-7",
		OwnerRef:              "owner-1",
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(f.calls) != 1 || f.calls[0].pattern != "collaboration-save" {
		t.Fatalf("expected one collaboration-save call, got %+v", f.calls)
	}
	data, ok := f.calls[0].data.(SaveData)
	if !ok {
		t.Fatalf("save data type = %T, want SaveData", f.calls[0].data)
	}
	// Assert the exact index-only payload shape (the blob is NOT here).
	want := SaveData{
		ID:                    "doc-1",
		ContentType:           "whiteboard",
		Version:               4,
		ContentPointer:        "file-uuid",
		BlobStore:             "file-service",
		AuthorizationPolicyID: "pol-7",
		OwnerRef:              "owner-1",
	}
	if data != want {
		t.Errorf("SaveData = %+v, want %+v", data, want)
	}

	// And the serialized JSON matches the documented field names verbatim.
	raw, _ := json.Marshal(data)
	for _, field := range []string{
		`"id":"doc-1"`, `"contentType":"whiteboard"`, `"version":4`,
		`"contentPointer":"file-uuid"`, `"blobStore":"file-service"`,
		`"authorizationPolicyId":"pol-7"`,
	} {
		if !contains(string(raw), field) {
			t.Errorf("save JSON %s missing %s", raw, field)
		}
	}
	// The blob bytes must never appear on this bus (index-only contract).
	if contains(string(raw), "binaryStateInBase64") || contains(string(raw), "content\":") {
		t.Errorf("save payload leaked blob content: %s", raw)
	}
}

func TestSaveDefaultsBlobStore(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternSave: SaveReply{Success: true}}}
	store := newWithRPC(f)
	if err := store.Save(context.Background(), model.Metadata{ID: "d", ContentType: model.ContentTypeMemo}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	data := f.calls[0].data.(SaveData)
	if data.BlobStore != string(model.BlobStoreInline) {
		t.Errorf("default blobStore = %q, want inline", data.BlobStore)
	}
}

func TestSaveServerFailureSurfaces(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternSave: SaveReply{Success: false, Error: "constraint violated"}}}
	store := newWithRPC(f)
	if err := store.Save(context.Background(), model.Metadata{ID: "d"}); err == nil {
		t.Error("expected Save to surface the server error")
	}
}

func TestSaveTransportErrorSurfaces(t *testing.T) {
	f := &fakeRPC{callErr: errors.New("channel closed")}
	store := newWithRPC(f)
	if err := store.Save(context.Background(), model.Metadata{ID: "d"}); err == nil {
		t.Error("expected Save to surface the transport error")
	}
}

func TestLoadFetchesAndMaps(t *testing.T) {
	seed := []byte{0x01, 0x02, 0x03}
	f := &fakeRPC{replies: map[string]any{PatternFetch: FetchReply{
		Found:                 true,
		ContentType:           "memo",
		Version:               2,
		ContentPointer:        "ptr",
		BlobStore:             "file-service",
		AuthorizationPolicyID: "pol-1",
		StorageBucketID:       "bucket-1",
		Content:               seed,
		OwnerRef:              "owner",
	}}}
	store := newWithRPC(f)

	meta, err := store.Load(context.Background(), "doc-9")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.calls[0].pattern != "collaboration-fetch" {
		t.Errorf("pattern = %q, want collaboration-fetch", f.calls[0].pattern)
	}
	if fd, ok := f.calls[0].data.(FetchData); !ok || fd.ID != "doc-9" {
		t.Errorf("fetch data = %+v", f.calls[0].data)
	}
	if meta.ID != "doc-9" || meta.ContentType != model.ContentTypeMemo ||
		meta.Version != 2 || meta.ContentPointer != "ptr" ||
		meta.BlobStore != model.BlobStoreFileService || meta.AuthorizationPolicyID != "pol-1" {
		t.Errorf("mapped metadata = %+v", meta)
	}
	// The document's own storage bucket must be carried through from the
	// collaboration-fetch reply so the BlobStore can persist snapshots into it.
	if meta.StorageBucketID != "bucket-1" {
		t.Errorf("StorageBucketID = %q, want bucket-1", meta.StorageBucketID)
	}
	// The stored content for the first-open seed (R4) must be surfaced on the
	// metadata so the room can materialize a never-yet-saved document from it.
	if !bytes.Equal(meta.SeedContent, seed) {
		t.Errorf("SeedContent = %v, want %v", meta.SeedContent, seed)
	}
}

// TestLoadRejectsUnknownContentType asserts a corrupt server reply carrying an
// unsupported contentType is rejected at the adapter boundary, not cast blindly
// into the domain to fail later with a weaker diagnostic.
func TestLoadRejectsUnknownContentType(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternFetch: FetchReply{
		Found: true, ContentType: "spreadsheet", BlobStore: "inline",
	}}}
	store := newWithRPC(f)
	if _, err := store.Load(context.Background(), "doc-x"); err == nil {
		t.Fatal("Load with unknown contentType: expected error, got nil")
	}
}

// TestLoadRejectsUnknownBlobStore asserts a server reply carrying a blobStore
// this service cannot read is rejected at the adapter boundary.
//
// There are exactly two backends this service can read: file-service and the
// in-process store. A row naming anything else must be refused, not tolerated —
// accepting it would mean reading the document through whichever store this
// process happens to be configured with, which is the wrong backend, silently, on
// the path where being wrong serves or overwrites the wrong document content.
func TestLoadRejectsUnknownBlobStore(t *testing.T) {
	for _, kind := range []string{"gdrive", "azure-blob", "memory"} {
		t.Run(kind, func(t *testing.T) {
			f := &fakeRPC{replies: map[string]any{PatternFetch: FetchReply{
				Found: true, ContentType: "memo", BlobStore: kind,
			}}}
			store := newWithRPC(f)
			if _, err := store.Load(context.Background(), "doc-x"); err == nil {
				t.Fatalf("Load with blobStore %q: expected an error — this service cannot read that backend, so serving the document would read from the wrong one", kind)
			}
		})
	}
}

// TestFetchReplyContentBase64WireShape pins the cross-repo wire contract: the
// first-open seed content rides collaboration-fetch as a base64 string (Go
// marshals []byte that way), so the NestJS server sends/receives it as base64 and
// it decodes back to the exact bytes. The field is omitted when empty so an
// already-snapshotted document carries no redundant payload.
func TestFetchReplyContentBase64WireShape(t *testing.T) {
	raw, err := json.Marshal(FetchReply{Found: true, ContentType: "memo", Content: []byte{0xDE, 0xAD, 0xBE, 0xEF}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// base64(DE AD BE EF) == "3q2+7w==".
	if !contains(string(raw), `"content":"3q2+7w=="`) {
		t.Errorf("content not base64-encoded on the wire: %s", raw)
	}

	var back FetchReply
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Equal(back.Content, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("round-tripped content = %v, want DEADBEEF", back.Content)
	}

	// Empty content is omitted entirely (omitempty), keeping the fetch reply for a
	// snapshotted document free of a redundant payload. (Match the "content" key
	// specifically — "contentType"/"contentPointer" legitimately remain.)
	rawEmpty, _ := json.Marshal(FetchReply{Found: true, ContentType: "memo"})
	if contains(string(rawEmpty), `"content":`) {
		t.Errorf("empty content must be omitted: %s", rawEmpty)
	}
}

func TestLoadNotFound(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternFetch: FetchReply{Found: false}}}
	store := newWithRPC(f)
	if _, err := store.Load(context.Background(), "absent"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("Load(absent) err = %v, want ErrNotFound", err)
	}
}

func TestLoadServerError(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternFetch: FetchReply{Error: "db down"}}}
	store := newWithRPC(f)
	if _, err := store.Load(context.Background(), "d"); err == nil || errors.Is(err, model.ErrNotFound) {
		t.Errorf("expected a non-NotFound error, got %v", err)
	}
}

func TestDeletePublishes(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternDelete: DeleteReply{Success: true}}}
	store := newWithRPC(f)
	if err := store.Delete(context.Background(), "doc-x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if f.calls[0].pattern != "collaboration-delete" {
		t.Errorf("pattern = %q, want collaboration-delete", f.calls[0].pattern)
	}
	if dd, ok := f.calls[0].data.(DeleteData); !ok || dd.ID != "doc-x" {
		t.Errorf("delete data = %+v", f.calls[0].data)
	}
}

func TestDeleteServerErrorSurfaces(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternDelete: DeleteReply{Error: "boom"}}}
	store := newWithRPC(f)
	if err := store.Delete(context.Background(), "d"); err == nil {
		t.Error("expected Delete to surface the server error")
	}
}

func TestDeleteSuccessFalseWithoutErrorSurfaces(t *testing.T) {
	// A {success:false} reply with no error string must still fail (mirrors Save),
	// rather than silently dropping the delete.
	f := &fakeRPC{replies: map[string]any{PatternDelete: DeleteReply{Success: false}}}
	store := newWithRPC(f)
	if err := store.Delete(context.Background(), "d"); err == nil {
		t.Error("expected Delete to fail when the server reports success=false")
	}
}

// TestSaveUnsuccessfulWithoutErrorStringSurfaces defends Save's bare
// !reply.Success branch (store.go:80): a reply that reports failure WITHOUT an
// error string must still be surfaced as an error — distinct from the
// reply.Error != "" path. Otherwise a silent server failure would look like a
// successful save.
func TestSaveUnsuccessfulWithoutErrorStringSurfaces(t *testing.T) {
	f := &fakeRPC{replies: map[string]any{PatternSave: SaveReply{Success: false}}} // no Error string
	store := newWithRPC(f)
	if err := store.Save(context.Background(), model.Metadata{ID: "d"}); err == nil {
		t.Error("expected Save to surface a failure even when the server sent no error string")
	}
}

// TestDeleteTransportErrorSurfaces defends Delete's transport-error branch
// (store.go:90): a failed RPC Call (broker down) must surface as an error, so a
// purge that never reached the server is not mistaken for a completed delete.
func TestDeleteTransportErrorSurfaces(t *testing.T) {
	f := &fakeRPC{callErr: errors.New("channel closed")}
	store := newWithRPC(f)
	if err := store.Delete(context.Background(), "d"); err == nil {
		t.Error("expected Delete to surface the transport error")
	}
}

func TestMarshalEnvelopeShape(t *testing.T) {
	// The NestJS RMQ request envelope { pattern, data, id } must serialize with
	// exactly those keys so a @MessagePattern consumer routes it.
	raw, err := marshalEnvelope("collaboration-save", "corr-1", SaveData{ID: "d", ContentType: "memo"})
	if err != nil {
		t.Fatalf("marshalEnvelope: %v", err)
	}
	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	for _, key := range []string{"pattern", "data", "id"} {
		if _, ok := env[key]; !ok {
			t.Errorf("envelope missing %q: %s", key, raw)
		}
	}
	if string(env["pattern"]) != `"collaboration-save"` {
		t.Errorf("pattern = %s", env["pattern"])
	}
	if string(env["id"]) != `"corr-1"` {
		t.Errorf("id = %s", env["id"])
	}
	// data is nested, not stringified.
	var sd SaveData
	if err := json.Unmarshal(env["data"], &sd); err != nil {
		t.Fatalf("data not a nested object: %v", err)
	}
	if sd.ID != "d" {
		t.Errorf("nested data id = %q", sd.ID)
	}
}

func TestContributionEmitsEvent(t *testing.T) {
	f := &fakeRPC{}
	store := newWithRPC(f)
	if err := store.Contribution(context.Background(), "doc-c", []string{"actor-1", "actor-2"}); err != nil {
		t.Fatalf("Contribution: %v", err)
	}
	if len(f.emits) != 1 || f.emits[0].pattern != PatternContribution {
		t.Fatalf("expected one collaboration-contribution emit, got %+v", f.emits)
	}
	data, ok := f.emits[0].data.(ContributionData)
	if !ok || data.ID != "doc-c" || len(data.Users) != 2 ||
		data.Users[0].ID != "actor-1" || data.Users[1].ID != "actor-2" {
		t.Fatalf("contribution emit payload = %+v", f.emits[0].data)
	}
}

// TestLoadTransportErrorSurfaces defends Load's transport-error branch
// (store.go:37): a failed RPC Call (broker/transport down) must surface as a
// non-NotFound error, so the caller does not mistake a transient outage for "the
// document does not exist".
func TestLoadTransportErrorSurfaces(t *testing.T) {
	f := &fakeRPC{callErr: errors.New("channel closed")}
	store := newWithRPC(f)
	_, err := store.Load(context.Background(), "d")
	if err == nil {
		t.Fatal("expected Load to surface the transport error")
	}
	if errors.Is(err, model.ErrNotFound) {
		t.Error("a transport failure must not be reported as ErrNotFound")
	}
}

// TestContributionEmitErrorSurfaces defends Contribution's emit-error branch
// (store.go:108): although the contribution event is fire-and-forget, a publish
// failure must still be surfaced to the caller (the room decides whether to
// retry the metric), not silently swallowed.
func TestContributionEmitErrorSurfaces(t *testing.T) {
	f := &fakeRPC{emitErr: errors.New("bus down")}
	store := newWithRPC(f)
	if err := store.Contribution(context.Background(), "doc-c", []string{"actor-1"}); err == nil {
		t.Error("expected Contribution to surface the emit error")
	}
}

func TestContributionPayloadShape(t *testing.T) {
	// The fire-and-forget contribution event carries the per-window actor ids
	// (carried forward from both legacy dialects, unified id field).
	raw, _ := json.Marshal(ContributionData{ID: "doc-1", Users: []User{{ID: "a"}, {ID: "b"}}})
	if !contains(string(raw), `"id":"doc-1"`) || !contains(string(raw), `"users":[{"id":"a"},{"id":"b"}]`) {
		t.Errorf("contribution shape = %s", raw)
	}
}

func TestInfoReplyOptionalFields(t *testing.T) {
	// maxCollaborators and isMultiUser are optional (whiteboard omits isMultiUser;
	// both omit maxCollaborators when unknown).
	raw, _ := json.Marshal(InfoReply{Read: true, Update: false})
	if contains(string(raw), "maxCollaborators") || contains(string(raw), "isMultiUser") {
		t.Errorf("unset optionals leaked: %s", raw)
	}
	maxColl := 8
	multi := true
	raw, _ = json.Marshal(InfoReply{Read: true, Update: true, MaxCollaborators: &maxColl, IsMultiUser: &multi})
	if !contains(string(raw), `"maxCollaborators":8`) || !contains(string(raw), `"isMultiUser":true`) {
		t.Errorf("set optionals missing: %s", raw)
	}
}

func TestConnectValidates(t *testing.T) {
	if _, _, err := Connect(Config{Queue: "q"}); err == nil {
		t.Error("expected Connect without URL to error")
	}
	if _, _, err := Connect(Config{URL: "amqp://x"}); err == nil {
		t.Error("expected Connect without Queue to error")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
