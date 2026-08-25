package fileservice

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/antst/go-yjs/backend/persistence"
)

// TestAnOversizeCheckpointIsRefusedNotBuffered is the regression for an
// unbounded read on the room-materialisation path.
//
// LoadCheckpoint used a bare io.ReadAll on the file-service response — the only
// body read in the service without a limit (the authzeval client caps at 64 KiB,
// create's decode at 64 KiB, readErrBody at 2 KiB). MaxUploadSize guarded only
// writes, so a blob this service did NOT write — a content migration, a repaired
// file, the wrong object behind the pointer — was materialised whole in memory
// before anything checked its size: one OOM per concurrent cold open, with no
// error naming a limit.
//
// The producer is concrete: server's content migration writes checkpoint blobs
// out of band, under no obligation to respect this service's MAX_DOC_BYTES.
func TestAnOversizeCheckpointIsRefusedNotBuffered(t *testing.T) {
	const limit = 1 << 10
	body := strings.NewReader(strings.Repeat("x", limit+1))

	_, err := readBounded(body, limit, "file-abc")
	if err == nil {
		t.Fatal("an oversize checkpoint was accepted; the read is unbounded again")
	}
	if !errors.Is(err, persistence.ErrCorrupt) {
		t.Fatalf("error = %v, want ErrCorrupt so the caller does not treat it as an empty document", err)
	}
	if !strings.Contains(err.Error(), "1024-byte") {
		t.Fatalf("error = %v, want it to name the limit", err)
	}
}

// TestACheckpointExactlyAtTheLimitIsAccepted is the +1 half, and it is the reason
// the implementation reads limit+1 rather than limit.
//
// io.LimitReader(limit) cannot distinguish "exactly at the limit" from "truncated
// at the limit" — both produce exactly limit bytes. Implemented that way, this
// test forces the choice: either a legal maximal document is rejected, or a
// TRUNCATED one is accepted as complete. The second is the dangerous half,
// because a truncated Yjs update decodes to a shorter document with NO error, so
// the next save would persist the loss.
//
// Non-vacuity: change limit+1 to limit in readBounded and this test fails.
func TestACheckpointExactlyAtTheLimitIsAccepted(t *testing.T) {
	const limit = 1 << 10
	body := strings.NewReader(strings.Repeat("x", limit))

	got, err := readBounded(body, limit, "file-abc")
	if err != nil {
		t.Fatalf("a checkpoint exactly at the limit was refused: %v", err)
	}
	if len(got) != limit {
		t.Fatalf("read %d bytes, want the whole %d-byte document", len(got), limit)
	}
}

// TestAnUnsetLimitReadsEverything preserves the standalone/dev configuration,
// where MaxUploadSize is unset and no bound applies.
func TestAnUnsetLimitReadsEverything(t *testing.T) {
	body := strings.NewReader(strings.Repeat("x", 4096))
	got, err := readBounded(body, 0, "file-abc")
	if err != nil {
		t.Fatalf("unset limit must not bound the read: %v", err)
	}
	if len(got) != 4096 {
		t.Fatalf("read %d bytes, want 4096", len(got))
	}
}

// TestChunkedBodyWithoutContentLengthIsStillBounded pins that the bound comes
// from the bytes actually read, not from a Content-Length header. A chunked
// response declares no length, so a limit trusting the header would not apply at
// all — which is the shape an out-of-band writer is most likely to produce.
func TestChunkedBodyWithoutContentLengthIsStillBounded(t *testing.T) {
	const limit = 512
	// A reader with no length information whatsoever.
	body := io.MultiReader(
		strings.NewReader(strings.Repeat("a", 400)),
		strings.NewReader(strings.Repeat("b", 400)),
	)
	if _, err := readBounded(body, limit, "file-chunked"); err == nil {
		t.Fatal("a chunked oversize body was accepted; the bound must come from bytes read, not Content-Length")
	}
}
