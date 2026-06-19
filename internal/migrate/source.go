package migrate

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Source streams legacy documents to migrate, in a stable order, in batches.
// It is the migration's INBOUND seam to the legacy stores.
//
// The Alkemio production source is the `server` migration read-path (server task
// S-T005: CollaborationMigrationService.readAll, paginated id-ASC). As of this
// build that service is IN-PROCESS NestJS ONLY — it has no HTTP/RMQ/CLI surface a
// Go tool can call (verified against server PR #6171). So the concrete external
// source is STUBBED pending that endpoint; see the package report. The shipped
// implementation here is JSONLSource, which reads the exact same
// LegacyContentRecord JSONL the server read-path emits — so when S-T005 grows a
// `pnpm cli collab:migration-export > legacy.jsonl` (or an HTTP/RMQ streamer),
// its output feeds this driver unchanged, and an HTTPSource/RMQSource can be
// dropped in behind this interface without touching the driver.
type Source interface {
	// Next returns the next legacy record and true, or a zero record and false at
	// the end of the stream. A non-nil error aborts the run (the driver has
	// already persisted everything up to this point — the run is resumable).
	Next() (LegacyRecord, bool, error)
}

// JSONLSource reads newline-delimited LegacyContentRecord JSON (one object per
// line) from any reader — a file produced by the server export, or a piped
// stream. It is deterministic (it preserves the producer's order, which the
// server emits id-ASC), which is what makes the migration resumable: re-running
// over the same input re-processes documents in the same order, and already-
// migrated ones short-circuit as Skipped (idempotency lives in the driver).
type JSONLSource struct {
	sc *bufio.Scanner
}

// NewJSONLSource wraps r as a JSONL legacy-record source. The scanner buffer is
// raised to 16 MiB/line because a single legacy whiteboard's Excalidraw JSON (or
// a long memo's base64 update) can be large.
func NewJSONLSource(r io.Reader) *JSONLSource {
	sc := bufio.NewScanner(r)
	const maxLine = 16 << 20 // 16 MiB
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	return &JSONLSource{sc: sc}
}

// Next decodes the next non-blank line. Blank lines are skipped so a trailing
// newline (or human-edited corpus) does not abort the run.
func (s *JSONLSource) Next() (LegacyRecord, bool, error) {
	for s.sc.Scan() {
		line := strings.TrimSpace(s.sc.Text())
		if line == "" {
			continue
		}
		var rec LegacyRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return LegacyRecord{}, false, fmt.Errorf("decode legacy record: %w", err)
		}
		if rec.ID == "" {
			return LegacyRecord{}, false, errors.New("legacy record missing id")
		}
		return rec, true, nil
	}
	if err := s.sc.Err(); err != nil {
		return LegacyRecord{}, false, fmt.Errorf("read legacy stream: %w", err)
	}
	return LegacyRecord{}, false, nil
}

// SliceSource is an in-memory Source over a fixed slice — used by the tests and
// the seed/dry-run corpus. It also documents the Source contract by example.
type SliceSource struct {
	recs []LegacyRecord
	i    int
}

// NewSliceSource returns a Source over recs.
func NewSliceSource(recs []LegacyRecord) *SliceSource {
	return &SliceSource{recs: recs}
}

// Next walks the slice once.
func (s *SliceSource) Next() (LegacyRecord, bool, error) {
	if s.i >= len(s.recs) {
		return LegacyRecord{}, false, nil
	}
	rec := s.recs[s.i]
	s.i++
	return rec, true, nil
}
