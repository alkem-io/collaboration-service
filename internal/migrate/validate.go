package migrate

import (
	"fmt"

	ycrdt "github.com/skyterra/y-crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// ValidationConfig tunes the post-conversion checks (SC-003/007/009).
type ValidationConfig struct {
	// MaxSizeRatio is the SC-007 size-baseline ceiling: the v2 snapshot may be at
	// most this multiple of the legacy input size before the document is flagged
	// for review (a large blow-up signals a bad conversion). Zero disables the
	// ratio check. A small absolute floor (SizeFloorBytes) exempts tiny docs,
	// where fixed CRDT framing overhead dominates and ratios are meaningless.
	MaxSizeRatio float64
	// SizeFloorBytes exempts snapshots at/below this size from the ratio check
	// (CRDT framing overhead floor). Default 4 KiB.
	SizeFloorBytes int
}

// DefaultValidationConfig is the build-ahead baseline: a 3x size ceiling above a
// 4 KiB floor. Memo v1→v2 is near-parity; whiteboard JSON→CRDT typically shrinks
// or stays close, so 3x is a generous regression tripwire, not a tight bound —
// tune against the real corpus baseline captured by infra-ops T004.
func DefaultValidationConfig() ValidationConfig {
	return ValidationConfig{
		MaxSizeRatio:   3.0,
		SizeFloorBytes: 4 << 10,
	}
}

// Validate runs the post-conversion invariants on a converted snapshot:
//
//   - SC-003 / SC-009 round-trip: the v2 snapshot decodes cleanly into a fresh
//     authoritative Y.Doc (the exact loadSnapshot path), and re-encoding that doc
//     to v2 converges (idempotent — re-encoding the rehydrated doc yields a state
//     vector identical to the source), proving the snapshot is self-consistent and
//     reloadable. A snapshot that cannot rehydrate is the failure this guards.
//   - SC-007 size baseline: the snapshot is not pathologically larger than the
//     legacy input (ratio ceiling above a floor).
//
// A failed check returns a non-nil error carrying the reason; the driver flags
// the document (it never persists or drops an invalid conversion).
func Validate(conv Conversion, cfg ValidationConfig) error {
	if conv.Empty {
		return nil
	}
	if len(conv.Snapshot) == 0 {
		return fmt.Errorf("conversion produced an empty snapshot")
	}

	// Round-trip: rehydrate exactly as Room.loadSnapshot does (ApplyUpdateV2 into
	// a fresh GC'd doc). A codec panic OR a decode-to-empty (garbage bytes the v2
	// decoder no-ops on) means the snapshot is not a faithful reload of non-empty
	// content — Validate is only ever called on a non-empty conversion (Empty
	// returns early above), so the rehydrated doc MUST carry state.
	doc := service.NewMigrationDoc("validate")
	if !tryApply(func() { ycrdt.ApplyUpdateV2(doc, conv.Snapshot, migrationOrigin) }) || !docHasState(doc) {
		return fmt.Errorf("snapshot failed v2 round-trip: not reloadable")
	}

	// Convergence: re-encoding the rehydrated doc must reproduce the same logical
	// state. We compare state vectors (the per-client clocks) rather than raw
	// bytes — a v2 re-encode of an equal state yields an equal state vector, which
	// is the CRDT-meaningful equality (raw bytes can differ by GC/ordering).
	svSource := ycrdt.EncodeStateVector(doc, nil, ycrdt.NewUpdateEncoderV1())
	redoc := service.NewMigrationDoc("validate")
	resnap := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	if !tryApply(func() { ycrdt.ApplyUpdateV2(redoc, resnap, migrationOrigin) }) {
		return fmt.Errorf("snapshot failed re-encode round-trip")
	}
	svReencoded := ycrdt.EncodeStateVector(redoc, nil, ycrdt.NewUpdateEncoderV1())
	if !bytesEqual(svSource, svReencoded) {
		return fmt.Errorf("snapshot did not converge on re-encode (state vector mismatch)")
	}

	// SC-007 size baseline.
	if cfg.MaxSizeRatio > 0 && conv.LegacyBytes > 0 {
		floor := cfg.SizeFloorBytes
		if floor == 0 {
			floor = 4 << 10
		}
		if len(conv.Snapshot) > floor {
			ratio := float64(len(conv.Snapshot)) / float64(conv.LegacyBytes)
			if ratio > cfg.MaxSizeRatio {
				return fmt.Errorf("snapshot %d bytes is %.1fx the legacy %d bytes (ceiling %.1fx)",
					len(conv.Snapshot), ratio, conv.LegacyBytes, cfg.MaxSizeRatio)
			}
		}
	}
	return nil
}

// bytesEqual is a small equality helper kept local so the package needs no extra
// import for a single comparison.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
