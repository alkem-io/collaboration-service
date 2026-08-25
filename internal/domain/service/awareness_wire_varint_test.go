package service

import "testing"

// TestVarUintLenMatchesTheLib0Encoding covers the multi-byte branch.
//
// The length is used to size an awareness frame's header before writing it, so an
// off-by-one produces a frame the peer cannot parse — and only for values at or
// above the byte boundaries, which is why a single small-value test leaves the
// loop uncovered and the bug latent until a room accumulates 128 awareness
// clients or a large clock.
func TestVarUintLenMatchesTheLib0Encoding(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{1, 1},
		{127, 1},     // last single-byte value
		{128, 2},     // first two-byte value
		{16383, 2},   // last two-byte value
		{16384, 3},   // first three-byte value
		{1 << 21, 4}, // boundary walk continues
		{1 << 28, 5},
		{^uint64(0), 10}, // maximum: 64 bits at 7 payload bits per byte
	} {
		if got := varUintLen(tc.v); got != tc.want {
			t.Errorf("varUintLen(%d) = %d, want %d; a wrong header length produces an awareness frame the peer cannot parse", tc.v, got, tc.want)
		}
	}
}
