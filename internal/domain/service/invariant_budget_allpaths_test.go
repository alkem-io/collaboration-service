package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

// TestInvBudgetAllPaths — INV-BUDGET-ALLPATHS (spec 002 FR-005). The MaxDocBytes
// budget must be enforced on EVERY mutation entry point, not just the local write
// path. A peer-pod update cannot be REJECTED (that would diverge from the pod that
// already accepted it), but it MUST be CHECKED — an over-budget peer update is logged,
// proving the cross-pod path runs the budget. RED before 002: handlePeer applied with
// no budget check at all, so the cross-pod path could grow the doc past the cap
// silently. With the single applyUpdate chokepoint, every entry point is covered.
func TestInvBudgetAllPaths(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	deps := newTestDeps()
	room, err := newRoom(context.Background(), "doc-budget-allpaths", model.ContentTypeMemo,
		deps.Deps, RoomConfig{SendBuffer: 16, SaveDebounce: time.Hour, IdleTimeout: time.Hour, BackendTimeout: 5 * time.Second},
		NopMetrics{}, zap.New(core))
	if err != nil {
		t.Fatalf("newRoom: %v", err)
	}
	room.cfg.Limits.MaxDocBytes = 1 // any non-trivial update blows the cap

	// A cross-pod peer update that would exceed the cap. It cannot be rejected, but
	// the budget MUST be checked — the chokepoint logs it.
	room.handlePeer(updateBytesFor(t, "peer content well over the one-byte cap"), false)

	if logs.FilterMessageSnippet("exceed MaxDocBytes").Len() == 0 {
		t.Fatal("over-budget peer update was not checked on the cross-pod path — the byte budget is local-only (FR-005)")
	}
}
