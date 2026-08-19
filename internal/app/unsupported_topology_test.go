package app

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// TestUnsupportedTopologyIsWarnedBeforeServing is FR-022b.
//
// Multi-pod fan-out with a durable store has no ownership mechanism: every pod
// flushes the whole document on its own schedule, nothing decides which write
// wins, and two pods that diverge overwrite each other — the later writer
// silently discarding edits it never received. That failure appears only under
// conditions nobody reproduces on purpose (a dropped message, a partition, a
// restart), which is exactly why it has to be said at startup rather than
// discovered.
//
// The assertions are on the three things that make the warning actionable: it is
// at WARN or above (an INFO line is lost in startup noise), it names BOTH keys
// (an operator has to know which two settings to change), and it says what to do
// instead.
func TestUnsupportedTopologyIsWarnedBeforeServing(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	cfg := &config.Config{HubMode: config.HubRedis, CheckpointStore: config.CheckpointStoreFileService}

	warnUnsupportedTopology(cfg, zap.New(core))

	entries := logs.FilterMessageSnippet("UNSUPPORTED").All()
	if len(entries) != 1 {
		t.Fatalf("got %d warnings, want exactly 1 for the unsupported combination", len(entries))
	}
	e := entries[0]
	if e.Level < zapcore.WarnLevel {
		t.Fatalf("logged at %v; must be WARN or above or it is lost in startup noise", e.Level)
	}
	fields := e.ContextMap()
	if _, ok := fields["HUB_MODE"]; !ok {
		t.Fatalf("the warning does not name HUB_MODE; an operator cannot tell which setting to change. fields: %v", fields)
	}
	if _, ok := fields["CHECKPOINT_STORE"]; !ok {
		t.Fatalf("the warning does not name CHECKPOINT_STORE. fields: %v", fields)
	}
	if s, _ := fields["supported"].(string); !strings.Contains(s, "inmemory") {
		t.Fatalf("the warning does not say what to run instead: %v", fields["supported"])
	}
}

// TestSupportedTopologiesAreNotWarnedAbout is the other half: a warning that
// fires for every configuration is noise, and an operator who sees it on a
// single-pod deployment learns to ignore it — which is worse than not warning,
// because the one case that matters then goes unread too.
func TestSupportedTopologiesAreNotWarnedAbout(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.Config
	}{
		{"single pod, durable", &config.Config{HubMode: config.HubInMemory, CheckpointStore: config.CheckpointStoreFileService}},
		{"multi pod, non-durable", &config.Config{HubMode: config.HubRedis, CheckpointStore: config.CheckpointStoreInline}},
		{"single pod, non-durable", &config.Config{HubMode: config.HubInMemory, CheckpointStore: config.CheckpointStoreInline}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			warnUnsupportedTopology(tc.cfg, zap.New(core))
			if n := logs.FilterMessageSnippet("UNSUPPORTED").Len(); n != 0 {
				t.Fatalf("warned about a supported topology; a warning that fires for everything trains operators to ignore the one that matters")
			}
		})
	}
}
