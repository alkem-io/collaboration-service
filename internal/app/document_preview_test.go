package app

import (
	"testing"

	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
)

func TestPrivateArtifactPublicationFollowsTheDurableTopology(t *testing.T) {
	for _, durable := range []bool{false, true} {
		cfg := standaloneConfig()
		if durable {
			cfg.CheckpointStore = config.CheckpointStoreFileService
			cfg.FileService.BaseURL = "http://127.0.0.1:1"
		}
		deps, closeDeps, err := buildDeps(cfg, zap.NewNop())
		if err != nil {
			t.Fatal(err)
		}
		closeDeps()
		if (deps.PreviewFiles != nil) != durable {
			t.Fatalf("private artifact writer does not follow backend selection: durable=%t", durable)
		}
		if deps.PreviewCheckpoint == nil {
			t.Fatal("private memo endpoint needs its read port on both topologies")
		}
	}
}

func TestUnusablePrivateFileServiceURLStopsApplicationAssembly(t *testing.T) {
	cfg := standaloneConfig()
	cfg.CheckpointStore = config.CheckpointStoreFileService
	cfg.FileService.BaseURL = "http://127.0.0.1:4003/?query=cannot-scope-private-files"
	a, err := New(cfg, zap.NewNop())
	if a != nil {
		a.Close()
	}
	if a != nil || err == nil {
		t.Fatal("application exposed its listeners without a usable private artifact backend")
	}
}
