package app

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
)

// TestBuildDepsCleansUpWhatItOpenedWhenALaterStageFails covers buildDeps' error
// branches, and the property they exist for.
//
// buildDeps opens things in order — broadcaster, then metadata, then checkpoint,
// then auth — registering a closer for each. A failure at ANY stage must run
// every closer already registered, in reverse. Returning early without doing so
// leaks the redis pool and the RabbitMQ connection of a service that then exits,
// which is invisible locally (the process dies and the OS reclaims it) and shows
// up in a crash-looping pod as connection counts climbing on redis and the broker
// until they start refusing new ones — including from the healthy pods.
//
// Each case fails a LATER stage than the one before, so the reverse-order cleanup
// is exercised with a growing set of already-open resources.
func TestBuildDepsCleansUpWhatItOpenedWhenALaterStageFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.Config
	}{
		{
			// Stage 1: nothing open yet.
			name: "broadcaster",
			cfg: &config.Config{
				Fanout: config.FanoutRedis,
				Redis:  config.RedisConfig{URL: "not-a-redis-url"},
			},
		},
		{
			// Stage 2: the broadcaster is already open and must be closed.
			name: "metadata after a live broadcaster",
			cfg: &config.Config{
				Fanout:    config.FanoutRedis,
				MetaStore: config.MetaStoreRabbitMQ,
				RabbitMQ:  config.RabbitMQConfig{URL: "amqp://%zz-invalid", Queue: "q"},
			},
		},
		{
			// Stage 3: reached only with a usable metadata store; file-service with
			// no base URL fails the checkpoint build.
			name: "checkpoint after a live metadata store",
			cfg: &config.Config{
				MetaStore: config.MetaStoreInMemory,
				BlobStore: config.BlobStoreFileService,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cfg.Fanout == config.FanoutRedis && tc.name != "broadcaster" {
				// Give the later cases a REAL redis so the broadcaster genuinely opens
				// and the cleanup has something to close.
				tc.cfg.Redis = config.RedisConfig{URL: "redis://" + startMiniredis(t)}
			}

			deps, cleanup, err := buildDeps(tc.cfg, zap.NewNop())
			if err == nil {
				t.Fatal("buildDeps must fail when a stage cannot be built")
			}
			if cleanup != nil {
				t.Fatal("a failed buildDeps must not hand back a cleanup; it has already run its own, and calling it twice would double-close")
			}
			if deps.Metadata != nil || deps.Broadcaster != nil {
				t.Fatal("a failed buildDeps must return zero Deps, not a half-wired set")
			}
		})
	}
}

// TestBuildDepsWiresTheStandaloneDefaults is the success path, so the failures
// above cannot pass against a buildDeps that never succeeds.
func TestBuildDepsWiresTheStandaloneDefaults(t *testing.T) {
	deps, cleanup, err := buildDeps(&config.Config{
		Fanout:    config.FanoutInMemory,
		MetaStore: config.MetaStoreInMemory,
		BlobStore: config.BlobStoreInline,
		AuthMode:  config.AuthModeOpen,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	if cleanup == nil {
		t.Fatal("a successful buildDeps must return a cleanup")
	}
	t.Cleanup(cleanup)

	if deps.Broadcaster == nil || deps.Metadata == nil || deps.Checkpoint == nil || deps.Auth == nil || deps.AuthZ == nil {
		t.Fatalf("buildDeps left a dependency unwired: %+v", deps)
	}
}

// startMiniredis runs an in-process redis and returns its address.
func startMiniredis(t *testing.T) string {
	t.Helper()
	return miniredis.RunT(t).Addr()
}
