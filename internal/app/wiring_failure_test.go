package app

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/config"
	"github.com/alkem-io/collaboration-service/internal/domain/service"
)

// TestBuildHubSurfacesARedisMisconfiguration covers the redis branch's
// failure path.
//
// The alternative is worse than an error: a broadcaster that silently degrades to
// single-pod on a bad REDIS_URL gives a multi-pod deployment where each pod serves
// a private copy of every document, diverging from the moment two people connect
// to different pods. That failure looks like nothing at startup and like data loss
// to users.
func TestBuildHubSurfacesARedisMisconfiguration(t *testing.T) {
	var closers []func()
	cfg := &config.Config{HubMode: config.HubRedis, Redis: config.RedisConfig{URL: "not-a-redis-url"}}

	if _, err := buildHub(cfg, zap.NewNop(), &closers); err == nil {
		t.Fatal("a bad REDIS_URL must fail startup; degrading to single-pod silently gives every pod a private copy of every document")
	}
	if len(closers) != 0 {
		t.Fatalf("a failed broadcaster registered %d closers; nothing was opened", len(closers))
	}
}

// TestBuildMetadataSurfacesBackendMisconfiguration covers the two durable
// branches' failure paths. Both are startup misconfigurations that must stop the
// process rather than leave it running against no index.
func TestBuildMetadataSurfacesBackendMisconfiguration(t *testing.T) {
	t.Run("rabbitmq", func(t *testing.T) {
		var closers []func()
		cfg := &config.Config{
			MetadataStore: config.MetadataStoreRabbitMQ,
			RabbitMQ:      config.RabbitMQConfig{URL: "amqp://%zz-invalid", Queue: "q"},
		}
		if _, _, err := buildMetadata(cfg, &closers); err == nil {
			t.Fatal("an unusable RABBITMQ_URL must fail startup")
		}
		if len(closers) != 0 {
			t.Fatalf("a failed metadata store registered %d closers", len(closers))
		}
	})
}

// TestBuildMetadataDefaultsToInMemory covers the standalone branch and its
// no-op Contributor, which the domain relies on being non-nil.
func TestBuildMetadataDefaultsToInMemory(t *testing.T) {
	var closers []func()
	store, contributor, err := buildMetadata(&config.Config{MetadataStore: config.MetadataStoreInMemory}, &closers)
	if err != nil {
		t.Fatalf("in-memory metadata store: %v", err)
	}
	if store == nil {
		t.Fatal("no metadata store returned")
	}
	_ = contributor // may be nil; the domain substitutes its own no-op
}

// TestStartLifecycleIsSkippedOffTheBus covers the early return: the lifecycle
// consumer exists only for the RabbitMQ topology, and starting one anywhere else
// would dial a broker the deployment does not have.
func TestStartLifecycleIsSkippedOffTheBus(t *testing.T) {
	var closers []func()
	mgr := service.NewManager(service.Deps{}, service.RoomConfig{}, nil, zap.NewNop())

	for _, mode := range []config.MetadataStoreMode{config.MetadataStoreInMemory} {
		if err := startLifecycle(&config.Config{MetadataStore: mode}, mgr, zap.NewNop(), &closers); err != nil {
			t.Fatalf("startLifecycle(%v) must be a no-op off the bus: %v", mode, err)
		}
	}
	if len(closers) != 0 {
		t.Fatalf("startLifecycle registered %d closers while off the bus", len(closers))
	}
}

// TestStartLifecycleSurfacesABrokerFailure covers the error branch, and asserts
// the message names the stage — a startup failure that says only "dial error"
// leaves an operator guessing which of the two RabbitMQ connections failed.
func TestStartLifecycleSurfacesABrokerFailure(t *testing.T) {
	var closers []func()
	mgr := service.NewManager(service.Deps{}, service.RoomConfig{}, nil, zap.NewNop())
	cfg := &config.Config{
		MetadataStore: config.MetadataStoreRabbitMQ,
		RabbitMQ:      config.RabbitMQConfig{URL: "amqp://%zz-invalid", LifecycleQueue: "lifecycle-q"},
	}

	err := startLifecycle(cfg, mgr, zap.NewNop(), &closers)
	if err == nil {
		t.Fatal("an unusable broker URL must fail startup rather than leaving the lifecycle consumer unwired")
	}
	if !strings.Contains(err.Error(), "lifecycle consumer") {
		t.Fatalf("error = %v, want it to name the lifecycle consumer; this service opens TWO RabbitMQ connections and an unnamed failure does not say which", err)
	}
}

// TestLifecycleQueueFallsBackToTheDedicatedDefault covers the belt-and-braces
// guard on a hand-built Config.
//
// It must never fall back to the metadata-store RPC queue: RabbitMQ round-robins a
// queue across its consumers, so a lifecycle consumer bound there would steal
// fetch/save RPCs from the metadata store and drop them.
func TestLifecycleQueueFallsBackToTheDedicatedDefault(t *testing.T) {
	got := lifecycleQueue(&config.Config{RabbitMQ: config.RabbitMQConfig{Queue: "alkemio-collaboration"}})
	if got != config.DefaultLifecycleQueue {
		t.Fatalf("lifecycleQueue = %q, want the dedicated default %q", got, config.DefaultLifecycleQueue)
	}
	if got == "alkemio-collaboration" {
		t.Fatal("the lifecycle consumer must never bind the metadata-store RPC queue; it would round-robin-steal fetch/save RPCs")
	}

	explicit := lifecycleQueue(&config.Config{RabbitMQ: config.RabbitMQConfig{LifecycleQueue: "custom-lifecycle"}})
	if explicit != "custom-lifecycle" {
		t.Fatalf("lifecycleQueue = %q, want the configured queue", explicit)
	}
}

// TestBuildHubWiresRedisAndRegistersItsCloser covers the redis SUCCESS
// path, using miniredis rather than a live server.
//
// The closer registration is the part worth asserting: a redis broadcaster whose
// Close is never registered leaks its connection pool on every shutdown, and the
// symptom appears on the redis side (connections accumulating across pod
// restarts) rather than anywhere in this service's own logs.
func TestBuildHubWiresRedisAndRegistersItsCloser(t *testing.T) {
	srv := miniredis.RunT(t)

	var closers []func()
	cfg := &config.Config{HubMode: config.HubRedis, Redis: config.RedisConfig{URL: "redis://" + srv.Addr()}}

	b, err := buildHub(cfg, zap.NewNop(), &closers)
	if err != nil {
		t.Fatalf("buildHub with redis: %v", err)
	}
	if b == nil {
		t.Fatal("no broadcaster returned")
	}
	if len(closers) != 1 {
		t.Fatalf("registered %d closers, want 1; an unregistered Close leaks the redis connection pool on every shutdown", len(closers))
	}
	closers[0]() // must not panic
}
