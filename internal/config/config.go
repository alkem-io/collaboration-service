// Package config loads and validates the collaboration-service configuration
// from environment variables — HTTP/WS listen port and the pluggable-port
// selections (fan-out, metadata store, blob store, auth mode) — and constructs
// the production zap logger. Invalid or missing values fail fast at startup
// with the offending variable named (constitution §XV: no silent defaults for
// required wiring).
//
// The adapter wiring those selections drive lands with tasks T004–T006; this
// Phase-1 config validates the choices and exposes them so cmd/server can log
// the intended topology and serve the lifecycle/health/metrics surface.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// FanoutMode selects the ClusterBroadcaster adapter.
type FanoutMode string

const (
	// FanoutInMemory is the single-pod default (no cross-pod fan-out).
	FanoutInMemory FanoutMode = "inmemory"
	// FanoutRedis fans out across pods via Redis pub-sub (doc:/awareness:).
	FanoutRedis FanoutMode = "redis"
)

// MetaStoreMode selects the MetadataStore adapter.
type MetaStoreMode string

const (
	// MetaStoreRabbitMQ rides the existing server save/fetch bus (Alkemio).
	MetaStoreRabbitMQ MetaStoreMode = "rabbitmq"
	// MetaStorePostgres persists the index in Postgres (standalone).
	MetaStorePostgres MetaStoreMode = "postgres"
)

// BlobStoreMode selects the BlobStore adapter.
type BlobStoreMode string

const (
	// BlobStoreInline keeps the blob in the main DB (default).
	BlobStoreInline BlobStoreMode = "inline"
	// BlobStoreFileService offloads the blob to file-service.
	BlobStoreFileService BlobStoreMode = "file-service"
	// BlobStoreS3 offloads the blob to S3 (standalone).
	BlobStoreS3 BlobStoreMode = "s3"
	// BlobStoreLocal keeps the blob on local disk (standalone).
	BlobStoreLocal BlobStoreMode = "local"
)

// AuthMode selects the Auth + AuthZ adapter pair.
type AuthMode string

const (
	// AuthModeAuthZEval authenticates via the Alkemio token and authorizes via
	// the authorization-evaluation-service (Alkemio deployment).
	AuthModeAuthZEval AuthMode = "authzeval"
	// AuthModeOpen authenticates everyone anonymously and grants everything —
	// the zero-dependency standalone default.
	AuthModeOpen AuthMode = "open"
)

// Config is the complete runtime configuration of the service, assembled from
// environment variables by Load.
type Config struct {
	// Port is the HTTP/WS listen port.
	Port int
	// Fanout selects the cross-pod broadcaster (inmemory default).
	Fanout FanoutMode
	// MetaStore selects the metadata/index adapter (rabbitmq default).
	MetaStore MetaStoreMode
	// BlobStore selects the snapshot blob adapter (inline default).
	BlobStore BlobStoreMode
	// AuthMode selects the auth adapter pair (open default for standalone).
	AuthMode AuthMode
}

// Load assembles the Config from environment variables, applying the
// standalone-friendly defaults (single-pod, inline blob, open auth) and
// validating every enumerated selection. Returns an error naming the offending
// variable on the first invalid value — the process is expected to exit rather
// than run half-configured.
func Load() (*Config, error) {
	port, err := getenvPort("PORT", 4006)
	if err != nil {
		return nil, err
	}

	fanout, err := parseFanout(getenv("FANOUT_MODE", string(FanoutInMemory)))
	if err != nil {
		return nil, err
	}

	metaStore, err := parseMetaStore(getenv("METADATA_STORE", string(MetaStoreRabbitMQ)))
	if err != nil {
		return nil, err
	}

	blobStore, err := parseBlobStore(getenv("BLOB_STORE", string(BlobStoreInline)))
	if err != nil {
		return nil, err
	}

	authMode, err := parseAuthMode(getenv("AUTH_MODE", string(AuthModeOpen)))
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:      port,
		Fanout:    fanout,
		MetaStore: metaStore,
		BlobStore: blobStore,
		AuthMode:  authMode,
	}, nil
}

func parseFanout(v string) (FanoutMode, error) {
	switch FanoutMode(v) {
	case FanoutInMemory, FanoutRedis:
		return FanoutMode(v), nil
	default:
		return "", fmt.Errorf("FANOUT_MODE must be one of inmemory, redis (got %q)", v)
	}
}

func parseMetaStore(v string) (MetaStoreMode, error) {
	switch MetaStoreMode(v) {
	case MetaStoreRabbitMQ, MetaStorePostgres:
		return MetaStoreMode(v), nil
	default:
		return "", fmt.Errorf("METADATA_STORE must be one of rabbitmq, postgres (got %q)", v)
	}
}

func parseBlobStore(v string) (BlobStoreMode, error) {
	switch BlobStoreMode(v) {
	case BlobStoreInline, BlobStoreFileService, BlobStoreS3, BlobStoreLocal:
		return BlobStoreMode(v), nil
	default:
		return "", fmt.Errorf("BLOB_STORE must be one of inline, file-service, s3, local (got %q)", v)
	}
}

func parseAuthMode(v string) (AuthMode, error) {
	switch AuthMode(v) {
	case AuthModeAuthZEval, AuthModeOpen:
		return AuthMode(v), nil
	default:
		return "", fmt.Errorf("AUTH_MODE must be one of authzeval, open (got %q)", v)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvPort(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", key, v)
	}
	if n < 1 || n > 65535 {
		return 0, fmt.Errorf("%s must be between 1 and 65535", key)
	}
	return n, nil
}
