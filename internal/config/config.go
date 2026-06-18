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
	"net"
	"net/url"
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
	// MetaStoreInMemory keeps the document index in-process — the zero-dependency
	// standalone default (boots with no bus or DB, SC-012).
	MetaStoreInMemory MetaStoreMode = "inmemory"
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
	// Auth holds the handshake auth settings shared by every auth mode (the
	// request header the WS handshake reads the identity token from).
	Auth AuthConfig

	// Redis holds the redis fan-out settings (FANOUT_MODE=redis).
	Redis RedisConfig
	// RabbitMQ holds the metadata-bus settings (METADATA_STORE=rabbitmq).
	RabbitMQ RabbitMQConfig
	// Postgres holds the metadata-DB settings (METADATA_STORE=postgres).
	Postgres PostgresConfig
	// FileService holds the file-service blob settings (BLOB_STORE=file-service).
	FileService FileServiceConfig
	// S3 holds the S3 blob settings (BLOB_STORE=s3).
	S3 S3Config
	// LocalBlobRoot is the local blob root directory (BLOB_STORE=local).
	LocalBlobRoot string
	// AuthZEval holds the authzeval settings (AUTH_MODE=authzeval).
	AuthZEval AuthZEvalConfig
	// Limits holds the configurable enforcement bounds + presence cadences
	// (FR-014/FR-024, epic R9 defaults, OPEN-4).
	Limits LimitsConfig
}

// LimitsConfig holds the Wave-3 enforcement/presence tunables, all overridable by
// environment variable with epic R9 defaults (OPEN-4). Zero on a limit field
// disables that limit; a zero cadence disables that sweep.
type LimitsConfig struct {
	// MaxDocBytes rejects an update growing the encoded snapshot past this size
	// (MAX_DOC_BYTES, default 32 MiB).
	MaxDocBytes int
	// MaxConnsPerRoom caps concurrent connections per room (MAX_CONNS_PER_ROOM,
	// default 50; refined per-document by metadata maxCollaborators when known).
	MaxConnsPerRoom int
	// UpdateRatePerSec is the per-connection token-bucket refill rate
	// (UPDATE_RATE_PER_SEC, default 50 msg/s).
	UpdateRatePerSec int
	// UpdateBurst is the token-bucket depth (UPDATE_BURST, default = rate).
	UpdateBurst int
	// CollaboratorInactivitySeconds downgrades an idle collaborator to viewer
	// (COLLABORATOR_INACTIVITY_SECONDS, default 120s; 0 disables).
	CollaboratorInactivitySeconds int
	// ContributionWindowSeconds is the contribution-metric flush cadence
	// (CONTRIBUTION_WINDOW_SECONDS, default 60s; 0 disables).
	ContributionWindowSeconds int
}

// RedisConfig configures the redis fan-out broadcaster.
type RedisConfig struct {
	// URL is the redis:// connection string (REDIS_URL).
	URL string
}

// RabbitMQConfig configures the metadata bus.
type RabbitMQConfig struct {
	// URL is the amqp:// connection string (assembled from RABBITMQ_*).
	URL string
	// Queue is the Alkemio server collaboration metastore RPC queue
	// (RABBITMQ_QUEUE) — the save/fetch request queue the server's metastore
	// responder consumes.
	Queue string
	// LifecycleQueue is the SEPARATE queue the document lifecycle consumer binds
	// (LIFECYCLE_QUEUE, default alkemio-collaboration-lifecycle). It MUST NOT be the
	// metastore RPC queue: RabbitMQ round-robins a queue across its consumers, so
	// sharing one queue would let the lifecycle consumer steal metastore
	// fetch/save RPCs and silently drop them (memo joins then time out). Giving the
	// lifecycle consumer its own queue keeps the two consumers independent.
	LifecycleQueue string
}

// DefaultLifecycleQueue is the default queue name the document lifecycle consumer
// binds when LIFECYCLE_QUEUE is unset — distinct from the metastore RPC queue so
// the two consumers never share (and round-robin-steal) a queue.
const DefaultLifecycleQueue = "alkemio-collaboration-lifecycle"

// PostgresConfig configures the standalone metadata DB.
type PostgresConfig struct {
	// DSN is the postgres:// connection string (assembled from ALKEMIO_DATABASE_*).
	DSN string
}

// FileServiceConfig configures the file-service blob backend.
type FileServiceConfig struct {
	BaseURL         string
	StorageBucketID string
	AuthorizationID string
	MaxUploadSize   int64
}

// S3Config configures the S3 blob backend.
type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	KeyPrefix       string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
}

// AuthConfig holds the handshake auth settings common to both auth modes.
type AuthConfig struct {
	// TokenHeader is the request header the WS handshake reads the Alkemio
	// identity token from (AUTH_TOKEN_HEADER, default "Authorization"). The
	// Alkemio deployment terminates auth at the gateway and forwards the resolved
	// actor id in a header (e.g. X-Alkemio-Actor-Id), so it sets this to that
	// header; standalone/open mode keeps the bearer-style Authorization default.
	TokenHeader string
}

// DefaultAuthTokenHeader is the request header the WS handshake reads the
// identity token from when AUTH_TOKEN_HEADER is unset — the bearer-style default
// the standalone/open mode uses.
const DefaultAuthTokenHeader = "Authorization"

// AuthZEvalConfig configures the authzeval auth backend.
type AuthZEvalConfig struct {
	ServiceURL              string
	BreakerFailureThreshold int
	BreakerTimeoutSeconds   int
	BreakerHalfOpenMaxReqs  int
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

	metaStore, err := parseMetaStore(getenv("METADATA_STORE", string(MetaStoreInMemory)))
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

	cfg := &Config{
		Port:      port,
		Fanout:    fanout,
		MetaStore: metaStore,
		BlobStore: blobStore,
		AuthMode:  authMode,
		Auth: AuthConfig{
			TokenHeader: getenv("AUTH_TOKEN_HEADER", DefaultAuthTokenHeader),
		},
	}

	// Populate + fail-fast validate the settings for whichever non-default
	// adapter each selection asks for (constitution §XV: no half-configured
	// runs). The standalone defaults (inmemory / inline / open) need nothing.
	if err := loadAdapterConfig(cfg); err != nil {
		return nil, err
	}

	limits, err := loadLimitsConfig()
	if err != nil {
		return nil, err
	}
	cfg.Limits = limits
	return cfg, nil
}

// Default limit/presence values (epic R9, OPEN-4) — kept in sync with the domain
// service.DefaultRoomConfig so config and core agree on the standalone defaults.
const (
	defaultMaxDocBytes               = 32 << 20 // 32 MiB
	defaultMaxConnsPerRoom           = 50
	defaultUpdateRatePerSec          = 50
	defaultCollaboratorInactivitySec = 120
	defaultContributionWindowSec     = 60
)

// loadLimitsConfig reads the Wave-3 enforcement/presence tunables, applying the
// epic R9 defaults and failing fast on a negative value (a negative limit is a
// configuration error, not a disable — use 0 to disable).
func loadLimitsConfig() (LimitsConfig, error) {
	lc := LimitsConfig{
		MaxDocBytes:                   getenvInt("MAX_DOC_BYTES", defaultMaxDocBytes),
		MaxConnsPerRoom:               getenvInt("MAX_CONNS_PER_ROOM", defaultMaxConnsPerRoom),
		UpdateRatePerSec:              getenvInt("UPDATE_RATE_PER_SEC", defaultUpdateRatePerSec),
		UpdateBurst:                   getenvInt("UPDATE_BURST", 0),
		CollaboratorInactivitySeconds: getenvInt("COLLABORATOR_INACTIVITY_SECONDS", defaultCollaboratorInactivitySec),
		ContributionWindowSeconds:     getenvInt("CONTRIBUTION_WINDOW_SECONDS", defaultContributionWindowSec),
	}
	for name, v := range map[string]int{
		"MAX_DOC_BYTES":                   lc.MaxDocBytes,
		"MAX_CONNS_PER_ROOM":              lc.MaxConnsPerRoom,
		"UPDATE_RATE_PER_SEC":             lc.UpdateRatePerSec,
		"UPDATE_BURST":                    lc.UpdateBurst,
		"COLLABORATOR_INACTIVITY_SECONDS": lc.CollaboratorInactivitySeconds,
		"CONTRIBUTION_WINDOW_SECONDS":     lc.ContributionWindowSeconds,
	} {
		if v < 0 {
			return LimitsConfig{}, fmt.Errorf("%s must be >= 0 (0 disables)", name)
		}
	}
	return lc, nil
}

// loadAdapterConfig fills in and validates the backend-specific settings for the
// selected adapters, dispatching to one loader per port so each stays small and
// independently testable.
func loadAdapterConfig(cfg *Config) error {
	if err := loadFanoutConfig(cfg); err != nil {
		return err
	}
	if err := loadMetaStoreConfig(cfg); err != nil {
		return err
	}
	if err := loadBlobStoreConfig(cfg); err != nil {
		return err
	}
	return loadAuthConfig(cfg)
}

func loadFanoutConfig(cfg *Config) error {
	if cfg.Fanout != FanoutRedis {
		return nil
	}
	cfg.Redis.URL = os.Getenv("REDIS_URL")
	if cfg.Redis.URL == "" {
		return fmt.Errorf("FANOUT_MODE=redis requires REDIS_URL")
	}
	return nil
}

func loadMetaStoreConfig(cfg *Config) error {
	switch cfg.MetaStore {
	case MetaStoreRabbitMQ:
		cfg.RabbitMQ.URL = rabbitURL()
		cfg.RabbitMQ.Queue = getenv("RABBITMQ_QUEUE", "")
		if cfg.RabbitMQ.Queue == "" {
			return fmt.Errorf("METADATA_STORE=rabbitmq requires RABBITMQ_QUEUE")
		}
		// The lifecycle consumer gets its OWN queue, never the metastore RPC queue
		// (sharing one queue round-robin-steals fetch/save RPCs). Reject an explicit
		// LIFECYCLE_QUEUE that collides with RABBITMQ_QUEUE rather than silently
		// re-introducing the shared-queue bug.
		cfg.RabbitMQ.LifecycleQueue = getenv("LIFECYCLE_QUEUE", DefaultLifecycleQueue)
		if cfg.RabbitMQ.LifecycleQueue == cfg.RabbitMQ.Queue {
			return fmt.Errorf("LIFECYCLE_QUEUE must differ from RABBITMQ_QUEUE (%q) — a shared queue round-robin-steals metastore RPCs", cfg.RabbitMQ.Queue)
		}
	case MetaStorePostgres:
		cfg.Postgres.DSN = postgresDSN()
		if cfg.Postgres.DSN == "" {
			return fmt.Errorf("METADATA_STORE=postgres requires ALKEMIO_DATABASE_* (host/name/user)")
		}
	}
	return nil
}

func loadBlobStoreConfig(cfg *Config) error {
	switch cfg.BlobStore {
	case BlobStoreFileService:
		cfg.FileService = FileServiceConfig{
			BaseURL:         os.Getenv("FILE_SERVICE_URL"),
			StorageBucketID: os.Getenv("FILE_SERVICE_STORAGE_BUCKET_ID"),
			AuthorizationID: os.Getenv("FILE_SERVICE_AUTHORIZATION_ID"),
			MaxUploadSize:   getenvInt64("MAX_UPLOAD_SIZE", 0),
		}
		if cfg.FileService.BaseURL == "" || cfg.FileService.StorageBucketID == "" || cfg.FileService.AuthorizationID == "" {
			return fmt.Errorf("BLOB_STORE=file-service requires FILE_SERVICE_URL, FILE_SERVICE_STORAGE_BUCKET_ID, FILE_SERVICE_AUTHORIZATION_ID")
		}
	case BlobStoreS3:
		cfg.S3 = S3Config{
			Bucket:          os.Getenv("S3_BUCKET"),
			Region:          os.Getenv("S3_REGION"),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
			KeyPrefix:       os.Getenv("S3_KEY_PREFIX"),
			AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
			UsePathStyle:    os.Getenv("S3_USE_PATH_STYLE") == "true",
		}
		if cfg.S3.Bucket == "" || (cfg.S3.Region == "" && cfg.S3.Endpoint == "") {
			return fmt.Errorf("BLOB_STORE=s3 requires S3_BUCKET and S3_REGION (or S3_ENDPOINT)")
		}
	case BlobStoreLocal:
		cfg.LocalBlobRoot = os.Getenv("LOCAL_BLOB_ROOT")
		if cfg.LocalBlobRoot == "" {
			return fmt.Errorf("BLOB_STORE=local requires LOCAL_BLOB_ROOT")
		}
	}
	return nil
}

func loadAuthConfig(cfg *Config) error {
	if cfg.AuthMode != AuthModeAuthZEval {
		return nil
	}
	cfg.AuthZEval = AuthZEvalConfig{
		ServiceURL:              os.Getenv("AUTH_SERVICE_URL"),
		BreakerFailureThreshold: getenvInt("AUTH_BREAKER_FAILURE_THRESHOLD", 3),
		BreakerTimeoutSeconds:   getenvInt("AUTH_BREAKER_TIMEOUT_SECONDS", 15),
		BreakerHalfOpenMaxReqs:  getenvInt("AUTH_BREAKER_HALF_OPEN_MAX_REQUESTS", 2),
	}
	if cfg.AuthZEval.ServiceURL == "" {
		return fmt.Errorf("AUTH_MODE=authzeval requires AUTH_SERVICE_URL")
	}
	return nil
}

// rabbitURL assembles the amqp:// URL from RABBITMQ_* (the legacy convention),
// or returns RABBITMQ_URL verbatim when set.
func rabbitURL() string {
	if raw := os.Getenv("RABBITMQ_URL"); raw != "" {
		return raw
	}
	host := getenv("RABBITMQ_HOST", "localhost")
	port := getenv("RABBITMQ_PORT", "5672")
	user := getenv("RABBITMQ_USER", "guest")
	pass := getenv("RABBITMQ_PASSWORD", "guest")
	// Build via net/url so a user/password containing reserved characters
	// (@ : / # ?) is percent-escaped rather than producing an ambiguous URL.
	u := url.URL{Scheme: "amqp", User: url.UserPassword(user, pass), Host: net.JoinHostPort(host, port), Path: "/"}
	return u.String()
}

// postgresDSN assembles a postgres DSN from ALKEMIO_DATABASE_* (matching the
// .env.example convention), or returns DATABASE_URL verbatim when set.
func postgresDSN() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	host := os.Getenv("ALKEMIO_DATABASE_HOST")
	name := os.Getenv("ALKEMIO_DATABASE_NAME")
	user := os.Getenv("ALKEMIO_DATABASE_USERNAME")
	if host == "" || name == "" || user == "" {
		return ""
	}
	port := getenv("ALKEMIO_DATABASE_PORT", "5432")
	pass := os.Getenv("ALKEMIO_DATABASE_PASSWORD")
	sslmode := getenv("ALKEMIO_DATABASE_SSLMODE", "disable")
	// Build via net/url so credentials with reserved characters are escaped and
	// the DSN stays well-formed for pgx.ParseConfig.
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + name,
		RawQuery: url.Values{"sslmode": {sslmode}}.Encode(),
	}
	return u.String()
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
	case MetaStoreInMemory, MetaStoreRabbitMQ, MetaStorePostgres:
		return MetaStoreMode(v), nil
	default:
		return "", fmt.Errorf("METADATA_STORE must be one of inmemory, rabbitmq, postgres (got %q)", v)
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

// getenvInt reads an integer env var, falling back to a default when unset or
// unparseable (the breaker tunables are best-effort, not fail-fast).
func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// getenvInt64 reads an int64 env var, falling back to a default when unset or
// unparseable.
func getenvInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
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
