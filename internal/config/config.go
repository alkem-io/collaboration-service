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
	"strings"
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

// AuthMode selects the handshake-AuthN adapter (Wave 5: AuthN is named
// independently of AuthZ — see AuthZMode).
type AuthMode string

const (
	// AuthModeHeader trusts the actor id stamped in the gateway header
	// (option (a), gateway-terminated; the Alkemio prod default). This is the
	// renamed pre-Wave-5 gateway-terminated path — no behavioural change.
	AuthModeHeader AuthMode = "header"
	// AuthModeOIDC validates the handshake credential itself (option (b),
	// direct OIDC validation: BFF cookie session via Redis + Hydra RS256 bearer
	// via JWKS), mirroring the server's forward-auth controller.
	AuthModeOIDC AuthMode = "oidc"
	// AuthModeOpen authenticates everyone anonymously — the zero-dependency
	// standalone default.
	AuthModeOpen AuthMode = "open"

	// authModeLegacyAuthZEval is the RETIRED AUTH_MODE value, accepted as a
	// backward-compat alias for header AuthN + authzeval AuthZ (OPEN-5) so
	// existing deployments are unchanged. It is not a distinct mode — Load maps
	// it to AuthModeHeader + AuthZModeEval.
	authModeLegacyAuthZEval AuthMode = "authzeval"
)

// AuthZMode selects the per-document-AuthZ adapter, independently of AuthN
// (Wave 5). When unset it is derived from AuthMode.
type AuthZMode string

const (
	// AuthZModeEval delegates per-document read/update-content decisions to the
	// authorization-evaluation-service (h2c + gobreaker, fail-closed).
	AuthZModeEval AuthZMode = "authzeval"
	// AuthZModeOpen grants every privilege — the zero-dependency standalone
	// default (AuthZ bypassed).
	AuthZModeOpen AuthZMode = "open"
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
	// AuthMode selects the handshake-AuthN adapter (open default for standalone).
	AuthMode AuthMode
	// AuthZMode selects the per-document-AuthZ adapter, independently of AuthMode
	// (Wave 5). Derived from AuthMode when AUTHZ_MODE is unset.
	AuthZMode AuthZMode
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
	// AuthZEval holds the authzeval settings (AUTHZ_MODE=authzeval).
	AuthZEval AuthZEvalConfig
	// OIDC holds the direct-validation settings (AUTH_MODE=oidc): the BFF
	// cookie-session Redis store and the Hydra JWKS bearer validator.
	OIDC OIDCConfig
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
	// default 50). This is the global fallback; per-document refinement from a
	// document's maxCollaborators is a future enhancement, not yet wired in.
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
	// IdleReleaseSeconds releases an empty room this long after its last member
	// leaves (IDLE_RELEASE_SECONDS, default 30s; 0 releases immediately).
	IdleReleaseSeconds int
	// SaveDebounceMillis is the quiet period after the last edit before a snapshot
	// is persisted (SAVE_DEBOUNCE_MILLIS, default 500ms; 0 disables the periodic
	// debounce so a snapshot is persisted only on idle-release/close).
	SaveDebounceMillis int
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

// OIDCConfig configures the direct-validation handshake-AuthN adapter
// (AUTH_MODE=oidc). The env var names mirror the server's OIDC config (OPEN-7):
// HYDRA_JWKS_URL / HYDRA_ISSUER_URL / BEARER_AUD_ALLOW_LIST / OIDC_SESSION_COOKIE_NAME.
// Each path is INERT when its config is absent: no JWKS URL ⇒ bearer path off;
// no session-Redis URL ⇒ cookie path off. At least one path MUST be enabled.
type OIDCConfig struct {
	// SessionRedisURL is the redis:// store the BFF cookie session is looked up in
	// (SESSION_REDIS_URL, defaulting to REDIS_URL). Empty disables the cookie path.
	SessionRedisURL string
	// SessionCookieName is the BFF session cookie the bare sid is read from
	// (OIDC_SESSION_COOKIE_NAME, default alkemio_session; env-suffixed per
	// environment, e.g. alkemio_session_sandbox).
	SessionCookieName string
	// JWKSURL is the Hydra JWKS endpoint used for RS256 bearer signature
	// validation (HYDRA_JWKS_URL). Empty disables the bearer path.
	JWKSURL string
	// IssuerURL is the expected Hydra token issuer (HYDRA_ISSUER_URL); enforced
	// when set on the bearer path.
	IssuerURL string
	// BearerAudAllowList is the set of acceptable `aud` values on a bearer JWT
	// (BEARER_AUD_ALLOW_LIST, comma-separated). Empty accepts any audience.
	BearerAudAllowList []string
	// ClockSkewSeconds is the JWT clock tolerance (OIDC_CLOCK_SKEW_SECONDS,
	// default 30s, mirroring the server's jose clockTolerance).
	ClockSkewSeconds int
}

// DefaultOIDCSessionCookieName is the BFF session cookie name when
// OIDC_SESSION_COOKIE_NAME is unset — mirrors the server's oidc.cookie.name
// default. Per-env overlays suffix it (alkemio_session_sandbox, …).
const DefaultOIDCSessionCookieName = "alkemio_session"

// defaultOIDCClockSkewSeconds mirrors the server's 30s jose clockTolerance.
const defaultOIDCClockSkewSeconds = 30

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

	// AuthN mode (with the retired `authzeval` value handled as a backward-compat
	// alias) and the independently-selected AuthZ mode (derived from AuthN when
	// AUTHZ_MODE is unset). OPEN-5.
	authMode, authZMode, err := parseAuthModes(
		getenv("AUTH_MODE", string(AuthModeOpen)),
		os.Getenv("AUTHZ_MODE"),
	)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Port:      port,
		Fanout:    fanout,
		MetaStore: metaStore,
		BlobStore: blobStore,
		AuthMode:  authMode,
		AuthZMode: authZMode,
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
	defaultIdleReleaseSec            = 30  // matches service.DefaultRoomConfig().IdleTimeout
	defaultSaveDebounceMillis        = 500 // matches service.DefaultRoomConfig().SaveDebounce
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
		IdleReleaseSeconds:            getenvInt("IDLE_RELEASE_SECONDS", defaultIdleReleaseSec),
		SaveDebounceMillis:            getenvInt("SAVE_DEBOUNCE_MILLIS", defaultSaveDebounceMillis),
	}
	for name, v := range map[string]int{
		"MAX_DOC_BYTES":                   lc.MaxDocBytes,
		"MAX_CONNS_PER_ROOM":              lc.MaxConnsPerRoom,
		"UPDATE_RATE_PER_SEC":             lc.UpdateRatePerSec,
		"UPDATE_BURST":                    lc.UpdateBurst,
		"COLLABORATOR_INACTIVITY_SECONDS": lc.CollaboratorInactivitySeconds,
		"CONTRIBUTION_WINDOW_SECONDS":     lc.ContributionWindowSeconds,
		"IDLE_RELEASE_SECONDS":            lc.IdleReleaseSeconds,
		"SAVE_DEBOUNCE_MILLIS":            lc.SaveDebounceMillis,
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
		// AuthorizationID is OPTIONAL: snapshots are internal blobs whose access
		// is governed by the bucket and the (unauthenticated) internal API, not a
		// per-file authorization_policy row. When empty, the file-service create
		// is sent without an authorizationId and the row's authz column is NULL.
		// A fixed non-empty value MUST NOT be reused across snapshots — file's
		// UNIQUE(authorizationId) would then admit only one row per bucket.
		if cfg.FileService.BaseURL == "" || cfg.FileService.StorageBucketID == "" {
			return fmt.Errorf("BLOB_STORE=file-service requires FILE_SERVICE_URL, FILE_SERVICE_STORAGE_BUCKET_ID")
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

// loadAuthConfig fills + fail-fast-validates the AuthN (oidc) and AuthZ
// (authzeval) backend settings for whichever modes were selected. The header /
// open AuthN and open AuthZ paths need nothing.
func loadAuthConfig(cfg *Config) error {
	if err := loadAuthZEvalConfig(cfg); err != nil {
		return err
	}
	return loadOIDCConfig(cfg)
}

// loadAuthZEvalConfig populates the authzeval settings when AuthZ delegates to
// the authorization-evaluation-service (AUTHZ_MODE=authzeval, including via the
// legacy AUTH_MODE=authzeval alias). Keyed off AuthZMode so AuthZ config is
// independent of the AuthN mode (Wave 5).
func loadAuthZEvalConfig(cfg *Config) error {
	if cfg.AuthZMode != AuthZModeEval {
		return nil
	}
	cfg.AuthZEval = AuthZEvalConfig{
		ServiceURL:              os.Getenv("AUTH_SERVICE_URL"),
		BreakerFailureThreshold: getenvInt("AUTH_BREAKER_FAILURE_THRESHOLD", 3),
		BreakerTimeoutSeconds:   getenvInt("AUTH_BREAKER_TIMEOUT_SECONDS", 15),
		BreakerHalfOpenMaxReqs:  getenvInt("AUTH_BREAKER_HALF_OPEN_MAX_REQUESTS", 2),
	}
	if cfg.AuthZEval.ServiceURL == "" {
		return fmt.Errorf("AUTHZ_MODE=authzeval requires AUTH_SERVICE_URL")
	}
	return nil
}

// loadOIDCConfig populates the direct-validation settings when AUTH_MODE=oidc.
// The session-Redis URL defaults to REDIS_URL (OPEN-7); the JWKS/issuer/audience/
// cookie-name env names mirror the server's OIDC config. Each path is inert when
// its config is absent, but at least one MUST be enabled — an oidc adapter that
// can validate nothing is a misconfiguration (§XV).
func loadOIDCConfig(cfg *Config) error {
	if cfg.AuthMode != AuthModeOIDC {
		return nil
	}
	cfg.OIDC = OIDCConfig{
		// SESSION_REDIS_URL defaults to the fan-out REDIS_URL (single-Redis
		// deployments need no extra config); an isolated session store points it
		// elsewhere. Empty ⇒ cookie path disabled.
		SessionRedisURL:    getenv("SESSION_REDIS_URL", os.Getenv("REDIS_URL")),
		SessionCookieName:  getenv("OIDC_SESSION_COOKIE_NAME", DefaultOIDCSessionCookieName),
		JWKSURL:            os.Getenv("HYDRA_JWKS_URL"),
		IssuerURL:          os.Getenv("HYDRA_ISSUER_URL"),
		BearerAudAllowList: splitAndTrim(os.Getenv("BEARER_AUD_ALLOW_LIST")),
		ClockSkewSeconds:   getenvInt("OIDC_CLOCK_SKEW_SECONDS", defaultOIDCClockSkewSeconds),
	}
	if cfg.OIDC.JWKSURL == "" && cfg.OIDC.SessionRedisURL == "" {
		return fmt.Errorf("AUTH_MODE=oidc requires at least one credential path: HYDRA_JWKS_URL (bearer) and/or SESSION_REDIS_URL|REDIS_URL (cookie session)")
	}
	if cfg.OIDC.ClockSkewSeconds < 0 {
		return fmt.Errorf("OIDC_CLOCK_SKEW_SECONDS must be >= 0")
	}
	return nil
}

// splitAndTrim splits a comma-separated list, trims surrounding whitespace from
// each item, and drops empties. Returns nil for an empty/blank input.
func splitAndTrim(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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

// parseAuthModes resolves the AuthN mode (AUTH_MODE) and the AuthZ mode
// (AUTHZ_MODE) together, applying the Wave-5 split rules (OPEN-5):
//
//   - The retired AUTH_MODE=authzeval value is a backward-compat ALIAS for
//     header AuthN + authzeval AuthZ.
//   - When AUTHZ_MODE is unset it is DERIVED from the AuthN mode: open→open,
//     header/oidc→authzeval.
//   - An explicit AUTHZ_MODE always wins (AuthN and AuthZ select independently).
func parseAuthModes(authRaw, authZRaw string) (AuthMode, AuthZMode, error) {
	var (
		authN      AuthMode
		aliasAuthZ AuthZMode // forced AuthZ from the legacy alias, if any
	)
	switch AuthMode(authRaw) {
	case AuthModeHeader, AuthModeOIDC, AuthModeOpen:
		authN = AuthMode(authRaw)
	case authModeLegacyAuthZEval:
		// Legacy alias: header AuthN + authzeval AuthZ. The alias forces authzeval
		// AuthZ regardless of AUTHZ_MODE being unset (preserving the prior single
		// AUTH_MODE=authzeval behaviour exactly).
		authN = AuthModeHeader
		aliasAuthZ = AuthZModeEval
	default:
		return "", "", fmt.Errorf("AUTH_MODE must be one of header, oidc, open (got %q)", authRaw)
	}

	// Explicit AUTHZ_MODE wins over both the alias and the derivation.
	if authZRaw != "" {
		switch AuthZMode(authZRaw) {
		case AuthZModeEval, AuthZModeOpen:
			return authN, AuthZMode(authZRaw), nil
		default:
			return "", "", fmt.Errorf("AUTHZ_MODE must be one of authzeval, open (got %q)", authZRaw)
		}
	}
	if aliasAuthZ != "" {
		return authN, aliasAuthZ, nil
	}
	return authN, deriveAuthZMode(authN), nil
}

// deriveAuthZMode maps an AuthN mode to its default AuthZ mode when AUTHZ_MODE is
// unset: open AuthN bypasses AuthZ (open); header/oidc delegate to authzeval.
func deriveAuthZMode(authN AuthMode) AuthZMode {
	if authN == AuthModeOpen {
		return AuthZModeOpen
	}
	return AuthZModeEval
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
