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

// HubMode selects the ClusterBroadcaster adapter.
type HubMode string

const (
	// HubInMemory is the single-pod default (no cross-pod fan-out).
	HubInMemory HubMode = "inmemory"
	// HubRedis fans out across pods via Redis pub-sub (doc:/awareness:).
	HubRedis HubMode = "redis"
)

// MetadataStoreMode selects the MetadataStore adapter.
type MetadataStoreMode string

const (
	// MetadataStoreInMemory keeps the document index in-process — the zero-dependency
	// standalone default (boots with no bus or DB, SC-012).
	MetadataStoreInMemory MetadataStoreMode = "inmemory"
	// MetadataStoreRabbitMQ rides the existing server save/fetch bus (Alkemio).
	MetadataStoreRabbitMQ MetadataStoreMode = "rabbitmq"
)

// CheckpointStoreMode selects the checkpoint-store adapter (CHECKPOINT_STORE).
type CheckpointStoreMode string

const (
	// CheckpointStoreInline keeps the blob in the main DB (default).
	CheckpointStoreInline CheckpointStoreMode = "inline"
	// CheckpointStoreFileService offloads the blob to file-service.
	CheckpointStoreFileService CheckpointStoreMode = "file-service"
)

// AuthMode selects the handshake-AuthN adapter (Wave 5: AuthN is named
// independently of AuthZ — see AuthZMode).
type AuthMode string

const (
	// AuthModeHeader trusts the actor id stamped in the gateway header
	// (option (a), gateway-terminated; the Alkemio prod default).
	AuthModeHeader AuthMode = "header"
	// AuthModeOpen authenticates everyone anonymously — the zero-dependency
	// standalone default.
	AuthModeOpen AuthMode = "open"
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
	// HubMode selects the cross-pod hub. REQUIRED; no default.
	HubMode HubMode
	// MetadataStore selects the metadata/index adapter (rabbitmq default).
	MetadataStore MetadataStoreMode
	// CheckpointStore selects the checkpoint-store adapter. REQUIRED; no default.
	CheckpointStore CheckpointStoreMode
	// AuthMode selects the handshake-AuthN adapter (open default for standalone).
	AuthMode AuthMode
	// AuthZMode selects the per-document-AuthZ adapter, independently of AuthMode
	// (Wave 5). Derived from AuthMode when AUTHZ_MODE is unset.
	AuthZMode AuthZMode
	// Auth holds the handshake auth settings shared by every auth mode (the
	// request header the WS handshake reads the identity token from).
	Auth AuthConfig

	// Redis holds the redis fan-out settings (HUB_MODE=redis).
	Redis RedisConfig
	// RabbitMQ holds the metadata-bus settings (METADATA_STORE=rabbitmq).
	RabbitMQ RabbitMQConfig
	// FileService holds the file-service blob settings (CHECKPOINT_STORE=file-service).
	FileService FileServiceConfig
	// AuthZEval holds the authzeval settings (AUTHZ_MODE=authzeval).
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
	// (MAX_DOC_BYTES, default 30 MiB — deliberately below file-service's 32 MiB
	// rewrite-body cap; see defaultMaxDocBytes).
	MaxDocBytes int
	// MaxConnsPerRoom caps concurrent connections per room (MAX_CONNS_PER_ROOM,
	// default 50). This is the global fallback; per-document refinement from a
	// document's maxCollaborators is a future enhancement, not yet wired in.
	MaxConnsPerRoom int
	// UpdateRatePerSec is the per-connection token-bucket refill rate
	// (UPDATE_RATE_PER_SEC, default 0/off). A non-zero value counts every inbound
	// wire frame, including awareness and ephemeral traffic.
	UpdateRatePerSec int
	// UpdateBurst is the token-bucket depth (UPDATE_BURST, default = rate).
	UpdateBurst int
	// CollaboratorInactivitySeconds downgrades an idle collaborator to viewer
	// (COLLABORATOR_INACTIVITY_SECONDS, default 0/off). The current activity model
	// counts document mutations, not volatile cursor activity.
	CollaboratorInactivitySeconds int
	// ContributionWindowSeconds is the contribution-metric flush cadence
	// (CONTRIBUTION_WINDOW_SECONDS, default 600s; 0 disables).
	ContributionWindowSeconds int
	// IdleReleaseSeconds releases an empty room this long after its last member
	// leaves (IDLE_RELEASE_SECONDS, default 30s; 0 releases immediately).
	IdleReleaseSeconds int
	// SaveDebounceMillis is the interval from the first unsaved edit until a
	// snapshot is persisted (SAVE_DEBOUNCE_MILLIS, default 2000ms). Further edits
	// do not reset it. 0 disables periodic saves, leaving idle-release/close.
	SaveDebounceMillis int
	// FlushFailureThreshold is how many CONSECUTIVE failed flushes a document
	// tolerates before durability escalation tears the room down and discards the
	// unsaved edits (FLUSH_FAILURE_THRESHOLD, default 5; 0 uses the default).
	//
	// It is a tolerance for TRANSIENT backend faults, not a retry budget to be
	// maximised: every additional attempt is another window in which edits keep
	// accumulating with nowhere durable to go. Raising it trades a longer degraded
	// window for fewer disconnects; lowering it does the reverse.
	FlushFailureThreshold int
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
	// Queue is the Alkemio server collaboration metadata-store RPC queue
	// (RABBITMQ_QUEUE) — the save/fetch request queue the server's metadata-store
	// responder consumes.
	Queue string
	// LifecycleQueue is the SEPARATE queue the document lifecycle consumer binds
	// (LIFECYCLE_QUEUE, default alkemio-collaboration-lifecycle). It MUST NOT be the
	// metadata-store RPC queue: RabbitMQ round-robins a queue across its consumers, so
	// sharing one queue would let the lifecycle consumer steal metadata-store
	// fetch/save RPCs and silently drop them (memo joins then time out). Giving the
	// lifecycle consumer its own queue keeps the two consumers independent.
	LifecycleQueue string
}

// DefaultLifecycleQueue is the default queue name the document lifecycle consumer
// binds when LIFECYCLE_QUEUE is unset — distinct from the metadata-store RPC queue so
// the two consumers never share (and round-robin-steal) a queue.
const DefaultLifecycleQueue = "alkemio-collaboration-lifecycle"

// FileServiceConfig configures the file-service blob backend.
type FileServiceConfig struct {
	BaseURL string
	// StorageBucketID is the FALLBACK snapshot bucket (standalone / no-metadata).
	// The normal path uploads each snapshot into the document's OWN bucket,
	// carried per document on the collaboration-fetch metadata.
	StorageBucketID string
	MaxUploadSize   int64
}

// AuthConfig holds the handshake auth settings common to both auth modes.
type AuthConfig struct {
	// ActorIDHeader names the request header the WS handshake reads the
	// gateway-stamped actor id from. It carries an actor UUID, not a token: the
	// `header` adapter trusts the value verbatim AS the actor id.
	//
	// There is NO default: `header` mode requires it explicitly, and `open` mode
	// ignores the credential entirely, so an unset value simply yields an empty
	// credential.
	ActorIDHeader string
}

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

	fanout, err := parseHubMode(os.Getenv("HUB_MODE"))
	if err != nil {
		return nil, err
	}

	metadataStore, err := parseMetadataStore(getenv("METADATA_STORE", string(MetadataStoreInMemory)))
	if err != nil {
		return nil, err
	}

	checkpointStore, err := parseCheckpointStore(os.Getenv("CHECKPOINT_STORE"))
	if err != nil {
		return nil, err
	}

	// AuthN mode and the independently-selected AuthZ mode (derived from AuthN
	// when AUTHZ_MODE is unset). OPEN-5.
	authMode, authZMode, err := parseAuthModes(
		getenv("AUTH_MODE", string(AuthModeOpen)),
		os.Getenv("AUTHZ_MODE"),
	)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Port:            port,
		HubMode:         fanout,
		MetadataStore:   metadataStore,
		CheckpointStore: checkpointStore,
		AuthMode:        authMode,
		AuthZMode:       authZMode,
		Auth: AuthConfig{
			// AUTH_TOKEN_HEADER is the deployed env name and is kept as-is; it
			// supplies the actor-id header NAME. ("Token" is legacy — bearer/token
			// AuthN was removed with the direct-validation adapter.)
			ActorIDHeader: os.Getenv("AUTH_TOKEN_HEADER"),
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
	// MaxDocBytes is capped BELOW file-service's 32 MiB request-body limit on
	// PUT /internal/file/{id}/content, not at it. A document sitting exactly on a
	// 32 MiB budget would encode to slightly more than 32 MiB once v2 framing is
	// added, so the snapshot would be refused by the transport after passing our
	// own budget check — the document would be accepted and then permanently
	// unsaveable. 30 MiB leaves headroom for framing.
	defaultMaxDocBytes               = 30 << 20 // 30 MiB
	defaultMaxConnsPerRoom           = 50
	defaultUpdateRatePerSec          = 0
	defaultCollaboratorInactivitySec = 0
	defaultContributionWindowSec     = 600
	defaultIdleReleaseSec            = 30   // matches service.DefaultRoomConfig().IdleTimeout
	defaultSaveDebounceMillis        = 2000 // matches service.DefaultRoomConfig().SaveDebounce
	defaultFlushFailureThreshold     = 5    // matches service.DefaultRoomConfig().Limits.FlushFailureThreshold
)

// loadLimitsConfig reads the Wave-3 enforcement/presence tunables, applying the
// epic R9 defaults and failing fast on a negative value (a negative limit is a
// configuration error, not a disable — use 0 to disable).
func loadLimitsConfig() (LimitsConfig, error) {
	var lc LimitsConfig
	for _, f := range []struct {
		key string
		def int
		dst *int
	}{
		{"MAX_DOC_BYTES", defaultMaxDocBytes, &lc.MaxDocBytes},
		{"MAX_CONNS_PER_ROOM", defaultMaxConnsPerRoom, &lc.MaxConnsPerRoom},
		{"UPDATE_RATE_PER_SEC", defaultUpdateRatePerSec, &lc.UpdateRatePerSec},
		{"UPDATE_BURST", 0, &lc.UpdateBurst},
		{"COLLABORATOR_INACTIVITY_SECONDS", defaultCollaboratorInactivitySec, &lc.CollaboratorInactivitySeconds},
		{"CONTRIBUTION_WINDOW_SECONDS", defaultContributionWindowSec, &lc.ContributionWindowSeconds},
		{"IDLE_RELEASE_SECONDS", defaultIdleReleaseSec, &lc.IdleReleaseSeconds},
		{"SAVE_DEBOUNCE_MILLIS", defaultSaveDebounceMillis, &lc.SaveDebounceMillis},
		{"FLUSH_FAILURE_THRESHOLD", defaultFlushFailureThreshold, &lc.FlushFailureThreshold},
	} {
		v, err := getenvInt(f.key, f.def)
		if err != nil {
			return LimitsConfig{}, err
		}
		*f.dst = v
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
		"FLUSH_FAILURE_THRESHOLD":         lc.FlushFailureThreshold,
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
	if err := rejectUnsupportedTopology(cfg); err != nil {
		return err
	}
	if err := loadHubConfig(cfg); err != nil {
		return err
	}
	if err := loadMetadataStoreConfig(cfg); err != nil {
		return err
	}
	if err := loadCheckpointStoreConfig(cfg); err != nil {
		return err
	}
	return loadAuthConfig(cfg)
}

// rejectUnsupportedTopology refuses multi-pod fan-out with a durable store.
//
// Every pod serving a document holds its own authoritative copy and flushes the
// WHOLE document on its own schedule. The hub carries edits between them, but
// nothing decides which pod's flush wins: there is no ownership, no fence, and the
// checkpoint store replaces rather than merges. Two pods that briefly diverge — a
// dropped pub/sub message, a partition, a restart — each write a complete state
// over the other's, and the later writer silently discards whatever the earlier
// one had that it never received.
//
// Rejected at startup rather than warned about, because the configuration does not
// prevent the write that loses the data: "read-heavy" is an expectation about
// traffic, not a read-only contract, and one write is enough. A single-pod
// deployment has the supported equivalent (HUB_MODE=inmemory with the same durable
// store), so refusing this pair removes no capability. Multi-pod durable operation
// returns when an ownership mechanism does.
func rejectUnsupportedTopology(cfg *Config) error {
	if cfg.HubMode != HubRedis || cfg.CheckpointStore != CheckpointStoreFileService {
		return nil
	}
	return fmt.Errorf(
		"unsupported topology: HUB_MODE=redis with CHECKPOINT_STORE=file-service has no document ownership mechanism, " +
			"so every pod flushes the whole document and two pods that diverge overwrite each other, " +
			"silently discarding edits the later writer never received; " +
			"supported: a single pod (HUB_MODE=inmemory) with CHECKPOINT_STORE=file-service")
}

func loadHubConfig(cfg *Config) error {
	if cfg.HubMode != HubRedis {
		return nil
	}
	cfg.Redis.URL = os.Getenv("REDIS_URL")
	if cfg.Redis.URL == "" {
		return fmt.Errorf("HUB_MODE=redis requires REDIS_URL")
	}
	return nil
}

func loadMetadataStoreConfig(cfg *Config) error {
	// Only rabbitmq carries adapter settings; `inmemory` (the in-process store)
	// has none. Mirrors loadCheckpointStoreConfig's shape.
	if cfg.MetadataStore == MetadataStoreRabbitMQ {
		cfg.RabbitMQ.URL = rabbitURL()
		cfg.RabbitMQ.Queue = getenv("RABBITMQ_QUEUE", "")
		if cfg.RabbitMQ.Queue == "" {
			return fmt.Errorf("METADATA_STORE=rabbitmq requires RABBITMQ_QUEUE")
		}
		// The lifecycle consumer gets its OWN queue, never the metadata-store RPC queue
		// (sharing one queue round-robin-steals fetch/save RPCs). Reject an explicit
		// LIFECYCLE_QUEUE that collides with RABBITMQ_QUEUE rather than silently
		// re-introducing the shared-queue bug.
		cfg.RabbitMQ.LifecycleQueue = getenv("LIFECYCLE_QUEUE", DefaultLifecycleQueue)
		if cfg.RabbitMQ.LifecycleQueue == cfg.RabbitMQ.Queue {
			return fmt.Errorf("LIFECYCLE_QUEUE must differ from RABBITMQ_QUEUE (%q) — a shared queue round-robin-steals metadata-store RPCs", cfg.RabbitMQ.Queue)
		}
	}
	return nil
}

func loadCheckpointStoreConfig(cfg *Config) error {
	// Only file-service carries adapter settings; `inline` (the in-process store)
	// has none.
	if cfg.CheckpointStore != CheckpointStoreFileService {
		return nil
	}
	maxUpload, err := getenvInt64("MAX_UPLOAD_SIZE", 0)
	if err != nil {
		return err
	}
	// A negative cap is a configuration error, not a disable: the fileservice
	// guard is `limit > 0`, so a negative MAX_UPLOAD_SIZE silently turns the upload
	// cap OFF (uploads of any size pass) rather than rejecting oversize snapshots.
	// Use 0 to mean "fall back to file-service's own ceiling" — reject anything
	// below it, consistently with loadLimitsConfig (§XV: no silent safety-limit
	// corruption).
	if maxUpload < 0 {
		return fmt.Errorf("MAX_UPLOAD_SIZE must be >= 0 (0 uses file-service's default ceiling)")
	}
	cfg.FileService = FileServiceConfig{
		BaseURL:         os.Getenv("FILE_SERVICE_URL"),
		StorageBucketID: os.Getenv("FILE_SERVICE_STORAGE_BUCKET_ID"),
		MaxUploadSize:   maxUpload,
	}
	// No authorizationId is configured: snapshots are internal blobs whose access
	// is governed by the document's own authz and the (unauthenticated) internal
	// API, not a per-file authorization_policy row. The file-service create is sent
	// without an authorizationId so the row's authz column is NULL — file's
	// UNIQUE(authorizationId) permits many NULLs, so every snapshot persists (a
	// reused fixed id would admit only one row per bucket).
	// FILE_SERVICE_STORAGE_BUCKET_ID is the FALLBACK bucket only; the normal path
	// uploads into the document's own bucket (per-document, from the
	// collaboration-fetch metadata).
	if cfg.FileService.BaseURL == "" || cfg.FileService.StorageBucketID == "" {
		return fmt.Errorf("CHECKPOINT_STORE=file-service requires FILE_SERVICE_URL, FILE_SERVICE_STORAGE_BUCKET_ID")
	}
	return nil
}

// loadAuthConfig fills + fail-fast-validates the AuthN (header) and AuthZ
// (authzeval) backend settings for whichever modes were selected. The open AuthN
// and open AuthZ paths need nothing; the header path validates the actor-id
// header is gateway-owned (loadHeaderAuthConfig).
func loadAuthConfig(cfg *Config) error {
	if err := loadHeaderAuthConfig(cfg); err != nil {
		return err
	}
	return loadAuthZEvalConfig(cfg)
}

// loadHeaderAuthConfig fail-fast-validates the gateway-terminated `header` AuthN
// mode (AUTH_MODE=header). The header
// adapter TRUSTS AUTH_TOKEN_HEADER verbatim as the actor id, so that header MUST
// be a dedicated gateway-owned header the client cannot set. The bearer-style
// default ("Authorization") is client-controllable — accepting it would let any
// client stamp its own actor id and impersonate anyone — so header mode requires
// AUTH_TOKEN_HEADER to be set to something other than "Authorization" (e.g.
// X-Alkemio-Actor-Id, the gateway-resolved header), and requires it to be SET —
// there is no default to fall back to. Open mode is unaffected: the open adapter
// ignores the header, so an unset name is correct there.
func loadHeaderAuthConfig(cfg *Config) error {
	if cfg.AuthMode != AuthModeHeader {
		return nil
	}
	if cfg.Auth.ActorIDHeader == "" {
		return fmt.Errorf(
			"AUTH_MODE=header requires AUTH_TOKEN_HEADER to name the dedicated gateway-owned header carrying the actor id (e.g. X-Alkemio-Actor-Id); there is no default",
		)
	}
	if strings.EqualFold(cfg.Auth.ActorIDHeader, "Authorization") {
		return fmt.Errorf(
			"AUTH_MODE=header requires AUTH_TOKEN_HEADER to be a dedicated gateway-owned header (e.g. X-Alkemio-Actor-Id), not the client-controllable %q: the header adapter trusts it as the actor id",
			"Authorization",
		)
	}
	return nil
}

// loadAuthZEvalConfig populates the authzeval settings when AuthZ delegates to
// the authorization-evaluation-service (AUTHZ_MODE=authzeval). Keyed off
// AuthZMode so AuthZ config is
// independent of the AuthN mode (Wave 5).
func loadAuthZEvalConfig(cfg *Config) error {
	if cfg.AuthZMode != AuthZModeEval {
		return nil
	}
	failureThreshold, err := getenvInt("AUTH_BREAKER_FAILURE_THRESHOLD", 3)
	if err != nil {
		return err
	}
	breakerTimeout, err := getenvInt("AUTH_BREAKER_TIMEOUT_SECONDS", 15)
	if err != nil {
		return err
	}
	halfOpenMax, err := getenvInt("AUTH_BREAKER_HALF_OPEN_MAX_REQUESTS", 2)
	if err != nil {
		return err
	}
	cfg.AuthZEval = AuthZEvalConfig{
		ServiceURL:              os.Getenv("AUTH_SERVICE_URL"),
		BreakerFailureThreshold: failureThreshold,
		BreakerTimeoutSeconds:   breakerTimeout,
		BreakerHalfOpenMaxReqs:  halfOpenMax,
	}
	if cfg.AuthZEval.ServiceURL == "" {
		return fmt.Errorf("AUTHZ_MODE=authzeval requires AUTH_SERVICE_URL")
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

// parseHubMode resolves HUB_MODE. It is MANDATORY — see parseCheckpointStore for the
// reasoning, which applies here for the same reason in a weaker form: an absent
// HUB_MODE silently running single-pod is a correctness problem for a deployment
// that believed it had cross-pod fan-out.
func parseHubMode(v string) (HubMode, error) {
	switch HubMode(v) {
	case HubInMemory, HubRedis:
		return HubMode(v), nil
	case "":
		return "", fmt.Errorf("HUB_MODE must be set explicitly to one of inmemory, redis; there is no default")
	default:
		return "", fmt.Errorf("HUB_MODE must be one of inmemory, redis (got %q)", v)
	}
}

func parseMetadataStore(v string) (MetadataStoreMode, error) {
	switch MetadataStoreMode(v) {
	case MetadataStoreInMemory, MetadataStoreRabbitMQ:
		return MetadataStoreMode(v), nil
	default:
		return "", fmt.Errorf("METADATA_STORE must be one of inmemory, rabbitmq (got %q)", v)
	}
}

func parseCheckpointStore(v string) (CheckpointStoreMode, error) {
	switch CheckpointStoreMode(v) {
	case CheckpointStoreInline, CheckpointStoreFileService:
		return CheckpointStoreMode(v), nil
	case "":
		// MANDATORY, with no default, because the only safe default does not exist.
		//
		// Defaulting to inline means a deployment that omits the key — or sets a
		// different one, which is the same thing to os.Getenv — boots healthy on the
		// non-durable in-process store and loses every document on restart. That is
		// silent data loss, and the omission is invisible: nothing in the logs
		// distinguishes "chose inline" from "never said". Defaulting to file-service
		// instead would break every test and local run, and would fail at the first
		// save rather than at boot.
		//
		// So the key is required and local/test configuration says inline out loud.
		// This deliberately gives up zero-CONFIG standalone; zero-DEPENDENCY
		// standalone is unaffected (CHECKPOINT_STORE=inline needs nothing running).
		return "", fmt.Errorf("CHECKPOINT_STORE must be set explicitly to one of inline, file-service; there is no default (inline is the non-durable in-process store for tests and local development, file-service is the durable one)")
	default:
		// FAIL, never fall back. buildCheckpoint answers anything it does not
		// recognise with the IN-PROCESS store, so an unrecognised selector would
		// bring the service up healthy, serving normally, and losing every document
		// on the next restart — exactly the silent default FR-022f forbids. The
		// error names the supported values so the fix travels with the message.
		return "", fmt.Errorf("CHECKPOINT_STORE must be one of inline, file-service (got %q); inline is the non-durable in-process store used by tests and local development, file-service is the durable one", v)
	}
}

// parseAuthModes resolves the AuthN mode (AUTH_MODE) and the AuthZ mode
// (AUTHZ_MODE) together (OPEN-5):
//
//   - When AUTHZ_MODE is unset it is DERIVED from the AuthN mode: open→open,
//     header→authzeval.
//   - An explicit AUTHZ_MODE always wins (AuthN and AuthZ select independently).
func parseAuthModes(authRaw, authZRaw string) (AuthMode, AuthZMode, error) {
	var authN AuthMode
	switch AuthMode(authRaw) {
	case AuthModeHeader, AuthModeOpen:
		authN = AuthMode(authRaw)
	default:
		return "", "", fmt.Errorf("AUTH_MODE must be one of header, open (got %q)", authRaw)
	}

	// Explicit AUTHZ_MODE wins over the derivation.
	if authZRaw != "" {
		switch AuthZMode(authZRaw) {
		case AuthZModeEval, AuthZModeOpen:
			return authN, AuthZMode(authZRaw), nil
		default:
			return "", "", fmt.Errorf("AUTHZ_MODE must be one of authzeval, open (got %q)", authZRaw)
		}
	}
	return authN, deriveAuthZMode(authN), nil
}

// deriveAuthZMode maps an AuthN mode to its default AuthZ mode when AUTHZ_MODE is
// unset: open AuthN bypasses AuthZ (open); header delegates to authzeval.
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

// getenvInt reads an integer env var, falling back to a default when unset. A
// SET-but-malformed value is a configuration error (fail-fast, §XV): these
// helpers back hard limits and safety-sensitive adapter settings (MAX_DOC_BYTES,
// SAVE_DEBOUNCE_MILLIS, the breaker tunables), so a typo
// must not silently fall back to a default and quietly change runtime behavior.
func getenvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", key, v)
	}
	return n, nil
}

// getenvInt64 reads an int64 env var, falling back to a default when unset. As
// with getenvInt, a SET-but-malformed value fails fast rather than silently
// using the default.
func getenvInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", key, v)
	}
	return n, nil
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
