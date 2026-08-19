package config

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("HUB_MODE", "")
	t.Setenv("METADATA_STORE", "")
	t.Setenv("CHECKPOINT_STORE", "")
	t.Setenv("AUTH_MODE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with no env: unexpected error %v", err)
	}

	// Standalone-friendly defaults: single binary, zero external deps.
	if cfg.Port != 4006 {
		t.Errorf("default Port = %d, want 4006", cfg.Port)
	}
	if cfg.Fanout != FanoutInMemory {
		t.Errorf("default Fanout = %q, want inmemory", cfg.Fanout)
	}
	if cfg.AuthMode != AuthModeOpen {
		t.Errorf("default AuthMode = %q, want open", cfg.AuthMode)
	}
	if cfg.Auth.TokenHeader != DefaultAuthTokenHeader {
		t.Errorf("default Auth.TokenHeader = %q, want %q", cfg.Auth.TokenHeader, DefaultAuthTokenHeader)
	}
}

// TestAuthTokenHeaderOverride asserts AUTH_TOKEN_HEADER overrides the handshake
// header the WS adapter reads the identity token from — the seam the Alkemio
// deployment uses to point the handshake at the gateway's resolved actor-id
// header (X-Alkemio-Actor-Id) while standalone/open mode keeps Authorization.
func TestAuthTokenHeaderOverride(t *testing.T) {
	t.Setenv("AUTH_MODE", "")
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error %v", err)
	}
	if cfg.Auth.TokenHeader != "X-Alkemio-Actor-Id" {
		t.Errorf("Auth.TokenHeader = %q, want %q", cfg.Auth.TokenHeader, "X-Alkemio-Actor-Id")
	}
}

func TestLoadRejectsUnknownEnum(t *testing.T) {
	t.Setenv("HUB_MODE", "kafka")
	if _, err := Load(); err == nil {
		t.Fatal("Load() with HUB_MODE=kafka: expected error, got nil")
	}
}

// TestLoadRejectsUnknownAdapterSelections asserts every pluggable-port selector
// fails fast on an unrecognised value (§XV: no half-configured runs) — one bad
// value per selector exercises each parse function's reject branch.
func TestLoadRejectsUnknownAdapterSelections(t *testing.T) {
	for _, c := range []struct{ key, val string }{
		{"METADATA_STORE", "cassandra"},
		{"CHECKPOINT_STORE", "gcs"},
		{"AUTH_MODE", "ldap"},
	} {
		t.Run(c.key, func(t *testing.T) {
			// Pin every selector to a known-good value first, then override the one
			// under test with the bad value — so the rejection is attributable to
			// this selector and cannot be a false pass from unrelated ambient env.
			t.Setenv("HUB_MODE", "inmemory")
			t.Setenv("METADATA_STORE", "inmemory")
			t.Setenv("CHECKPOINT_STORE", "inline")
			t.Setenv("AUTH_MODE", "open")
			t.Setenv(c.key, c.val)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() with %s=%s: expected error, got nil", c.key, c.val)
			}
		})
	}
}

func TestLoadRejectsBadPort(t *testing.T) {
	t.Setenv("PORT", "70000")
	if _, err := Load(); err == nil {
		t.Fatal("Load() with PORT=70000: expected out-of-range error, got nil")
	}
}

func TestRedisRequiresURL(t *testing.T) {
	t.Setenv("HUB_MODE", "redis")
	t.Setenv("REDIS_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("HUB_MODE=redis without REDIS_URL: expected error")
	}
}

func TestRedisLoadsURL(t *testing.T) {
	t.Setenv("HUB_MODE", "redis")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Redis.URL != "redis://localhost:6379" {
		t.Errorf("Redis.URL = %q", cfg.Redis.URL)
	}
}

func TestRabbitMQRequiresQueue(t *testing.T) {
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "")
	if _, err := Load(); err == nil {
		t.Fatal("METADATA_STORE=rabbitmq without RABBITMQ_QUEUE: expected error")
	}
}

func TestRabbitMQAssemblesURL(t *testing.T) {
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "alkemio-collaboration")
	t.Setenv("RABBITMQ_HOST", "rmq")
	t.Setenv("RABBITMQ_USER", "u")
	t.Setenv("RABBITMQ_PASSWORD", "p")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantURL := fmt.Sprintf("amqp://%s:%s@rmq:5672/", "u", "p")
	if cfg.RabbitMQ.URL != wantURL {
		t.Errorf("RabbitMQ.URL = %q, want %q", cfg.RabbitMQ.URL, wantURL)
	}
	if cfg.RabbitMQ.Queue != "alkemio-collaboration" {
		t.Errorf("RabbitMQ.Queue = %q", cfg.RabbitMQ.Queue)
	}
}

// TestLifecycleQueueDefaultsToDedicatedQueue proves the lifecycle consumer gets
// its OWN queue by default — distinct from the metadata-store RPC queue — so it never
// round-robin-steals fetch/save RPCs (the shared-queue bug).
func TestLifecycleQueueDefaultsToDedicatedQueue(t *testing.T) {
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "alkemio-collaboration")
	t.Setenv("LIFECYCLE_QUEUE", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RabbitMQ.LifecycleQueue != DefaultLifecycleQueue {
		t.Errorf("RabbitMQ.LifecycleQueue = %q, want default %q", cfg.RabbitMQ.LifecycleQueue, DefaultLifecycleQueue)
	}
	if cfg.RabbitMQ.LifecycleQueue == cfg.RabbitMQ.Queue {
		t.Errorf("lifecycle queue %q must differ from metadata-store queue %q", cfg.RabbitMQ.LifecycleQueue, cfg.RabbitMQ.Queue)
	}
}

// TestLifecycleQueueOverride proves LIFECYCLE_QUEUE overrides the default.
func TestLifecycleQueueOverride(t *testing.T) {
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "alkemio-collaboration")
	t.Setenv("LIFECYCLE_QUEUE", "my-lifecycle-q")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RabbitMQ.LifecycleQueue != "my-lifecycle-q" {
		t.Errorf("RabbitMQ.LifecycleQueue = %q, want %q", cfg.RabbitMQ.LifecycleQueue, "my-lifecycle-q")
	}
}

// TestLifecycleQueueRejectsCollision proves an explicit LIFECYCLE_QUEUE equal to
// RABBITMQ_QUEUE is rejected at load — re-introducing the shared queue is a
// configuration error, not silently accepted.
func TestLifecycleQueueRejectsCollision(t *testing.T) {
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "alkemio-collaboration")
	t.Setenv("LIFECYCLE_QUEUE", "alkemio-collaboration")
	if _, err := Load(); err == nil {
		t.Fatal("LIFECYCLE_QUEUE == RABBITMQ_QUEUE: expected error, got nil")
	}
}

func TestRabbitMQEscapesCredentials(t *testing.T) {
	// A password with reserved URL characters must be percent-escaped so the
	// assembled amqp URL stays well-formed (and parseable by the amqp client).
	t.Setenv("METADATA_STORE", "rabbitmq")
	t.Setenv("RABBITMQ_QUEUE", "q")
	t.Setenv("RABBITMQ_HOST", "rmq")
	t.Setenv("RABBITMQ_USER", "u@ser")
	t.Setenv("RABBITMQ_PASSWORD", "p@ss:w/rd")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	u, err := url.Parse(cfg.RabbitMQ.URL)
	if err != nil {
		t.Fatalf("assembled amqp URL is not parseable: %q (%v)", cfg.RabbitMQ.URL, err)
	}
	if u.User.Username() != "u@ser" {
		t.Errorf("username round-trip = %q", u.User.Username())
	}
	if pw, _ := u.User.Password(); pw != "p@ss:w/rd" {
		t.Errorf("password round-trip = %q", pw)
	}
	if u.Host != "rmq:5672" {
		t.Errorf("host = %q", u.Host)
	}
}

func TestPostgresRequiresDSNParts(t *testing.T) {
	t.Setenv("METADATA_STORE", "postgres")
	if _, err := Load(); err == nil {
		t.Fatal("METADATA_STORE=postgres without ALKEMIO_DATABASE_*: expected error")
	}
}

func TestPostgresAssemblesDSN(t *testing.T) {
	t.Setenv("METADATA_STORE", "postgres")
	t.Setenv("ALKEMIO_DATABASE_HOST", "db")
	t.Setenv("ALKEMIO_DATABASE_NAME", "collab")
	t.Setenv("ALKEMIO_DATABASE_USERNAME", "u")
	t.Setenv("ALKEMIO_DATABASE_PASSWORD", "p")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := fmt.Sprintf("postgres://%s:%s@db:5432/collab?sslmode=disable", "u", "p")
	if cfg.Postgres.DSN != want {
		t.Errorf("Postgres.DSN = %q, want %q", cfg.Postgres.DSN, want)
	}
}

func TestFileServiceRequiresSettings(t *testing.T) {
	t.Setenv("CHECKPOINT_STORE", "file-service")
	if _, err := Load(); err == nil {
		t.Fatal("CHECKPOINT_STORE=file-service without settings: expected error")
	}
}

func TestFileServiceLoadsSettings(t *testing.T) {
	t.Setenv("CHECKPOINT_STORE", "file-service")
	t.Setenv("FILE_SERVICE_URL", "http://fs:4003")
	t.Setenv("FILE_SERVICE_STORAGE_BUCKET_ID", "bucket-uuid")
	t.Setenv("FILE_SERVICE_AUTHORIZATION_ID", "auth-uuid")
	t.Setenv("MAX_UPLOAD_SIZE", "1048576")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.FileService.BaseURL != "http://fs:4003" || cfg.FileService.MaxUploadSize != 1048576 {
		t.Errorf("FileService = %+v", cfg.FileService)
	}
}

// TestMaxUploadSizeRejectsNegative asserts a negative MAX_UPLOAD_SIZE fails fast
// rather than silently disabling the upload cap. The fileservice Put guard is
// `limit > 0`, so a negative value would turn oversize rejection OFF (any-size
// uploads pass) — a safety-limit corruption, not a disable (0 means "use
// file-service's default ceiling").
//
// Non-vacuity: remove the `if maxUpload < 0` guard in loadBlobStoreConfig and this
// test fails — Load returns nil for MAX_UPLOAD_SIZE=-1, admitting an uncapped store.
func TestMaxUploadSizeRejectsNegative(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("CHECKPOINT_STORE", "file-service")
	t.Setenv("FILE_SERVICE_URL", "http://fs:4003")
	t.Setenv("FILE_SERVICE_STORAGE_BUCKET_ID", "bucket-uuid")
	t.Setenv("MAX_UPLOAD_SIZE", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("MAX_UPLOAD_SIZE=-1: expected a fail-fast error (negative disables the cap), got nil")
	}
}

// TestMaxUploadSizeZeroIsAllowed asserts 0 is accepted: it is the documented
// "fall back to file-service's own 32 MiB ceiling" sentinel, not an error.
func TestMaxUploadSizeZeroIsAllowed(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("CHECKPOINT_STORE", "file-service")
	t.Setenv("FILE_SERVICE_URL", "http://fs:4003")
	t.Setenv("FILE_SERVICE_STORAGE_BUCKET_ID", "bucket-uuid")
	t.Setenv("MAX_UPLOAD_SIZE", "0")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("MAX_UPLOAD_SIZE=0 should be accepted (use file-service default): %v", err)
	}
	if cfg.FileService.MaxUploadSize != 0 {
		t.Fatalf("MaxUploadSize = %d, want 0", cfg.FileService.MaxUploadSize)
	}
}

// TestUnsupportedBlobStoreValuesFailStartup is FR-022d.
//
// There are exactly two persistence paths: file-service for production, and the
// in-process store for the test suite and local development. Anything else must
// FAIL startup, because buildCheckpoint answers a value it does not recognise
// with the in-process store — so an unsupported selector brings the service up
// healthy, serving normally, and losing every document on the next restart.
//
// The error must also name the supported values. A bare "invalid value" leaves
// the operator guessing at the exact moment they most need the answer.
//
// Non-vacuity: widen parseBlobStore's accepted set and this fails on the missing
// error; drop the supported names from the message and it fails on the substring
// checks.
func TestUnsupportedBlobStoreValuesFailStartup(t *testing.T) {
	for _, unsupported := range []string{"gdrive", "azure-blob", "memory"} {
		t.Run(unsupported, func(t *testing.T) {
			t.Setenv("CHECKPOINT_STORE", unsupported)
			_, err := Load()
			if err == nil {
				t.Fatalf("CHECKPOINT_STORE=%s must fail startup: an unrecognised selector falls back to the non-durable in-process store, so the service comes up healthy and loses every document on restart", unsupported)
			}
			msg := err.Error()
			if !strings.Contains(msg, "file-service") || !strings.Contains(msg, "inline") {
				t.Fatalf("the error must name the supported values (file-service / inline), got: %s", msg)
			}
		})
	}
}

// TestAuthZEvalRequiresServiceURL covers the DERIVED path: header AuthN with
// AUTHZ_MODE unset resolves AuthZ to authzeval, which cannot run without
// AUTH_SERVICE_URL. The sibling ...ViaAuthZMode covers the EXPLICIT selection.
//
// The error is matched by content, not merely by being non-nil. This test
// previously set AUTH_MODE=authzeval and asserted only err != nil; once that
// retired value was removed, parseAuthModes rejected the mode before the
// service-URL check was ever reached, so it stayed green while proving nothing
// about the requirement it names.
func TestAuthZEvalRequiresServiceURL(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "header")
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")
	t.Setenv("AUTHZ_MODE", "")
	t.Setenv("AUTH_SERVICE_URL", "")
	_, err := Load()
	if err == nil {
		t.Fatal("derived authzeval AuthZ without AUTH_SERVICE_URL: expected error")
	}
	if !strings.Contains(err.Error(), "AUTH_SERVICE_URL") {
		t.Fatalf("error must name the missing setting, got %v", err)
	}
}

func TestAuthZEvalLoadsBreakerDefaults(t *testing.T) {
	t.Setenv("AUTH_MODE", "header")
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")
	t.Setenv("AUTH_SERVICE_URL", "http://auth:6060")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthZEval.BreakerFailureThreshold != 3 || cfg.AuthZEval.BreakerTimeoutSeconds != 15 {
		t.Errorf("breaker defaults = %+v", cfg.AuthZEval)
	}
}

func TestLimitsDefaults(t *testing.T) {
	// Clear any ambient limit overrides so the test asserts the built-in defaults
	// regardless of the runner's environment (getenv treats "" as unset).
	for _, k := range []string{
		"MAX_DOC_BYTES", "MAX_CONNS_PER_ROOM", "UPDATE_RATE_PER_SEC", "UPDATE_BURST",
		"COLLABORATOR_INACTIVITY_SECONDS", "CONTRIBUTION_WINDOW_SECONDS",
		"IDLE_RELEASE_SECONDS", "SAVE_DEBOUNCE_MILLIS",
	} {
		t.Setenv(k, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Limits.MaxDocBytes != 30<<20 {
		t.Errorf("MaxDocBytes = %d, want 30MiB", cfg.Limits.MaxDocBytes)
	}
	if cfg.Limits.MaxConnsPerRoom != 50 || cfg.Limits.UpdateRatePerSec != 50 {
		t.Errorf("limit defaults = %+v", cfg.Limits)
	}
	if cfg.Limits.CollaboratorInactivitySeconds != 120 || cfg.Limits.ContributionWindowSeconds != 60 {
		t.Errorf("presence cadence defaults = %+v", cfg.Limits)
	}
	if cfg.Limits.IdleReleaseSeconds != 30 || cfg.Limits.SaveDebounceMillis != 500 {
		t.Errorf("room cadence defaults = %+v", cfg.Limits)
	}
}

func TestLimitsOverridable(t *testing.T) {
	t.Setenv("MAX_DOC_BYTES", "1048576")
	t.Setenv("MAX_CONNS_PER_ROOM", "8")
	t.Setenv("UPDATE_RATE_PER_SEC", "20")
	t.Setenv("COLLABORATOR_INACTIVITY_SECONDS", "0") // disable
	t.Setenv("IDLE_RELEASE_SECONDS", "0")            // immediate release
	t.Setenv("SAVE_DEBOUNCE_MILLIS", "25")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Limits.MaxDocBytes != 1048576 || cfg.Limits.MaxConnsPerRoom != 8 ||
		cfg.Limits.UpdateRatePerSec != 20 || cfg.Limits.CollaboratorInactivitySeconds != 0 {
		t.Errorf("overridden limits = %+v", cfg.Limits)
	}
	if cfg.Limits.IdleReleaseSeconds != 0 || cfg.Limits.SaveDebounceMillis != 25 {
		t.Errorf("overridden room cadence = %+v", cfg.Limits)
	}
}

func TestLimitsRejectNegative(t *testing.T) {
	t.Setenv("MAX_CONNS_PER_ROOM", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("a negative limit should fail fast")
	}
}

// TestNumericEnvRejectsMalformed asserts a SET-but-unparseable numeric env var
// fails fast rather than silently falling back to its default — a typo in a hard
// limit or safety-sensitive setting must not quietly change runtime behavior.
func TestNumericEnvRejectsMalformed(t *testing.T) {
	cases := []struct {
		key, val string
		// extra env required for the loader that reads the key to run at all.
		extra map[string]string
	}{
		{key: "MAX_DOC_BYTES", val: "not-a-number"},
		{key: "SAVE_DEBOUNCE_MILLIS", val: "12.5"},
		{key: "OIDC_CLOCK_SKEW_SECONDS", val: "abc", extra: map[string]string{ //nolint:gosec // G101: test-fixture env values (URLs/header names), not credentials.
			"AUTH_MODE": "oidc", "AUTH_TOKEN_HEADER": "X-Alkemio-Actor-Id",
			"AUTH_SERVICE_URL": "http://auth:6060",
			"HYDRA_JWKS_URL":   "http://hydra/.well-known/jwks.json",
		}},
		{key: "MAX_UPLOAD_SIZE", val: "ten", extra: map[string]string{
			"CHECKPOINT_STORE":               "file-service",
			"FILE_SERVICE_URL":               "http://files:4000",
			"FILE_SERVICE_STORAGE_BUCKET_ID": "bucket-1",
		}},
		{key: "AUTH_BREAKER_TIMEOUT_SECONDS", val: "soon", extra: map[string]string{ //nolint:gosec // G101: test-fixture env values (URLs/header names), not credentials.
			"AUTH_MODE": "header", "AUTH_TOKEN_HEADER": "X-Alkemio-Actor-Id",
			"AUTHZ_MODE": "authzeval", "AUTH_SERVICE_URL": "http://auth:6060",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			pinKnownGood(t)
			for k, v := range tc.extra {
				t.Setenv(k, v)
			}
			t.Setenv(tc.key, tc.val)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%q: expected a fail-fast parse error, got nil", tc.key, tc.val)
			}
		})
	}
}

// --- Wave 5 (T018.1): AuthN/AuthZ mode split + backward-compat alias ---

// pinKnownGood pins every non-auth selector to a known-good value so an
// auth-mode test cannot false-pass (or false-fail) on unrelated ambient env.
func pinKnownGood(t *testing.T) {
	t.Helper()
	t.Setenv("HUB_MODE", "inmemory")
	t.Setenv("METADATA_STORE", "inmemory")
	t.Setenv("CHECKPOINT_STORE", "inline")
}

// TestDefaultAuthZModeDerivesFromOpen asserts the standalone default — AUTH_MODE
// unset (open) — derives AUTHZ_MODE=open (zero-dependency, AuthZ bypassed).
func TestDefaultAuthZModeDerivesFromOpen(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "")
	t.Setenv("AUTHZ_MODE", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthMode != AuthModeOpen {
		t.Errorf("AuthMode = %q, want open", cfg.AuthMode)
	}
	if cfg.AuthZMode != AuthZModeOpen {
		t.Errorf("AuthZMode = %q, want derived open", cfg.AuthZMode)
	}
}

// TestHeaderModeDerivesAuthZEval asserts AUTH_MODE=header (the renamed
// gateway-terminated AuthN) derives AUTHZ_MODE=authzeval when unset.
func TestHeaderModeDerivesAuthZEval(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "header")
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")
	t.Setenv("AUTHZ_MODE", "")
	t.Setenv("AUTH_SERVICE_URL", "http://auth:6060")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthMode != AuthModeHeader {
		t.Errorf("AuthMode = %q, want header", cfg.AuthMode)
	}
	if cfg.AuthZMode != AuthZModeEval {
		t.Errorf("AuthZMode = %q, want derived authzeval", cfg.AuthZMode)
	}
}

// TestOIDCModeDerivesAuthZEval asserts AUTH_MODE=oidc derives AUTHZ_MODE=authzeval
// when unset (oidc is an Alkemio AuthN strategy, so per-doc authZ still delegates).
func TestOIDCModeDerivesAuthZEval(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "")
	t.Setenv("AUTH_SERVICE_URL", "http://auth:6060")
	t.Setenv("HYDRA_JWKS_URL", "http://hydra/.well-known/jwks.json")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthMode != AuthModeOIDC {
		t.Errorf("AuthMode = %q, want oidc", cfg.AuthMode)
	}
	if cfg.AuthZMode != AuthZModeEval {
		t.Errorf("AuthZMode = %q, want derived authzeval", cfg.AuthZMode)
	}
}

// TestAuthZModeOverrideIndependentOfAuthN asserts AUTHZ_MODE is honoured
// independently — oidc AuthN with an explicit open AuthZ (defense-in-depth
// AuthN without delegating authZ) is a valid combination.
func TestAuthZModeOverrideIndependentOfAuthN(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "http://hydra/.well-known/jwks.json")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthMode != AuthModeOIDC || cfg.AuthZMode != AuthZModeOpen {
		t.Errorf("AuthMode/AuthZMode = %q/%q, want oidc/open", cfg.AuthMode, cfg.AuthZMode)
	}
}

// TestAuthModeRejectsUnknown asserts an unrecognised AUTH_MODE fails fast.
func TestAuthModeRejectsUnknown(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "kerberos")
	if _, err := Load(); err == nil {
		t.Fatal("AUTH_MODE=kerberos: expected error, got nil")
	}
}

// TestAuthZModeRejectsUnknown asserts an unrecognised AUTHZ_MODE fails fast.
func TestAuthZModeRejectsUnknown(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "open")
	t.Setenv("AUTHZ_MODE", "ldap")
	if _, err := Load(); err == nil {
		t.Fatal("AUTHZ_MODE=ldap: expected error, got nil")
	}
}

// TestAuthZEvalRequiresServiceURLViaAuthZMode asserts selecting authzeval AuthZ
// explicitly (AUTHZ_MODE=authzeval) with open AuthN still requires
// AUTH_SERVICE_URL — the authzeval-config requirement now keys off AUTHZ_MODE.
func TestAuthZEvalRequiresServiceURLViaAuthZMode(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "header")
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")
	t.Setenv("AUTHZ_MODE", "authzeval")
	t.Setenv("AUTH_SERVICE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("AUTHZ_MODE=authzeval without AUTH_SERVICE_URL: expected error")
	}
}

// TestHeaderModeRejectsBearerHeader asserts AUTH_MODE=header fails fast unless a
// dedicated gateway-owned actor-id header is configured: leaving AUTH_TOKEN_HEADER
// at the client-controllable default ("Authorization") would let any client stamp
// its own actor id, so it is rejected (the header adapter trusts the value).
func TestHeaderModeRejectsBearerHeader(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "header")
	t.Setenv("AUTH_SERVICE_URL", "http://auth:6060")
	// AUTH_TOKEN_HEADER unset → defaults to Authorization → must be rejected.
	if _, err := Load(); err == nil {
		t.Fatal("AUTH_MODE=header with default Authorization token header: expected error")
	}
	// Explicitly setting it to Authorization (any case) is likewise rejected.
	t.Setenv("AUTH_TOKEN_HEADER", "authorization")
	if _, err := Load(); err == nil {
		t.Fatal("AUTH_MODE=header with AUTH_TOKEN_HEADER=authorization: expected error")
	}
	// A dedicated gateway header is accepted.
	t.Setenv("AUTH_TOKEN_HEADER", "X-Alkemio-Actor-Id")
	if _, err := Load(); err != nil {
		t.Fatalf("AUTH_MODE=header with a gateway header: unexpected error %v", err)
	}
}

// TestOIDCRequiresAtLeastOnePath asserts oidc AuthN with NEITHER a JWKS URL nor a
// session-Redis URL is rejected — an oidc adapter that can validate nothing is a
// misconfiguration (both paths inert ⇒ every credential is unvalidatable).
func TestOIDCRequiresAtLeastOnePath(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "")
	t.Setenv("SESSION_REDIS_URL", "")
	t.Setenv("REDIS_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("AUTH_MODE=oidc with neither JWKS nor session Redis: expected error")
	}
}

// TestOIDCBearerOnlyValid asserts oidc with only a JWKS URL configured loads
// (bearer-only degrade) and mirrors the server's OIDC env names + defaults.
func TestOIDCBearerOnlyValid(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "http://hydra/.well-known/jwks.json")
	t.Setenv("HYDRA_ISSUER_URL", "http://hydra/")
	t.Setenv("BEARER_AUD_ALLOW_LIST", "alkemio-web, synapse-client")
	t.Setenv("SESSION_REDIS_URL", "")
	t.Setenv("REDIS_URL", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OIDC.JWKSURL != "http://hydra/.well-known/jwks.json" {
		t.Errorf("OIDC.JWKSURL = %q", cfg.OIDC.JWKSURL)
	}
	if cfg.OIDC.IssuerURL != "http://hydra/" {
		t.Errorf("OIDC.IssuerURL = %q", cfg.OIDC.IssuerURL)
	}
	// Allow-list is comma-split and whitespace-trimmed.
	if len(cfg.OIDC.BearerAudAllowList) != 2 ||
		cfg.OIDC.BearerAudAllowList[0] != "alkemio-web" ||
		cfg.OIDC.BearerAudAllowList[1] != "synapse-client" {
		t.Errorf("OIDC.BearerAudAllowList = %#v", cfg.OIDC.BearerAudAllowList)
	}
	// Cookie path off (no session Redis); cookie name still defaulted.
	if cfg.OIDC.SessionRedisURL != "" {
		t.Errorf("SessionRedisURL = %q, want empty (cookie path off)", cfg.OIDC.SessionRedisURL)
	}
	if cfg.OIDC.SessionCookieName != DefaultOIDCSessionCookieName {
		t.Errorf("SessionCookieName = %q, want default %q", cfg.OIDC.SessionCookieName, DefaultOIDCSessionCookieName)
	}
}

// TestOIDCSessionRedisDefaultsToRedisURL asserts SESSION_REDIS_URL defaults to
// REDIS_URL when unset (OPEN-7) — a single-Redis deployment needs no extra config.
func TestOIDCSessionRedisDefaultsToRedisURL(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "")
	t.Setenv("SESSION_REDIS_URL", "")
	t.Setenv("REDIS_URL", "redis://shared:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OIDC.SessionRedisURL != "redis://shared:6379" {
		t.Errorf("SessionRedisURL = %q, want fallback to REDIS_URL", cfg.OIDC.SessionRedisURL)
	}
}

// TestOIDCSessionRedisOverridesRedisURL asserts an explicit SESSION_REDIS_URL
// takes precedence over REDIS_URL (isolated session store).
func TestOIDCSessionRedisOverridesRedisURL(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "")
	t.Setenv("SESSION_REDIS_URL", "redis://sessions:6379")
	t.Setenv("REDIS_URL", "redis://fanout:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OIDC.SessionRedisURL != "redis://sessions:6379" {
		t.Errorf("SessionRedisURL = %q, want explicit override", cfg.OIDC.SessionRedisURL)
	}
}

// TestOIDCCookieNameOverride asserts OIDC_SESSION_COOKIE_NAME overrides the
// default (env-suffixed cookie per environment), mirroring the server.
func TestOIDCCookieNameOverride(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "")
	t.Setenv("REDIS_URL", "redis://shared:6379")
	t.Setenv("OIDC_SESSION_COOKIE_NAME", "alkemio_session_sandbox")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OIDC.SessionCookieName != "alkemio_session_sandbox" {
		t.Errorf("SessionCookieName = %q", cfg.OIDC.SessionCookieName)
	}
}

// TestOIDCRejectsNegativeClockSkew asserts a negative OIDC_CLOCK_SKEW_SECONDS
// fails fast (a negative tolerance is a misconfiguration).
func TestOIDCRejectsNegativeClockSkew(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "http://hydra/.well-known/jwks.json")
	t.Setenv("OIDC_CLOCK_SKEW_SECONDS", "-5")
	if _, err := Load(); err == nil {
		t.Fatal("OIDC_CLOCK_SKEW_SECONDS=-5: expected error, got nil")
	}
}

// TestOIDCAudAllowListAllBlankIsEmpty asserts a BEARER_AUD_ALLOW_LIST that is all
// separators/whitespace yields a nil allow-list (splitAndTrim drops empties).
func TestOIDCAudAllowListAllBlankIsEmpty(t *testing.T) {
	pinKnownGood(t)
	t.Setenv("AUTH_MODE", "oidc")
	t.Setenv("AUTHZ_MODE", "open")
	t.Setenv("HYDRA_JWKS_URL", "http://hydra/.well-known/jwks.json")
	t.Setenv("BEARER_AUD_ALLOW_LIST", " , ,  ,")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.OIDC.BearerAudAllowList) != 0 {
		t.Errorf("BearerAudAllowList = %#v, want empty", cfg.OIDC.BearerAudAllowList)
	}
}

// TestMetadataStoreKeyWasNotRenamed guards the deliberate exception. It still
// selects port.MetadataStore — the one port that did not move to a core
// contract — so renaming it would have been churn with a deployment cost and no
// meaning behind it.
func TestMetadataStoreKeyWasNotRenamed(t *testing.T) {
	t.Setenv("METADATA_STORE", "inmemory")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("METADATA_STORE must still be accepted: %v", err)
	}
	if cfg.MetadataStore != MetadataStoreInMemory {
		t.Fatalf("MetadataStore = %q, want inmemory", cfg.MetadataStore)
	}
}

// TestRenamedKeysAreRead asserts the NEW names actually work, so the tests above
// cannot pass against a config that rejects everything.
func TestRenamedKeysAreRead(t *testing.T) {
	t.Setenv("CHECKPOINT_STORE", "inline")
	t.Setenv("HUB_MODE", "inmemory")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with the renamed keys: %v", err)
	}
	if cfg.BlobStore != BlobStoreInline {
		t.Fatalf("CHECKPOINT_STORE was not read: %q", cfg.BlobStore)
	}
	if cfg.Fanout != FanoutInMemory {
		t.Fatalf("HUB_MODE was not read: %q", cfg.Fanout)
	}
}

// TestURLOverridesTakePrecedenceOverComponentParts covers the two "verbatim URL
// wins" branches.
//
// Both backends accept either a full connection URL or host/port/credential
// parts. The URL form exists for deployments that carry a single secret rather
// than five, and it must WIN when set — a config that silently rebuilt the URL
// from empty component defaults would connect to localhost while the operator
// believed they had pointed it at a managed broker or database.
func TestURLOverridesTakePrecedenceOverComponentParts(t *testing.T) {
	t.Run("RABBITMQ_URL", func(t *testing.T) {
		// Credential-shaped only because a real RABBITMQ_URL is; the point of the
		// test is that the URL wins verbatim, not what it contains.
		const brokerURL = "amqp://user:" + "pw" + "@broker.example:5672/vhost" //nolint:gosec // G101: test fixture, not a credential
		t.Setenv("RABBITMQ_URL", brokerURL)
		t.Setenv("RABBITMQ_HOST", "ignored-host")
		t.Setenv("RABBITMQ_QUEUE", "alkemio-collaboration")
		t.Setenv("METADATA_STORE", "rabbitmq")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.RabbitMQ.URL != brokerURL {
			t.Fatalf("RabbitMQ URL = %q; the explicit URL must win over the component parts, or the service connects to the wrong broker while the operator believes otherwise", cfg.RabbitMQ.URL)
		}
	})

	t.Run("DATABASE_URL", func(t *testing.T) {
		const dbURL = "postgres://user:" + "pw" + "@db.example:5432/collab" //nolint:gosec // G101: test fixture, not a credential
		t.Setenv("DATABASE_URL", dbURL)
		t.Setenv("ALKEMIO_DATABASE_HOST", "ignored-host")
		t.Setenv("METADATA_STORE", "postgres")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Postgres.DSN != dbURL {
			t.Fatalf("Postgres DSN = %q; the explicit URL must win over the component parts", cfg.Postgres.DSN)
		}
	})
}

// TestPortAndIntegerParsingRejectNonsense covers the numeric guards.
//
// A port outside 1–65535 or a non-numeric value must fail startup rather than
// fall back to a default: silently substituting one would bind a different port
// from the one the operator configured, and the service would look healthy while
// nothing could reach it.
func TestPortAndIntegerParsingRejectNonsense(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"non-numeric port", "PORT", "eighty"},
		{"port zero", "PORT", "0"},
		{"port above the range", "PORT", "70000"},
		{"non-numeric size", "MAX_UPLOAD_SIZE", "big"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if tc.key == "MAX_UPLOAD_SIZE" {
				t.Setenv("CHECKPOINT_STORE", "file-service")
				t.Setenv("FILE_SERVICE_URL", "http://file-service:4003")
				t.Setenv("FILE_SERVICE_STORAGE_BUCKET_ID", "bucket")
			}
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%q must fail startup rather than silently falling back to a default", tc.key, tc.value)
			}
		})
	}
}
