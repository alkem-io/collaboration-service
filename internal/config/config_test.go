package config

import (
	"fmt"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("FANOUT_MODE", "")
	t.Setenv("METADATA_STORE", "")
	t.Setenv("BLOB_STORE", "")
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
}

func TestLoadRejectsUnknownEnum(t *testing.T) {
	t.Setenv("FANOUT_MODE", "kafka")
	if _, err := Load(); err == nil {
		t.Fatal("Load() with FANOUT_MODE=kafka: expected error, got nil")
	}
}

func TestLoadRejectsBadPort(t *testing.T) {
	t.Setenv("PORT", "70000")
	if _, err := Load(); err == nil {
		t.Fatal("Load() with PORT=70000: expected out-of-range error, got nil")
	}
}

func TestRedisRequiresURL(t *testing.T) {
	t.Setenv("FANOUT_MODE", "redis")
	t.Setenv("REDIS_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("FANOUT_MODE=redis without REDIS_URL: expected error")
	}
}

func TestRedisLoadsURL(t *testing.T) {
	t.Setenv("FANOUT_MODE", "redis")
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
	t.Setenv("BLOB_STORE", "file-service")
	if _, err := Load(); err == nil {
		t.Fatal("BLOB_STORE=file-service without settings: expected error")
	}
}

func TestFileServiceLoadsSettings(t *testing.T) {
	t.Setenv("BLOB_STORE", "file-service")
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

func TestS3RequiresBucketAndRegion(t *testing.T) {
	t.Setenv("BLOB_STORE", "s3")
	t.Setenv("S3_BUCKET", "")
	if _, err := Load(); err == nil {
		t.Fatal("BLOB_STORE=s3 without bucket: expected error")
	}
}

func TestLocalRequiresRoot(t *testing.T) {
	t.Setenv("BLOB_STORE", "local")
	if _, err := Load(); err == nil {
		t.Fatal("BLOB_STORE=local without LOCAL_BLOB_ROOT: expected error")
	}
}

func TestAuthZEvalRequiresServiceURL(t *testing.T) {
	t.Setenv("AUTH_MODE", "authzeval")
	if _, err := Load(); err == nil {
		t.Fatal("AUTH_MODE=authzeval without AUTH_SERVICE_URL: expected error")
	}
}

func TestAuthZEvalLoadsBreakerDefaults(t *testing.T) {
	t.Setenv("AUTH_MODE", "authzeval")
	t.Setenv("AUTH_SERVICE_URL", "http://auth:6060")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthZEval.BreakerFailureThreshold != 3 || cfg.AuthZEval.BreakerTimeoutSeconds != 15 {
		t.Errorf("breaker defaults = %+v", cfg.AuthZEval)
	}
}
