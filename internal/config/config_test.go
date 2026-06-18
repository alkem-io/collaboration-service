package config

import "testing"

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
