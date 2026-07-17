package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Force the vars empty so getEnv falls through to the documented defaults,
	// regardless of the ambient environment the test runs in.
	t.Setenv("CONDUIT_ENV", "")
	t.Setenv("CONDUIT_LOG_LEVEL", "")
	t.Setenv("CONDUIT_SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("CONDUIT_POSTGRES_DSN", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.Env != "dev" {
		t.Errorf("Env = %q, want %q", cfg.Env, "dev")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 10*time.Second)
	}
	wantDSN := "postgres://conduit:conduit@localhost:5432/conduit?sslmode=disable"
	if cfg.DatabaseURL != wantDSN {
		t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, wantDSN)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("CONDUIT_ENV", "prod")
	t.Setenv("CONDUIT_SHUTDOWN_TIMEOUT_SECONDS", "30")
	t.Setenv("CONDUIT_POSTGRES_DSN", "postgres://user:pass@db:5432/other?sslmode=require")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}
	if cfg.Env != "prod" {
		t.Errorf("Env = %q, want %q", cfg.Env, "prod")
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, 30*time.Second)
	}
	wantDSN := "postgres://user:pass@db:5432/other?sslmode=require"
	if cfg.DatabaseURL != wantDSN {
		t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, wantDSN)
	}
}

func TestLoadInvalidTimeout(t *testing.T) {
	t.Setenv("CONDUIT_SHUTDOWN_TIMEOUT_SECONDS", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("Load() = nil error, want parse error for invalid timeout")
	}
}
