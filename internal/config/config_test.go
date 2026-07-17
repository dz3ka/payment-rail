package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Force the vars empty so getEnv falls through to the documented defaults,
	// regardless of the ambient environment the test runs in.
	t.Setenv("PAYMENT_RAIL_ENV", "")
	t.Setenv("PAYMENT_RAIL_LOG_LEVEL", "")
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("PAYMENT_RAIL_POSTGRES_DSN", "")
	t.Setenv("PAYMENT_RAIL_SIGNER_GRPC_ADDR", "")
	t.Setenv("PAYMENT_RAIL_SIGNER_KEYRING", "")

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
	wantDSN := "postgres://payment_rail:payment_rail@localhost:5432/payment_rail?sslmode=disable"
	if cfg.DatabaseURL != wantDSN {
		t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, wantDSN)
	}
	if cfg.SignerGRPCAddr != "127.0.0.1:9090" {
		t.Errorf("SignerGRPCAddr = %q, want %q", cfg.SignerGRPCAddr, "127.0.0.1:9090")
	}
	if cfg.SignerKeyring != "signer.keyring.json" {
		t.Errorf("SignerKeyring = %q, want %q", cfg.SignerKeyring, "signer.keyring.json")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_ENV", "prod")
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "30")
	t.Setenv("PAYMENT_RAIL_POSTGRES_DSN", "postgres://user:pass@db:5432/other?sslmode=require")
	t.Setenv("PAYMENT_RAIL_SIGNER_GRPC_ADDR", "0.0.0.0:7000")
	t.Setenv("PAYMENT_RAIL_SIGNER_KEYRING", "/etc/payment-rail/keys.json")

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
	if cfg.SignerGRPCAddr != "0.0.0.0:7000" {
		t.Errorf("SignerGRPCAddr = %q, want %q", cfg.SignerGRPCAddr, "0.0.0.0:7000")
	}
	if cfg.SignerKeyring != "/etc/payment-rail/keys.json" {
		t.Errorf("SignerKeyring = %q, want %q", cfg.SignerKeyring, "/etc/payment-rail/keys.json")
	}
}

func TestLoadInvalidTimeout(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("Load() = nil error, want parse error for invalid timeout")
	}
}
