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
	t.Setenv("PAYMENT_RAIL_CHAIN_RPC_URL", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_ID", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_KEY_ID", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_FROM_ADDRESS", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_USDC_ADDRESS", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_GAS_LIMIT_CAP", "")
	t.Setenv("PAYMENT_RAIL_CHAIN_MAX_FEE_PER_GAS_CAP_WEI", "")

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
	if cfg.ChainRPCURL != "" {
		t.Errorf("ChainRPCURL = %q, want %q", cfg.ChainRPCURL, "")
	}
	if cfg.ChainID != 11155111 {
		t.Errorf("ChainID = %d, want %d", cfg.ChainID, 11155111)
	}
	if cfg.ChainKeyID != "" {
		t.Errorf("ChainKeyID = %q, want %q", cfg.ChainKeyID, "")
	}
	if cfg.ChainFromAddress != "" {
		t.Errorf("ChainFromAddress = %q, want %q", cfg.ChainFromAddress, "")
	}
	if cfg.ChainUSDCAddress != "" {
		t.Errorf("ChainUSDCAddress = %q, want %q", cfg.ChainUSDCAddress, "")
	}
	if cfg.ChainGasLimitCap != 300000 {
		t.Errorf("ChainGasLimitCap = %d, want %d", cfg.ChainGasLimitCap, 300000)
	}
	if cfg.ChainMaxFeePerGasCapWei != "100000000000" {
		t.Errorf("ChainMaxFeePerGasCapWei = %q, want %q", cfg.ChainMaxFeePerGasCapWei, "100000000000")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_ENV", "prod")
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "30")
	t.Setenv("PAYMENT_RAIL_POSTGRES_DSN", "postgres://user:pass@db:5432/other?sslmode=require")
	t.Setenv("PAYMENT_RAIL_SIGNER_GRPC_ADDR", "0.0.0.0:7000")
	t.Setenv("PAYMENT_RAIL_SIGNER_KEYRING", "/etc/payment-rail/keys.json")
	t.Setenv("PAYMENT_RAIL_CHAIN_RPC_URL", "https://sepolia.example/rpc")
	t.Setenv("PAYMENT_RAIL_CHAIN_ID", "1")
	t.Setenv("PAYMENT_RAIL_CHAIN_KEY_ID", "payments-hot")
	t.Setenv("PAYMENT_RAIL_CHAIN_FROM_ADDRESS", "0x1111111111111111111111111111111111111111")
	t.Setenv("PAYMENT_RAIL_CHAIN_USDC_ADDRESS", "0x2222222222222222222222222222222222222222")
	t.Setenv("PAYMENT_RAIL_CHAIN_GAS_LIMIT_CAP", "500000")
	t.Setenv("PAYMENT_RAIL_CHAIN_MAX_FEE_PER_GAS_CAP_WEI", "250000000000")

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
	if cfg.ChainRPCURL != "https://sepolia.example/rpc" {
		t.Errorf("ChainRPCURL = %q, want %q", cfg.ChainRPCURL, "https://sepolia.example/rpc")
	}
	if cfg.ChainID != 1 {
		t.Errorf("ChainID = %d, want %d", cfg.ChainID, 1)
	}
	if cfg.ChainKeyID != "payments-hot" {
		t.Errorf("ChainKeyID = %q, want %q", cfg.ChainKeyID, "payments-hot")
	}
	if cfg.ChainFromAddress != "0x1111111111111111111111111111111111111111" {
		t.Errorf("ChainFromAddress = %q, want %q", cfg.ChainFromAddress, "0x1111111111111111111111111111111111111111")
	}
	if cfg.ChainUSDCAddress != "0x2222222222222222222222222222222222222222" {
		t.Errorf("ChainUSDCAddress = %q, want %q", cfg.ChainUSDCAddress, "0x2222222222222222222222222222222222222222")
	}
	if cfg.ChainGasLimitCap != 500000 {
		t.Errorf("ChainGasLimitCap = %d, want %d", cfg.ChainGasLimitCap, 500000)
	}
	if cfg.ChainMaxFeePerGasCapWei != "250000000000" {
		t.Errorf("ChainMaxFeePerGasCapWei = %q, want %q", cfg.ChainMaxFeePerGasCapWei, "250000000000")
	}
}

func TestLoadInvalidTimeout(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("Load() = nil error, want parse error for invalid timeout")
	}
}

func TestLoadInvalidChainID(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_CHAIN_ID", "notanumber")

	if _, err := Load(); err == nil {
		t.Fatal("Load() = nil error, want parse error for invalid chain id")
	}
}

func TestLoadInvalidGasLimitCap(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_CHAIN_GAS_LIMIT_CAP", "xyz")

	if _, err := Load(); err == nil {
		t.Fatal("Load() = nil error, want parse error for invalid gas limit cap")
	}
}
