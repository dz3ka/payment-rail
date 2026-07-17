// Package config loads configuration common to all Payment Rail services from the
// environment. Individual services extend this as they gain real dependencies
// (Postgres DSN, Kafka brokers, signer address, ...) in later milestones.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds settings shared by every Payment Rail binary.
type Config struct {
	Env             string        // deployment env: "dev" | "staging" | "prod"
	LogLevel        string        // "debug" | "info" | "warn" | "error"
	ShutdownTimeout time.Duration // graceful-shutdown budget per service
	DatabaseURL     string        // Postgres DSN (lib/pq URL form) for the ledger store
	// SignerGRPCAddr defaults to loopback on purpose: slice-1 has no
	// mTLS/caller-auth yet, so the signer must not bind a public interface.
	SignerGRPCAddr string // listen/dial addr for the isolated gRPC signer
	SignerKeyring  string // filesystem path to the signer's key manifest
}

// Load reads configuration from environment variables, applying documented
// defaults. It returns an error (wrapped with %w) rather than panicking so the
// caller controls the failure path.
func Load() (Config, error) {
	cfg := Config{
		Env:             getEnv("PAYMENT_RAIL_ENV", "dev"),
		LogLevel:        getEnv("PAYMENT_RAIL_LOG_LEVEL", "info"),
		ShutdownTimeout: 10 * time.Second,
		DatabaseURL:     getEnv("PAYMENT_RAIL_POSTGRES_DSN", "postgres://payment_rail:payment_rail@localhost:5432/payment_rail?sslmode=disable"),
		SignerGRPCAddr:  getEnv("PAYMENT_RAIL_SIGNER_GRPC_ADDR", "127.0.0.1:9090"),
		SignerKeyring:   getEnv("PAYMENT_RAIL_SIGNER_KEYRING", "signer.keyring.json"),
	}

	if v := os.Getenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS %q: %w", v, err)
		}
		cfg.ShutdownTimeout = time.Duration(secs) * time.Second
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
