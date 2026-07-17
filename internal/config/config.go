// Package config loads configuration common to all Conduit services from the
// environment. Individual services extend this as they gain real dependencies
// (Postgres DSN, Kafka brokers, signer address, ...) in later milestones.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds settings shared by every Conduit binary.
type Config struct {
	Env             string        // deployment env: "dev" | "staging" | "prod"
	LogLevel        string        // "debug" | "info" | "warn" | "error"
	ShutdownTimeout time.Duration // graceful-shutdown budget per service
	DatabaseURL     string        // Postgres DSN (lib/pq URL form) for the ledger store
}

// Load reads configuration from environment variables, applying documented
// defaults. It returns an error (wrapped with %w) rather than panicking so the
// caller controls the failure path.
func Load() (Config, error) {
	cfg := Config{
		Env:             getEnv("CONDUIT_ENV", "dev"),
		LogLevel:        getEnv("CONDUIT_LOG_LEVEL", "info"),
		ShutdownTimeout: 10 * time.Second,
		DatabaseURL:     getEnv("CONDUIT_POSTGRES_DSN", "postgres://conduit:conduit@localhost:5432/conduit?sslmode=disable"),
	}

	if v := os.Getenv("CONDUIT_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse CONDUIT_SHUTDOWN_TIMEOUT_SECONDS %q: %w", v, err)
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
