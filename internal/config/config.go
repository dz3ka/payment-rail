// Package config loads configuration common to all Payment Rail services from the
// environment. Individual services extend this as they gain real dependencies
// (Postgres DSN, Kafka brokers, signer address, ...) in later milestones.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	// Chain adapter (M2): connection details and policy caps for the EVM payment
	// driver. Addresses are plain strings here — the adapter validates them so
	// config stays go-ethereum-free. ChainMaxFeePerGasCapWei is a decimal-wei
	// string the driver parses to *big.Int, keeping config big.Int-free.
	ChainRPCURL             string // JSON-RPC endpoint for the target chain
	ChainID                 uint64 // EIP-155 chain id (default: Ethereum Sepolia)
	ChainKeyID              string // signer key id that authorizes chain payments
	ChainFromAddress        string // sender address the signer key controls
	ChainUSDCAddress        string // USDC token contract address
	ChainGasLimitCap        uint64 // reject intents needing more gas than this
	ChainMaxFeePerGasCapWei string // max fee-per-gas cap, decimal wei
	// Chain watcher (M3): confirmation-tracking knobs for the chainwatcher service.
	// WatcherConfirmations is the depth N a tx must reach to be treated as final;
	// WatcherPollInterval is the cadence the watcher polls the node at.
	WatcherConfirmations uint64        // confirmation depth threshold N (>= 1)
	WatcherPollInterval  time.Duration // poll cadence
	// Outbox relay (M4): transport + cadence for the outboxrelay service, which
	// forwards committed outbox rows to Kafka. KafkaBrokers is the bootstrap broker
	// list; OutboxPollInterval is how often the relay drains unsent rows. Topic and
	// batch size stay as internal/outbox constants, not config.
	KafkaBrokers       []string      // Kafka bootstrap brokers ("host:port,...")
	OutboxPollInterval time.Duration // relay drain cadence
	// Webhook delivery (M4): cadence for the webhookd delivery-worker poll loop.
	// KafkaBrokers is reused; the consumer group id is an internal/webhook constant.
	WebhookPollInterval time.Duration // delivery-worker poll cadence
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

		ChainRPCURL:             getEnv("PAYMENT_RAIL_CHAIN_RPC_URL", ""),
		ChainID:                 11155111,
		ChainKeyID:              getEnv("PAYMENT_RAIL_CHAIN_KEY_ID", ""),
		ChainFromAddress:        getEnv("PAYMENT_RAIL_CHAIN_FROM_ADDRESS", ""),
		ChainUSDCAddress:        getEnv("PAYMENT_RAIL_CHAIN_USDC_ADDRESS", ""),
		ChainGasLimitCap:        300000,
		ChainMaxFeePerGasCapWei: getEnv("PAYMENT_RAIL_CHAIN_MAX_FEE_PER_GAS_CAP_WEI", "100000000000"),

		WatcherConfirmations: 12,
		WatcherPollInterval:  15 * time.Second,

		KafkaBrokers:        splitBrokers(getEnv("PAYMENT_RAIL_KAFKA_BROKERS", "localhost:19092")),
		OutboxPollInterval:  5 * time.Second,
		WebhookPollInterval: 5 * time.Second,
	}

	if v := os.Getenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS %q: %w", v, err)
		}
		cfg.ShutdownTimeout = time.Duration(secs) * time.Second
	}

	if v := os.Getenv("PAYMENT_RAIL_CHAIN_ID"); v != "" {
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_CHAIN_ID %q: %w", v, err)
		}
		cfg.ChainID = id
	}

	if v := os.Getenv("PAYMENT_RAIL_CHAIN_GAS_LIMIT_CAP"); v != "" {
		limit, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_CHAIN_GAS_LIMIT_CAP %q: %w", v, err)
		}
		cfg.ChainGasLimitCap = limit
	}

	if v := os.Getenv("PAYMENT_RAIL_WATCHER_CONFIRMATIONS"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_WATCHER_CONFIRMATIONS %q: %w", v, err)
		}
		cfg.WatcherConfirmations = n
	}

	if v := os.Getenv("PAYMENT_RAIL_WATCHER_POLL_INTERVAL_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_WATCHER_POLL_INTERVAL_SECONDS %q: %w", v, err)
		}
		cfg.WatcherPollInterval = time.Duration(secs) * time.Second
	}

	if v := os.Getenv("PAYMENT_RAIL_OUTBOX_POLL_INTERVAL_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_OUTBOX_POLL_INTERVAL_SECONDS %q: %w", v, err)
		}
		cfg.OutboxPollInterval = time.Duration(secs) * time.Second
	}

	if v := os.Getenv("PAYMENT_RAIL_WEBHOOK_POLL_INTERVAL_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_WEBHOOK_POLL_INTERVAL_SECONDS %q: %w", v, err)
		}
		cfg.WebhookPollInterval = time.Duration(secs) * time.Second
	}

	return cfg, nil
}

// splitBrokers turns a comma-separated broker list into a slice, trimming spaces
// and dropping empty segments so "a:1, ,b:2," yields ["a:1", "b:2"].
func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			brokers = append(brokers, p)
		}
	}
	return brokers
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
