// Package service provides the shared bootstrap used by every Conduit binary so
// each cmd/<svc>/main.go stays a thin entrypoint. It wires config, a structured
// logger, and signal-driven graceful shutdown — the skeleton that later
// milestones hang real servers and consumers off of.
package service

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/version"
)

// RunFunc is a service's real work. It receives a context cancelled on
// SIGINT/SIGTERM; it should start its listeners and block on ctx.Done().
type RunFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) error

// Run bootstraps and runs a named service, exiting non-zero on failure.
func Run(name string, run RunFunc) {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "service", name, "err", err)
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(cfg.LogLevel),
	})).With("service", name, "version", version.Version)

	// Context cancels on the first termination signal; a second signal is left
	// to the OS default (hard kill) so a wedged shutdown can still be aborted.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "env", cfg.Env, "build", version.String())
	if err := run(ctx, cfg, log); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}

func logLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
