// Command api serves Conduit's external REST payments API: create, get, list,
// and cancel payments, with client-supplied idempotency keys on create.
//
// It rides the payments service (which owns the ledger transaction) and an
// idempotency store; main stays thin — it opens the pool, wires the routes, and
// runs the HTTP server with signal-driven graceful shutdown. Handlers,
// middleware, and wire DTOs live in sibling files in this package.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/payments"
	"github.com/dz3ka/payment-rail/internal/service"
)

// listenAddr is the API's bind address. It is fixed rather than configurable
// because nothing else consumes it yet; promote it to config when a second
// deployment needs a different port.
const listenAddr = ":8080"

func main() {
	service.Run("api", run)
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	sqlDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("api: open database: %w", err)
	}
	defer sqlDB.Close()
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("api: ping database: %w", err)
	}

	svc := payments.NewService(sqlDB, log)
	idem := payments.NewIdempotencyStore(sqlDB)
	// The sweeper blocks until ctx is done, so it owns its own goroutine; it
	// bounds the idempotency table without a separate cron.
	go idem.RunSweeper(ctx, 1*time.Hour, 24*time.Hour, log)

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: newServer(svc, idem, log).routes(),
		// Timeouts bound a slow or hostile client on this money-moving endpoint.
		// ReadHeaderTimeout closes the classic slowloris hole; ReadTimeout caps a
		// client that dribbles the body byte-by-byte (the per-request body size is
		// separately capped by MaxBytesReader in the idempotency middleware);
		// WriteTimeout bounds a slow reader. MaxHeaderBytes caps header memory.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	// errCh carries a fatal listen error so an early bind failure surfaces
	// instead of hanging on ctx.Done(); a clean Shutdown yields ErrServerClosed,
	// which is not an error.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	log.Info("api listening", "addr", listenAddr)

	select {
	case err := <-errCh:
		return fmt.Errorf("api: serve: %w", err)
	case <-ctx.Done():
	}

	// Drain in-flight requests within the shutdown budget; a fresh context is
	// used because ctx is already canceled.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("api: shutdown: %w", err)
	}
	return nil
}
