// Command signer is the network-isolated signing service. It holds private keys,
// signs only well-formed EIP-1559 transactions, and enforces per-key spend
// limits independently of the rest of the system, exposed over a loopback-only
// gRPC endpoint (ADR: no mTLS/caller-auth in slice 1, so it must not bind a
// public interface).
//
// main stays thin, mirroring cmd/api: it loads config, builds the keyring and
// signer, binds the listener, wires the gRPC adapter, and runs the server with
// signal-driven graceful shutdown. The adapter and its proto<->domain mapping
// live in server.go.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
	"github.com/dz3ka/payment-rail/internal/signer"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

func main() {
	service.Run("signer", run)
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// Load the keyring first: with no keys there is nothing to sign, so a missing
	// or malformed manifest must fail the process rather than start a signer that
	// rejects every request. LoadKeyring fails closed on bad permissions or hex.
	kr, err := signer.LoadKeyring(cfg.SignerKeyring)
	if err != nil {
		return fmt.Errorf("signer: load keyring: %w", err)
	}
	svc := signer.NewSigner(kr)

	// Bind before serving so a bad address surfaces here, not asynchronously.
	lis, err := net.Listen("tcp", cfg.SignerGRPCAddr)
	if err != nil {
		return fmt.Errorf("signer: listen on %s: %w", cfg.SignerGRPCAddr, err)
	}

	// Cap the receive size well below gRPC's 4 MB default: a SignTransactionRequest
	// is tiny (its largest field, calldata, is <=68 bytes for the only shapes we
	// sign). A tight bound keeps an attacker-supplied `data` from forcing a large
	// allocation (and its defensive copy) before validation ever rejects it —
	// defense-in-depth on a key-holding service's trust boundary.
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(64 << 10))
	signerpb.RegisterSignerServiceServer(grpcServer, NewServer(svc, log))

	// errCh carries a fatal serve error so an early failure surfaces instead of
	// hanging on ctx.Done(); GracefulStop makes Serve return nil, so a clean
	// shutdown yields no error here.
	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()
	// lis.Addr() reports the concrete bound address (useful when the port is 0).
	log.Info("signer listening", "addr", lis.Addr().String())

	select {
	case err := <-errCh:
		return fmt.Errorf("signer: serve: %w", err)
	case <-ctx.Done():
	}

	// GracefulStop is the gRPC analogue of http.Server.Shutdown: it stops
	// accepting new RPCs and blocks until in-flight ones drain. It takes no
	// context, so we bound it with the same shutdown budget cmd/api uses and fall
	// back to a hard Stop() if a slow client would otherwise wedge the drain.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(cfg.ShutdownTimeout):
		grpcServer.Stop()
		<-stopped
	}
	return nil
}
