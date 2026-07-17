package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dz3ka/payment-rail/internal/signer"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// Test fixture: one key ("hot") bound to chain 1 with a spend ceiling. The
// happy-path value sits under the limit; the over-limit case pushes above it.
const (
	testKeyID      = "hot"
	testChainID    = 1
	testSpendLimit = "1000000000" // 1e9 wei, decimal per the manifest contract
)

// testRecipient is a fixed non-zero destination: any non-zero 20-byte address
// passes the domain's contract-creation guard.
var testRecipient = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

// newTestClient builds the full adapter stack over an in-memory bufconn: an
// ephemeral key on disk (mode 0600) behind a temp manifest, LoadKeyring, a
// *signer.Signer, the gRPC Server, and a connected client. It is fully hermetic
// — no Postgres, no real network, no env — and returns the client plus the
// key's derived address for signature-recovery assertions.
func newTestClient(t *testing.T) (signerpb.SignerServiceClient, common.Address) {
	t.Helper()

	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() = %v", err)
	}
	addr := crypto.PubkeyToAddress(priv.PublicKey)

	// Write the raw hex key with owner-only perms; the loader refuses any file
	// readable by group or world. key_file is relative to the manifest's dir.
	dir := t.TempDir()
	keyHex := hex.EncodeToString(crypto.FromECDSA(priv))
	if err := os.WriteFile(filepath.Join(dir, "hot.key"), []byte(keyHex), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	manifest := fmt.Sprintf(`[{"key_id":%q,"key_file":"hot.key","chain_id":%d,"spend_limit":%q}]`,
		testKeyID, testChainID, testSpendLimit)
	manifestPath := filepath.Join(dir, "keyring.json")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	kr, err := signer.LoadKeyring(manifestPath)
	if err != nil {
		t.Fatalf("LoadKeyring() = %v", err)
	}

	// Register the adapter on an in-memory bufconn server; discard its logs.
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := grpc.NewServer()
	signerpb.RegisterSignerServiceServer(srv, NewServer(signer.NewSigner(kr), discard))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }() // returns ErrServerStopped after Stop
	t.Cleanup(srv.Stop)

	// passthrough:/// keeps the custom dialer authoritative so the client talks
	// to the bufconn listener rather than resolving a real address.
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return signerpb.NewSignerServiceClient(conn), addr
}

// validRequest is a well-formed native (ETH) transfer under the spend limit.
// Error tests mutate a copy to exercise one failure at a time.
func validRequest() *signerpb.SignTransactionRequest {
	return &signerpb.SignTransactionRequest{
		KeyId:                testKeyID,
		ChainId:              testChainID,
		Nonce:                7,
		To:                   testRecipient.Bytes(),
		Value:                big.NewInt(1_000_000).Bytes(),
		GasLimit:             21_000,
		MaxFeePerGas:         big.NewInt(50_000).Bytes(),
		MaxPriorityFeePerGas: big.NewInt(1_000).Bytes(),
	}
}

func TestSignTransaction_HappyPath(t *testing.T) {
	client, addr := newTestClient(t)

	resp, err := client.SignTransaction(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("SignTransaction() = %v, want nil", err)
	}

	// Recover the sender from the broadcast bytes: a valid EIP-1559 signature
	// must recover to exactly the key's address.
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(resp.GetRawTransaction()); err != nil {
		t.Fatalf("UnmarshalBinary(raw_transaction) = %v", err)
	}
	from, err := types.Sender(types.NewLondonSigner(big.NewInt(testChainID)), tx)
	if err != nil {
		t.Fatalf("Sender() = %v", err)
	}
	if from != addr {
		t.Fatalf("recovered sender = %s, want %s", from, addr)
	}
	// resp.from is the 0x-checksummed rendering of the same address.
	if resp.GetFrom() != addr.Hex() {
		t.Fatalf("resp.From = %s, want %s", resp.GetFrom(), addr.Hex())
	}
	// tx_hash must match a hash recomputed from the decoded transaction.
	if got := common.BytesToHash(resp.GetTxHash()); got != tx.Hash() {
		t.Fatalf("resp.TxHash = %s, want %s", got, tx.Hash())
	}
}

func TestSignTransaction_ErrorMapping(t *testing.T) {
	client, _ := newTestClient(t)

	cases := []struct {
		name   string
		mutate func(*signerpb.SignTransactionRequest)
		want   codes.Code
	}{
		// ErrUnknownKey → NotFound.
		{"unknown key_id", func(r *signerpb.SignTransactionRequest) { r.KeyId = "no-such-key" }, codes.NotFound},
		// ErrChainMismatch → InvalidArgument.
		{"chain mismatch", func(r *signerpb.SignTransactionRequest) { r.ChainId = testChainID + 1 }, codes.InvalidArgument},
		// Boundary length reject: a 19-byte destination must not be padded into a
		// valid address.
		{"to wrong length", func(r *signerpb.SignTransactionRequest) { r.To = make([]byte, 19) }, codes.InvalidArgument},
		// Empty to = contract creation; rejected at the boundary (len != 20).
		{"contract creation (empty to)", func(r *signerpb.SignTransactionRequest) { r.To = nil }, codes.InvalidArgument},
		// Boundary length reject: a uint256 field cannot exceed 32 bytes.
		{"value exceeds 32 bytes", func(r *signerpb.SignTransactionRequest) { r.Value = make([]byte, 33) }, codes.InvalidArgument},
		// ErrMalformedTx from the domain: calldata is not the allowlisted shape.
		{"non-allowlisted calldata", func(r *signerpb.SignTransactionRequest) { r.Data = []byte{0x01, 0x02, 0x03, 0x04} }, codes.InvalidArgument},
		// ErrSpendLimitExceeded → ResourceExhausted (2e9 > 1e9 limit).
		{"value over spend limit", func(r *signerpb.SignTransactionRequest) { r.Value = big.NewInt(2_000_000_000).Bytes() }, codes.ResourceExhausted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			tc.mutate(req)

			_, err := client.SignTransaction(context.Background(), req)
			if err == nil {
				t.Fatalf("SignTransaction() = nil error, want code %s", tc.want)
			}
			if got := status.Code(err); got != tc.want {
				t.Fatalf("status.Code(err) = %s, want %s", got, tc.want)
			}
		})
	}
}
