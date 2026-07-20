package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/settlement"
	"github.com/dz3ka/payment-rail/internal/signer"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// This is the headline full-wire, fully-hermetic proof for the EVM chain
// adapter slice: a client-encoded payment request survives real gRPC
// serialization, the real signer domain (real policy + real EIP-1559 signing),
// and broadcast to a real in-process EVM that mines it with a recoverable
// sender. No live network, no Postgres, no env gate.
//
// The stack under test is the WHOLE production path except the one component
// that cannot be imported: cmd/signer's gRPC server adapter lives in *its* own
// package main, so domainSignerServer below MIRRORS cmd/signer/server.go's
// decode/encode (toDomainRequest + mapSignError) faithfully and calls a REAL
// signer.Signer. Everything else is production code: the production signerClient
// (cmd/paymentrailctl/signerclient.go) dials it, and the real evm.Adapter drives
// the whole thing.

const (
	wireKeyID = "hot" // matches evm.Config.KeyID and the keyring manifest key_id
	// Generous ceiling for the happy path (1e12 base units); the concurrent
	// submits sum to a few million, far under it.
	wireSpendLimitHigh = "1000000000000"
	// Tight ceiling (1 USDC) for the rejection path; a 2 USDC transfer exceeds it.
	wireSpendLimitLow = "1000000"
)

var (
	// wireRecipient is the payment's destination: any nonzero 20-byte address
	// clears the domain's contract-creation guard.
	wireRecipient = common.HexToAddress("0x00000000000000000000000000000000000000AB")
	// wireToken is the ERC-20 the adapter routes USDC through. It is a codeless
	// address: a transfer() to an account with no deployed code still estimates
	// gas and mines (it just does nothing on-chain), so no ERC-20 deploy is
	// needed to prove the build/nonce/gas/sign/broadcast wiring end to end.
	wireToken = common.HexToAddress("0x000000000000000000000000000000000000C0DE")
)

// domainSignerServer is an in-test SignerServiceServer that mirrors the real
// gRPC signer adapter (cmd/signer/server.go): it validates byte-lengths at the
// trust boundary, converts the wire message into a signer.SignRequest, calls a
// REAL signer.Signer, and maps the domain's sentinel errors onto gRPC status
// codes. It exists only because the production adapter lives in cmd/signer's
// package main and cannot be imported here; its decode/encode is a faithful copy
// so the test exercises a realistic server boundary, not a stub shortcut.
type domainSignerServer struct {
	signerpb.UnimplementedSignerServiceServer
	signer *signer.Signer
}

// Compile-time proof the in-test server satisfies the generated service contract.
var _ signerpb.SignerServiceServer = (*domainSignerServer)(nil)

func (s *domainSignerServer) SignTransaction(ctx context.Context, req *signerpb.SignTransactionRequest) (*signerpb.SignTransactionResponse, error) {
	domReq, err := mirrorToDomainRequest(req)
	if err != nil {
		return nil, err
	}
	signed, err := s.signer.Sign(ctx, domReq)
	if err != nil {
		return nil, mirrorMapSignError(err)
	}
	return &signerpb.SignTransactionResponse{
		RawTransaction: signed.RawTransaction,
		TxHash:         signed.TxHash.Bytes(),
		From:           signed.From.Hex(), // 0x-prefixed, EIP-55 checksummed
	}, nil
}

// mirrorToDomainRequest reproduces cmd/signer/server.go's toDomainRequest: it
// length-checks the 20-byte destination and the <=32-byte uint256 fields before
// the lossy common.BytesToAddress / big.Int.SetBytes conversions, so a malformed
// wire value is rejected at the boundary rather than silently padded/truncated.
func mirrorToDomainRequest(req *signerpb.SignTransactionRequest) (signer.SignRequest, error) {
	if len(req.GetTo()) != 20 {
		return signer.SignRequest{}, status.Error(codes.InvalidArgument, "malformed transaction: destination address must be 20 bytes")
	}
	value, err := mirrorToUint256(req.GetValue(), "value")
	if err != nil {
		return signer.SignRequest{}, err
	}
	maxFee, err := mirrorToUint256(req.GetMaxFeePerGas(), "max_fee_per_gas")
	if err != nil {
		return signer.SignRequest{}, err
	}
	maxTip, err := mirrorToUint256(req.GetMaxPriorityFeePerGas(), "max_priority_fee_per_gas")
	if err != nil {
		return signer.SignRequest{}, err
	}
	return signer.SignRequest{
		KeyID:                req.GetKeyId(),
		ChainID:              req.GetChainId(),
		Nonce:                req.GetNonce(),
		GasLimit:             req.GetGasLimit(),
		To:                   common.BytesToAddress(req.GetTo()),
		Value:                value,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: maxTip,
		Data:                 req.GetData(),
	}, nil
}

// mirrorToUint256 mirrors server.go's toUint256: a field over 32 bytes cannot be
// a uint256 and is rejected; empty decodes to a non-nil zero via SetBytes.
func mirrorToUint256(b []byte, field string) (*big.Int, error) {
	if len(b) > 32 {
		return nil, status.Errorf(codes.InvalidArgument, "malformed transaction: %s must be at most 32 bytes", field)
	}
	return new(big.Int).SetBytes(b), nil
}

// mirrorMapSignError mirrors server.go's mapSignError: each domain sentinel maps
// to the same gRPC status code the production adapter emits, so the client sees
// a realistic error surface (ResourceExhausted for a spend-limit breach, etc.).
func mirrorMapSignError(err error) error {
	switch {
	case errors.Is(err, signer.ErrUnknownKey):
		return status.Error(codes.NotFound, "unknown key")
	case errors.Is(err, signer.ErrChainMismatch):
		return status.Error(codes.InvalidArgument, "chain_id does not match key")
	case errors.Is(err, signer.ErrMalformedTx):
		return status.Error(codes.InvalidArgument, "malformed transaction")
	case errors.Is(err, signer.ErrSpendLimitExceeded):
		return status.Error(codes.ResourceExhausted, "spend limit exceeded")
	default:
		return status.Error(codes.Internal, "internal signing error")
	}
}

// wireStack is the fully assembled hermetic stack: a real evm.Adapter wired to
// the production signerClient (over an in-memory bufconn to the mirrored signer
// domain) and to go-ethereum's in-process simulated EVM.
type wireStack struct {
	adapter *evm.Adapter
	backend *simulated.Backend
	client  simulated.Client
	from    common.Address
	cfg     evm.Config
	chainID *big.Int
}

// newWireStack builds the whole stack around one ephemeral key bound to the
// simulated backend's own chain id, with the given spend limit. The chain id is
// DERIVED from the running backend (not hardcoded) and reused for both the
// keyring's per-key chain binding and evm.Config.ChainID, so VerifyChainID and
// the signer's replay guard agree.
func newWireStack(ctx context.Context, t *testing.T, spendLimit string) wireStack {
	t.Helper()

	// The signing key: its derived address is both the keyring key's address and
	// the genesis-funded sender the adapter is configured with.
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() = %v", err)
	}
	from := crypto.PubkeyToAddress(priv.PublicKey)

	// Fund the sender generously so gas across several concurrent submits never
	// runs the account dry.
	balance := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18)) // 1000 ETH
	backend := simulated.NewBackend(types.GenesisAlloc{from: {Balance: balance}})
	t.Cleanup(func() { _ = backend.Close() })
	client := backend.Client()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("ChainID() = %v", err)
	}

	// Ephemeral keyring on disk: raw hex key at 0600 (the loader refuses any
	// group/world-accessible file) and a JSON manifest whose key_file is relative
	// to the manifest's own directory (see internal/signer/keyring.go).
	dir := t.TempDir()
	keyHex := hex.EncodeToString(crypto.FromECDSA(priv))
	if err := os.WriteFile(filepath.Join(dir, "hot.key"), []byte(keyHex), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	manifest := fmt.Sprintf(`[{"key_id":%q,"key_file":"hot.key","chain_id":%d,"spend_limit":%q}]`,
		wireKeyID, chainID.Uint64(), spendLimit)
	manifestPath := filepath.Join(dir, "keyring.json")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	kr, err := signer.LoadKeyring(manifestPath)
	if err != nil {
		t.Fatalf("LoadKeyring() = %v", err)
	}

	// Stand the mirrored signer domain up on an in-memory bufconn; discard logs.
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := grpc.NewServer()
	signerpb.RegisterSignerServiceServer(srv, &domainSignerServer{signer: signer.NewSigner(kr)})
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }() // returns ErrServerStopped after Stop
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// The production evm.Signer port: the real signerClient over the wire.
	sc := newSignerClient(signerpb.NewSignerServiceClient(conn))

	cfg := evm.Config{
		KeyID:              wireKeyID,
		ChainID:            chainID.Uint64(),
		From:               from,
		Token:              wireToken,
		GasLimitCap:        500_000,
		MaxFeePerGasCapWei: new(big.Int).Mul(big.NewInt(1e9), big.NewInt(1_000_000)), // 1e6 gwei — generous
	}
	adapter, err := evm.NewAdapter(client, sc, cfg, discard)
	if err != nil {
		t.Fatalf("NewAdapter() = %v", err)
	}

	return wireStack{
		adapter: adapter,
		backend: backend,
		client:  client,
		from:    from,
		cfg:     cfg,
		chainID: chainID,
	}
}

// wantERC20TransferCalldata rebuilds the expected 68-byte transfer(address,uint256)
// calldata independently of the evm package's packER20Transfer, so the assertion
// is a genuine cross-check: selector 0xa9059cbb, then the recipient right-aligned
// in a 32-byte word, then the amount big-endian in a 32-byte word.
func wantERC20TransferCalldata(to common.Address, amount *big.Int) []byte {
	data := make([]byte, 68)
	copy(data[0:4], []byte{0xa9, 0x05, 0x9c, 0xbb})
	copy(data[16:36], to.Bytes())
	amount.FillBytes(data[36:68])
	return data
}

// TestSubmit_FullWire_MinesWithRecoverableSender is the headline proof: one
// payment crosses the real gRPC wire, is really signed by the signer domain, and
// mines on the in-process EVM with a sender that recovers to the configured key.
// It then fires concurrent submits to prove the nonce allocator hands out
// strictly sequential nonces over that same wire.
func TestSubmit_FullWire_MinesWithRecoverableSender(t *testing.T) {
	ctx := context.Background()
	s := newWireStack(ctx, t, wireSpendLimitHigh)

	if err := s.adapter.VerifyChainID(ctx); err != nil {
		t.Fatalf("VerifyChainID() = %v, want nil", err)
	}

	// --- Single payment: sign over the wire, broadcast, mine, verify. ---
	amount := big.NewInt(1_000_000) // 1 USDC (6 decimals)
	h, err := s.adapter.Submit(ctx, chain.PaymentIntent{
		KeyID:  wireKeyID,
		Asset:  "USDC",
		To:     wireRecipient.Hex(),
		Amount: amount,
	})
	if err != nil {
		t.Fatalf("Submit() = %v, want nil", err)
	}
	if h == "" {
		t.Fatalf("Submit() returned empty tx hash")
	}

	s.backend.Commit() // mine the pending tx

	sig := types.LatestSignerForChainID(s.chainID)
	tx, pending, err := s.client.TransactionByHash(ctx, common.HexToHash(string(h)))
	if err != nil {
		t.Fatalf("TransactionByHash(%s) = %v", h, err)
	}
	if pending {
		t.Fatalf("tx %s still pending after Commit", h)
	}
	receipt, err := s.client.TransactionReceipt(ctx, tx.Hash())
	if err != nil {
		t.Fatalf("TransactionReceipt() = %v", err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("tx mined with status %d, want success", receipt.Status)
	}

	// The signature must recover to the configured sender: the real signer signed
	// with the real key, and those bytes survived the wire round-trip intact.
	sender, err := types.Sender(sig, tx)
	if err != nil {
		t.Fatalf("Sender() = %v", err)
	}
	if sender != s.from {
		t.Fatalf("recovered sender = %s, want %s", sender, s.from)
	}
	// An ERC-20 transfer moves no ETH and targets the token with the exact 68-byte
	// transfer() calldata.
	if tx.Value().Sign() != 0 {
		t.Errorf("tx Value = %s, want 0", tx.Value())
	}
	if tx.To() == nil || *tx.To() != s.cfg.Token {
		t.Errorf("tx To = %v, want %s", tx.To(), s.cfg.Token)
	}
	if want := wantERC20TransferCalldata(wireRecipient, amount); !bytes.Equal(tx.Data(), want) {
		t.Errorf("tx Data = %x, want %x", tx.Data(), want)
	}

	// --- Concurrent submits: prove the nonce allocator over the real wire. ---
	// Fire several BEFORE mining, then mine them all in one block and assert the
	// nonces are strictly sequential and each tx recovers to the same sender.
	const n = 4
	var (
		mu     sync.Mutex
		hashes []common.Hash
		wg     sync.WaitGroup
		errs   = make(chan error, n)
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, err := s.adapter.Submit(ctx, chain.PaymentIntent{
				KeyID:  wireKeyID,
				Asset:  "USDC",
				To:     wireRecipient.Hex(),
				Amount: big.NewInt(1_000_000),
			})
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			hashes = append(hashes, common.HexToHash(string(ch)))
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Submit() = %v", err)
	}
	if len(hashes) != n {
		t.Fatalf("collected %d tx hashes, want %d", len(hashes), n)
	}

	s.backend.Commit() // mine every pending tx into one block

	nonces := make([]uint64, 0, n)
	for _, hash := range hashes {
		mtx, pending, err := s.client.TransactionByHash(ctx, hash)
		if err != nil {
			t.Fatalf("TransactionByHash(%s) = %v", hash, err)
		}
		if pending {
			t.Errorf("tx %s still pending after Commit", hash)
		}
		who, err := types.Sender(sig, mtx)
		if err != nil {
			t.Fatalf("Sender() = %v", err)
		}
		if who != s.from {
			t.Errorf("concurrent tx sender = %s, want %s", who, s.from)
		}
		nonces = append(nonces, mtx.Nonce())
	}

	// The single payment above used nonce 0, so the n concurrent txs must occupy
	// the strictly sequential block 1..n with no gaps or reuse.
	sort.Slice(nonces, func(i, j int) bool { return nonces[i] < nonces[j] })
	for i, got := range nonces {
		if want := uint64(i + 1); got != want {
			t.Fatalf("mined concurrent nonces = %v, want strictly sequential 1..%d", nonces, n)
		}
	}
}

// TestSubmit_FullWire_SpendLimitRejectionMapsToSignerRejected proves the
// rejection path across the whole wire: an over-limit transfer is declined by
// the real signer domain (ErrSpendLimitExceeded → ResourceExhausted), survives
// the gRPC boundary, and the adapter surfaces it as the neutral
// chain.ErrSignerRejected — with nothing mined.
func TestSubmit_FullWire_SpendLimitRejectionMapsToSignerRejected(t *testing.T) {
	ctx := context.Background()
	s := newWireStack(ctx, t, wireSpendLimitLow) // 1 USDC ceiling

	// No transaction should ever exist for the sender; capture the baseline.
	before, err := s.client.NonceAt(ctx, s.from, nil)
	if err != nil {
		t.Fatalf("NonceAt() = %v", err)
	}

	// 2 USDC exceeds the 1 USDC per-key spend limit.
	_, err = s.adapter.Submit(ctx, chain.PaymentIntent{
		KeyID:  wireKeyID,
		Asset:  "USDC",
		To:     wireRecipient.Hex(),
		Amount: big.NewInt(2_000_000),
	})
	if err == nil {
		t.Fatalf("Submit() = nil error, want chain.ErrSignerRejected")
	}
	if !errors.Is(err, chain.ErrSignerRejected) {
		t.Fatalf("Submit() error = %v, want errors.Is chain.ErrSignerRejected", err)
	}

	// Committing must mine nothing for the sender: a declined sign never broadcasts.
	s.backend.Commit()
	after, err := s.client.NonceAt(ctx, s.from, nil)
	if err != nil {
		t.Fatalf("NonceAt() = %v", err)
	}
	if after != before {
		t.Fatalf("sender nonce advanced from %d to %d; a rejected payment must mine nothing", before, after)
	}
}

// TestSubmit_InvalidPaymentIDFailsFast proves the --payment-id guard rejects a
// malformed id BEFORE any config load or network dial, so a bad id can never
// broadcast an unlinkable transaction. It is fully hermetic: --to and --amount
// are valid, so the only thing that can fail is the uuid parse.
func TestSubmit_InvalidPaymentIDFailsFast(t *testing.T) {
	err := runSubmit([]string{
		"--to", wireRecipient.Hex(),
		"--amount", "1000000",
		"--payment-id", "not-a-uuid",
	})
	if err == nil {
		t.Fatal("runSubmit() = nil, want an error for a malformed --payment-id")
	}
	if !strings.Contains(err.Error(), "payment-id") {
		t.Fatalf("runSubmit() error = %v, want it to name --payment-id", err)
	}
}

// TestSubmit_PaymentIDLink_Integration proves the persistence seam --payment-id
// wires: linking a payment to a tx hash writes a settlements row queryable by
// that hash with the right payment_id. It is DSN-gated (mirrors the other
// *_integration tests) since it needs a live Postgres.
//
// It drives settlement.NewRecorder(db.New(sqlDB)) — the EXACT composition
// runSubmit builds after a successful broadcast — rather than runSubmit itself,
// because the full command additionally dials a live chain RPC and the isolated
// signer, neither of which a DSN alone provides. This isolates the one part of
// the submit path that touches Postgres, which is all WP4 added there.
func TestSubmit_PaymentIDLink_Integration(t *testing.T) {
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run the settlement-link integration test")
	}

	ctx := context.Background()
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	paymentID := seedPayment(ctx, t, sqlDB)
	// A unique 32-byte 0x-hex hash per run (two uuids = 32 bytes) so repeated
	// runs on the shared dev DB never collide on the tx_hash UNIQUE.
	a, b := uuid.New(), uuid.New()
	txHash := "0x" + hex.EncodeToString(a[:]) + hex.EncodeToString(b[:])

	// The production composition runSubmit builds for the link step.
	recorder := settlement.NewRecorder(db.New(sqlDB))
	if err := recorder.Link(ctx, paymentID, txHash); err != nil {
		t.Fatalf("Link() = %v, want nil", err)
	}

	// The settlement is now queryable by tx hash and points back at the payment.
	sett, err := db.New(sqlDB).GetSettlementByTxHash(ctx, txHash)
	if err != nil {
		t.Fatalf("GetSettlementByTxHash(%s) = %v", txHash, err)
	}
	if sett.PaymentID != paymentID {
		t.Fatalf("linked settlement payment_id = %s, want %s", sett.PaymentID, paymentID)
	}
	if sett.Status != "pending" {
		t.Errorf("fresh settlement status = %q, want %q", sett.Status, "pending")
	}

	// Link is idempotent: a re-link of the same tx is a no-op, not a duplicate.
	if err := recorder.Link(ctx, paymentID, txHash); err != nil {
		t.Fatalf("second Link() = %v, want nil (idempotent)", err)
	}
}

// seedPayment inserts the minimum FK chain a settlement needs — two accounts, a
// journal entry, and a completed payment — via raw SQL, returning the payment
// id. Fresh uuids keep repeated runs on the shared dev DB independent.
func seedPayment(ctx context.Context, t *testing.T, sqlDB *sql.DB) uuid.UUID {
	t.Helper()
	const asset = "USDC"
	var srcID, dstID, entryID, paymentID uuid.UUID
	if err := sqlDB.QueryRowContext(ctx,
		`INSERT INTO accounts (name, kind, asset) VALUES ($1, 'user', $2) RETURNING id`,
		"src-"+uuid.NewString(), asset,
	).Scan(&srcID); err != nil {
		t.Fatalf("seed source account: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx,
		`INSERT INTO accounts (name, kind, asset) VALUES ($1, 'user', $2) RETURNING id`,
		"dst-"+uuid.NewString(), asset,
	).Scan(&dstID); err != nil {
		t.Fatalf("seed dest account: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx,
		`INSERT INTO journal_entries (kind, external_ref, asset) VALUES ('payment', $1, $2) RETURNING id`,
		uuid.NewString(), asset,
	).Scan(&entryID); err != nil {
		t.Fatalf("seed journal entry: %v", err)
	}
	if err := sqlDB.QueryRowContext(ctx,
		`INSERT INTO payments (status, asset, amount, source_account_id, dest_account_id, journal_entry_id)
		 VALUES ('completed', $1, 1000000, $2, $3, $4) RETURNING id`,
		asset, srcID, dstID, entryID,
	).Scan(&paymentID); err != nil {
		t.Fatalf("seed payment: %v", err)
	}
	return paymentID
}
