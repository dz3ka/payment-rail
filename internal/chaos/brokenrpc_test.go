//go:build chaos

package chaos

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/settlement"
)

// brokenRPCRecipient is any nonzero 20-byte address: it clears the adapter's
// zero-address guard so a broadcast is actually attempted.
var brokenRPCRecipient = common.HexToAddress("0x00000000000000000000000000000000000000AB")

// brokenRPCToken is a codeless ERC-20 address: a transfer() to it still estimates
// gas on the simulated backend (it runs no code), so the whole build/price/nonce
// path executes without deploying a token — the same trick the full-wire submit
// test uses.
var brokenRPCToken = common.HexToAddress("0x000000000000000000000000000000000000C0DE")

// brokenSendRPC wraps a real simulated.Backend client so the six read-side Submit
// methods (ChainID/PendingNonceAt/EstimateGas/SuggestGasTipCap/HeaderByNumber and
// CallContract) work for free, and shadows ONLY SendTransaction with a toggle: it
// injects a broadcast error while failing, and otherwise counts a successful send
// WITHOUT touching the real backend — the counter is the "broadcast exactly once"
// probe, and not delegating keeps the arbitrary test-signed tx off the real chain.
type brokenSendRPC struct {
	simulated.Client // promotes every ethRPC method; SendTransaction is shadowed below.

	mu       sync.Mutex
	failSend bool
	sendErr  error
	sent     int
}

func (r *brokenSendRPC) SendTransaction(_ context.Context, _ *types.Transaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failSend {
		return r.sendErr
	}
	r.sent++
	return nil
}

func (r *brokenSendRPC) setFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failSend = v
}

func (r *brokenSendRPC) broadcasts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sent
}

// recordingSigner is an evm.Signer that signs a real EIP-1559 tx with its key (so
// the adapter's signed.From == cfg.From check passes and UnmarshalBinary succeeds)
// and records the nonce it was asked to sign each call. The recorded nonces are how
// the test proves a failed broadcast did not burn one: the retry must reuse it.
type recordingSigner struct {
	from common.Address
	key  *ecdsa.PrivateKey

	mu     sync.Mutex
	nonces []uint64
}

func (s *recordingSigner) Sign(_ context.Context, req evm.SignerRequest) (evm.SignedTx, error) {
	s.mu.Lock()
	s.nonces = append(s.nonces, req.Nonce)
	s.mu.Unlock()

	chainID := new(big.Int).SetUint64(req.ChainID)
	to := req.To
	tx, err := types.SignNewTx(s.key, types.LatestSignerForChainID(chainID), &types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     req.Nonce,
		GasTipCap: req.MaxPriorityFeePerGas,
		GasFeeCap: req.MaxFeePerGas,
		Gas:       req.GasLimit,
		To:        &to,
		Value:     req.Value,
		Data:      req.Data,
	})
	if err != nil {
		return evm.SignedTx{}, err
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		return evm.SignedTx{}, err
	}
	return evm.SignedTx{RawTransaction: raw, TxHash: tx.Hash(), From: s.from}, nil
}

func (s *recordingSigner) signedNonces() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, len(s.nonces))
	copy(out, s.nonces)
	return out
}

// TestBrokenRPC proves the adapter tolerates a chain node returning errors mid-flow.
// WP3A drives the Submit path against a wrapped simulated backend whose broadcast is
// toggled to fail then heal, and asserts a failed broadcast surfaces chain.ErrBroadcast
// AND does not burn a nonce (the retry reuses it, gap-free). WP3B drives the settle
// path and asserts a redelivered confirm settles exactly once — the RPC-retry-safety
// proof at the ledger boundary. WP3A needs no DB; WP3B is DSN-gated via the harness.
func TestBrokenRPC(t *testing.T) {
	ctx := context.Background()

	// WP3A — a broadcast failure must not consume a nonce. The adapter allocates the
	// nonce inside the sign+broadcast critical section and commits the high-water only
	// on success (nonce.go), so a failed SendTransaction must leave the nonce free for
	// the next attempt — no gap that would wedge every later tx for the sender.
	t.Run("broadcast_failure_does_not_burn_nonce", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("GenerateKey: %v", err)
		}
		from := crypto.PubkeyToAddress(key.PublicKey)

		// A genesis-funded sender so EstimateGas against the codeless token succeeds.
		balance := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18))
		backend := simulated.NewBackend(types.GenesisAlloc{from: {Balance: balance}})
		t.Cleanup(func() { _ = backend.Close() })
		client := backend.Client()

		chainID, err := client.ChainID(ctx)
		if err != nil {
			t.Fatalf("ChainID: %v", err)
		}

		rpc := &brokenSendRPC{
			Client:   client,
			failSend: true,
			sendErr:  errors.New("chaos: rpc connection refused"),
		}
		sgn := &recordingSigner{from: from, key: key}
		cfg := evm.Config{
			KeyID:              "hot",
			ChainID:            chainID.Uint64(),
			From:               from,
			Token:              brokenRPCToken,
			GasLimitCap:        500_000,
			MaxFeePerGasCapWei: new(big.Int).Mul(big.NewInt(1e9), big.NewInt(1_000_000)),
		}
		adapter, err := evm.NewAdapter(rpc, sgn, cfg, quietLogger())
		if err != nil {
			t.Fatalf("NewAdapter: %v", err)
		}

		intent := chain.PaymentIntent{
			KeyID:  "hot",
			Asset:  "USDC",
			To:     brokenRPCRecipient.Hex(),
			Amount: big.NewInt(1_000_000),
		}

		// Baseline nonce the chain would hand out (0 for a txless account).
		before, err := rpc.PendingNonceAt(ctx, from)
		if err != nil {
			t.Fatalf("PendingNonceAt(before): %v", err)
		}

		// Broadcast fails: the adapter signs (allocating nonce `before`) then the
		// SendTransaction error surfaces as chain.ErrBroadcast (adapter.go:210-212).
		if _, err := adapter.Submit(ctx, intent); !errors.Is(err, chain.ErrBroadcast) {
			t.Fatalf("Submit under broadcast failure = %v, want errors.Is chain.ErrBroadcast", err)
		}
		if got := rpc.broadcasts(); got != 0 {
			t.Fatalf("successful broadcasts after failed send = %d, want 0", got)
		}

		// Heal the RPC and retry: it broadcasts exactly once and returns a hash.
		rpc.setFail(false)
		h, err := adapter.Submit(ctx, intent)
		if err != nil {
			t.Fatalf("Submit after RPC heal = %v, want nil", err)
		}
		if h == "" {
			t.Fatal("Submit after RPC heal returned an empty tx hash")
		}
		if got := rpc.broadcasts(); got != 1 {
			t.Fatalf("successful broadcasts after heal = %d, want exactly 1", got)
		}

		// The nonce was NOT burned: both the failed attempt and the successful retry
		// signed the SAME nonce (`before`) — gap-free, exactly what nonce.go's
		// commit-on-success discipline guarantees.
		gotNonces := sgn.signedNonces()
		wantNonces := []uint64{before, before}
		if len(gotNonces) != len(wantNonces) {
			t.Fatalf("signed nonces = %v, want %v", gotNonces, wantNonces)
		}
		for i := range wantNonces {
			if gotNonces[i] != wantNonces[i] {
				t.Fatalf("signed nonces = %v, want %v (gap-free reuse of nonce %d)", gotNonces, wantNonces, before)
			}
		}
		// And the chain's view never advanced (we never really broadcast), so no gap.
		after, err := rpc.PendingNonceAt(ctx, from)
		if err != nil {
			t.Fatalf("PendingNonceAt(after): %v", err)
		}
		if after != before {
			t.Fatalf("chain pending nonce advanced from %d to %d; a failed broadcast must burn nothing", before, after)
		}
	})

	// WP3B — confirm idempotency under redelivery. A transient RPC error makes the
	// watcher log-and-continue and redeliver the SAME Confirmed status on a later
	// poll. Delivering it TWICE through a real Sink must settle exactly once (the
	// double-settle guard keyed on settle:<paymentID>:<blockHash> holds) and the
	// asset must still converge — the RPC-retry-safety proof at the ledger boundary.
	t.Run("confirm_redelivery_settles_once", func(t *testing.T) {
		dbh := requireChaosDB(t)

		asset := chaosAsset()
		src := seedFundedAccount(ctx, t, dbh, asset, 1000)
		dst := seedFundedAccount(ctx, t, dbh, asset, 0)
		seedHouseAccount(ctx, t, dbh, asset)
		const amt = 600
		txHash := "0x" + strings.ReplaceAll(uuid.NewString(), "-", "")

		pid := seedPaymentAndLink(ctx, t, dbh, asset, src, dst, amt, txHash)
		assertBalance(ctx, t, dbh, dst, amt) // provisional credit, settlement pending.

		confirmed := evm.Status{
			TxHash:      chain.TxHash(txHash),
			Phase:       evm.PhaseConfirmed,
			BlockHash:   "0x" + strings.Repeat("cd", 32),
			BlockNumber: 200,
		}

		sink := settlement.NewSink(ledger.NewSQLStore(dbh), quietLogger())

		// First delivery settles.
		if err := sink.OnStatus(ctx, confirmed); err != nil {
			t.Fatalf("first OnStatus: %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("settlement status after first confirm = %q, want settled", got)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 1)
		assertBalance(ctx, t, dbh, dst, 0)

		// Redelivery of the SAME confirm (same block hash) is a no-op: no second
		// settle entry, no double debit of the provisional credit.
		if err := sink.OnStatus(ctx, confirmed); err != nil {
			t.Fatalf("redelivered OnStatus: %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("settlement status after redeliver = %q, want settled", got)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 1)
		assertBalance(ctx, t, dbh, dst, 0)

		// End state: ledger closed and the asset reconciles against the settled amount.
		assertConverged(ctx, t, dbh, asset, amt)
	})
}
