package evm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// assetUSDC is the only asset symbol this adapter routes. USDC is an ERC-20, so
// every payment becomes a token transfer(); a second asset would be a second
// token address and route, not a change to this one.
const assetUSDC = "USDC"

// Package sentinels for failures specific to EVM execution. They sit alongside
// (not instead of) the chain.* neutral sentinels: a caller speaking the neutral
// port matches chain.ErrBroadcast et al, while an EVM-aware caller can tell a
// gas-cap rejection from a fee-cap rejection with these. Return sites wrap them
// with %w; messages never carry amounts, recipients, or key material.
var (
	// ErrNonceUnavailable means the node could not tell us the account's pending
	// nonce, so no transaction could be safely numbered.
	ErrNonceUnavailable = errors.New("evm: nonce unavailable")
	// ErrGasEstimation means gas or fee pricing failed at an RPC boundary
	// (estimate, tip, or base-fee query).
	ErrGasEstimation = errors.New("evm: gas estimation failed")
	// ErrGasCapExceeded means the buffered gas limit would exceed GasLimitCap —
	// a policy rejection, not an estimation failure.
	ErrGasCapExceeded = errors.New("evm: gas limit cap exceeded")
	// ErrFeeCapExceeded means the computed max fee per gas would exceed
	// MaxFeePerGasCapWei — a policy rejection.
	ErrFeeCapExceeded = errors.New("evm: max fee per gas cap exceeded")
)

// Config is the adapter's own typed configuration, resolved by the composition
// root from the string-based config.Config. Addresses arrive already parsed and
// the fee cap already a *big.Int, so the adapter never re-parses config text.
type Config struct {
	KeyID              string
	ChainID            uint64
	From, Token        common.Address
	GasLimitCap        uint64
	MaxFeePerGasCapWei *big.Int
}

// Adapter is the EVM implementation of chain.Adapter. It is safe for
// concurrent use: its only mutable state is the nonceAllocator, which serializes
// per sender.
type Adapter struct {
	rpc    ethRPC
	signer Signer
	cfg    Config
	nonces *nonceAllocator
	log    *slog.Logger
}

// Compile-time proof the adapter satisfies the neutral port. If this breaks, the
// port and the adapter have drifted — fix the adapter, not this line.
var _ chain.Adapter = (*Adapter)(nil)

// NewAdapter validates the resolved config and wires the adapter. The config is
// operator-supplied (from the environment), so it is a real trust boundary: an
// empty key id or zero address would produce transactions the signer or chain
// silently rejects, so we fail loudly at construction instead. A nil logger
// falls back to slog.Default() (mirrors NewServer / ledger.NewService).
func NewAdapter(rpc ethRPC, signer Signer, cfg Config, log *slog.Logger) (*Adapter, error) {
	if cfg.KeyID == "" {
		return nil, errors.New("evm: config KeyID is required")
	}
	if cfg.From == (common.Address{}) {
		return nil, errors.New("evm: config From address is required")
	}
	if cfg.Token == (common.Address{}) {
		return nil, errors.New("evm: config Token address is required")
	}
	if cfg.GasLimitCap == 0 {
		return nil, errors.New("evm: config GasLimitCap must be positive")
	}
	if cfg.MaxFeePerGasCapWei == nil || cfg.MaxFeePerGasCapWei.Sign() <= 0 {
		return nil, errors.New("evm: config MaxFeePerGasCapWei must be positive")
	}
	if log == nil {
		log = slog.Default()
	}

	// Own the fee cap: copy it so a later mutation of the caller's *big.Int cannot
	// move the ceiling every Submit prices against (the pointer-semantics discipline
	// the signer's spendBucket uses for its limit).
	cfg.MaxFeePerGasCapWei = new(big.Int).Set(cfg.MaxFeePerGasCapWei)

	return &Adapter{
		rpc:    rpc,
		signer: signer,
		cfg:    cfg,
		nonces: newNonceAllocator(rpc),
		log:    log,
	}, nil
}

// VerifyChainID checks the node speaks the configured chain before any payment
// is signed. It is the wrong-network guard: a signer key is bound to one chain
// id, and pointing the adapter at the wrong RPC (e.g. mainnet instead of a
// testnet) must fail fast, not sign a live-value transaction against the wrong
// chain. Call once at startup.
func (a *Adapter) VerifyChainID(ctx context.Context) error {
	got, err := a.rpc.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("evm: query chain id: %w", err)
	}
	if got == nil || got.Cmp(new(big.Int).SetUint64(a.cfg.ChainID)) != 0 {
		return fmt.Errorf("evm: chain id mismatch: node reports %v, configured %d", got, a.cfg.ChainID)
	}
	return nil
}

// Submit executes one payment intent as an EIP-1559 ERC-20 transfer. The order
// is deliberate: validate the intent at the boundary, pack calldata, price gas
// OUTSIDE the nonce lock (pricing does not depend on the nonce and must not hold
// up other senders' allocations), then allocate the nonce and sign+broadcast as
// one critical section. Errors wrap the neutral chain.* sentinels so callers of
// the port match a stable cause; EVM-specific pricing failures also carry the
// package sentinels.
func (a *Adapter) Submit(ctx context.Context, intent chain.PaymentIntent) (txHash chain.TxHash, err error) {
	start := time.Now()
	var gasLimit uint64
	// One redacted structured line per outcome (D9): the fields below and nothing
	// derived from the amount, recipient, calldata, or raw transaction bytes.
	defer func() {
		a.logResult(ctx, err, gasLimit, string(txHash), time.Since(start))
	}()

	// 1. Validate the intent. Asset first (is there a route at all?), then the
	// recipient shape, the amount range, and the key binding. Messages name the
	// failure without echoing the recipient or amount.
	if intent.Asset != assetUSDC {
		return "", fmt.Errorf("evm: asset %q has no route: %w", intent.Asset, chain.ErrUnsupportedAsset)
	}
	if !common.IsHexAddress(intent.To) {
		return "", fmt.Errorf("evm: recipient is not a valid address: %w", chain.ErrInvalidIntent)
	}
	recipient := common.HexToAddress(intent.To)
	if recipient == (common.Address{}) {
		return "", fmt.Errorf("evm: recipient is the zero address: %w", chain.ErrInvalidIntent)
	}
	if intent.Amount == nil || intent.Amount.Sign() <= 0 || intent.Amount.BitLen() > uint256Bits {
		return "", fmt.Errorf("evm: amount is not a positive uint256: %w", chain.ErrInvalidIntent)
	}
	if intent.KeyID != "" && intent.KeyID != a.cfg.KeyID {
		return "", fmt.Errorf("evm: intent key_id is not bound to this adapter: %w", chain.ErrInvalidIntent)
	}

	// 2. Pack the transfer(recipient, amount) calldata (exactly 68 bytes).
	data, err := packERC20Transfer(recipient, intent.Amount)
	if err != nil {
		return "", err // already wraps chain.ErrInvalidIntent
	}

	// 3. Price gas and fees against the caps, outside the nonce lock. The estimate
	// call targets the token with the real calldata but carries no nonce (the
	// node does not need one to estimate).
	token := a.cfg.Token
	gp, err := gasEstimate(ctx, a.rpc, a.cfg, ethereum.CallMsg{
		From:  a.cfg.From,
		To:    &token,
		Value: big.NewInt(0),
		Data:  data,
	})
	if err != nil {
		return "", err
	}
	gasLimit = gp.gasLimit

	// 4. Allocate the nonce and sign+broadcast inside the per-sender critical
	// section, so a failed broadcast leaves the nonce free (gap-free) and no two
	// concurrent submissions for cfg.From reuse a nonce.
	err = a.nonces.withNonce(ctx, a.cfg.From, func(nonce uint64) error {
		signed, signErr := a.signer.Sign(ctx, SignerRequest{
			KeyID:                a.cfg.KeyID,
			ChainID:              a.cfg.ChainID,
			Nonce:                nonce,
			GasLimit:             gp.gasLimit,
			To:                   a.cfg.Token,
			Value:                big.NewInt(0),
			MaxFeePerGas:         gp.maxFee,
			MaxPriorityFeePerGas: gp.tip,
			Data:                 data,
		})
		if signErr != nil {
			return fmt.Errorf("evm: signer declined: %w", chain.ErrSignerRejected)
		}
		// The signer returns the sender its signature recovers to. If it is not the
		// account we configured, the wrong key signed: abort BEFORE broadcast so we
		// never put a mis-signed transaction on the wire.
		if signed.From != a.cfg.From {
			return fmt.Errorf("evm: signed sender does not match configured from: %w", chain.ErrSignerRejected)
		}

		var tx types.Transaction
		if err := tx.UnmarshalBinary(signed.RawTransaction); err != nil {
			return fmt.Errorf("evm: decode signed transaction: %w", chain.ErrBroadcast)
		}
		if err := a.rpc.SendTransaction(ctx, &tx); err != nil {
			return fmt.Errorf("evm: broadcast transaction: %w", chain.ErrBroadcast)
		}

		txHash = chain.TxHash(signed.TxHash.Hex())
		return nil
	})
	if err != nil {
		return "", err
	}
	return txHash, nil
}

// logResult emits one structured record per Submit outcome. It logs only the
// non-sensitive fields the runbook needs — outcome, chain, sender, tx hash, gas
// limit, latency — and never the amount, recipient, calldata, or raw bytes
// (mirrors cmd/signer's logResult discipline). Expected client rejections are
// info/warn; an unexpected fault is error.
func (a *Adapter) logResult(ctx context.Context, err error, gasLimit uint64, txHash string, dur time.Duration) {
	attrs := []any{
		"outcome", submitOutcome(err),
		"chain_id", a.cfg.ChainID,
		"from", a.cfg.From.Hex(),
		"tx_hash", txHash,
		"gas_limit", gasLimit,
		"duration_ms", dur.Milliseconds(),
	}
	switch {
	case err == nil:
		a.log.InfoContext(ctx, "payment submitted", attrs...)
	case errors.Is(err, chain.ErrUnsupportedAsset), errors.Is(err, chain.ErrInvalidIntent):
		a.log.InfoContext(ctx, "submit rejected: invalid intent", attrs...)
	case errors.Is(err, ErrGasCapExceeded), errors.Is(err, ErrFeeCapExceeded):
		a.log.WarnContext(ctx, "submit rejected: cap exceeded", attrs...)
	case errors.Is(err, chain.ErrSignerRejected):
		a.log.WarnContext(ctx, "submit rejected: signer declined", attrs...)
	default:
		a.log.ErrorContext(ctx, "submit failed", attrs...)
	}
}

// submitOutcome maps an error to a stable, non-sensitive outcome label for the
// structured log. It reads the wrapped sentinel, so the label survives the human
// context added at the return site.
func submitOutcome(err error) string {
	switch {
	case err == nil:
		return "submitted"
	case errors.Is(err, chain.ErrUnsupportedAsset):
		return "unsupported_asset"
	case errors.Is(err, chain.ErrInvalidIntent):
		return "invalid_intent"
	case errors.Is(err, ErrGasCapExceeded):
		return "gas_cap_exceeded"
	case errors.Is(err, ErrFeeCapExceeded):
		return "fee_cap_exceeded"
	case errors.Is(err, ErrGasEstimation):
		return "gas_estimation_failed"
	case errors.Is(err, ErrNonceUnavailable):
		return "nonce_unavailable"
	case errors.Is(err, chain.ErrSignerRejected):
		return "signer_rejected"
	case errors.Is(err, chain.ErrBroadcast):
		return "broadcast_failed"
	default:
		return "error"
	}
}
