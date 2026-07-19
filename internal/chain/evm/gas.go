package evm

import (
	"context"
	"fmt"
	"math/big"
	"math/bits"

	"github.com/ethereum/go-ethereum"
)

// Gas and fee knobs are constants, not config, on purpose: they are safety
// margins, not operator-tunable policy. The operator's only knobs are the hard
// caps (GasLimitCap, MaxFeePerGasCapWei); these two shape the *estimate* into a
// request that has headroom to mine without blowing past those caps.
const (
	// gasLimitBufferPct inflates the node's gas estimate by 25%. EstimateGas is a
	// lower bound that can fall short when the transaction runs against slightly
	// different state at inclusion time; the buffer keeps the tx from reverting
	// out-of-gas, while the GasLimitCap keeps the buffer from running away.
	gasLimitBufferPct uint64 = 125
	// baseFeeHeadroom multiplies the current base fee when setting the max fee, so
	// the transaction stays includable across a few blocks of base-fee growth
	// (each block can raise the base fee by at most 12.5%). Being an int64 keeps
	// the big.Int multiply below allocation-free at the call site.
	baseFeeHeadroom int64 = 2
)

// gasParams is the priced result of gasEstimate: the buffered gas limit and the
// EIP-1559 fee pair, all validated against the configured caps. maxFee and tip
// are freshly allocated and owned by the caller.
type gasParams struct {
	gasLimit uint64
	maxFee   *big.Int
	tip      *big.Int
}

// gasEstimate prices one transaction: it estimates gas, applies the buffer and
// gas cap, then reads the tip and base fee to compute a capped max fee. Every
// RPC that feeds the price (estimate, tip, header) is a boundary call; a failure
// of any of them means we could not price the transaction, so they surface as
// ErrGasEstimation with context. A price that exceeds a hard cap is a policy
// rejection (ErrGasCapExceeded / ErrFeeCapExceeded), not an estimation failure.
//
// The signer's precondition maxFee >= tip holds by construction here: maxFee =
// baseFee*baseFeeHeadroom + tip with baseFee >= 0 and baseFeeHeadroom >= 1, so
// maxFee is always at least tip. We do not clamp it — that would be validating
// an arithmetic impossibility.
func gasEstimate(ctx context.Context, rpc ethRPC, cfg Config, msg ethereum.CallMsg) (gasParams, error) {
	estimate, err := rpc.EstimateGas(ctx, msg)
	if err != nil {
		return gasParams{}, fmt.Errorf("evm: estimate gas: %w", ErrGasEstimation)
	}

	// gasLimit = estimate * gasLimitBufferPct / 100, computed overflow-safe. A
	// hostile or misbehaving node could return an estimate near 2^64 whose
	// buffered product wraps mod 2^64 to a small value that slips under the cap
	// and gets signed as an under-gassed tx. bits.Mul64 yields the full 128-bit
	// product; a non-zero high word means the estimate is absurdly large, which
	// we treat as a cap rejection rather than letting the arithmetic wrap.
	hi, lo := bits.Mul64(estimate, gasLimitBufferPct)
	if hi != 0 {
		return gasParams{}, fmt.Errorf("evm: gas estimate %d too large to buffer: %w", estimate, ErrGasCapExceeded)
	}
	gasLimit := lo / 100
	if gasLimit > cfg.GasLimitCap {
		return gasParams{}, fmt.Errorf("evm: buffered gas limit %d exceeds cap %d: %w", gasLimit, cfg.GasLimitCap, ErrGasCapExceeded)
	}

	tip, err := rpc.SuggestGasTipCap(ctx)
	if err != nil {
		return gasParams{}, fmt.Errorf("evm: suggest gas tip: %w", ErrGasEstimation)
	}

	header, err := rpc.HeaderByNumber(ctx, nil)
	if err != nil {
		return gasParams{}, fmt.Errorf("evm: latest header: %w", ErrGasEstimation)
	}
	// A nil base fee means a pre-EIP-1559 header: this adapter only signs
	// dynamic-fee transactions, so a chain that cannot price one is a
	// misconfiguration we refuse rather than underprice into a stuck tx.
	if header.BaseFee == nil {
		return gasParams{}, fmt.Errorf("evm: chain header has no base fee (pre-EIP-1559?): %w", ErrGasEstimation)
	}

	// maxFee = baseFee*headroom + tip, all freshly allocated so we never mutate
	// the header's base fee or the tip the RPC handed us.
	maxFee := new(big.Int).Mul(header.BaseFee, big.NewInt(baseFeeHeadroom))
	maxFee.Add(maxFee, tip)
	if maxFee.Cmp(cfg.MaxFeePerGasCapWei) > 0 {
		return gasParams{}, fmt.Errorf("evm: max fee per gas exceeds cap: %w", ErrFeeCapExceeded)
	}

	return gasParams{gasLimit: gasLimit, maxFee: maxFee, tip: tip}, nil
}
