package evm

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// erc20WordLen is the size of the single 32-byte word an ERC-20 balanceOf(address)
// returns. A shorter return means the call hit a non-contract address (or a token
// that does not implement the method), which we surface as an error rather than
// decoding garbage into a balance.
const erc20WordLen = 32

// BalanceReader is the EVM implementation of chain.BalanceReader. It reads a
// token holder's balance with a read-only eth_call (balanceOf), so it carries no
// signer, no nonce, and no mutable state and is safe for concurrent use. It
// shares the ethRPC seam with the Adapter: the same client that broadcasts
// payments answers balance queries.
type BalanceReader struct {
	rpc ethRPC
	log *slog.Logger
}

// Compile-time proof the reader satisfies the neutral port. If this breaks, the
// port and the reader have drifted — fix the reader, not this line.
var _ chain.BalanceReader = (*BalanceReader)(nil)

// NewBalanceReader wires the reader over the RPC seam. A nil logger falls back to
// slog.Default() (mirrors NewAdapter / NewServer).
func NewBalanceReader(rpc ethRPC, log *slog.Logger) *BalanceReader {
	if log == nil {
		log = slog.Default()
	}
	return &BalanceReader{rpc: rpc, log: log}
}

// BalanceOf reads the holder's balance of the token in base units via a
// read-only balanceOf(address) call against the latest block. It fails closed:
// a malformed token or holder address is rejected before any RPC, an RPC
// transport error is wrapped through RedactRPCError so a managed-node endpoint's
// API key never reaches the caller or logs, and a short/empty return (a
// non-contract address, or a token missing the method) is an error rather than a
// balance decoded from too few bytes.
func (r *BalanceReader) BalanceOf(ctx context.Context, token, holder string) (*big.Int, error) {
	if !common.IsHexAddress(token) {
		return nil, fmt.Errorf("evm: token is not a valid address: %w", chain.ErrInvalidIntent)
	}
	if !common.IsHexAddress(holder) {
		return nil, fmt.Errorf("evm: holder is not a valid address: %w", chain.ErrInvalidIntent)
	}
	tokenAddr := common.HexToAddress(token)

	msg := ethereum.CallMsg{
		To:   &tokenAddr,
		Data: packERC20BalanceOf(common.HexToAddress(holder)),
	}
	out, err := r.rpc.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("evm: balanceOf call failed: %s", RedactRPCError(err))
	}
	// A well-formed ERC-20 returns exactly one 32-byte word. Anything shorter (an
	// empty return from a non-contract address) cannot be a balance, so we refuse
	// to SetBytes a truncated buffer.
	if len(out) < erc20WordLen {
		return nil, fmt.Errorf("evm: balanceOf returned %d bytes, want at least %d (not a token contract?)", len(out), erc20WordLen)
	}
	return new(big.Int).SetBytes(out[:erc20WordLen]), nil
}
