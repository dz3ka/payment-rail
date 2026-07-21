package chain

import (
	"context"
	"math/big"
)

// BalanceReader reads a token holder's on-chain balance in base units. It is a
// read-only port: reconciliation asks the chain "what does the holder actually
// hold?" without ever moving value. Like Adapter, it stays chain-neutral and
// stdlib-only (context, math/big) so a non-EVM adapter can satisfy it without
// dragging go-ethereum or any one adapter's types into its callers.
//
// token and holder are addresses in the chain's native string form (0x-hex for
// EVM); the returned amount is in the token's smallest unit (e.g. USDC's 6
// decimals). Implementations fail closed: a malformed address or an unreadable
// balance is an error, never a silent zero.
type BalanceReader interface {
	BalanceOf(ctx context.Context, token, holder string) (*big.Int, error)
}
