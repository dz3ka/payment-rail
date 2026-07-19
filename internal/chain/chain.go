// Package chain defines the chain-neutral port that payment execution targets.
// EVM lives in internal/chain/evm; a future Solana adapter would live in
// internal/chain/solana implementing the same Adapter without importing evm.
//
// The port stays deliberately minimal and dependency-free: it imports only the
// standard library (context, errors, math/big) so that adding a non-EVM chain
// never drags go-ethereum, the wire protocol, or any one adapter's types into
// the callers that speak in terms of this interface.
package chain

import (
	"context"
	"errors"
	"math/big"
)

// PaymentIntent is a chain-neutral request to move value to a recipient. It is
// expressed in terms every chain can honor: a signer key id, an asset symbol, a
// recipient in the chain's native string form, and an amount in the asset's
// smallest unit. Adapters translate this into their own transaction types.
type PaymentIntent struct {
	KeyID  string   // signer key id that authorizes this payment
	Asset  string   // asset symbol, e.g. "USDC"
	To     string   // recipient in the chain's native string form (0x-hex for EVM)
	Amount *big.Int // smallest unit (e.g. USDC has 6 decimals); must be > 0
}

// TxHash is a chain's identifier for a submitted transaction, in its native
// string form (0x-hex for EVM). It is opaque to callers of the port.
type TxHash string

// Adapter is the port payment execution targets. One implementation exists
// per chain family; callers depend on this interface, not on any adapter.
type Adapter interface {
	// Submit broadcasts the intent to the chain and returns the resulting
	// transaction hash. It returns one of the sentinel errors below (wrapped
	// with %w) when the intent cannot be executed.
	Submit(ctx context.Context, intent PaymentIntent) (TxHash, error)
}

// Sentinel errors. Adapters wrap these with %w so a specific cause and human
// context travel together; callers match them with errors.Is. Messages never
// carry secrets or key material.
var (
	// ErrInvalidIntent means the intent is malformed: a missing key id, a
	// non-positive amount, or a recipient the adapter cannot parse.
	ErrInvalidIntent = errors.New("chain: invalid payment intent")
	// ErrUnsupportedAsset means the adapter has no route for the intent's asset.
	ErrUnsupportedAsset = errors.New("chain: unsupported asset")
	// ErrSignerRejected means the signer declined to authorize the transaction
	// (unknown key, chain mismatch, spend limit, or a policy violation).
	ErrSignerRejected = errors.New("chain: signer rejected transaction")
	// ErrBroadcast means the signed transaction could not be broadcast to the
	// chain (RPC failure, rejected by the node, or timed out).
	ErrBroadcast = errors.New("chain: broadcast failed")
)
