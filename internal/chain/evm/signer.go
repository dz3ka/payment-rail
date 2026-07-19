// Package evm is the EVM implementation of the chain-neutral Adapter port
// (see internal/chain). It turns a chain.PaymentIntent into a signed, broadcast
// EIP-1559 ERC-20 transfer: it packs calldata, prices gas under hard caps,
// allocates a chain-authoritative nonce, asks the isolated signer to sign, and
// broadcasts the raw transaction to the node.
//
// The package is deliberately proto-free (mirroring internal/signer): it talks
// to the signer through the Signer port and to the chain through the ethRPC
// seam, so neither the gRPC wire types nor a concrete *ethclient.Client leak
// into the adapter's logic. The composition root wires the real implementations
// in; tests wire fakes and the go-ethereum simulated backend.
package evm

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// SignerRequest is the adapter's fully-priced, nonce-bound description of one
// transaction to sign. It is the boundary DTO the adapter hands to the Signer
// port; the gRPC client in the composition root maps it onto the wire. The
// uint256 fields are *big.Int because Ethereum amounts do not fit any Go
// integer — the same idiom signer.SignRequest uses.
type SignerRequest struct {
	KeyID                                     string
	ChainID, Nonce, GasLimit                  uint64
	To                                        common.Address
	Value, MaxFeePerGas, MaxPriorityFeePerGas *big.Int
	Data                                      []byte
}

// SignedTx is the result of a successful sign: the broadcast-ready RLP bytes,
// the transaction hash, and the sender the signature recovers to. These are all
// value/byte types safe to carry across the process boundary — no key material.
type SignedTx struct {
	RawTransaction []byte
	TxHash         common.Hash
	From           common.Address
}

// Signer is the port the adapter uses to get a transaction signed by the
// isolated signer service. The gRPC client implementation lives in the caller
// (composition root), keeping this package proto-free.
type Signer interface {
	Sign(ctx context.Context, req SignerRequest) (SignedTx, error)
}
