// Package signer is Conduit's isolated transaction-signing domain. It holds
// private keys, signs only well-formed EIP-1559 (dynamic-fee) transactions whose
// calldata matches a strict allowlist, and enforces a per-key cumulative spend
// limit — all with no knowledge of the wire protocol. The gRPC adapter (a
// separate package) maps proto messages to and from the domain types here, so
// this package never imports the generated protobuf code: the domain is the
// stable core, the transport is replaceable around it.
//
// The security posture is deliberate: this package validates every field at the
// boundary (nothing past validate is trusted from the caller), never reimplements
// RLP encoding or EIP-1559 hashing (that is delegated to go-ethereum, the code we
// most want to be battle-tested), and never lets private key material or
// monetary amounts leak into errors or logs.
package signer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// SignRequest is a request to sign one EIP-1559 transaction. The uint256 fields
// are *big.Int because Ethereum amounts do not fit in any Go integer and because
// big.Int has no usable value form (its zero value is a valid 0, but copying one
// around by value is a footgun); passing pointers is the idiom. That the fields
// are pointers is exactly why Sign takes a defensive copy — see deepCopy.
type SignRequest struct {
	KeyID    string
	ChainID  uint64
	Nonce    uint64
	GasLimit uint64
	To       common.Address
	Value    *big.Int
	// MaxFeePerGas is the EIP-1559 total fee cap; MaxPriorityFeePerGas is the tip.
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	Data                 []byte
}

// SignedTx is the result of a successful sign: the broadcast-ready RLP bytes,
// the transaction hash, and the sender the signature recovers to. These are all
// value/byte types safe to hand across the process boundary — no key material.
type SignedTx struct {
	RawTransaction []byte
	TxHash         common.Hash
	From           common.Address
}

// Sentinel errors. Callers match these with errors.Is; return sites wrap them
// with %w so a specific cause and human context travel together. Messages never
// carry amounts or key material.
var (
	// ErrUnknownKey means no key is registered for the request's key_id.
	ErrUnknownKey = errors.New("signer: unknown key_id")
	// ErrChainMismatch means the request's chain_id is not the key's chain —
	// the signer's replay-protection guard.
	ErrChainMismatch = errors.New("signer: chain_id does not match key")
	// ErrMalformedTx means the request failed a structural/allowlist check
	// (bad destination, invalid uint256, out-of-range gas, or non-allowlisted
	// calldata).
	ErrMalformedTx = errors.New("signer: malformed transaction")
	// ErrSpendLimitExceeded means committing the charge would push the key's
	// cumulative spend past its configured limit.
	ErrSpendLimitExceeded = errors.New("signer: spend limit exceeded")
)

// Signer is the orchestrator. It owns a Keyring; each key in the ring carries
// its own spend limiter (see spendBucket), so the Signer needs no extra state —
// the per-key lock lives with the key it protects.
type Signer struct {
	keyring *Keyring
}

// NewSigner wraps a loaded keyring in an orchestrator ready to sign.
func NewSigner(kr *Keyring) *Signer {
	return &Signer{keyring: kr}
}

// Sign validates a request at the trust boundary, then — inside the requested
// key's spend-limit critical section — signs the transaction and commits the
// charge. The charge is committed only if signing succeeds.
//
// The critical section is {check spend → sign → commit} as one unit, held under
// the key's mutex (see spendBucket.charge). Signing happens *inside* the lock so
// two concurrent requests on the same key can never both see budget, both sign,
// and together overspend. Requests on different keys never contend.
func (s *Signer) Sign(ctx context.Context, r SignRequest) (SignedTx, error) {
	// Honor caller cancellation before doing any work. Signing is CPU-bound and
	// fast, but the context is part of the contract and a cancelled request
	// should not consume a key's lock.
	if err := ctx.Err(); err != nil {
		return SignedTx{}, err
	}

	// Defensive copy at the trust boundary. SignRequest's amount fields and Data
	// slice are references into the caller's memory. If we validated those and
	// then signed from the same references, a caller could mutate an amount in
	// the window between the spend-limit check and signing — a TOCTOU race that
	// would let a small, approved charge be signed as a large transfer. Copying
	// every reference field into signer-owned memory up front guarantees the
	// value we check is exactly the value we sign.
	r = r.deepCopy()

	key, charged, err := validate(s.keyring, r)
	if err != nil {
		return SignedTx{}, err
	}

	// charge holds key.bucket's lock across the check, the sign callback, and
	// the commit. The signed bytes are produced here, under the lock, from the
	// already-copied request.
	return key.bucket.charge(charged, func() (SignedTx, error) {
		return signTx(key, r)
	})
}

// deepCopy returns a SignRequest that shares no mutable state with the receiver.
// The receiver is already a value copy (Sign takes r by value), so the struct's
// scalar fields are independent; deepCopy additionally duplicates the pointer
// and slice fields — the ones that would otherwise still alias the caller.
func (r SignRequest) deepCopy() SignRequest {
	c := r
	c.Value = copyBig(r.Value)
	c.MaxFeePerGas = copyBig(r.MaxFeePerGas)
	c.MaxPriorityFeePerGas = copyBig(r.MaxPriorityFeePerGas)
	if r.Data != nil {
		c.Data = bytes.Clone(r.Data)
	}
	return c
}

// copyBig clones a *big.Int, preserving nil so downstream validation can still
// distinguish "missing" from "zero".
func copyBig(x *big.Int) *big.Int {
	if x == nil {
		return nil
	}
	return new(big.Int).Set(x)
}

// signTx builds and signs the EIP-1559 dynamic-fee transaction. It never
// hand-rolls RLP or the signing hash — go-ethereum owns that security-critical
// encoding — it only maps the validated request onto types.DynamicFeeTx and
// asks the London signer (post-EIP-1559 rules, bound to the chain id) to sign
// with the key. r is assumed already validated and copied.
func signTx(key *keyEntry, r SignRequest) (SignedTx, error) {
	chainID := new(big.Int).SetUint64(r.ChainID)

	// to is a local so &to cannot alias anything the caller holds. DynamicFeeTx.To
	// is *common.Address; a nil pointer would mean contract creation, which
	// policy already rejected, so we always pass a real address.
	to := r.To
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     r.Nonce,
		GasTipCap: r.MaxPriorityFeePerGas,
		GasFeeCap: r.MaxFeePerGas,
		Gas:       r.GasLimit,
		To:        &to,
		Value:     r.Value,
		Data:      r.Data,
	})

	signed, err := types.SignTx(tx, types.NewLondonSigner(chainID), key.privateKey)
	if err != nil {
		// Past the validated boundary this is a bug, not caller error; surface
		// it. go-ethereum's sign errors concern tx fields, not key bytes.
		return SignedTx{}, fmt.Errorf("signer: sign transaction: %w", err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return SignedTx{}, fmt.Errorf("signer: encode signed transaction: %w", err)
	}
	return SignedTx{
		RawTransaction: raw,
		TxHash:         signed.Hash(),
		From:           key.address,
	}, nil
}
