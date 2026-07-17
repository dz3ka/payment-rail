package signer

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// minGasLimit is the intrinsic gas floor: no Ethereum transaction can cost
	// fewer than 21000 gas, so a request below it is malformed by definition.
	minGasLimit = 21_000
	// maxGasLimit caps gas at a mainnet block's gas ceiling (30M). A request
	// above this could never be included in a block, so we reject it at the
	// boundary rather than sign a transaction that can never be mined. It is a
	// sanity bound, not a policy knob — hence a constant, not config.
	maxGasLimit = 30_000_000

	// erc20TransferCalldataLen is the exact size of a transfer(address,uint256)
	// call: 4-byte selector + 32-byte (left-padded) address + 32-byte amount.
	erc20TransferCalldataLen = 68
)

// erc20TransferSelector is the first four bytes of keccak256("transfer(address,uint256)").
// It is the only calldata function the signer will sign (besides an empty
// native transfer): the allowlist is the whole point of an isolated signer.
var erc20TransferSelector = [4]byte{0xa9, 0x05, 0x9c, 0xbb}

// uint256 is the maximum bit length of the fixed-width big-endian fields we
// accept (Value, fee caps, token amounts): 32 bytes == 256 bits.
const uint256Bits = 256

// validate is the trust boundary. It runs the fixed validation order and, on
// success, returns the resolved key and the *charged amount* — the value the
// spend limiter must account for. The charged amount is a freshly allocated
// big.Int owned by the signer, so nothing the caller still holds can change it
// after this returns.
//
// Order matters and is deliberate: identity first (which key?), then the key's
// chain binding (replay protection), then structural shape, then the calldata
// allowlist that decides what is actually being spent. The spend-limit check
// itself is NOT here — it belongs to the limiter, which must hold the amount
// and the sign step in one critical section.
func validate(kr *Keyring, r SignRequest) (*keyEntry, *big.Int, error) {
	// 1. Known key_id.
	key, ok := kr.lookup(r.KeyID)
	if !ok {
		return nil, nil, fmt.Errorf("signer: no key registered for the requested key_id: %w", ErrUnknownKey)
	}

	// 2. Chain binding. A key carries exactly one chain_id, and that single
	// value IS its allowlist: signing a transaction for any other chain would
	// expose the key to cross-chain replay (EIP-1559 embeds the chain id in the
	// signed payload, so a signature is only replay-safe on its own chain).
	if r.ChainID != key.chainID {
		return nil, nil, fmt.Errorf("signer: request chain_id does not match the key's chain: %w", ErrChainMismatch)
	}

	// 3. Destination. To is a value type (a 20-byte array), so it is always
	// present and always 20 bytes structurally; the zero address means "no
	// recipient", i.e. contract creation, which an isolated payment signer must
	// never do. Reject it.
	if r.To == (common.Address{}) {
		return nil, nil, fmt.Errorf("signer: destination address is missing (contract creation is not permitted): %w", ErrMalformedTx)
	}

	// 4. uint256 numeric fields and gas bounds. Each amount must be a non-nil,
	// non-negative value that fits in 256 bits. Values are never echoed in
	// messages — only the field name — because amounts are sensitive.
	for _, f := range []struct {
		name string
		v    *big.Int
	}{
		{"value", r.Value},
		{"max_fee_per_gas", r.MaxFeePerGas},
		{"max_priority_fee_per_gas", r.MaxPriorityFeePerGas},
	} {
		if f.v == nil {
			return nil, nil, fmt.Errorf("signer: %s is required: %w", f.name, ErrMalformedTx)
		}
		if f.v.Sign() < 0 || f.v.BitLen() > uint256Bits {
			return nil, nil, fmt.Errorf("signer: %s is not a valid uint256: %w", f.name, ErrMalformedTx)
		}
	}
	// A zero fee cap could never pay for inclusion; reject it up front.
	if r.MaxFeePerGas.Sign() == 0 {
		return nil, nil, fmt.Errorf("signer: max_fee_per_gas must be positive: %w", ErrMalformedTx)
	}
	// The tip cannot exceed the total fee cap (EIP-1559 requires maxFee >= tip).
	if r.MaxFeePerGas.Cmp(r.MaxPriorityFeePerGas) < 0 {
		return nil, nil, fmt.Errorf("signer: max_fee_per_gas is below max_priority_fee_per_gas: %w", ErrMalformedTx)
	}
	if r.GasLimit < minGasLimit || r.GasLimit > maxGasLimit {
		return nil, nil, fmt.Errorf("signer: gas_limit %d outside [%d, %d]: %w", r.GasLimit, minGasLimit, maxGasLimit, ErrMalformedTx)
	}

	// 5. Calldata allowlist — this is what fixes the charged amount.
	charged, err := chargedAmount(r)
	if err != nil {
		return nil, nil, err
	}
	return key, charged, nil
}

// chargedAmount classifies the calldata and returns the value that counts
// against the key's spend limit, as a fresh big.Int. Only two shapes are
// allowed:
//
//   - empty calldata  → a native (ETH) transfer; the charged amount is Value.
//   - an exact ERC-20 transfer(address,uint256) call → the charged amount is the
//     decoded token amount, and Value must be zero (a token transfer moves no ETH).
//
// Any other calldata is rejected: an isolated signer signs only payments it can
// account for, and it cannot price arbitrary contract calls.
func chargedAmount(r SignRequest) (*big.Int, error) {
	if len(r.Data) == 0 {
		return new(big.Int).Set(r.Value), nil
	}

	// Reject anything that is not exactly an ERC-20 transfer: wrong length,
	// wrong selector, or a nonzero Value riding alongside token calldata.
	if len(r.Data) != erc20TransferCalldataLen ||
		!bytes.Equal(r.Data[:4], erc20TransferSelector[:]) ||
		r.Value.Sign() != 0 {
		return nil, fmt.Errorf("signer: calldata is not an allowed ERC-20 transfer: %w", ErrMalformedTx)
	}

	// The token amount is the trailing 32-byte word; bytes [16:36] hold the
	// recipient, which is part of the signed call and needs no further checking
	// here beyond the fixed 68-byte shape.
	return new(big.Int).SetBytes(r.Data[36:68]), nil
}
