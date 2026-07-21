package evm

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/dz3ka/payment-rail/internal/chain"
)

const (
	// erc20TransferCalldataLen is the exact size the signer policy allowlists for
	// a transfer(address,uint256) call: 4-byte selector + 32-byte (left-padded)
	// recipient + 32-byte amount. The signer rejects anything of a different
	// length, so we build to this size exactly.
	erc20TransferCalldataLen = 68
	// uint256Bits is the width of the fixed 32-byte amount word: a larger value
	// cannot be encoded without overflowing the word.
	uint256Bits = 256
	// erc20BalanceOfCalldataLen is the size of a balanceOf(address) call: 4-byte
	// selector + 32-byte (left-padded) holder address.
	erc20BalanceOfCalldataLen = 36
)

// erc20TransferSelector is the first four bytes of keccak256("transfer(address,uint256)").
// It must match the single selector the isolated signer allowlists.
var erc20TransferSelector = [4]byte{0xa9, 0x05, 0x9c, 0xbb}

// erc20BalanceOfSelector is the first four bytes of keccak256("balanceOf(address)").
// balanceOf is a read-only call (eth_call), so unlike the transfer selector it is
// not gated by the signer's allowlist.
var erc20BalanceOfSelector = [4]byte{0x70, 0xa0, 0x82, 0x31}

// packERC20Transfer builds the calldata for transfer(to, amount) in the exact
// 68-byte layout the signer policy accepts: selector ++ recipient (right-aligned
// in a 32-byte word, high 12 bytes zero) ++ amount (big-endian, 32 bytes). It
// validates amount here — at the boundary — because a nil/non-positive/oversized
// amount is caller error, not a bug to surface deeper: nil and BitLen>256 cannot
// be encoded, and a non-positive transfer is never a real payment.
//
// The recipient is right-aligned in its 32-byte word (address in the low 20
// bytes, high 12 zero) by copying the 20 address bytes into the trailing 20
// bytes of the pre-zeroed word; big.Int.FillBytes writes the amount as a
// fixed-width big-endian word, so we never hand-roll the padding.
func packERC20Transfer(to common.Address, amount *big.Int) ([]byte, error) {
	if amount == nil {
		return nil, fmt.Errorf("evm: transfer amount is required: %w", chain.ErrInvalidIntent)
	}
	if amount.Sign() <= 0 {
		return nil, fmt.Errorf("evm: transfer amount must be positive: %w", chain.ErrInvalidIntent)
	}
	if amount.BitLen() > uint256Bits {
		return nil, fmt.Errorf("evm: transfer amount does not fit in uint256: %w", chain.ErrInvalidIntent)
	}

	data := make([]byte, erc20TransferCalldataLen)
	copy(data[0:4], erc20TransferSelector[:])
	// The address occupies the low 20 bytes of the 32-byte word at data[4:36],
	// i.e. data[16:36]; the high 12 bytes stay zero from the initial allocation.
	copy(data[16:36], to.Bytes())
	// FillBytes writes amount big-endian into the trailing 32-byte word, panicking
	// only if the value does not fit — which the BitLen guard above has ruled out.
	amount.FillBytes(data[36:68])
	return data, nil
}

// packERC20BalanceOf builds the calldata for balanceOf(holder) in the 36-byte
// layout eth_call expects: selector ++ holder (right-aligned in a 32-byte word,
// high 12 bytes zero). It mirrors packERC20Transfer's address packing — the
// holder occupies the low 20 bytes of the word, the high 12 stay zero from the
// initial allocation. No amount is involved, so unlike the transfer packer there
// is nothing to validate: any address is a legitimate balance query.
func packERC20BalanceOf(holder common.Address) []byte {
	data := make([]byte, erc20BalanceOfCalldataLen)
	copy(data[0:4], erc20BalanceOfSelector[:])
	// The address occupies the low 20 bytes of the 32-byte word at data[4:36],
	// i.e. data[16:36]; the high 12 bytes stay zero from the initial allocation.
	copy(data[16:36], holder.Bytes())
	return data
}
