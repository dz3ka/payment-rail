package evm

import (
	"bytes"
	"errors"
	"math/big"
	"testing"

	"github.com/dz3ka/payment-rail/internal/chain"
)

func TestPackERC20TransferLayout(t *testing.T) {
	amount := big.NewInt(1_234_567)
	data, err := packERC20Transfer(testTo, amount)
	if err != nil {
		t.Fatalf("packERC20Transfer: %v", err)
	}

	if len(data) != 68 {
		t.Fatalf("calldata length = %d, want 68", len(data))
	}

	// Selector is keccak256("transfer(address,uint256)")[:4].
	if got := data[:4]; !bytes.Equal(got, []byte{0xa9, 0x05, 0x9c, 0xbb}) {
		t.Errorf("selector = %x, want a9059cbb", got)
	}

	// Recipient right-aligned in the word at data[4:36]: high 12 bytes zero, low
	// 20 bytes the address.
	if hi := data[4:16]; !bytes.Equal(hi, make([]byte, 12)) {
		t.Errorf("recipient word high bytes = %x, want all zero", hi)
	}
	if lo := data[16:36]; !bytes.Equal(lo, testTo.Bytes()) {
		t.Errorf("recipient bytes = %x, want %x", lo, testTo.Bytes())
	}

	// Amount is big-endian in the trailing word.
	if got := new(big.Int).SetBytes(data[36:68]); got.Cmp(amount) != 0 {
		t.Errorf("decoded amount = %s, want %s", got, amount)
	}
}

func TestPackERC20TransferMaxUint256(t *testing.T) {
	// 2^256 - 1 has BitLen 256 and must be accepted (fits the word exactly).
	maxAmount := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	data, err := packERC20Transfer(testTo, maxAmount)
	if err != nil {
		t.Fatalf("packERC20Transfer(2^256-1): %v", err)
	}
	if got := new(big.Int).SetBytes(data[36:68]); got.Cmp(maxAmount) != 0 {
		t.Errorf("decoded amount = %s, want %s", got, maxAmount)
	}
}

func TestPackERC20TransferRejectsInvalidAmount(t *testing.T) {
	tests := []struct {
		name   string
		amount *big.Int
	}{
		{"nil", nil},
		{"zero", big.NewInt(0)},
		{"negative", big.NewInt(-1)},
		{"overflows uint256", new(big.Int).Lsh(big.NewInt(1), 256)}, // 2^256, BitLen 257
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := packERC20Transfer(testTo, tc.amount)
			if !errors.Is(err, chain.ErrInvalidIntent) {
				t.Fatalf("err = %v, want wrap of chain.ErrInvalidIntent", err)
			}
		})
	}
}

// Guard against a silent selector drift away from the signer's allowlist.
func TestSelectorMatchesSignerPolicy(t *testing.T) {
	if erc20TransferSelector != [4]byte{0xa9, 0x05, 0x9c, 0xbb} {
		t.Fatalf("selector = %x, want a9059cbb", erc20TransferSelector)
	}
}
