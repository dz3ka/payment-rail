package evm

import (
	"context"
	"errors"
	"math/big"
	"net/url"
	"strings"
	"testing"

	"github.com/dz3ka/payment-rail/internal/chain"
)

const (
	testTokenHex  = "0x2222222222222222222222222222222222222222"
	testHolderHex = "0x33333333333333333333333333333333333333AB"
)

// word32 left-pads v into a 32-byte big-endian word, the shape an ERC-20
// balanceOf returns.
func word32(v *big.Int) []byte {
	out := make([]byte, 32)
	v.FillBytes(out)
	return out
}

func TestBalanceOfHappyPath(t *testing.T) {
	want := big.NewInt(1_000_000) // 1 USDC at 6 decimals
	rpc := newFakeRPC()
	rpc.callOut = word32(want)

	r := NewBalanceReader(rpc, nil)
	got, err := r.BalanceOf(context.Background(), testTokenHex, testHolderHex)
	if err != nil {
		t.Fatalf("BalanceOf: %v", err)
	}
	if got.Cmp(want) != 0 {
		t.Errorf("balance = %s, want %s", got, want)
	}
}

// A 32-byte return of all 0xff decodes to 2^256-1 without truncation.
func TestBalanceOfMaxWord(t *testing.T) {
	want := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	rpc := newFakeRPC()
	rpc.callOut = word32(want)

	r := NewBalanceReader(rpc, nil)
	got, err := r.BalanceOf(context.Background(), testTokenHex, testHolderHex)
	if err != nil {
		t.Fatalf("BalanceOf: %v", err)
	}
	if got.Cmp(want) != 0 {
		t.Errorf("balance = %s, want %s", got, want)
	}
}

func TestBalanceOfRejectsMalformedAddress(t *testing.T) {
	tests := []struct {
		name          string
		token, holder string
	}{
		{"bad token", "0xnothex", testHolderHex},
		{"short token", "0x1234", testHolderHex},
		{"bad holder", testTokenHex, "not-an-address"},
		{"empty holder", testTokenHex, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newFakeRPC()
			// callErr is set so that any RPC dispatch would be observable, proving we
			// rejected the address BEFORE calling CallContract.
			rpc.callErr = errors.New("CallContract must not be reached")

			r := NewBalanceReader(rpc, nil)
			_, err := r.BalanceOf(context.Background(), tc.token, tc.holder)
			if !errors.Is(err, chain.ErrInvalidIntent) {
				t.Fatalf("err = %v, want wrap of chain.ErrInvalidIntent", err)
			}
		})
	}
}

// A non-contract address returns empty (or short) data; decoding it as a balance
// would be silent corruption, so it must be an error.
func TestBalanceOfRejectsShortReturn(t *testing.T) {
	for _, out := range [][]byte{nil, {}, make([]byte, 31)} {
		rpc := newFakeRPC()
		rpc.callOut = out

		r := NewBalanceReader(rpc, nil)
		_, err := r.BalanceOf(context.Background(), testTokenHex, testHolderHex)
		if err == nil {
			t.Fatalf("len(out)=%d: want error, got nil", len(out))
		}
	}
}

// An RPC transport error must be wrapped through RedactRPCError so a managed-node
// endpoint's API key never reaches the returned error.
func TestBalanceOfRedactsRPCError(t *testing.T) {
	const apiKey = "SUPERSECRETAPIKEY123"
	rpc := newFakeRPC()
	rpc.callErr = &url.Error{
		Op:  "Post",
		URL: "https://mainnet.infura.io/v3/" + apiKey,
		Err: errors.New("connection refused"),
	}

	r := NewBalanceReader(rpc, nil)
	_, err := r.BalanceOf(context.Background(), testTokenHex, testHolderHex)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if strings.Contains(err.Error(), apiKey) {
		t.Errorf("error leaks API key: %v", err)
	}
	// The redacted host and underlying cause should survive.
	if !strings.Contains(err.Error(), "mainnet.infura.io") {
		t.Errorf("error dropped host, want it retained: %v", err)
	}
}
