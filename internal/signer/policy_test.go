package signer

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// TestValidate covers the trust boundary: every malformed shape must map to the
// right sentinel, and the two allowed shapes must return the correct charged
// amount. validate returns (key, charged, err); each case builds a valid base
// request and applies exactly one mutation so the failure under test is isolated.
func TestValidate(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, _ := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))

	// over32Bytes is 2^256 — one bit too wide to be a uint256.
	over32Bytes := new(big.Int).Lsh(big.NewInt(1), 256)

	tests := []struct {
		name        string
		mutate      func(r *SignRequest)
		wantErr     error    // nil => expect success
		wantCharged *big.Int // checked only on success
	}{
		{
			name:    "unknown key_id",
			mutate:  func(r *SignRequest) { r.KeyID = "cold" },
			wantErr: ErrUnknownKey,
		},
		{
			name:    "chain mismatch",
			mutate:  func(r *SignRequest) { r.ChainID = chainID + 1 },
			wantErr: ErrChainMismatch,
		},
		{
			// To is a value type, so "empty/short To" is structurally the zero
			// address, which is contract creation — rejected.
			name:    "contract creation (zero destination)",
			mutate:  func(r *SignRequest) { r.To = common.Address{} },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "nil value",
			mutate:  func(r *SignRequest) { r.Value = nil },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "uint256 field wider than 32 bytes",
			mutate:  func(r *SignRequest) { r.MaxFeePerGas = over32Bytes },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "max fee is zero",
			mutate:  func(r *SignRequest) { r.MaxFeePerGas = big.NewInt(0); r.MaxPriorityFeePerGas = big.NewInt(0) },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "fee inversion (tip above cap)",
			mutate:  func(r *SignRequest) { r.MaxPriorityFeePerGas = new(big.Int).Add(r.MaxFeePerGas, big.NewInt(1)) },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "gas below intrinsic floor",
			mutate:  func(r *SignRequest) { r.GasLimit = minGasLimit - 1 },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "gas above cap",
			mutate:  func(r *SignRequest) { r.GasLimit = maxGasLimit + 1 },
			wantErr: ErrMalformedTx,
		},
		{
			name:    "non-ERC-20 calldata",
			mutate:  func(r *SignRequest) { r.Data = []byte{0x01, 0x02, 0x03, 0x04} },
			wantErr: ErrMalformedTx,
		},
		{
			name: "ERC-20-shaped call carrying nonzero value",
			mutate: func(r *SignRequest) {
				r.Data = erc20TransferData(testRecipient, big.NewInt(5))
				r.Value = big.NewInt(1) // an ERC-20 transfer must move no ETH
			},
			wantErr: ErrMalformedTx,
		},
		{
			name:        "native transfer charges value",
			mutate:      func(r *SignRequest) { r.Value = big.NewInt(777) },
			wantCharged: big.NewInt(777),
		},
		{
			name: "ERC-20 transfer charges decoded amount",
			mutate: func(r *SignRequest) {
				r.Value = big.NewInt(0)
				r.Data = erc20TransferData(testRecipient, big.NewInt(4242))
			},
			wantCharged: big.NewInt(4242),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validNativeReq(keyID, chainID)
			tt.mutate(&r)

			_, charged, err := validate(kr, r)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("validate() err = %v, want errors.Is %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validate() = %v, want nil", err)
			}
			if charged.Cmp(tt.wantCharged) != 0 {
				t.Fatalf("charged = %s, want %s", charged, tt.wantCharged)
			}
		})
	}
}

// TestValidate_ChargedAmountIsDefensiveCopy proves the charged amount is signer-
// owned: for a native transfer it must not alias the request's Value pointer, so
// a later mutation of Value cannot change the charge already computed.
func TestValidate_ChargedAmountIsDefensiveCopy(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, _ := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))

	r := validNativeReq(keyID, chainID)
	r.Value = big.NewInt(500)
	_, charged, err := validate(kr, r)
	if err != nil {
		t.Fatalf("validate() = %v", err)
	}
	r.Value.SetInt64(1)
	if charged.Cmp(big.NewInt(500)) != 0 {
		t.Fatalf("charged = %s after mutating Value, want 500", charged)
	}
}
