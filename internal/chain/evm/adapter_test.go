package evm

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// validSignedTx builds a real, decodable signed transaction so the adapter's
// UnmarshalBinary + broadcast path runs. From is overridden by the caller to
// script the sender-match check; the recovered sender of the raw bytes is
// irrelevant to the adapter, which trusts the DTO's From and TxHash.
func validSignedTx(t *testing.T, req SignerRequest) SignedTx {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	chainID := new(big.Int).SetUint64(req.ChainID)
	to := req.To
	tx, err := types.SignNewTx(priv, types.LatestSignerForChainID(chainID), &types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     req.Nonce,
		GasTipCap: req.MaxPriorityFeePerGas,
		GasFeeCap: req.MaxFeePerGas,
		Gas:       req.GasLimit,
		To:        &to,
		Value:     req.Value,
		Data:      req.Data,
	})
	if err != nil {
		t.Fatalf("sign tx: %v", err)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal tx: %v", err)
	}
	return SignedTx{RawTransaction: raw, TxHash: tx.Hash(), From: crypto.PubkeyToAddress(priv.PublicKey)}
}

// happySigner signs a real tx and reports From == cfg.From so the adapter accepts it.
func happySigner(t *testing.T) *fakeSigner {
	return &fakeSigner{signFn: func(_ context.Context, req SignerRequest) (SignedTx, error) {
		st := validSignedTx(t, req)
		st.From = testFrom
		return st, nil
	}}
}

func validIntent() chain.PaymentIntent {
	return chain.PaymentIntent{KeyID: testKeyID, Asset: "USDC", To: testTo.Hex(), Amount: big.NewInt(1_000_000)}
}

func TestNewAdapterValidatesConfig(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Config)
	}{
		{"empty key id", func(c *Config) { c.KeyID = "" }},
		{"zero from", func(c *Config) { c.From = common.Address{} }},
		{"zero token", func(c *Config) { c.Token = common.Address{} }},
		{"zero gas cap", func(c *Config) { c.GasLimitCap = 0 }},
		{"nil fee cap", func(c *Config) { c.MaxFeePerGasCapWei = nil }},
		{"zero fee cap", func(c *Config) { c.MaxFeePerGasCapWei = big.NewInt(0) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.mut(&cfg)
			if _, err := NewAdapter(newFakeRPC(), happySigner(t), cfg, nil); err == nil {
				t.Fatal("NewAdapter succeeded, want error")
			}
		})
	}

	if _, err := NewAdapter(newFakeRPC(), happySigner(t), testConfig(), nil); err != nil {
		t.Fatalf("NewAdapter(valid) = %v, want nil", err)
	}
}

// The fee cap is copied at construction so a later mutation of the caller's
// *big.Int cannot move the ceiling Submit prices against.
func TestNewAdapterCopiesFeeCap(t *testing.T) {
	cfg := testConfig()
	feeCap := big.NewInt(100_000_000_000)
	cfg.MaxFeePerGasCapWei = feeCap
	a, err := NewAdapter(newFakeRPC(), happySigner(t), cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	feeCap.SetInt64(1) // mutate the caller's pointer
	if a.cfg.MaxFeePerGasCapWei.Cmp(big.NewInt(100_000_000_000)) != 0 {
		t.Fatalf("adapter fee cap moved to %s after caller mutation", a.cfg.MaxFeePerGasCapWei)
	}
}

func TestVerifyChainID(t *testing.T) {
	rpc := newFakeRPC()
	a, err := NewAdapter(rpc, happySigner(t), testConfig(), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	if err := a.VerifyChainID(context.Background()); err != nil {
		t.Errorf("VerifyChainID(matching) = %v, want nil", err)
	}

	rpc.chainID = big.NewInt(999) // wrong network
	if err := a.VerifyChainID(context.Background()); err == nil {
		t.Error("VerifyChainID(mismatch) = nil, want error")
	}
}

func TestSubmitIntentValidation(t *testing.T) {
	tests := []struct {
		name   string
		mut    func(*chain.PaymentIntent)
		target error
	}{
		{"unsupported asset", func(i *chain.PaymentIntent) { i.Asset = "ETH" }, chain.ErrUnsupportedAsset},
		{"malformed recipient", func(i *chain.PaymentIntent) { i.To = "not-an-address" }, chain.ErrInvalidIntent},
		{"zero recipient", func(i *chain.PaymentIntent) { i.To = (common.Address{}).Hex() }, chain.ErrInvalidIntent},
		{"nil amount", func(i *chain.PaymentIntent) { i.Amount = nil }, chain.ErrInvalidIntent},
		{"zero amount", func(i *chain.PaymentIntent) { i.Amount = big.NewInt(0) }, chain.ErrInvalidIntent},
		{"negative amount", func(i *chain.PaymentIntent) { i.Amount = big.NewInt(-1) }, chain.ErrInvalidIntent},
		{"oversized amount", func(i *chain.PaymentIntent) { i.Amount = new(big.Int).Lsh(big.NewInt(1), 256) }, chain.ErrInvalidIntent},
		{"mismatched key id", func(i *chain.PaymentIntent) { i.KeyID = "other-key" }, chain.ErrInvalidIntent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newFakeRPC()
			a, err := NewAdapter(rpc, happySigner(t), testConfig(), nil)
			if err != nil {
				t.Fatalf("NewAdapter: %v", err)
			}
			intent := validIntent()
			tc.mut(&intent)

			_, err = a.Submit(context.Background(), intent)
			if !errors.Is(err, tc.target) {
				t.Fatalf("err = %v, want wrap of %v", err, tc.target)
			}
			if rpc.sentCount() != 0 {
				t.Errorf("SendTransaction called %d times on an invalid intent, want 0", rpc.sentCount())
			}
		})
	}
}

func TestSubmitSignerRejected(t *testing.T) {
	rpc := newFakeRPC()
	sgn := &fakeSigner{signFn: func(context.Context, SignerRequest) (SignedTx, error) {
		return SignedTx{}, errors.New("signer said no")
	}}
	a, err := NewAdapter(rpc, sgn, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = a.Submit(context.Background(), validIntent())
	if !errors.Is(err, chain.ErrSignerRejected) {
		t.Fatalf("err = %v, want chain.ErrSignerRejected", err)
	}
	if rpc.sentCount() != 0 {
		t.Errorf("SendTransaction called after signer rejection")
	}
}

func TestSubmitFromMismatchAbortsBeforeBroadcast(t *testing.T) {
	rpc := newFakeRPC()
	sgn := &fakeSigner{signFn: func(_ context.Context, req SignerRequest) (SignedTx, error) {
		st := validSignedTx(t, req)
		st.From = common.HexToAddress("0x9999999999999999999999999999999999999999") // not cfg.From
		return st, nil
	}}
	a, err := NewAdapter(rpc, sgn, testConfig(), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = a.Submit(context.Background(), validIntent())
	if err == nil {
		t.Fatal("Submit succeeded on a sender mismatch, want error")
	}
	if rpc.sentCount() != 0 {
		t.Fatalf("SendTransaction called %d times despite sender mismatch, want 0", rpc.sentCount())
	}
}

func TestSubmitBroadcastError(t *testing.T) {
	rpc := newFakeRPC()
	rpc.sendErr = errors.New("node rejected")
	a, err := NewAdapter(rpc, happySigner(t), testConfig(), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	_, err = a.Submit(context.Background(), validIntent())
	if !errors.Is(err, chain.ErrBroadcast) {
		t.Fatalf("err = %v, want chain.ErrBroadcast", err)
	}
}

func TestSubmitHappyPath(t *testing.T) {
	rpc := newFakeRPC()
	a, err := NewAdapter(rpc, happySigner(t), testConfig(), nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	hash, err := a.Submit(context.Background(), validIntent())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if hash == "" {
		t.Fatal("Submit returned an empty tx hash")
	}
	if rpc.sentCount() != 1 {
		t.Fatalf("SendTransaction called %d times, want 1", rpc.sentCount())
	}
}
