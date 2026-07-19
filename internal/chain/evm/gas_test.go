package evm

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestGasEstimateBufferApplied(t *testing.T) {
	rpc := newFakeRPC()
	rpc.estimate = 50_000 // *125/100 = 62500

	gp, err := gasEstimate(context.Background(), rpc, testConfig(), ethereum.CallMsg{})
	if err != nil {
		t.Fatalf("gasEstimate: %v", err)
	}
	if gp.gasLimit != 62_500 {
		t.Errorf("gasLimit = %d, want 62500 (50000 * 125 / 100)", gp.gasLimit)
	}
	// maxFee = baseFee*2 + tip = 1gwei*2 + 1gwei = 3gwei.
	wantMaxFee := big.NewInt(3_000_000_000)
	if gp.maxFee.Cmp(wantMaxFee) != 0 {
		t.Errorf("maxFee = %s, want %s", gp.maxFee, wantMaxFee)
	}
}

func TestGasEstimateMaxFeeAtLeastTip(t *testing.T) {
	rpc := newFakeRPC()
	// Even with a zero base fee (edge of the arithmetic), maxFee must stay >= tip.
	rpc.header = &types.Header{BaseFee: big.NewInt(0)}
	rpc.tip = big.NewInt(7_000_000_000)

	gp, err := gasEstimate(context.Background(), rpc, testConfig(), ethereum.CallMsg{})
	if err != nil {
		t.Fatalf("gasEstimate: %v", err)
	}
	if gp.maxFee.Cmp(gp.tip) < 0 {
		t.Errorf("maxFee %s < tip %s violates signer precondition", gp.maxFee, gp.tip)
	}
}

func TestGasEstimateOverGasCap(t *testing.T) {
	rpc := newFakeRPC()
	rpc.estimate = 50_000 // buffered to 62500
	cfg := testConfig()
	cfg.GasLimitCap = 60_000 // below 62500

	_, err := gasEstimate(context.Background(), rpc, cfg, ethereum.CallMsg{})
	if !errors.Is(err, ErrGasCapExceeded) {
		t.Fatalf("err = %v, want ErrGasCapExceeded", err)
	}
}

func TestGasEstimateBufferOverflowRejected(t *testing.T) {
	rpc := newFakeRPC()
	// A hostile/misbehaving node returns an estimate whose *125 product wraps
	// uint64: naive (estimate*125/100) would fold to ~21001, slipping under the
	// cap and signing an under-gassed tx. The overflow-safe multiply must reject.
	rpc.estimate = 3_689_348_814_741_927_124

	_, err := gasEstimate(context.Background(), rpc, testConfig(), ethereum.CallMsg{})
	if !errors.Is(err, ErrGasCapExceeded) {
		t.Fatalf("err = %v, want ErrGasCapExceeded for an overflowing estimate", err)
	}
}

func TestGasEstimateOverFeeCap(t *testing.T) {
	rpc := newFakeRPC() // maxFee = 3gwei
	cfg := testConfig()
	cfg.MaxFeePerGasCapWei = big.NewInt(2_000_000_000) // 2gwei, below 3gwei

	_, err := gasEstimate(context.Background(), rpc, cfg, ethereum.CallMsg{})
	if !errors.Is(err, ErrFeeCapExceeded) {
		t.Fatalf("err = %v, want ErrFeeCapExceeded", err)
	}
}

func TestGasEstimateRPCErrors(t *testing.T) {
	boom := errors.New("rpc down")
	tests := []struct {
		name  string
		setup func(*fakeRPC)
	}{
		{"estimate error", func(r *fakeRPC) { r.estimateErr = boom }},
		{"tip error", func(r *fakeRPC) { r.tipErr = boom }},
		{"header error", func(r *fakeRPC) { r.headerErr = boom }},
		{"nil base fee", func(r *fakeRPC) { r.header = &types.Header{BaseFee: nil} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := newFakeRPC()
			tc.setup(rpc)
			_, err := gasEstimate(context.Background(), rpc, testConfig(), ethereum.CallMsg{})
			if !errors.Is(err, ErrGasEstimation) {
				t.Fatalf("err = %v, want ErrGasEstimation", err)
			}
		})
	}
}
