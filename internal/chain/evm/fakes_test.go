package evm

import (
	"context"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Shared test fixtures for the in-package unit tests. The integration test
// (evm_integration_test.go) uses the real simulated backend instead of these.

const testChainID uint64 = 1337

var (
	testFrom  = common.HexToAddress("0x1111111111111111111111111111111111111111")
	testToken = common.HexToAddress("0x2222222222222222222222222222222222222222")
	testTo    = common.HexToAddress("0x33333333333333333333333333333333333333AB")
)

const testKeyID = "key-1"

// testConfig is a valid adapter Config the unit tests mutate as needed.
func testConfig() Config {
	return Config{
		KeyID:              testKeyID,
		ChainID:            testChainID,
		From:               testFrom,
		Token:              testToken,
		GasLimitCap:        300_000,
		MaxFeePerGasCapWei: big.NewInt(100_000_000_000), // 100 gwei
	}
}

// fakeRPC is a canned-value ethRPC. Fields set sensible defaults via newFakeRPC;
// a test overrides the field(s) whose behavior it exercises. SendTransaction
// captures broadcast transactions under a mutex so concurrent Submits are safe
// under -race.
type fakeRPC struct {
	mu sync.Mutex

	chainID  *big.Int
	chainErr error

	nonce    uint64
	nonceErr error

	header    *types.Header
	headerErr error

	tip    *big.Int
	tipErr error

	estimate    uint64
	estimateErr error

	sendErr error
	sent    []*types.Transaction
}

func newFakeRPC() *fakeRPC {
	return &fakeRPC{
		chainID:  new(big.Int).SetUint64(testChainID),
		nonce:    0,
		header:   &types.Header{BaseFee: big.NewInt(1_000_000_000)}, // 1 gwei base fee
		tip:      big.NewInt(1_000_000_000),                         // 1 gwei tip
		estimate: 50_000,
	}
}

func (f *fakeRPC) ChainID(_ context.Context) (*big.Int, error) {
	return f.chainID, f.chainErr
}

func (f *fakeRPC) PendingNonceAt(_ context.Context, _ common.Address) (uint64, error) {
	if f.nonceErr != nil {
		return 0, f.nonceErr
	}
	return f.nonce, nil
}

func (f *fakeRPC) HeaderByNumber(_ context.Context, _ *big.Int) (*types.Header, error) {
	return f.header, f.headerErr
}

func (f *fakeRPC) SuggestGasTipCap(_ context.Context) (*big.Int, error) {
	return f.tip, f.tipErr
}

func (f *fakeRPC) EstimateGas(_ context.Context, _ ethereum.CallMsg) (uint64, error) {
	return f.estimate, f.estimateErr
}

func (f *fakeRPC) SendTransaction(_ context.Context, tx *types.Transaction) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.mu.Lock()
	f.sent = append(f.sent, tx)
	f.mu.Unlock()
	return nil
}

func (f *fakeRPC) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// fakeSigner drives the adapter's signer port from a closure so each test scripts
// the exact Sign outcome it needs.
type fakeSigner struct {
	signFn func(ctx context.Context, req SignerRequest) (SignedTx, error)
}

func (s *fakeSigner) Sign(ctx context.Context, req SignerRequest) (SignedTx, error) {
	return s.signFn(ctx, req)
}
