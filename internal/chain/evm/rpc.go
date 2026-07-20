package evm

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ethRPC is the seam over the Ethereum JSON-RPC client. It names only the
// handful of calls the adapter makes, so the adapter depends on an interface it
// owns rather than on the concrete *ethclient.Client. The method set is a strict
// subset of go-ethereum's client surface: both *ethclient.Client and the
// simulated backend's Client() satisfy it (asserted in rpc_test.go), so the
// same adapter runs against a live node and a hermetic in-memory chain unchanged.
type ethRPC interface {
	ChainID(ctx context.Context) (*big.Int, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}
