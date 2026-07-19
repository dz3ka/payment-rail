package evm

import (
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
)

// Compile-time proof that both the live JSON-RPC client and the simulated
// backend's client satisfy the ethRPC seam. If go-ethereum changes a signature,
// this fails to compile and we fix the interface to match — we never cast.
var (
	_ ethRPC = (*ethclient.Client)(nil)
	_ ethRPC = (simulated.Client)(nil)
)
