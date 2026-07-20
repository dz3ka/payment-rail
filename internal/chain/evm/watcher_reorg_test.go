package evm

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// TestWatcherDetectsReorgOnSimulatedBackend proves the reorg path end-to-end
// against go-ethereum's in-memory EVM — a genuine chain, not a scripted
// fakeReader. It mines a real signed transaction into a block, lets the watcher
// observe it as Mined, then forks the chain from before that block and extends
// the side chain until it is canonical. On the next poll the tx's block is no
// longer canonical, so the watcher must surface PhaseReorged. Fully hermetic:
// no env gate, no network — the whole reorg happens in process.
func TestWatcherDetectsReorgOnSimulatedBackend(t *testing.T) {
	ctx := context.Background()

	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	from := crypto.PubkeyToAddress(priv.PublicKey)

	balance := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18)) // 1000 ETH
	backend := simulated.NewBackend(types.GenesisAlloc{from: {Balance: balance}})
	defer backend.Close()
	client := backend.Client()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("chain id: %v", err)
	}

	// The fork point: the current head (genesis) — the parent the side chain will
	// branch from, so the block carrying our tx is the one that gets evicted.
	forkParent, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		t.Fatalf("head header: %v", err)
	}

	// Sign and broadcast a real EIP-1559 value transfer, then mine it into block 1.
	to := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	tx, err := types.SignNewTx(priv, types.LatestSignerForChainID(chainID), &types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     0,
		GasTipCap: big.NewInt(1e9),  // 1 gwei tip
		GasFeeCap: big.NewInt(1e11), // 100 gwei cap — well over the sim base fee
		Gas:       21000,
		To:        &to,
		Value:     big.NewInt(1e15), // 0.001 ETH
	})
	if err != nil {
		t.Fatalf("sign tx: %v", err)
	}
	if err := client.SendTransaction(ctx, tx); err != nil {
		t.Fatalf("send tx: %v", err)
	}
	backend.Commit() // block 1 seals the tx

	// Deep N so the tx stays Mined (never confirms) across the reorg window.
	w, err := NewWatcher(client, 10, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	txHash := chain.TxHash(tx.Hash().Hex())
	if err := w.Track(txHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	// The watcher sees the tx mined and canonical.
	got := w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].BlockNumber != 1 {
		t.Fatalf("mined at block %d, want 1", got[0].BlockNumber)
	}

	// Reorg: branch from before block 1 and extend the side chain until it is
	// STRICTLY longer than the original (length 2 > 1). A side chain only becomes
	// canonical once longer, so two commits guarantee the switch deterministically
	// (equal length is only a probabilistic flip) and evict the tx's block 1.
	if err := backend.Fork(forkParent.Hash()); err != nil {
		t.Fatalf("Fork: %v", err)
	}
	backend.Commit()
	backend.Commit()

	// The tx's recorded block is no longer canonical ⇒ Reorged (detected either
	// by the receipt disappearing or by the canonical block at height 1 having a
	// different hash — both routes land here).
	requirePhases(t, w.poll(ctx), PhaseReorged)

	// Terminal: the tx is dropped from tracking, so further polls are silent.
	requirePhases(t, w.poll(ctx))
}
