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

// TestWatcherReversesAndReappliesOnSimulatedBackend proves the full M3 slice-2
// reverse+reapply cycle end-to-end against go-ethereum's in-memory EVM — a genuine
// chain, not a scripted fakeReader. It mines a real signed transaction, lets the
// watcher observe it Mined then Confirmed, then forks the chain from before that
// block and extends the side chain until it is canonical. The simulated backend
// re-injects the evicted tx into the pool, so it is re-mined on the new canonical
// chain. The watcher must therefore surface PhaseReorged (the confirmation is no
// longer terminal) and then a fresh PhaseConfirmed anchored to the NEW block hash.
// Fully hermetic: no env gate, no network — the whole reorg happens in process.
func TestWatcherReversesAndReappliesOnSimulatedBackend(t *testing.T) {
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

	// N=2 so the tx confirms after one further block and re-confirms after the reorg.
	// Finality depth well above any height this test reaches, so eviction never races
	// the reorg the test is exercising.
	w, err := NewWatcher(client, 2, 10, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	txHash := chain.TxHash(tx.Hash().Hex())
	if err := w.Track(txHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	// The watcher sees the tx mined and canonical at block 1 (depth 1 < N).
	got := w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].BlockNumber != 1 {
		t.Fatalf("mined at block %d, want 1", got[0].BlockNumber)
	}

	// One more block ⇒ depth 2 == N ⇒ Confirmed on the original chain.
	backend.Commit() // block 2
	confirmed := w.poll(ctx)
	requirePhases(t, confirmed, PhaseConfirmed)
	oldBlockHash := confirmed[0].BlockHash

	// Reorg: branch from before block 1 and extend the side chain until it is
	// STRICTLY longer than the 2-block main chain (three commits ⇒ length 3 > 2).
	// A side chain only becomes canonical once longer, so this switch is
	// deterministic and evicts the tx's block 1. The backend re-injects the evicted
	// tx into the pool, so the first side-chain block re-mines it.
	if err := backend.Fork(forkParent.Hash()); err != nil {
		t.Fatalf("Fork: %v", err)
	}
	backend.Commit()
	backend.Commit()
	backend.Commit()

	// Poll the cycle to completion: the confirmed anchor diverges ⇒ PhaseReorged
	// (the entry resets to pending), then the re-mined tx is observed afresh and,
	// already buried >= N deep on the new chain, re-confirms with the NEW block
	// hash. Confirmed is no longer terminal, so both emits are surfaced.
	var phases []Phase
	var reappliedHash string
	for i := 0; i < 5; i++ {
		for _, s := range w.poll(ctx) {
			phases = append(phases, s.Phase)
			if s.Phase == PhaseConfirmed {
				reappliedHash = s.BlockHash
			}
		}
		if len(phases) > 0 && phases[len(phases)-1] == PhaseConfirmed {
			break
		}
	}

	if len(phases) < 2 || phases[0] != PhaseReorged || phases[len(phases)-1] != PhaseConfirmed {
		t.Fatalf("phase sequence = %v, want [reorged ... confirmed]", phases)
	}
	if reappliedHash == "" {
		t.Fatal("reapplied confirmation carried no block hash")
	}
	if reappliedHash == oldBlockHash {
		t.Fatalf("reapplied confirmation reused the evicted block hash %s; want the new canonical block", oldBlockHash)
	}
}
