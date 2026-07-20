package evm

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"sort"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// simSigner is a hermetic stand-in for the isolated signer: it signs a REAL
// EIP-1559 transaction with an ephemeral key so the adapter's marshal/broadcast
// path exercises genuine bytes against a genuine EVM. The full signer-over-gRPC
// wire proof is WP5's job; this keeps WP4 on adapter build/nonce/gas/broadcast.
type simSigner struct {
	priv    *ecdsa.PrivateKey
	from    common.Address
	chainID *big.Int
}

func (s *simSigner) Sign(_ context.Context, req SignerRequest) (SignedTx, error) {
	to := req.To
	tx, err := types.SignNewTx(s.priv, types.LatestSignerForChainID(s.chainID), &types.DynamicFeeTx{
		ChainID:   s.chainID,
		Nonce:     req.Nonce,
		GasTipCap: req.MaxPriorityFeePerGas,
		GasFeeCap: req.MaxFeePerGas,
		Gas:       req.GasLimit,
		To:        &to,
		Value:     req.Value,
		Data:      req.Data,
	})
	if err != nil {
		return SignedTx{}, err
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		return SignedTx{}, err
	}
	return SignedTx{RawTransaction: raw, TxHash: tx.Hash(), From: s.from}, nil
}

// TestAdapterEndToEndSimulated drives the adapter against go-ethereum's in-memory
// chain: it verifies the network, fires concurrent Submits, mines them, and
// checks each transaction mined with a unique sequential nonce and the exact
// ERC-20 calldata. Fully hermetic — no env gate, no network.
//
// Note: cfg.Token points at an address with NO deployed code. A transfer() to a
// codeless account still estimates and mines (it just does nothing on-chain), so
// this proves the adapter's build/nonce/gas/broadcast wiring, not real ERC-20
// token movement — that needs a deployed token and is out of scope here.
func TestAdapterEndToEndSimulated(t *testing.T) {
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

	cfg := Config{
		KeyID:              "sim-key",
		ChainID:            chainID.Uint64(),
		From:               from,
		Token:              testToken, // dummy codeless address (see note above)
		GasLimitCap:        500_000,
		MaxFeePerGasCapWei: new(big.Int).Mul(big.NewInt(1e9), big.NewInt(1_000_000)), // 1e6 gwei — generous
	}

	adapter, err := NewAdapter(client, &simSigner{priv: priv, from: from, chainID: chainID}, cfg, nil)
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	if err := adapter.VerifyChainID(ctx); err != nil {
		t.Fatalf("VerifyChainID: %v", err)
	}

	const n = 5
	recipient := testTo
	amount := big.NewInt(1_000_000)
	wantData, err := packERC20Transfer(recipient, amount)
	if err != nil {
		t.Fatalf("packERC20Transfer: %v", err)
	}

	var (
		mu     sync.Mutex
		hashes []common.Hash
		wg     sync.WaitGroup
		errs   = make(chan error, n)
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := adapter.Submit(ctx, chain.PaymentIntent{
				KeyID:  cfg.KeyID,
				Asset:  "USDC",
				To:     recipient.Hex(),
				Amount: amount,
			})
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			hashes = append(hashes, common.HexToHash(string(h)))
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Submit: %v", err)
	}
	if len(hashes) != n {
		t.Fatalf("collected %d tx hashes, want %d", len(hashes), n)
	}

	backend.Commit() // mine every pending tx into one block

	signer := types.LatestSignerForChainID(chainID)
	nonces := make([]uint64, 0, n)
	for _, h := range hashes {
		receipt, err := client.TransactionReceipt(ctx, h)
		if err != nil {
			t.Fatalf("receipt for %s: %v", h.Hex(), err)
		}
		if receipt.Status != types.ReceiptStatusSuccessful {
			t.Errorf("tx %s mined with status %d, want success", h.Hex(), receipt.Status)
		}

		tx, pending, err := client.TransactionByHash(ctx, h)
		if err != nil {
			t.Fatalf("tx %s: %v", h.Hex(), err)
		}
		if pending {
			t.Errorf("tx %s still pending after Commit", h.Hex())
		}

		sender, err := types.Sender(signer, tx)
		if err != nil {
			t.Fatalf("recover sender: %v", err)
		}
		if sender != from {
			t.Errorf("recovered sender %s, want %s", sender.Hex(), from.Hex())
		}
		if tx.To() == nil || *tx.To() != cfg.Token {
			t.Errorf("tx To = %v, want %s", tx.To(), cfg.Token.Hex())
		}
		if tx.Value().Sign() != 0 {
			t.Errorf("tx Value = %s, want 0", tx.Value())
		}
		if !bytes.Equal(tx.Data(), wantData) {
			t.Errorf("tx Data = %x, want %x", tx.Data(), wantData)
		}
		nonces = append(nonces, tx.Nonce())
	}

	sort.Slice(nonces, func(i, j int) bool { return nonces[i] < nonces[j] })
	for i, got := range nonces {
		if got != uint64(i) {
			t.Fatalf("mined nonces = %v, want strictly sequential 0..%d", nonces, n-1)
		}
	}
}
