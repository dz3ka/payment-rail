package signer

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// testRecipient is a fixed, non-zero destination used across tests: any non-zero
// address passes the contract-creation guard.
var testRecipient = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

// validNativeReq is a well-formed native (ETH) transfer request for keyID on
// chainID. Table tests mutate a copy of it to exercise a single failure at a
// time; on its own it must validate and sign cleanly.
func validNativeReq(keyID string, chainID uint64) SignRequest {
	return SignRequest{
		KeyID:                keyID,
		ChainID:              chainID,
		Nonce:                7,
		GasLimit:             21_000,
		To:                   testRecipient,
		Value:                big.NewInt(1_000_000),
		MaxFeePerGas:         big.NewInt(50_000),
		MaxPriorityFeePerGas: big.NewInt(1_000),
	}
}

// erc20TransferData builds the calldata for transfer(address,uint256): the
// 4-byte selector, the left-padded recipient, and the 32-byte big-endian amount.
func erc20TransferData(to common.Address, amount *big.Int) []byte {
	d := make([]byte, erc20TransferCalldataLen)
	copy(d[:4], erc20TransferSelector[:])
	copy(d[16:36], to[:])
	amount.FillBytes(d[36:68])
	return d
}

// newTestKeyring builds an in-memory keyring around a freshly generated key, so
// no secret is ever committed and tests need no files. It returns the ring and
// the key's derived address for signature-recovery assertions.
func newTestKeyring(t *testing.T, keyID string, chainID uint64, limit *big.Int) (*Keyring, common.Address) {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey() = %v", err)
	}
	addr := crypto.PubkeyToAddress(priv.PublicKey)
	kr := &Keyring{keys: map[string]*keyEntry{
		keyID: {privateKey: priv, address: addr, chainID: chainID, bucket: newSpendBucket(limit)},
	}}
	return kr, addr
}

func TestSign_HappyPath_RecoversSenderAndHash(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, addr := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))
	s := NewSigner(kr)

	out, err := s.Sign(context.Background(), validNativeReq(keyID, chainID))
	if err != nil {
		t.Fatalf("Sign() = %v, want nil", err)
	}

	// Decode the broadcast bytes and recover the sender: a valid EIP-1559
	// signature must recover to exactly the key's address.
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(out.RawTransaction); err != nil {
		t.Fatalf("UnmarshalBinary(raw) = %v", err)
	}
	from, err := types.Sender(types.NewLondonSigner(new(big.Int).SetUint64(chainID)), tx)
	if err != nil {
		t.Fatalf("Sender() = %v", err)
	}
	if from != addr {
		t.Fatalf("recovered from = %s, want %s", from, addr)
	}
	if out.From != addr {
		t.Fatalf("SignedTx.From = %s, want %s", out.From, addr)
	}
	// The returned hash must match a hash recomputed from the decoded tx.
	if tx.Hash() != out.TxHash {
		t.Fatalf("TxHash = %s, want %s", out.TxHash, tx.Hash())
	}
	if tx.Type() != types.DynamicFeeTxType {
		t.Fatalf("tx type = %d, want dynamic-fee (%d)", tx.Type(), types.DynamicFeeTxType)
	}
}

func TestSign_SuccessAdvancesCounter_FailureDoesNot(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, _ := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))
	s := NewSigner(kr)
	bucket := kr.keys[keyID].bucket

	// A rejected request (wrong chain) must not consume any budget.
	bad := validNativeReq(keyID, chainID)
	bad.ChainID = chainID + 1
	if _, err := s.Sign(context.Background(), bad); err == nil {
		t.Fatal("Sign() with chain mismatch = nil, want error")
	}
	if bucket.spent.Sign() != 0 {
		t.Fatalf("spent = %s after a failed sign, want 0", bucket.spent)
	}

	// A successful native transfer advances the counter by exactly Value.
	req := validNativeReq(keyID, chainID)
	if _, err := s.Sign(context.Background(), req); err != nil {
		t.Fatalf("Sign() = %v, want nil", err)
	}
	if bucket.spent.Cmp(req.Value) != 0 {
		t.Fatalf("spent = %s, want %s", bucket.spent, req.Value)
	}
}

func TestSign_DefensiveCopy_CallerCannotMutateChargedAmount(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, _ := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))
	s := NewSigner(kr)
	bucket := kr.keys[keyID].bucket

	req := validNativeReq(keyID, chainID)
	original := new(big.Int).Set(req.Value)
	if _, err := s.Sign(context.Background(), req); err != nil {
		t.Fatalf("Sign() = %v, want nil", err)
	}
	// Mutating the caller's big.Int after the call must not retroactively change
	// what was charged: the signer copied it at the boundary.
	req.Value.SetInt64(0)
	if bucket.spent.Cmp(original) != 0 {
		t.Fatalf("spent = %s after caller mutated Value, want %s", bucket.spent, original)
	}
}

func TestSign_CancelledContext(t *testing.T) {
	const (
		keyID   = "hot"
		chainID = uint64(1)
	)
	kr, _ := newTestKeyring(t, keyID, chainID, big.NewInt(1_000_000_000))
	s := NewSigner(kr)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Sign(ctx, validNativeReq(keyID, chainID)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sign() = %v, want context.Canceled", err)
	}
}
