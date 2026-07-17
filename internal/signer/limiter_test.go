package signer

import (
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
)

// okSign is a no-op sign step: the limiter tests exercise the counting/locking
// logic, not real signing, so the callback just reports success.
func okSign() (SignedTx, error) { return SignedTx{}, nil }

func TestSpendBucket_SingleOverLimitRejected(t *testing.T) {
	b := newSpendBucket(big.NewInt(100))
	if _, err := b.charge(big.NewInt(150), okSign); !errors.Is(err, ErrSpendLimitExceeded) {
		t.Fatalf("charge(150) = %v, want ErrSpendLimitExceeded", err)
	}
	if b.spent.Sign() != 0 {
		t.Fatalf("spent = %s after a rejected charge, want 0", b.spent)
	}
}

func TestSpendBucket_CumulativeCrossingRejected(t *testing.T) {
	b := newSpendBucket(big.NewInt(100))
	for i, amt := range []int64{40, 40} {
		if _, err := b.charge(big.NewInt(amt), okSign); err != nil {
			t.Fatalf("charge #%d(%d) = %v, want nil", i, amt, err)
		}
	}
	// spent is now 80; the third charge of 40 crosses 100 and must be rejected.
	if _, err := b.charge(big.NewInt(40), okSign); !errors.Is(err, ErrSpendLimitExceeded) {
		t.Fatalf("crossing charge(40) = %v, want ErrSpendLimitExceeded", err)
	}
	if b.spent.Cmp(big.NewInt(80)) != 0 {
		t.Fatalf("spent = %s, want 80", b.spent)
	}
}

func TestSpendBucket_FailedSignDoesNotCommit(t *testing.T) {
	b := newSpendBucket(big.NewInt(100))
	wantErr := errors.New("boom")
	_, err := b.charge(big.NewInt(40), func() (SignedTx, error) { return SignedTx{}, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("charge() = %v, want boom", err)
	}
	if b.spent.Sign() != 0 {
		t.Fatalf("spent = %s after a failed sign, want 0", b.spent)
	}
}

// TestSpendBucket_ConcurrentOneKeyOnlyKFit is the proof of the invariant under
// contention: N goroutines hammer one key with equal charges where only K fit
// under the limit. Exactly K must succeed, and the committed total must equal
// the sum of exactly those K charges — never more.
func TestSpendBucket_ConcurrentOneKeyOnlyKFit(t *testing.T) {
	const (
		amt = int64(10)
		K   = 7
		N   = 100
	)
	limit := big.NewInt(amt * K)
	b := newSpendBucket(limit)

	var succeeded atomic.Int64
	var wg sync.WaitGroup
	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			if _, err := b.charge(big.NewInt(amt), okSign); err == nil {
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := succeeded.Load(); got != K {
		t.Fatalf("successful charges = %d, want %d", got, K)
	}
	// Reading spent after wg.Wait is safe: every charge happened-before the wait.
	if want := big.NewInt(amt * K); b.spent.Cmp(want) != 0 {
		t.Fatalf("committed total = %s, want %s", b.spent, want)
	}
}

// TestSpendBucket_Property_AdmittedNeverExceedLimit is the property form of the
// invariant: for any sequence of charges, the sum of admitted amounts never
// exceeds the limit and always equals the committed total.
func TestSpendBucket_Property_AdmittedNeverExceedLimit(t *testing.T) {
	f := func(raw []uint16) bool {
		limit := big.NewInt(1000)
		b := newSpendBucket(limit)
		admitted := new(big.Int)
		for _, r := range raw {
			amt := big.NewInt(int64(r))
			if _, err := b.charge(amt, okSign); err == nil {
				admitted.Add(admitted, amt)
			}
		}
		return admitted.Cmp(limit) <= 0 && admitted.Cmp(b.spent) == 0
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}
