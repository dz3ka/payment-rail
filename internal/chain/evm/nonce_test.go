package evm

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

// The core M2 property: N concurrent allocations for one sender hand out unique,
// gap-free, strictly increasing nonces starting from PendingNonceAt. MUST pass
// under -race.
func TestWithNonceConcurrentUniqueIncreasing(t *testing.T) {
	const base uint64 = 5
	const n = 50

	alloc := newNonceAllocator(&fakeRPC{nonce: base})

	var mu sync.Mutex
	got := make([]uint64, 0, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := alloc.withNonce(context.Background(), testFrom, func(nonce uint64) error {
				mu.Lock()
				got = append(got, nonce)
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("withNonce: %v", err)
			}
		}()
	}
	wg.Wait()

	if len(got) != n {
		t.Fatalf("allocated %d nonces, want %d", len(got), n)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i, v := range got {
		want := base + uint64(i)
		if v != want {
			t.Fatalf("sorted nonce[%d] = %d, want %d (must be unique and gap-free from base)", i, v, want)
		}
	}
}

// A callback error leaves the high-water untouched, so the next call reuses the
// nonce — a failed submission must not open a gap that wedges the account.
func TestWithNonceErrorReusesNonce(t *testing.T) {
	alloc := newNonceAllocator(&fakeRPC{nonce: 10})
	ctx := context.Background()
	boom := errors.New("broadcast failed")

	var seen []uint64
	record := func(fail bool) error {
		return alloc.withNonce(ctx, testFrom, func(nonce uint64) error {
			seen = append(seen, nonce)
			if fail {
				return boom
			}
			return nil
		})
	}

	if err := record(true); !errors.Is(err, boom) {
		t.Fatalf("first call err = %v, want boom", err)
	}
	if err := record(false); err != nil {
		t.Fatalf("second call err = %v, want nil", err)
	}
	if err := record(false); err != nil {
		t.Fatalf("third call err = %v, want nil", err)
	}

	// Failed 10 (no advance) → reuse 10 → advance to 11.
	want := []uint64{10, 10, 11}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("seen = %v, want %v", seen, want)
		}
	}
}

// Local high-water dominates a stale PendingNonceAt: even though the fake always
// reports the same base, successive allocations still advance.
func TestWithNonceHighWaterDominatesStalePending(t *testing.T) {
	alloc := newNonceAllocator(&fakeRPC{nonce: 3})
	ctx := context.Background()

	for want := uint64(3); want <= 5; want++ {
		var got uint64
		if err := alloc.withNonce(ctx, testFrom, func(n uint64) error { got = n; return nil }); err != nil {
			t.Fatalf("withNonce: %v", err)
		}
		if got != want {
			t.Fatalf("nonce = %d, want %d", got, want)
		}
	}
}

func TestWithNoncePendingErrorSurfaces(t *testing.T) {
	alloc := newNonceAllocator(&fakeRPC{nonceErr: errors.New("rpc down")})

	err := alloc.withNonce(context.Background(), testFrom, func(uint64) error {
		t.Fatal("fn must not run when the pending nonce is unavailable")
		return nil
	})
	if !errors.Is(err, ErrNonceUnavailable) {
		t.Fatalf("err = %v, want ErrNonceUnavailable", err)
	}
}
