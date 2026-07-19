package evm

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// nonceAllocator hands out gap-free, strictly increasing nonces for each sender,
// serializing concurrent submissions on the same account. It mirrors the
// signer's spendBucket concurrency pattern: the mutex is held across the WHOLE
// {read → act → commit} critical section, not merely around a counter bump, so
// two goroutines submitting for the same sender can never both observe the same
// nonce and race to broadcast conflicting transactions.
//
// The allocator is chain-authoritative: each allocation takes the max of its
// local high-water and the node's PendingNonceAt, so it self-heals if a nonce is
// consumed out of band (another process, or a restart that zeroes the map) — it
// never hands out a nonce the chain already considers used.
type nonceAllocator struct {
	rpc ethRPC
	mu  sync.Mutex
	// next is the per-sender high-water: the next nonce to use. It is only ever
	// advanced on a committed (successful) allocation, so a failed submission
	// leaves the nonce free for the next caller — gap-free by construction.
	next map[common.Address]uint64
}

func newNonceAllocator(rpc ethRPC) *nonceAllocator {
	return &nonceAllocator{
		rpc:  rpc,
		next: make(map[common.Address]uint64),
	}
}

// withNonce allocates the next nonce for from, runs fn with it, and commits the
// allocation (advances the high-water to nonce+1) ONLY if fn returns nil. On an
// fn error the high-water is left untouched so the very next call reuses the
// nonce — a failed broadcast must not burn a nonce and open a gap that wedges
// every later transaction for the account.
//
// The lock spans the whole method: the pending-nonce query, fn (which signs and
// broadcasts), and the commit are one critical section per sender. Holding it
// across fn is the entire point — releasing it earlier would let a second caller
// read the same PendingNonceAt before the first has broadcast, producing two
// transactions with an identical nonce. Different senders never contend.
func (n *nonceAllocator) withNonce(ctx context.Context, from common.Address, fn func(nonce uint64) error) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// PendingNonceAt counts the account's transactions the node has already seen
	// (mined + pending), i.e. the next unused nonce from the chain's view. Taking
	// the max with our local high-water covers the window where we have signed a
	// nonce the node has not yet observed in its pending pool.
	pending, err := n.rpc.PendingNonceAt(ctx, from)
	if err != nil {
		return fmt.Errorf("evm: query pending nonce: %w", ErrNonceUnavailable)
	}

	nonce := pending
	if hw := n.next[from]; hw > nonce {
		nonce = hw
	}

	if err := fn(nonce); err != nil {
		// Commit nothing: the nonce stays available for the next allocation.
		return err
	}

	n.next[from] = nonce + 1
	return nil
}
