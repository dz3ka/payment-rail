//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/settlement"
)

// chaosBlockHash returns a distinct, valid 0x-hex 32-byte block hash for the
// given nibble. The settle/reverse external_ref scheme keys on the raw
// st.BlockHash string (settlement.go:193, :275), so distinct hashes are what
// make a re-mine at a new block post a distinct, non-colliding journal entry —
// this helper hands each scripted block an unambiguous, well-formed identity.
func chaosBlockHash(nibble byte) string {
	return "0x" + strings.Repeat(string(nibble), 64)
}

// txSettledBlockHash reads back the block hash a settled row is currently
// anchored to (NULL while pending/reorged), so a scenario can prove a re-settle
// re-anchored onto the NEW canonical block rather than the reorged-away one.
func txSettledBlockHash(ctx context.Context, t *testing.T, dbh *sql.DB, txHash string) (string, bool) {
	t.Helper()
	var bh sql.NullString
	if err := dbh.QueryRowContext(ctx,
		`SELECT settled_block_hash FROM settlements WHERE tx_hash = $1`, txHash,
	).Scan(&bh); err != nil {
		t.Fatalf("txSettledBlockHash query: %v", err)
	}
	return bh.String, bh.Valid
}

// settlementReversalCount returns how many settlement.reversal journal entries
// exist for paymentID — the mirror of assertSettleEntryCount, scoped by the
// reverse:<paymentID>:<blockHash> external_ref the reverse path keys on
// (settlement.go:275). Each reorg of a settled tx posts exactly one.
func settlementReversalCount(ctx context.Context, t *testing.T, dbh *sql.DB, paymentID uuid.UUID) int {
	t.Helper()
	var n int
	if err := dbh.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM journal_entries
		WHERE kind = 'settlement.reversal' AND external_ref LIKE $1`,
		"reverse:"+paymentID.String()+":%",
	).Scan(&n); err != nil {
		t.Fatalf("settlementReversalCount query: %v", err)
	}
	return n
}

// TestReorgChaos drives scripted reorg sequences harder than the unit tests, over
// real Postgres, and asserts POSTGRES-SIDE CONVERGENCE ONLY. The watcher's reorg
// DETECTION is already unit-tested (internal/chain/evm/watcher_reorg_test.go); here
// we hand-build the exported evm.Status the watcher would emit and deliver it via
// DIRECT Sink.OnStatus calls, so the subject under test is the LEDGER side: that a
// Confirmed→Reorged→Confirmed lifecycle nets to exactly one active settle, converges,
// and survives a process restart. Every status is delivered synchronously with an
// explicit, distinct block hash/number — no watcher Run loop, no timers, no
// goroutines — so the suite is deterministic under -race.
func TestReorgChaos(t *testing.T) {
	dbh := requireChaosDB(t)
	ctx := context.Background()

	// Subtest 1 — a deep reorg cycle nets to exactly one settle. Confirmed(A) settles;
	// Reorged reverses it (provisional credit restored); Confirmed(B) at a new,
	// deeper block re-settles under a distinct block-hash-scoped external_ref. The two
	// settles plus one reversal net to a single active settle, and the asset converges.
	// A second Reorged→Confirmed(C) cycle proves the reverse+reapply loop is idempotently
	// repeatable — the ledger keeps landing on one net settle no matter how many times
	// the tx is bounced in and out of the canonical chain.
	t.Run("deep_reorg_converges_to_one_net_settle", func(t *testing.T) {
		asset := chaosAsset()
		src := seedFundedAccount(ctx, t, dbh, asset, 1000)
		dst := seedFundedAccount(ctx, t, dbh, asset, 0)
		seedHouseAccount(ctx, t, dbh, asset)
		const amt = 600
		txHash := "0x" + strings.ReplaceAll(uuid.NewString(), "-", "")

		pid := seedPaymentAndLink(ctx, t, dbh, asset, src, dst, amt, txHash)
		// The committed payment already moved the provisional credit onto dst.
		assertBalance(ctx, t, dbh, dst, amt)

		sink := settlement.NewSink(ledger.NewSQLStore(dbh), quietLogger())

		blockA := chaosBlockHash('a')
		blockB := chaosBlockHash('b')
		blockC := chaosBlockHash('c')

		// Confirmed(A): settle. The provisional dst credit is released into the house
		// account (dst→0, house→amt) and the row anchors to A.
		confirmA := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseConfirmed, BlockHash: blockA, BlockNumber: 100, Depth: 6}
		if err := sink.OnStatus(ctx, confirmA); err != nil {
			t.Fatalf("OnStatus Confirmed(A): %v", err)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 1)
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("status after Confirmed(A) = %q, want settled", got)
		}
		if bh, ok := txSettledBlockHash(ctx, t, dbh, txHash); !ok || bh != blockA {
			t.Fatalf("settled anchor after Confirmed(A) = %q(valid=%v), want %q", bh, ok, blockA)
		}
		assertBalance(ctx, t, dbh, dst, 0)

		// Reorged: block A disappears. The settle is reversed (house→dst credit
		// restored: dst back to amt), the row flips to reorged and its anchor is NULLed.
		// The watcher emits Reorged against the OLD anchor, so BlockHash is A here.
		reorgA := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseReorged, BlockHash: blockA, BlockNumber: 100, Depth: 0}
		if err := sink.OnStatus(ctx, reorgA); err != nil {
			t.Fatalf("OnStatus Reorged(A): %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "reorged" {
			t.Fatalf("status after Reorged = %q, want reorged", got)
		}
		if _, ok := txSettledBlockHash(ctx, t, dbh, txHash); ok {
			t.Fatalf("settled anchor after Reorged still set, want NULL")
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 1) // reversal is a distinct kind
		if n := settlementReversalCount(ctx, t, dbh, pid); n != 1 {
			t.Fatalf("reversal entries after Reorged = %d, want 1", n)
		}
		assertBalance(ctx, t, dbh, dst, amt) // provisional credit restored

		// Confirmed(B): re-mine at a new, deeper block. settle posts again under the
		// distinct settle:<pid>:<B> external_ref (a second settle entry), dst→0 again,
		// and the row re-anchors to B.
		confirmB := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseConfirmed, BlockHash: blockB, BlockNumber: 140, Depth: 6}
		if err := sink.OnStatus(ctx, confirmB); err != nil {
			t.Fatalf("OnStatus Confirmed(B): %v", err)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 2) // A and B settle entries both exist
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("status after Confirmed(B) = %q, want settled", got)
		}
		if bh, ok := txSettledBlockHash(ctx, t, dbh, txHash); !ok || bh != blockB {
			t.Fatalf("settled anchor after Confirmed(B) = %q(valid=%v), want %q", bh, ok, blockB)
		}
		assertBalance(ctx, t, dbh, dst, 0)

		// NET effect is exactly one active settle: two settles + one reversal net to a
		// single settled amount, not double. Convergence + clean reconcile prove it.
		assertConverged(ctx, t, dbh, asset, amt)
		assertReconcileClean(ctx, t, dbh, asset, amt)

		// One more Reorged→Confirmed(C) cycle: idempotent repeatability. The net stays
		// one settle no matter how many reverse+reapply cycles the tx endures.
		reorgB := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseReorged, BlockHash: blockB, BlockNumber: 140, Depth: 0}
		if err := sink.OnStatus(ctx, reorgB); err != nil {
			t.Fatalf("OnStatus Reorged(B): %v", err)
		}
		assertBalance(ctx, t, dbh, dst, amt)
		if n := settlementReversalCount(ctx, t, dbh, pid); n != 2 {
			t.Fatalf("reversal entries after Reorged(B) = %d, want 2", n)
		}

		confirmC := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseConfirmed, BlockHash: blockC, BlockNumber: 200, Depth: 6}
		if err := sink.OnStatus(ctx, confirmC); err != nil {
			t.Fatalf("OnStatus Confirmed(C): %v", err)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 3)
		if bh, ok := txSettledBlockHash(ctx, t, dbh, txHash); !ok || bh != blockC {
			t.Fatalf("settled anchor after Confirmed(C) = %q(valid=%v), want %q", bh, ok, blockC)
		}
		assertBalance(ctx, t, dbh, dst, 0)
		assertConverged(ctx, t, dbh, asset, amt)
		assertReconcileClean(ctx, t, dbh, asset, amt)
	})

	// Subtest 2 — a reorg that happens while the process is DOWN still re-tracks and
	// converges after a restart. Confirmed(A) settles on a first Sink; that Sink is
	// then DISCARDED (modelling process death). A FRESH Sink over the same DB — with no
	// in-memory carryover — resolves the SAME persisted settlement row by tx_hash
	// (settlement.go:147, GetSettlementByTxHash) and applies the reversal + re-settle
	// against it. This exercises the recovery path: a settled/reorged row is fully
	// resolvable by a new process from persisted state alone.
	t.Run("reorg_across_restart_re_tracks_and_converges", func(t *testing.T) {
		asset := chaosAsset()
		src := seedFundedAccount(ctx, t, dbh, asset, 1000)
		dst := seedFundedAccount(ctx, t, dbh, asset, 0)
		seedHouseAccount(ctx, t, dbh, asset)
		const amt = 750
		txHash := "0x" + strings.ReplaceAll(uuid.NewString(), "-", "")

		pid := seedPaymentAndLink(ctx, t, dbh, asset, src, dst, amt, txHash)
		assertBalance(ctx, t, dbh, dst, amt)

		blockA := chaosBlockHash('d')
		blockB := chaosBlockHash('e')

		// Process 1: settle at block A, then "crash" (drop the Sink reference).
		sink1 := settlement.NewSink(ledger.NewSQLStore(dbh), quietLogger())
		confirmA := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseConfirmed, BlockHash: blockA, BlockNumber: 100, Depth: 6}
		if err := sink1.OnStatus(ctx, confirmA); err != nil {
			t.Fatalf("sink1 OnStatus Confirmed(A): %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("status after sink1 Confirmed(A) = %q, want settled", got)
		}
		assertBalance(ctx, t, dbh, dst, 0)
		sink1 = nil // the process is gone; no in-memory state carries over
		_ = sink1

		// Process 2 (restart): a brand-new Sink over the same DB. The reorg happened
		// while we were down; the fresh Sink learns of it only through the statuses it
		// is now handed, and resolves the persisted row purely by tx_hash.
		sink2 := settlement.NewSink(ledger.NewSQLStore(dbh), quietLogger())

		reorgA := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseReorged, BlockHash: blockA, BlockNumber: 100, Depth: 0}
		if err := sink2.OnStatus(ctx, reorgA); err != nil {
			t.Fatalf("sink2 OnStatus Reorged: %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "reorged" {
			t.Fatalf("status after sink2 Reorged = %q, want reorged", got)
		}
		if n := settlementReversalCount(ctx, t, dbh, pid); n != 1 {
			t.Fatalf("reversal entries after restart Reorged = %d, want 1", n)
		}
		assertBalance(ctx, t, dbh, dst, amt) // provisional credit restored by the new process

		confirmB := evm.Status{TxHash: chain.TxHash(txHash), Phase: evm.PhaseConfirmed, BlockHash: blockB, BlockNumber: 150, Depth: 6}
		if err := sink2.OnStatus(ctx, confirmB); err != nil {
			t.Fatalf("sink2 OnStatus Confirmed(B): %v", err)
		}
		if got := settlementStatus(ctx, t, dbh, txHash); got != "settled" {
			t.Fatalf("status after sink2 Confirmed(B) = %q, want settled", got)
		}
		if bh, ok := txSettledBlockHash(ctx, t, dbh, txHash); !ok || bh != blockB {
			t.Fatalf("settled anchor after restart re-settle = %q(valid=%v), want %q", bh, ok, blockB)
		}
		assertSettleEntryCount(ctx, t, dbh, pid, 2) // A (process 1) + B (process 2)
		assertBalance(ctx, t, dbh, dst, 0)

		// The restarted process landed the ledger on one net settle from persisted
		// state alone.
		assertConverged(ctx, t, dbh, asset, amt)
		assertReconcileClean(ctx, t, dbh, asset, amt)
	})
}
