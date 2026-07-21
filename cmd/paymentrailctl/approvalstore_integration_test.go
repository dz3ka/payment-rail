package main

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/audit"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/policy"
)

// TestApprovalStoreIntegration exercises the real *sql.DB-backed pgApprovalStore
// against a live Postgres (skipped unless PAYMENT_RAIL_TEST_DSN is set — the same
// gate the ledger, settlement, and velocity integration tests use). The four-eyes
// gate itself (WP3) is proven hermetically; this file proves the one thing a fake
// can't: the real SQL path — INSERT round-trip of the frozen intent, decide-returns-
// error ⇒ ROLLBACK ⇒ row stays pending, the status guard on MarkBroadcast, and that
// the SELECT ... FOR UPDATE row lock actually serializes concurrent claims so N
// racing approvers yield exactly one winner.
//
// Isolation mirrors the other integration tests: each subtest mints fresh
// uuid-suffixed proposer/approver/key identities and lets Propose generate the row
// id, so subtests never collide on the shared dev DB — no truncation needed.
func TestApprovalStoreIntegration(t *testing.T) {
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run the approval store integration test")
	}

	ctx := context.Background()
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	store := newApprovalStore(sqlDB)

	// newIntent builds a unique, self-contained intent so each subtest's rows are
	// disjoint. To/KeyID carry the uuid so a later direct inspection (if any) can
	// key on them; PaymentID is left "" (the NULL round-trip is covered implicitly).
	newIntent := func(amount *big.Int) policy.Intent {
		u := uuid.NewString()
		return policy.Intent{
			To:     "0xto-" + u,
			Asset:  "USDC",
			KeyID:  "key-" + u,
			Amount: amount,
		}
	}

	// scanChain reads the whole hash-chained audit log (WP2) so a subtest can find
	// its own operator rows and re-verify the chain still links after the operator
	// appends interleave with any state-transition rows already present.
	scanChain := func(t *testing.T) []db.AuditLog {
		t.Helper()
		rows, err := db.New(sqlDB).ScanAuditChain(ctx)
		if err != nil {
			t.Fatalf("ScanAuditChain: %v", err)
		}
		return rows
	}

	// countByActorAction counts audit rows for a given operator handle + action.
	// Actor is uuid-unique per subtest, so this isolates a subtest's rows on the
	// shared dev DB without truncation — the same isolation the rest of the file uses.
	countByActorAction := func(rows []db.AuditLog, actor, action string) int {
		n := 0
		for _, r := range rows {
			if r.Actor == actor && r.Action == action {
				n++
			}
		}
		return n
	}

	// Case 1: propose → claim happy path. The claimed intent must equal the parked
	// one field-for-field (amount via Cmp, since it round-trips int64→*big.Int), and
	// MarkBroadcast on the freshly-approved row must succeed.
	t.Run("propose then claim returns frozen intent and broadcasts", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		approver := "appr-" + uuid.NewString()
		in := newIntent(big.NewInt(500))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		// F9: a successful Propose commits exactly one operator.propose audit row.
		if got := countByActorAction(scanChain(t), proposer, "operator.propose"); got != 1 {
			t.Errorf("operator.propose rows for proposer = %d, want 1", got)
		}

		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, approver})
		got, err := store.Claim(ctx, id, approver, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approver)
		})
		if err != nil {
			t.Fatalf("Claim = %v, want nil", err)
		}
		if got.To != in.To || got.Asset != in.Asset || got.KeyID != in.KeyID || got.PaymentID != in.PaymentID {
			t.Errorf("claimed intent = %+v, want %+v (fields must round-trip)", got, in)
		}
		if got.Amount.Cmp(in.Amount) != 0 {
			t.Errorf("claimed amount = %s, want %s", got.Amount, in.Amount)
		}

		// F9: a successful Claim commits exactly one operator.approve audit row, and
		// the full chain must still verify OK after the operator appends interleave
		// with any state-transition rows already in the log.
		rows := scanChain(t)
		if n := countByActorAction(rows, approver, "operator.approve"); n != 1 {
			t.Errorf("operator.approve rows for approver = %d, want 1", n)
		}
		if res, err := audit.Verify(rows); err != nil || !res.OK {
			t.Errorf("audit.Verify after operator appends = (%+v, %v), want OK/nil", res, err)
		}

		if err := store.MarkBroadcast(ctx, id, "0xdeadbeef"); err != nil {
			t.Fatalf("MarkBroadcast = %v, want nil", err)
		}
	})

	// Case 2: self-approval rejected, row stays pending. decide returns
	// ErrSelfApproval, which must survive errors.Is AND roll the tx back so the row
	// is still claimable — a second Claim by a distinct valid approver then succeeds.
	t.Run("self approval rejected leaves row pending", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		approver := "appr-" + uuid.NewString()
		in := newIntent(big.NewInt(750))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		// Proposer is in the allowlist, so the reject is ErrSelfApproval (distinctness),
		// NOT ErrUnknownApprover (which the gate checks first).
		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, approver})
		_, err = store.Claim(ctx, id, proposer, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, proposer)
		})
		if !errors.Is(err, policy.ErrSelfApproval) {
			t.Fatalf("self-approval Claim = %v, want errors.Is ErrSelfApproval", err)
		}

		// F9 fail-closed: the rejected self-approval rolled back, so NO partial
		// operator.approve append may survive for the proposer's rejected attempt.
		if n := countByActorAction(scanChain(t), proposer, "operator.approve"); n != 0 {
			t.Errorf("operator.approve rows for rejected self-approver = %d, want 0 (rollback)", n)
		}

		// Row must still be pending: a valid distinct approver can still claim it.
		if _, err := store.Claim(ctx, id, approver, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approver)
		}); err != nil {
			t.Fatalf("re-Claim after self-approval = %v, want nil (row must stay pending)", err)
		}
	})

	// Case 3: unknown approver rejected, row stays pending. Same rollback property as
	// case 2 but via the unknown-approver branch.
	t.Run("unknown approver rejected leaves row pending", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		known := "appr-" + uuid.NewString()
		stranger := "stranger-" + uuid.NewString()
		in := newIntent(big.NewInt(600))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		// Gate knows proposer + known, but NOT stranger.
		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, known})
		_, err = store.Claim(ctx, id, stranger, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, stranger)
		})
		if !errors.Is(err, policy.ErrUnknownApprover) {
			t.Fatalf("unknown-approver Claim = %v, want errors.Is ErrUnknownApprover", err)
		}

		// F9 fail-closed: the rejected unknown-approver claim rolled back, so no
		// operator.approve append may survive for the stranger's rejected attempt.
		if n := countByActorAction(scanChain(t), stranger, "operator.approve"); n != 0 {
			t.Errorf("operator.approve rows for rejected stranger = %d, want 0 (rollback)", n)
		}

		if _, err := store.Claim(ctx, id, known, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, known)
		}); err != nil {
			t.Fatalf("re-Claim after unknown approver = %v, want nil (row must stay pending)", err)
		}
	})

	// Case 4: double-claim rejected. Once approved, a second claim (even by another
	// valid approver) hits the non-pending guard ⇒ ErrAlreadyApproved. MarkBroadcast
	// works once, then a second broadcast fails the status/tx_hash guard.
	t.Run("double claim rejected and broadcast is once-only", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		approverA := "apprA-" + uuid.NewString()
		approverB := "apprB-" + uuid.NewString()
		in := newIntent(big.NewInt(900))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, approverA, approverB})
		if _, err := store.Claim(ctx, id, approverA, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approverA)
		}); err != nil {
			t.Fatalf("first Claim = %v, want nil", err)
		}

		_, err = store.Claim(ctx, id, approverB, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approverB)
		})
		if !errors.Is(err, ErrAlreadyApproved) {
			t.Fatalf("second Claim = %v, want errors.Is ErrAlreadyApproved", err)
		}

		if err := store.MarkBroadcast(ctx, id, "0xfeed"); err != nil {
			t.Fatalf("first MarkBroadcast = %v, want nil", err)
		}
		if err := store.MarkBroadcast(ctx, id, "0xfeed"); err == nil {
			t.Fatalf("second MarkBroadcast = nil, want a non-nil guard error (already broadcast)")
		}
	})

	// Case 5: unknown id. A well-formed but never-proposed uuid is ErrApprovalNotFound.
	t.Run("claim on unknown id is not found", func(t *testing.T) {
		approver := "appr-" + uuid.NewString()
		gate := policy.NewApprovalGate(big.NewInt(1), []string{approver})
		_, err := store.Claim(ctx, uuid.NewString(), approver, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approver)
		})
		if !errors.Is(err, ErrApprovalNotFound) {
			t.Fatalf("Claim on unknown id = %v, want errors.Is ErrApprovalNotFound", err)
		}
	})

	// Case 6: concurrency / FOR UPDATE proof. N goroutines race to claim ONE approval,
	// each with a distinct valid approver. If the locked read + guarded UPDATE were not
	// serialized, several would pass the pending check and all approve. Exactly one must
	// win; the rest must fail with ErrAlreadyApproved — mirroring velocity's mutex-
	// guarded counter style.
	t.Run("concurrent claims yield a single winner", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		in := newIntent(big.NewInt(1234))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		const n = 20
		// Each goroutine opens a tx and blocks on the row lock, so the pool must admit
		// all N at once or they starve rather than serialize.
		sqlDB.SetMaxOpenConns(n + 5)

		approvers := make([]string, n)
		allow := make([]string, 0, n+1)
		allow = append(allow, proposer)
		for i := range approvers {
			approvers[i] = "appr-" + uuid.NewString()
			allow = append(allow, approvers[i])
		}
		gate := policy.NewApprovalGate(big.NewInt(1), allow)

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners int
			denied  int
		)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(approver string) {
				defer wg.Done()
				_, err := store.Claim(ctx, id, approver, func(pa policy.PendingApproval) error {
					return gate.Authorize(pa.Proposer, approver)
				})
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					winners++
				case errors.Is(err, ErrAlreadyApproved):
					denied++
				default:
					t.Errorf("unexpected concurrent Claim error = %v", err)
				}
			}(approvers[i])
		}
		wg.Wait()

		if winners != 1 {
			t.Errorf("winners = %d, want exactly 1 (FOR UPDATE must serialize the claim)", winners)
		}
		if denied != n-1 {
			t.Errorf("denied = %d, want %d (all losers see ErrAlreadyApproved)", denied, n-1)
		}
	})

	// Case 7: overflow fails closed at propose-time. An amount past int64 range can't be
	// a Postgres BIGINT, so Propose rejects it before any INSERT and returns no id.
	t.Run("overflow amount fails closed at propose", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		huge := new(big.Int).Lsh(big.NewInt(1), 64) // 2^64 > math.MaxInt64
		in := newIntent(huge)

		id, err := store.Propose(ctx, proposer, in)
		if err == nil {
			t.Fatalf("Propose(overflow) = nil, want a non-nil operational error")
		}
		if id != "" {
			t.Errorf("Propose(overflow) id = %q, want \"\" (no row inserted)", id)
		}
	})

	// Case 8: reopen a claimed-but-unbroadcast approval. After Claim marks the row
	// approved (tx_hash NULL), Reopen reverts it to pending so a distinct approver
	// can re-Claim it — the auto-recovery for a pre-send broadcast failure. Once a
	// hash is recorded (MarkBroadcast), the tx_hash-IS-NULL guard makes Reopen a
	// no-op ⇒ operator-facing error, so a sent payment can never be resurrected.
	t.Run("reopen reverts an unbroadcast approval and refuses a broadcast one", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		approverA := "apprA-" + uuid.NewString()
		approverB := "apprB-" + uuid.NewString()
		in := newIntent(big.NewInt(1500))

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}

		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, approverA, approverB})
		if _, err := store.Claim(ctx, id, approverA, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approverA)
		}); err != nil {
			t.Fatalf("first Claim = %v, want nil", err)
		}

		// Nothing was broadcast (tx_hash NULL): Reopen must revert the row to pending.
		if err := store.Reopen(ctx, id); err != nil {
			t.Fatalf("Reopen = %v, want nil (approved, tx_hash NULL is reopenable)", err)
		}

		// The reopened row is pending again: a distinct valid approver can re-Claim it.
		if _, err := store.Claim(ctx, id, approverB, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approverB)
		}); err != nil {
			t.Fatalf("re-Claim after Reopen = %v, want nil (row must be pending again)", err)
		}

		// Now record a broadcast; the tx_hash guard must make a subsequent Reopen a
		// no-op that surfaces an operator-facing error (never resurrect a sent payment).
		if err := store.MarkBroadcast(ctx, id, "0x"+uuid.NewString()); err != nil {
			t.Fatalf("MarkBroadcast = %v, want nil", err)
		}
		if err := store.Reopen(ctx, id); err == nil {
			t.Fatalf("Reopen after broadcast = nil, want a non-nil guard error (already sent)")
		}
	})

	// Case 9: F9 aggregate anchoring. When the proposal carries a linked payment id,
	// BOTH operator rows must anchor to that payment id (AggregateType "payment") so
	// `audit verify`/browsing shows one coherent story per payment — proposer's
	// propose and approver's approve sharing the settlement's aggregate id.
	t.Run("operator actions anchor to the linked payment id", func(t *testing.T) {
		proposer := "prop-" + uuid.NewString()
		approver := "appr-" + uuid.NewString()
		paymentID := uuid.NewString()
		in := newIntent(big.NewInt(2000))
		in.PaymentID = paymentID

		id, err := store.Propose(ctx, proposer, in)
		if err != nil {
			t.Fatalf("Propose = %v, want nil", err)
		}
		gate := policy.NewApprovalGate(big.NewInt(1), []string{proposer, approver})
		if _, err := store.Claim(ctx, id, approver, func(pa policy.PendingApproval) error {
			return gate.Authorize(pa.Proposer, approver)
		}); err != nil {
			t.Fatalf("Claim = %v, want nil", err)
		}

		rows := scanChain(t)
		for _, want := range []struct{ actor, action string }{
			{proposer, "operator.propose"},
			{approver, "operator.approve"},
		} {
			var found *db.AuditLog
			for i := range rows {
				if rows[i].Actor == want.actor && rows[i].Action == want.action {
					found = &rows[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("no %s row for %s", want.action, want.actor)
			}
			if found.AggregateType != "payment" || found.AggregateID != paymentID {
				t.Errorf("%s aggregate = %s/%s, want payment/%s", want.action, found.AggregateType, found.AggregateID, paymentID)
			}
		}
		if res, err := audit.Verify(rows); err != nil || !res.OK {
			t.Errorf("audit.Verify = (%+v, %v), want OK/nil", res, err)
		}
	})
}
