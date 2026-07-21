package ledger

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
)

// fakeStore is an in-memory Store whose ExecTx runs fn directly against a shared
// fakeQuerier — there is no real transaction and no rollback. That is enough to
// exercise PostEntry's orchestration: the abort paths under test all return
// before writing anything, so "no partial write" holds without rollback.
type fakeStore struct {
	q *fakeQuerier
}

// ExecTx ignores ctx (a real Store would begin/commit a tx with it); the fake
// just runs fn against shared state. ctx cancellation is honored inside the
// fakeQuerier methods, mirroring how QueryContext respects a cancelled context.
func (s *fakeStore) ExecTx(_ context.Context, fn func(q db.Querier) error) error {
	return fn(s.q)
}

// fakeQuerier is an in-memory implementation of db.Querier. Balances are never
// stored: GetAccountBalance recomputes Σcredit − Σdebit from the recorded lines,
// mirroring the real SQL. It enforces UNIQUE(kind, external_ref) by returning a
// 23505 *pq.Error, letting the duplicate-mapping path be tested without a DB.
type fakeQuerier struct {
	accounts map[uuid.UUID]db.Account
	entries  map[uuid.UUID]db.JournalEntry
	lines    []db.EntryLine
	seenRef  map[string]struct{} // enforces UNIQUE(kind, external_ref)
	nextLine int64
}

var _ db.Querier = (*fakeQuerier)(nil)

// newFake returns a wired store plus the underlying querier for seeding and
// assertions.
func newFake() (*fakeStore, *fakeQuerier) {
	q := &fakeQuerier{
		accounts: make(map[uuid.UUID]db.Account),
		entries:  make(map[uuid.UUID]db.JournalEntry),
		seenRef:  make(map[string]struct{}),
	}
	return &fakeStore{q: q}, q
}

// seedAccount registers an account and, if opening != 0, gives it that opening
// balance by recording a synthetic credit line — balances are always derived,
// never stored, so this is how "starting money" is expressed.
func (q *fakeQuerier) seedAccount(kind, asset string, opening int64) uuid.UUID {
	id := uuid.New()
	q.accounts[id] = db.Account{ID: id, Kind: kind, Asset: asset, Status: "active", CreatedAt: time.Now()}
	if opening != 0 {
		q.nextLine++
		q.lines = append(q.lines, db.EntryLine{
			ID:        q.nextLine,
			EntryID:   uuid.New(),
			AccountID: id,
			Direction: string(Credit),
			Amount:    opening,
		})
	}
	return id
}

// totalBalance sums the net balance of every account, used to assert
// conservation of value across a posting.
func (q *fakeQuerier) totalBalance() int64 {
	var total int64
	for _, l := range q.lines {
		if l.Direction == string(Credit) {
			total += l.Amount
		} else {
			total -= l.Amount
		}
	}
	return total
}

func (q *fakeQuerier) CreateAccount(ctx context.Context, arg db.CreateAccountParams) (db.Account, error) {
	if err := ctx.Err(); err != nil {
		return db.Account{}, err
	}
	a := db.Account{ID: uuid.New(), Name: arg.Name, Kind: arg.Kind, Asset: arg.Asset, Status: "active", CreatedAt: time.Now()}
	q.accounts[a.ID] = a
	return a, nil
}

func (q *fakeQuerier) GetAccount(ctx context.Context, id uuid.UUID) (db.Account, error) {
	if err := ctx.Err(); err != nil {
		return db.Account{}, err
	}
	return q.accounts[id], nil
}

func (q *fakeQuerier) GetAccountBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var bal int64
	for _, l := range q.lines {
		if l.AccountID != accountID {
			continue
		}
		if l.Direction == string(Credit) {
			bal += l.Amount
		} else {
			bal -= l.Amount
		}
	}
	return bal, nil
}

func (q *fakeQuerier) GetAccountsForUpdate(ctx context.Context, ids []uuid.UUID) ([]db.Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]db.Account, 0, len(ids))
	for _, id := range ids {
		if a, ok := q.accounts[id]; ok {
			out = append(out, a)
		}
	}
	slices.SortFunc(out, func(a, b db.Account) int {
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	return out, nil
}

func (q *fakeQuerier) InsertJournalEntry(ctx context.Context, arg db.InsertJournalEntryParams) (db.JournalEntry, error) {
	if err := ctx.Err(); err != nil {
		return db.JournalEntry{}, err
	}
	key := arg.Kind + "\x00" + arg.ExternalRef
	if _, dup := q.seenRef[key]; dup {
		// Mirror Postgres: a unique-index violation surfaces as a 23505.
		return db.JournalEntry{}, &pq.Error{
			Code:       "23505",
			Constraint: "journal_entries_kind_external_ref_key",
			Message:    "duplicate key value violates unique constraint",
		}
	}
	q.seenRef[key] = struct{}{}
	je := db.JournalEntry{ID: uuid.New(), Kind: arg.Kind, ExternalRef: arg.ExternalRef, Asset: arg.Asset, CreatedAt: time.Now()}
	q.entries[je.ID] = je
	return je, nil
}

func (q *fakeQuerier) InsertEntryLine(ctx context.Context, arg db.InsertEntryLineParams) (db.EntryLine, error) {
	if err := ctx.Err(); err != nil {
		return db.EntryLine{}, err
	}
	q.nextLine++
	el := db.EntryLine{ID: q.nextLine, EntryID: arg.EntryID, AccountID: arg.AccountID, Direction: arg.Direction, Amount: arg.Amount}
	q.lines = append(q.lines, el)
	return el, nil
}

// Payments and idempotency methods round out db.Querier so fakeQuerier still
// satisfies the interface, but the ledger domain never calls them — those tables
// belong to the payments service (a later package). They panic if invoked so a
// mistaken call in a ledger test fails loudly rather than returning silent zeros.
func (q *fakeQuerier) InsertPayment(context.Context, db.InsertPaymentParams) (db.Payment, error) {
	panic("fakeQuerier.InsertPayment: not used by the ledger domain")
}

func (q *fakeQuerier) GetPayment(context.Context, uuid.UUID) (db.Payment, error) {
	panic("fakeQuerier.GetPayment: not used by the ledger domain")
}

func (q *fakeQuerier) GetAccountByNameAndAsset(context.Context, db.GetAccountByNameAndAssetParams) (db.Account, error) {
	panic("fakeQuerier.GetAccountByNameAndAsset: not used by the ledger domain")
}

func (q *fakeQuerier) ListPaymentsFirstPage(context.Context, int32) ([]db.Payment, error) {
	panic("fakeQuerier.ListPaymentsFirstPage: not used by the ledger domain")
}

func (q *fakeQuerier) ListPaymentsAfter(context.Context, db.ListPaymentsAfterParams) ([]db.Payment, error) {
	panic("fakeQuerier.ListPaymentsAfter: not used by the ledger domain")
}

func (q *fakeQuerier) CancelPayment(context.Context, db.CancelPaymentParams) (db.Payment, error) {
	panic("fakeQuerier.CancelPayment: not used by the ledger domain")
}

func (q *fakeQuerier) InsertIdempotencyKey(context.Context, db.InsertIdempotencyKeyParams) (db.IdempotencyKey, error) {
	panic("fakeQuerier.InsertIdempotencyKey: not used by the ledger domain")
}

func (q *fakeQuerier) GetIdempotencyKey(context.Context, string) (db.IdempotencyKey, error) {
	panic("fakeQuerier.GetIdempotencyKey: not used by the ledger domain")
}

func (q *fakeQuerier) CompleteIdempotencyKey(context.Context, db.CompleteIdempotencyKeyParams) error {
	panic("fakeQuerier.CompleteIdempotencyKey: not used by the ledger domain")
}

func (q *fakeQuerier) DeleteIdempotencyKey(context.Context, string) error {
	panic("fakeQuerier.DeleteIdempotencyKey: not used by the ledger domain")
}

func (q *fakeQuerier) DeleteExpiredIdempotencyKeys(context.Context, time.Time) (int64, error) {
	panic("fakeQuerier.DeleteExpiredIdempotencyKeys: not used by the ledger domain")
}

// Settlement methods belong to the settlement service (M3 / WP2); the ledger
// domain never calls them, so they panic like the payments/idempotency stubs.
func (q *fakeQuerier) InsertSettlement(context.Context, db.InsertSettlementParams) (db.Settlement, error) {
	panic("fakeQuerier.InsertSettlement: not used by the ledger domain")
}

func (q *fakeQuerier) GetSettlementByTxHash(context.Context, string) (db.Settlement, error) {
	panic("fakeQuerier.GetSettlementByTxHash: not used by the ledger domain")
}

func (q *fakeQuerier) MarkSettlementSettled(context.Context, db.MarkSettlementSettledParams) (db.Settlement, error) {
	panic("fakeQuerier.MarkSettlementSettled: not used by the ledger domain")
}

func (q *fakeQuerier) MarkSettlementReorged(context.Context, string) (db.Settlement, error) {
	panic("fakeQuerier.MarkSettlementReorged: not used by the ledger domain")
}

func (q *fakeQuerier) MarkSettlementFinalized(context.Context, string) (db.Settlement, error) {
	panic("fakeQuerier.MarkSettlementFinalized: not used by the ledger domain")
}

func (q *fakeQuerier) ListPendingSettlements(context.Context) ([]db.Settlement, error) {
	panic("fakeQuerier.ListPendingSettlements: not used by the ledger domain")
}

// Outbox methods belong to the event relay (M4); the ledger domain never calls
// them, so they panic like the settlement/payments stubs above.
func (q *fakeQuerier) InsertOutboxEvent(context.Context, db.InsertOutboxEventParams) error {
	panic("fakeQuerier.InsertOutboxEvent: not used by the ledger domain")
}

func (q *fakeQuerier) ClaimUnsentOutbox(context.Context, int32) ([]db.Outbox, error) {
	panic("fakeQuerier.ClaimUnsentOutbox: not used by the ledger domain")
}

func (q *fakeQuerier) MarkOutboxSent(context.Context, []uuid.UUID) (int64, error) {
	panic("fakeQuerier.MarkOutboxSent: not used by the ledger domain")
}

// Webhook delivery methods belong to the webhook service; the ledger domain
// never calls them, so they panic like the outbox/settlement stubs above.
func (q *fakeQuerier) FanOutDelivery(context.Context, db.FanOutDeliveryParams) (int64, error) {
	panic("fakeQuerier.FanOutDelivery: not used by the ledger domain")
}

func (q *fakeQuerier) ClaimDueDeliveries(context.Context, db.ClaimDueDeliveriesParams) ([]db.ClaimDueDeliveriesRow, error) {
	panic("fakeQuerier.ClaimDueDeliveries: not used by the ledger domain")
}

func (q *fakeQuerier) MarkDeliverySucceeded(context.Context, db.MarkDeliverySucceededParams) error {
	panic("fakeQuerier.MarkDeliverySucceeded: not used by the ledger domain")
}

func (q *fakeQuerier) MarkDeliveryRetry(context.Context, db.MarkDeliveryRetryParams) error {
	panic("fakeQuerier.MarkDeliveryRetry: not used by the ledger domain")
}

func (q *fakeQuerier) MarkDeliveryDeadLettered(context.Context, db.MarkDeliveryDeadLetteredParams) error {
	panic("fakeQuerier.MarkDeliveryDeadLettered: not used by the ledger domain")
}

func (q *fakeQuerier) ReplayDeadLettered(context.Context, uuid.UUID) (int64, error) {
	panic("fakeQuerier.ReplayDeadLettered: not used by the ledger domain")
}

// Velocity methods belong to the policy engine (M5 / slice 2); the ledger domain
// never calls them, so they panic like the webhook/outbox stubs above.
func (q *fakeQuerier) AcquireVelocityLock(context.Context, int64) error {
	panic("fakeQuerier.AcquireVelocityLock: not used by the ledger domain")
}

func (q *fakeQuerier) SumVelocityWindow(context.Context, db.SumVelocityWindowParams) (db.SumVelocityWindowRow, error) {
	panic("fakeQuerier.SumVelocityWindow: not used by the ledger domain")
}

func (q *fakeQuerier) InsertVelocityEvent(context.Context, db.InsertVelocityEventParams) error {
	panic("fakeQuerier.InsertVelocityEvent: not used by the ledger domain")
}

// Four-eyes approval methods belong to the approval queue (M5 / slice 3); the
// ledger domain never calls them, so they panic like the velocity stubs above.
func (q *fakeQuerier) InsertPaymentApproval(context.Context, db.InsertPaymentApprovalParams) (uuid.UUID, error) {
	panic("fakeQuerier.InsertPaymentApproval: not used by the ledger domain")
}

func (q *fakeQuerier) GetApprovalForUpdate(context.Context, uuid.UUID) (db.PaymentApproval, error) {
	panic("fakeQuerier.GetApprovalForUpdate: not used by the ledger domain")
}

func (q *fakeQuerier) MarkApprovalApproved(context.Context, db.MarkApprovalApprovedParams) (int64, error) {
	panic("fakeQuerier.MarkApprovalApproved: not used by the ledger domain")
}

func (q *fakeQuerier) MarkApprovalBroadcast(context.Context, db.MarkApprovalBroadcastParams) (int64, error) {
	panic("fakeQuerier.MarkApprovalBroadcast: not used by the ledger domain")
}

func (q *fakeQuerier) ReopenApproval(context.Context, uuid.UUID) (int64, error) {
	panic("fakeQuerier.ReopenApproval: not used by the ledger domain")
}
