package settlement

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
)

// --- hermetic fake ----------------------------------------------------------
//
// A _test.go file cannot be imported, so the ledger's in-memory fake is
// replicated here and extended with the settlement/payment table methods the
// Sink and Recorder call. It is the same shape as internal/ledger/fake_test.go:
// balances are derived from recorded lines (never stored), and InsertJournalEntry
// enforces UNIQUE(kind, external_ref) by returning a 23505 *pq.Error so the
// duplicate-entry path is exercised without a database.

type fakeStore struct{ q *fakeQuerier }

func (s *fakeStore) ExecTx(_ context.Context, fn func(q db.Querier) error) error {
	return fn(s.q)
}

type fakeQuerier struct {
	accounts    map[uuid.UUID]db.Account
	entries     map[uuid.UUID]db.JournalEntry
	lines       []db.EntryLine
	seenRef     map[string]struct{}
	payments    map[uuid.UUID]db.Payment
	settlements map[string]db.Settlement // keyed by tx_hash
	outbox      []db.InsertOutboxEventParams
	outboxErr   error // when set, InsertOutboxEvent fails, aborting the tx
	nextLine    int64
}

var _ db.Querier = (*fakeQuerier)(nil)

func newFake() (*fakeStore, *fakeQuerier) {
	q := &fakeQuerier{
		accounts:    make(map[uuid.UUID]db.Account),
		entries:     make(map[uuid.UUID]db.JournalEntry),
		seenRef:     make(map[string]struct{}),
		payments:    make(map[uuid.UUID]db.Payment),
		settlements: make(map[string]db.Settlement),
	}
	return &fakeStore{q: q}, q
}

// seedAccount registers a named, active account with an optional opening balance
// expressed as a synthetic credit line (balances are always derived).
func (q *fakeQuerier) seedAccount(name, kind, asset string, opening int64) uuid.UUID {
	id := uuid.New()
	q.accounts[id] = db.Account{ID: id, Name: name, Kind: kind, Asset: asset, Status: "active", CreatedAt: time.Now()}
	if opening != 0 {
		q.nextLine++
		q.lines = append(q.lines, db.EntryLine{
			ID:        q.nextLine,
			EntryID:   uuid.New(),
			AccountID: id,
			Direction: string(ledger.Credit),
			Amount:    opening,
		})
	}
	return id
}

// seedPayment inserts a completed payment row and returns it.
func (q *fakeQuerier) seedPayment(source, dest uuid.UUID, asset string, amount int64) db.Payment {
	p := db.Payment{
		ID:              uuid.New(),
		Status:          "completed",
		Asset:           asset,
		Amount:          amount,
		SourceAccountID: source,
		DestAccountID:   dest,
		JournalEntryID:  uuid.New(),
		CreatedAt:       time.Now(),
	}
	q.payments[p.ID] = p
	return p
}

// seedSettlement inserts a pending settlement row linking a payment to a tx hash.
func (q *fakeQuerier) seedSettlement(paymentID uuid.UUID, txHash string) db.Settlement {
	sett := db.Settlement{
		ID:        uuid.New(),
		PaymentID: paymentID,
		TxHash:    txHash,
		Status:    "pending",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	q.settlements[txHash] = sett
	return sett
}

func (q *fakeQuerier) balance(accountID uuid.UUID) int64 {
	var bal int64
	for _, l := range q.lines {
		if l.AccountID != accountID {
			continue
		}
		if l.Direction == string(ledger.Credit) {
			bal += l.Amount
		} else {
			bal -= l.Amount
		}
	}
	return bal
}

func (q *fakeQuerier) totalBalance() int64 {
	var total int64
	for _, l := range q.lines {
		if l.Direction == string(ledger.Credit) {
			total += l.Amount
		} else {
			total -= l.Amount
		}
	}
	return total
}

func (q *fakeQuerier) entryCount() int { return len(q.entries) }

// --- ledger-domain methods (copied verbatim in behavior) --------------------

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
	return q.balance(accountID), nil
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

// --- settlement/payment methods the Sink and Recorder exercise --------------

func (q *fakeQuerier) GetPayment(ctx context.Context, id uuid.UUID) (db.Payment, error) {
	if err := ctx.Err(); err != nil {
		return db.Payment{}, err
	}
	p, ok := q.payments[id]
	if !ok {
		return db.Payment{}, sql.ErrNoRows
	}
	return p, nil
}

func (q *fakeQuerier) GetAccountByNameAndAsset(ctx context.Context, arg db.GetAccountByNameAndAssetParams) (db.Account, error) {
	if err := ctx.Err(); err != nil {
		return db.Account{}, err
	}
	for _, a := range q.accounts {
		if a.Name == arg.Name && a.Asset == arg.Asset {
			return a, nil
		}
	}
	return db.Account{}, sql.ErrNoRows
}

func (q *fakeQuerier) InsertSettlement(ctx context.Context, arg db.InsertSettlementParams) (db.Settlement, error) {
	if err := ctx.Err(); err != nil {
		return db.Settlement{}, err
	}
	if _, ok := q.settlements[arg.TxHash]; ok {
		return db.Settlement{}, sql.ErrNoRows // ON CONFLICT (tx_hash) DO NOTHING
	}
	sett := db.Settlement{
		ID:        uuid.New(),
		PaymentID: arg.PaymentID,
		TxHash:    arg.TxHash,
		Status:    "pending",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	q.settlements[arg.TxHash] = sett
	return sett, nil
}

func (q *fakeQuerier) GetSettlementByTxHash(ctx context.Context, txHash string) (db.Settlement, error) {
	if err := ctx.Err(); err != nil {
		return db.Settlement{}, err
	}
	sett, ok := q.settlements[txHash]
	if !ok {
		return db.Settlement{}, sql.ErrNoRows
	}
	return sett, nil
}

func (q *fakeQuerier) MarkSettlementSettled(ctx context.Context, arg db.MarkSettlementSettledParams) (db.Settlement, error) {
	if err := ctx.Err(); err != nil {
		return db.Settlement{}, err
	}
	sett, ok := q.settlements[arg.TxHash]
	if !ok || (sett.Status != "pending" && sett.Status != "reorged") {
		return db.Settlement{}, sql.ErrNoRows // guarded WHERE status IN ('pending','reorged')
	}
	sett.Status = "settled"
	sett.SettleEntryID = arg.SettleEntryID
	// Model the anchor columns WP1 added: settle records the settling block so a
	// later finality/reorg check has provenance to compare against.
	sett.SettledBlockHash = arg.SettledBlockHash
	sett.SettledBlockNumber = arg.SettledBlockNumber
	sett.UpdatedAt = time.Now()
	q.settlements[arg.TxHash] = sett
	return sett, nil
}

func (q *fakeQuerier) MarkSettlementReorged(ctx context.Context, txHash string) (db.Settlement, error) {
	if err := ctx.Err(); err != nil {
		return db.Settlement{}, err
	}
	sett, ok := q.settlements[txHash]
	if !ok || sett.Status != "settled" {
		return db.Settlement{}, sql.ErrNoRows // guarded WHERE status = 'settled'
	}
	sett.Status = "reorged"
	// WP1: reorg NULLs the anchor so a reorged row carries no stale provenance.
	sett.SettledBlockHash = sql.NullString{}
	sett.SettledBlockNumber = sql.NullInt64{}
	sett.UpdatedAt = time.Now()
	q.settlements[txHash] = sett
	return sett, nil
}

func (q *fakeQuerier) MarkSettlementFinalized(ctx context.Context, txHash string) (db.Settlement, error) {
	if err := ctx.Err(); err != nil {
		return db.Settlement{}, err
	}
	sett, ok := q.settlements[txHash]
	if !ok || sett.Status != "settled" {
		return db.Settlement{}, sql.ErrNoRows // guarded WHERE status = 'settled'
	}
	sett.Status = "finalized"
	sett.UpdatedAt = time.Now()
	q.settlements[txHash] = sett
	return sett, nil
}

// --- methods outside this package's use, kept to satisfy db.Querier ---------

func (q *fakeQuerier) CreateAccount(context.Context, db.CreateAccountParams) (db.Account, error) {
	panic("fakeQuerier.CreateAccount: not used by the settlement domain")
}
func (q *fakeQuerier) InsertPayment(context.Context, db.InsertPaymentParams) (db.Payment, error) {
	panic("fakeQuerier.InsertPayment: not used by the settlement domain")
}
func (q *fakeQuerier) CancelPayment(context.Context, db.CancelPaymentParams) (db.Payment, error) {
	panic("fakeQuerier.CancelPayment: not used by the settlement domain")
}
func (q *fakeQuerier) ListPaymentsFirstPage(context.Context, int32) ([]db.Payment, error) {
	panic("fakeQuerier.ListPaymentsFirstPage: not used by the settlement domain")
}
func (q *fakeQuerier) ListPaymentsAfter(context.Context, db.ListPaymentsAfterParams) ([]db.Payment, error) {
	panic("fakeQuerier.ListPaymentsAfter: not used by the settlement domain")
}
func (q *fakeQuerier) ListPendingSettlements(context.Context) ([]db.Settlement, error) {
	panic("fakeQuerier.ListPendingSettlements: not used by the settlement domain")
}
func (q *fakeQuerier) InsertIdempotencyKey(context.Context, db.InsertIdempotencyKeyParams) (db.IdempotencyKey, error) {
	panic("fakeQuerier.InsertIdempotencyKey: not used by the settlement domain")
}
func (q *fakeQuerier) GetIdempotencyKey(context.Context, string) (db.IdempotencyKey, error) {
	panic("fakeQuerier.GetIdempotencyKey: not used by the settlement domain")
}
func (q *fakeQuerier) CompleteIdempotencyKey(context.Context, db.CompleteIdempotencyKeyParams) error {
	panic("fakeQuerier.CompleteIdempotencyKey: not used by the settlement domain")
}
func (q *fakeQuerier) DeleteIdempotencyKey(context.Context, string) error {
	panic("fakeQuerier.DeleteIdempotencyKey: not used by the settlement domain")
}
func (q *fakeQuerier) DeleteExpiredIdempotencyKeys(context.Context, time.Time) (int64, error) {
	panic("fakeQuerier.DeleteExpiredIdempotencyKeys: not used by the settlement domain")
}

// InsertOutboxEvent records the appended envelope so tests can assert the Sink
// emits exactly one outbox row per real transition (and none on a no-op).
func (q *fakeQuerier) InsertOutboxEvent(_ context.Context, arg db.InsertOutboxEventParams) error {
	if q.outboxErr != nil {
		return q.outboxErr
	}
	q.outbox = append(q.outbox, arg)
	return nil
}

// outboxTypes returns the event_type of every recorded outbox row, in order.
func (q *fakeQuerier) outboxTypes() []string {
	types := make([]string, len(q.outbox))
	for i, o := range q.outbox {
		types[i] = o.EventType
	}
	return types
}

func (q *fakeQuerier) ClaimUnsentOutbox(context.Context, int32) ([]db.Outbox, error) {
	panic("fakeQuerier.ClaimUnsentOutbox: not used by the settlement domain")
}

func (q *fakeQuerier) MarkOutboxSent(context.Context, []uuid.UUID) (int64, error) {
	panic("fakeQuerier.MarkOutboxSent: not used by the settlement domain")
}

func (q *fakeQuerier) FanOutDelivery(context.Context, db.FanOutDeliveryParams) (int64, error) {
	panic("fakeQuerier.FanOutDelivery: not used by the settlement domain")
}

func (q *fakeQuerier) ClaimDueDeliveries(context.Context, db.ClaimDueDeliveriesParams) ([]db.ClaimDueDeliveriesRow, error) {
	panic("fakeQuerier.ClaimDueDeliveries: not used by the settlement domain")
}

func (q *fakeQuerier) MarkDeliverySucceeded(context.Context, db.MarkDeliverySucceededParams) error {
	panic("fakeQuerier.MarkDeliverySucceeded: not used by the settlement domain")
}

func (q *fakeQuerier) MarkDeliveryRetry(context.Context, db.MarkDeliveryRetryParams) error {
	panic("fakeQuerier.MarkDeliveryRetry: not used by the settlement domain")
}

func (q *fakeQuerier) MarkDeliveryDeadLettered(context.Context, db.MarkDeliveryDeadLetteredParams) error {
	panic("fakeQuerier.MarkDeliveryDeadLettered: not used by the settlement domain")
}

func (q *fakeQuerier) ReplayDeadLettered(context.Context, uuid.UUID) (int64, error) {
	panic("fakeQuerier.ReplayDeadLettered: not used by the settlement domain")
}
func (q *fakeQuerier) AcquireVelocityLock(context.Context, int64) error {
	panic("fakeQuerier.AcquireVelocityLock: not used by the settlement domain")
}
func (q *fakeQuerier) SumVelocityWindow(context.Context, db.SumVelocityWindowParams) (db.SumVelocityWindowRow, error) {
	panic("fakeQuerier.SumVelocityWindow: not used by the settlement domain")
}
func (q *fakeQuerier) InsertVelocityEvent(context.Context, db.InsertVelocityEventParams) error {
	panic("fakeQuerier.InsertVelocityEvent: not used by the settlement domain")
}

// --- fixtures ---------------------------------------------------------------

const (
	asset    = "USDC"
	amount   = int64(500)
	txHash   = "0x" + "ab"
	blockA   = "0xaaaa"
	blockB   = "0xbbbb"
	blockNum = uint64(42) // the settling block's height, anchored by settle
)

// fixture wires accounts + a completed payment + a pending settlement whose
// post-Create balances are: source = 1000-amount, dest = amount (provisional
// credit), house = 0. It returns the fake, its querier, the payment, and the
// dest/house ids so tests can trace balances.
type fixture struct {
	store *fakeStore
	q     *fakeQuerier
	pay   db.Payment
	dest  uuid.UUID
	house uuid.UUID
}

func newFixture(t *testing.T, destOpening int64) fixture {
	t.Helper()
	store, q := newFake()
	source := q.seedAccount("source", "user", asset, 1000-amount)
	dest := q.seedAccount("dest", "user", asset, destOpening)
	house := q.seedAccount(houseAccountName, "clearing", asset, 0)
	pay := q.seedPayment(source, dest, asset, amount)
	q.seedSettlement(pay.ID, txHash)
	return fixture{store: store, q: q, pay: pay, dest: dest, house: house}
}

func status(phase evm.Phase, block string) evm.Status {
	return evm.Status{TxHash: chain.TxHash(txHash), Phase: phase, BlockHash: block, BlockNumber: blockNum}
}

// --- tests ------------------------------------------------------------------

//  1. A confirmed tx posts debit-dest/credit-house, flips the row to settled with
//     a settle_entry_id, and conserves total ledger value.
func TestOnStatus_Settle(t *testing.T) {
	f := newFixture(t, amount) // dest holds its provisional credit
	before := f.q.totalBalance()
	sink := NewSink(f.store, nil)

	if err := sink.OnStatus(context.Background(), status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}

	if got := f.q.balance(f.dest); got != 0 {
		t.Errorf("dest balance = %d, want 0 (provisional credit released)", got)
	}
	if got := f.q.balance(f.house); got != amount {
		t.Errorf("house balance = %d, want %d", got, amount)
	}
	if got := f.q.totalBalance(); got != before {
		t.Errorf("total balance = %d, want %d (conserved)", got, before)
	}
	sett := f.q.settlements[txHash]
	if sett.Status != "settled" {
		t.Errorf("status = %q, want settled", sett.Status)
	}
	if !sett.SettleEntryID.Valid {
		t.Error("settle_entry_id not set")
	}
	// Exactly one outbox row, keyed by tx_hash, describing the confirmation.
	if got := f.q.outboxTypes(); len(got) != 1 || got[0] != "settlement.confirmed" {
		t.Fatalf("outbox events = %v, want [settlement.confirmed]", got)
	}
	if got := f.q.outbox[0].AggregateID; got != txHash {
		t.Errorf("outbox aggregate_id = %q, want tx hash %q", got, txHash)
	}
}

//  2. Re-delivering the same Confirmed(A) is a no-op: no second entry, status
//     stays settled, balances unchanged.
func TestOnStatus_SettleIdempotent(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("first settle: %v", err)
	}
	entriesAfterFirst := f.q.entryCount()
	firstEntryID := f.q.settlements[txHash].SettleEntryID

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("redelivered settle: %v", err)
	}

	if got := f.q.entryCount(); got != entriesAfterFirst {
		t.Errorf("entry count = %d, want %d (no second post)", got, entriesAfterFirst)
	}
	if got := f.q.settlements[txHash]; got.Status != "settled" || got.SettleEntryID != firstEntryID {
		t.Errorf("row mutated on redelivery: %+v", got)
	}
	if got := f.q.balance(f.house); got != amount {
		t.Errorf("house balance = %d, want %d (unchanged)", got, amount)
	}
	// The redelivery is a no-op, so no second outbox row is appended.
	if got := f.q.outboxTypes(); len(got) != 1 || got[0] != "settlement.confirmed" {
		t.Errorf("outbox events = %v, want a single [settlement.confirmed]", got)
	}
}

//  3. A reorged tx posts debit-house/credit-dest and flips the row to reorged;
//     dest/house net back to their post-Create state.
func TestOnStatus_Reverse(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := sink.OnStatus(ctx, status(evm.PhaseReorged, blockA)); err != nil {
		t.Fatalf("reverse: %v", err)
	}

	if got := f.q.balance(f.dest); got != amount {
		t.Errorf("dest balance = %d, want %d (provisional credit restored)", got, amount)
	}
	if got := f.q.balance(f.house); got != 0 {
		t.Errorf("house balance = %d, want 0", got)
	}
	if got := f.q.settlements[txHash].Status; got != "reorged" {
		t.Errorf("status = %q, want reorged", got)
	}
	// One event per real transition: confirm then reorg, both keyed by tx_hash.
	if got := f.q.outboxTypes(); len(got) != 2 || got[0] != "settlement.confirmed" || got[1] != "settlement.reorged" {
		t.Errorf("outbox events = %v, want [settlement.confirmed settlement.reorged]", got)
	}
}

// A settle anchors the settling block's hash and number onto the row so a later
// finality/reorg check has provenance to compare against.
func TestOnStatus_SettleRecordsAnchor(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)

	if err := sink.OnStatus(context.Background(), status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}

	sett := f.q.settlements[txHash]
	if !sett.SettledBlockHash.Valid || sett.SettledBlockHash.String != blockA {
		t.Errorf("settled_block_hash = %+v, want valid %q", sett.SettledBlockHash, blockA)
	}
	if !sett.SettledBlockNumber.Valid || sett.SettledBlockNumber.Int64 != int64(blockNum) {
		t.Errorf("settled_block_number = %+v, want valid %d", sett.SettledBlockNumber, blockNum)
	}
}

// A reorg clears the anchor (WP1 NULLs the columns) so a reorged row carries no
// stale finality provenance.
func TestOnStatus_ReverseClearsAnchor(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := sink.OnStatus(ctx, status(evm.PhaseReorged, blockA)); err != nil {
		t.Fatalf("reverse: %v", err)
	}

	sett := f.q.settlements[txHash]
	if sett.SettledBlockHash.Valid || sett.SettledBlockNumber.Valid {
		t.Errorf("anchor not cleared on reorg: hash=%+v number=%+v", sett.SettledBlockHash, sett.SettledBlockNumber)
	}
}

// A Finalized on a settled tx flips the row to finalized as a pure status
// transition: settle already moved the money, so finalize posts NO journal entry.
func TestOnStatus_Finalize(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got := f.q.entryCount(); got != 1 {
		t.Fatalf("entry count after settle = %d, want 1", got)
	}
	entriesAfterSettle := f.q.entryCount()

	if err := sink.OnStatus(ctx, status(evm.PhaseFinalized, blockA)); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	if got := f.q.settlements[txHash].Status; got != "finalized" {
		t.Errorf("status = %q, want finalized", got)
	}
	if got := f.q.entryCount(); got != entriesAfterSettle {
		t.Errorf("entry count = %d, want %d (finalize posts no journal entry)", got, entriesAfterSettle)
	}
	// Finalize is a real transition, so it emits — one confirmed, one finalized.
	if got := f.q.outboxTypes(); len(got) != 2 || got[0] != "settlement.confirmed" || got[1] != "settlement.finalized" {
		t.Errorf("outbox events = %v, want [settlement.confirmed settlement.finalized]", got)
	}
}

// Redelivering Finalized on an already-finalized row is a benign no-op: the
// guarded UPDATE returns ErrNoRows, OnStatus returns nil, and nothing is posted.
func TestOnStatus_FinalizeIdempotent(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	if err := sink.OnStatus(ctx, status(evm.PhaseConfirmed, blockA)); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := sink.OnStatus(ctx, status(evm.PhaseFinalized, blockA)); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	entriesAfterFirst := f.q.entryCount()

	if err := sink.OnStatus(ctx, status(evm.PhaseFinalized, blockA)); err != nil {
		t.Fatalf("redelivered finalize: %v", err)
	}

	if got := f.q.settlements[txHash].Status; got != "finalized" {
		t.Errorf("status = %q, want finalized (unchanged)", got)
	}
	if got := f.q.entryCount(); got != entriesAfterFirst {
		t.Errorf("entry count = %d, want %d (no post on redelivery)", got, entriesAfterFirst)
	}
	// The redelivered finalize is a no-op, so no third outbox row is appended.
	if got := f.q.outboxTypes(); len(got) != 2 || got[1] != "settlement.finalized" {
		t.Errorf("outbox events = %v, want [settlement.confirmed settlement.finalized]", got)
	}
}

//  4. After a reverse, a Confirmed at a different block B settles again under a
//     distinct external_ref; final net = settled once.
func TestOnStatus_Reapply(t *testing.T) {
	f := newFixture(t, amount)
	sink := NewSink(f.store, nil)
	ctx := context.Background()

	for _, st := range []evm.Status{
		status(evm.PhaseConfirmed, blockA),
		status(evm.PhaseReorged, blockA),
		status(evm.PhaseConfirmed, blockB), // re-mined onto a new block
	} {
		if err := sink.OnStatus(ctx, st); err != nil {
			t.Fatalf("phase %s @ %s: %v", st.Phase, st.BlockHash, err)
		}
	}

	if got := f.q.balance(f.dest); got != 0 {
		t.Errorf("dest balance = %d, want 0 (settled once)", got)
	}
	if got := f.q.balance(f.house); got != amount {
		t.Errorf("house balance = %d, want %d (settled once)", got, amount)
	}
	if got := f.q.settlements[txHash].Status; got != "settled" {
		t.Errorf("status = %q, want settled", got)
	}
	// settle:A, reverse:A, settle:B — three distinct external_refs => 3 entries.
	if got := f.q.entryCount(); got != 3 {
		t.Errorf("entry count = %d, want 3 distinct external_refs", got)
	}
}

//  5. If the destination has spent the credited funds, a confirm cannot balance
//     and ErrInsufficientFunds propagates.
func TestOnStatus_InsufficientFunds(t *testing.T) {
	f := newFixture(t, 0) // dest already spent its provisional credit
	sink := NewSink(f.store, nil)

	err := sink.OnStatus(context.Background(), status(evm.PhaseConfirmed, blockA))
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	if got := f.q.settlements[txHash].Status; got != "pending" {
		t.Errorf("status = %q, want pending (settle rolled back)", got)
	}
	// The post failed before Emit, so no outbox row leaks from a rejected settle.
	if got := f.q.outboxTypes(); len(got) != 0 {
		t.Errorf("outbox events = %v, want none (settle rejected)", got)
	}
}

// A failing InsertOutboxEvent aborts the settle: OnStatus surfaces the error, so
// under a real transaction the settle_entry_id flip and the outbox row roll back
// together — the event and the state change are one atomic write.
func TestOnStatus_SettleOutboxErrorPropagates(t *testing.T) {
	f := newFixture(t, amount)
	f.q.outboxErr = errors.New("outbox insert failed")
	sink := NewSink(f.store, nil)

	err := sink.OnStatus(context.Background(), status(evm.PhaseConfirmed, blockA))
	if !errors.Is(err, f.q.outboxErr) {
		t.Fatalf("OnStatus err = %v, want the injected outbox error", err)
	}
	if got := len(f.q.outbox); got != 0 {
		t.Errorf("recorded %d outbox rows, want 0 (insert failed)", got)
	}
}

// 6. A Confirmed for an untracked tx_hash returns nil with no ledger effect.
func TestOnStatus_UntrackedTx(t *testing.T) {
	store, q := newFake()
	q.seedAccount(houseAccountName, "clearing", asset, 0)
	sink := NewSink(store, nil)

	st := evm.Status{TxHash: chain.TxHash("0xdead"), Phase: evm.PhaseConfirmed, BlockHash: blockA}
	if err := sink.OnStatus(context.Background(), st); err != nil {
		t.Fatalf("untracked: %v", err)
	}
	if got := q.entryCount(); got != 0 {
		t.Errorf("entry count = %d, want 0 (no ledger effect)", got)
	}
}

// 7. Recorder.Link inserts on first call and is idempotent on a duplicate.
func TestRecorderLink(t *testing.T) {
	_, q := newFake()
	rec := NewRecorder(q)
	ctx := context.Background()
	pid := uuid.New()

	if err := rec.Link(ctx, pid, txHash); err != nil {
		t.Fatalf("first link: %v", err)
	}
	if err := rec.Link(ctx, pid, txHash); err != nil {
		t.Fatalf("duplicate link: %v", err)
	}
	if got := len(q.settlements); got != 1 {
		t.Errorf("settlement rows = %d, want 1", got)
	}
	if got := q.settlements[txHash].PaymentID; got != pid {
		t.Errorf("payment_id = %s, want %s", got, pid)
	}

	// Re-linking the same tx_hash to a DIFFERENT payment must be rejected, not
	// reported as an idempotent success — otherwise an operator silently mis-binds
	// a broadcast transaction and that payment never settles.
	other := uuid.New()
	if err := rec.Link(ctx, other, txHash); err == nil {
		t.Fatalf("link of tx already bound to %s to different payment %s: want error, got nil", pid, other)
	}
	if got := q.settlements[txHash].PaymentID; got != pid {
		t.Errorf("after rejected relink, payment_id = %s, want unchanged %s", got, pid)
	}
}

//  8. Non-acting phases return nil and touch nothing. A panicking querier proves
//     zero DB interaction: OnStatus must return before ExecTx.
func TestOnStatus_NonActingPhases(t *testing.T) {
	sink := NewSink(panicStore{}, nil)
	for _, phase := range []evm.Phase{evm.PhasePending, evm.PhaseMined} {
		if err := sink.OnStatus(context.Background(), status(phase, "")); err != nil {
			t.Errorf("phase %s: err = %v, want nil", phase, err)
		}
	}
}

// panicStore fails loudly if ExecTx is ever entered, so a non-acting phase that
// wrongly opened a transaction would panic rather than pass silently.
type panicStore struct{}

func (panicStore) ExecTx(context.Context, func(q db.Querier) error) error {
	panic("ExecTx called for a non-acting phase")
}
