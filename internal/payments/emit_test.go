package payments

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
)

// This is an in-package unit test (the integration tests in payments_integration_test.go
// need a live Postgres). It drives Create/Cancel against an in-memory ledger.Store
// so the outbox-emit wiring can be asserted without a database, mirroring the fake
// in internal/settlement/settlement_test.go. The Service's tx seam is unexported,
// so the test builds the struct directly rather than through NewService.

type fakeStore struct{ q *fakeQuerier }

func (s *fakeStore) ExecTx(_ context.Context, fn func(q db.Querier) error) error {
	return fn(s.q)
}

// fakeQuerier implements only the methods Create/Cancel exercise; the embedded nil
// db.Querier promotes the rest so it satisfies the interface but nil-panics loudly
// if an unexpected method is ever called. Balances are derived from recorded lines.
type fakeQuerier struct {
	db.Querier
	accounts  map[uuid.UUID]db.Account
	lines     []db.EntryLine
	seenRef   map[string]struct{}
	payments  map[uuid.UUID]db.Payment
	outbox    []db.InsertOutboxEventParams
	outboxErr error
	nextLine  int64
}

func newFake() (*fakeStore, *fakeQuerier) {
	q := &fakeQuerier{
		accounts: make(map[uuid.UUID]db.Account),
		seenRef:  make(map[string]struct{}),
		payments: make(map[uuid.UUID]db.Payment),
	}
	return &fakeStore{q: q}, q
}

func (q *fakeQuerier) seedAccount(asset string, opening int64) uuid.UUID {
	id := uuid.New()
	q.accounts[id] = db.Account{ID: id, Kind: "user", Asset: asset, Status: "active", CreatedAt: time.Now()}
	if opening != 0 {
		q.nextLine++
		q.lines = append(q.lines, db.EntryLine{ID: q.nextLine, EntryID: uuid.New(), AccountID: id, Direction: string(ledger.Credit), Amount: opening})
	}
	return id
}

func (q *fakeQuerier) outboxTypes() []string {
	types := make([]string, len(q.outbox))
	for i, o := range q.outbox {
		types[i] = o.EventType
	}
	return types
}

// --- ledger-domain methods PostWithin calls -------------------------------

func (q *fakeQuerier) GetAccountsForUpdate(_ context.Context, ids []uuid.UUID) ([]db.Account, error) {
	out := make([]db.Account, 0, len(ids))
	for _, id := range ids {
		if a, ok := q.accounts[id]; ok {
			out = append(out, a)
		}
	}
	return out, nil
}

func (q *fakeQuerier) GetAccountBalance(_ context.Context, id uuid.UUID) (int64, error) {
	var bal int64
	for _, l := range q.lines {
		if l.AccountID != id {
			continue
		}
		if l.Direction == string(ledger.Credit) {
			bal += l.Amount
		} else {
			bal -= l.Amount
		}
	}
	return bal, nil
}

func (q *fakeQuerier) InsertJournalEntry(_ context.Context, arg db.InsertJournalEntryParams) (db.JournalEntry, error) {
	key := arg.Kind + "\x00" + arg.ExternalRef
	if _, dup := q.seenRef[key]; dup {
		return db.JournalEntry{}, errors.New("duplicate entry")
	}
	q.seenRef[key] = struct{}{}
	return db.JournalEntry{ID: uuid.New(), Kind: arg.Kind, ExternalRef: arg.ExternalRef, Asset: arg.Asset, CreatedAt: time.Now()}, nil
}

func (q *fakeQuerier) InsertEntryLine(_ context.Context, arg db.InsertEntryLineParams) (db.EntryLine, error) {
	q.nextLine++
	el := db.EntryLine{ID: q.nextLine, EntryID: arg.EntryID, AccountID: arg.AccountID, Direction: arg.Direction, Amount: arg.Amount}
	q.lines = append(q.lines, el)
	return el, nil
}

// --- payments methods -----------------------------------------------------

func (q *fakeQuerier) InsertPayment(_ context.Context, arg db.InsertPaymentParams) (db.Payment, error) {
	p := db.Payment{
		ID:              arg.ID,
		Status:          arg.Status,
		Asset:           arg.Asset,
		Amount:          arg.Amount,
		SourceAccountID: arg.SourceAccountID,
		DestAccountID:   arg.DestAccountID,
		JournalEntryID:  arg.JournalEntryID,
		CreatedAt:       time.Now(),
	}
	q.payments[p.ID] = p
	return p, nil
}

func (q *fakeQuerier) GetPayment(_ context.Context, id uuid.UUID) (db.Payment, error) {
	p, ok := q.payments[id]
	if !ok {
		return db.Payment{}, sql.ErrNoRows
	}
	return p, nil
}

func (q *fakeQuerier) CancelPayment(_ context.Context, arg db.CancelPaymentParams) (db.Payment, error) {
	p, ok := q.payments[arg.ID]
	if !ok || p.Status != "completed" { // guarded WHERE status = 'completed'
		return db.Payment{}, sql.ErrNoRows
	}
	p.Status = "canceled"
	p.ReversalEntryID = arg.ReversalEntryID
	q.payments[p.ID] = p
	return p, nil
}

func (q *fakeQuerier) InsertOutboxEvent(_ context.Context, arg db.InsertOutboxEventParams) error {
	if q.outboxErr != nil {
		return q.outboxErr
	}
	q.outbox = append(q.outbox, arg)
	return nil
}

// --- fixtures -------------------------------------------------------------

const asset = "USD"

func newService(store *fakeStore) *Service {
	return &Service{tx: store, log: slog.Default()}
}

func seededInput(q *fakeQuerier, amount int64) CreateInput {
	src := q.seedAccount(asset, amount)
	dst := q.seedAccount(asset, 0)
	return CreateInput{SourceAccountID: src, DestAccountID: dst, Asset: asset, Amount: amount}
}

// --- tests ----------------------------------------------------------------

// A successful Create records exactly one payment.created outbox row keyed by the
// payment id.
func TestCreate_EmitsPaymentCreated(t *testing.T) {
	store, q := newFake()
	svc := newService(store)

	pay, err := svc.Create(context.Background(), seededInput(q, 500))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := q.outboxTypes(); len(got) != 1 || got[0] != "payment.created" {
		t.Fatalf("outbox events = %v, want [payment.created]", got)
	}
	if got := q.outbox[0].AggregateID; got != pay.ID.String() {
		t.Errorf("aggregate_id = %q, want payment id %q", got, pay.ID)
	}
}

// A successful Cancel records exactly one payment.canceled row; the preceding
// Create's payment.created is the only other event.
func TestCancel_EmitsPaymentCanceled(t *testing.T) {
	store, q := newFake()
	svc := newService(store)
	ctx := context.Background()

	pay, err := svc.Create(ctx, seededInput(q, 500))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := svc.Cancel(ctx, pay.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := q.outboxTypes(); len(got) != 2 || got[0] != "payment.created" || got[1] != "payment.canceled" {
		t.Fatalf("outbox events = %v, want [payment.created payment.canceled]", got)
	}
	if got := q.outbox[1].AggregateID; got != pay.ID.String() {
		t.Errorf("cancel aggregate_id = %q, want payment id %q", got, pay.ID)
	}
}

// Canceling an already-canceled payment is a no-op transition: it returns
// ErrPaymentNotCancelable and records NO additional outbox row.
func TestCancel_AlreadyCanceledEmitsNothing(t *testing.T) {
	store, q := newFake()
	svc := newService(store)
	ctx := context.Background()

	pay, err := svc.Create(ctx, seededInput(q, 500))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := svc.Cancel(ctx, pay.ID); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	beforeSecond := len(q.outbox)

	_, err = svc.Cancel(ctx, pay.ID)
	if !errors.Is(err, ErrPaymentNotCancelable) {
		t.Fatalf("second Cancel err = %v, want ErrPaymentNotCancelable", err)
	}
	if got := len(q.outbox); got != beforeSecond {
		t.Errorf("outbox rows = %d, want unchanged %d (no-op emits nothing)", got, beforeSecond)
	}
}

// A failing InsertOutboxEvent aborts Create: the error surfaces, so under a real
// transaction the payment row and the outbox row roll back together.
func TestCreate_OutboxErrorPropagates(t *testing.T) {
	store, q := newFake()
	q.outboxErr = errors.New("outbox insert failed")
	svc := newService(store)

	_, err := svc.Create(context.Background(), seededInput(q, 500))
	if !errors.Is(err, q.outboxErr) {
		t.Fatalf("Create err = %v, want the injected outbox error", err)
	}
	if got := len(q.outbox); got != 0 {
		t.Errorf("recorded %d outbox rows, want 0 (insert failed)", got)
	}
}
