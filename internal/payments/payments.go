// Package payments is Payment Rail's API-facing money-movement service. It turns a
// request to move value into an atomic pair of writes — one balanced journal
// entry in the ledger plus one payments row that records it — and it undoes a
// payment the same way, posting a reversing entry alongside the status flip.
//
// The accounting truth lives in the ledger (see internal/ledger): payments never
// stores a balance and never bypasses double-entry. It rides the ledger's
// PostWithin seam so its own row and the journal entry commit in one transaction,
// and it surfaces the ledger's sentinels (ErrInsufficientFunds, ...) unchanged so
// the API layer maps them once.
package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/audit"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/outbox"
)

// paymentEvent is the small, stable body carried in a payment.* outbox envelope's
// "data" field: identifiers, asset/amount, and the resulting status — enough for a
// consumer to react without re-reading the payment. It deliberately omits internal
// ledger ids (journal_entry_id), which are not part of the public event contract.
type paymentEvent struct {
	ID     uuid.UUID `json:"id"`
	Asset  string    `json:"asset"`
	Amount int64     `json:"amount"`
	Source uuid.UUID `json:"source_account_id"`
	Dest   uuid.UUID `json:"dest_account_id"`
	Status string    `json:"status"`
}

// Sentinel errors. Callers match these with errors.Is; return sites wrap them
// with %w so context travels with the cause. Ledger sentinels
// (ErrInsufficientFunds, ErrInvalidEntry, ...) propagate unchanged from Create
// and Cancel — this package does not re-wrap them.
var (
	// ErrPaymentNotFound means no payment exists for the given id.
	ErrPaymentNotFound = errors.New("payments: payment not found")
	// ErrPaymentNotCancelable means the payment is not in a state that can be
	// canceled — it was already canceled, or a concurrent cancel won the race.
	ErrPaymentNotCancelable = errors.New("payments: payment not cancelable")
)

// Service creates, reads, lists, and cancels payments. Reads run on the pool
// (implicit single-statement transactions); writes run through the ledger's
// transactor seam so the payments row and its journal entry commit atomically.
type Service struct {
	db  *sql.DB
	tx  ledger.Store
	log *slog.Logger
}

// NewService builds a Service over an already-open pool. It constructs the
// production ledger transactor from the same handle so both layers share one
// connection pool. A nil logger falls back to slog.Default().
func NewService(db *sql.DB, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{db: db, tx: ledger.NewSQLStore(db), log: log}
}

// CreateInput is the contract for Create: who pays whom, in what asset, how much.
// Amount is in minor units and must be positive (the ledger rejects otherwise).
type CreateInput struct {
	SourceAccountID uuid.UUID
	DestAccountID   uuid.UUID
	Asset           string
	Amount          int64
}

// Create moves Amount from source to dest and records the payment. The journal
// entry (debit source, credit dest) and the payments row commit in one
// transaction: PostWithin posts the balanced entry under lock, and the payment
// is inserted with the same id used as the entry's external reference, so a
// payment and its ledger footprint are one-to-one and mutually idempotent.
//
// Ledger rejections (ErrInsufficientFunds, ErrInvalidEntry, ErrUnbalanced,
// ErrDuplicateEntry) propagate unchanged for the API layer to map.
func (s *Service) Create(ctx context.Context, in CreateInput) (db.Payment, error) {
	pid := uuid.New()
	entry := ledger.Entry{
		Kind:        "payment",
		ExternalRef: pid.String(),
		Asset:       in.Asset,
		Lines: []ledger.Line{
			{AccountID: in.SourceAccountID, Direction: ledger.Debit, Amount: in.Amount},
			{AccountID: in.DestAccountID, Direction: ledger.Credit, Amount: in.Amount},
		},
	}

	var payment db.Payment
	err := s.tx.ExecTx(ctx, func(q db.Querier) error {
		je, err := ledger.PostWithin(ctx, q, entry)
		if err != nil {
			return err
		}
		payment, err = q.InsertPayment(ctx, db.InsertPaymentParams{
			ID:              pid,
			Status:          "completed",
			Asset:           in.Asset,
			Amount:          in.Amount,
			SourceAccountID: in.SourceAccountID,
			DestAccountID:   in.DestAccountID,
			JournalEntryID:  je.ID,
		})
		if err != nil {
			return err
		}
		// A fresh insert is always a real transition (pid is new each call), so
		// emitting unconditionally here still emits exactly once per created
		// payment, in the same tx as the row it describes.
		if err := outbox.Emit(ctx, q, outbox.Event{
			Type:        "payment.created",
			AggregateID: pid.String(),
			Data: paymentEvent{
				ID:     payment.ID,
				Asset:  payment.Asset,
				Amount: payment.Amount,
				Source: payment.SourceAccountID,
				Dest:   payment.DestAccountID,
				Status: payment.Status,
			},
		}); err != nil {
			return err
		}
		// F9: record the same fact in the hash-chained audit log, as the last write
		// in this tx — a failed append rolls the payment back (fail-closed).
		return audit.Append(ctx, q, audit.Entry{
			Actor:         "system:payments",
			Action:        "payment.created",
			AggregateType: "payment",
			AggregateID:   pid.String(),
			Data: paymentEvent{
				ID:     payment.ID,
				Asset:  payment.Asset,
				Amount: payment.Amount,
				Source: payment.SourceAccountID,
				Dest:   payment.DestAccountID,
				Status: payment.Status,
			},
		})
	})
	if err != nil {
		return db.Payment{}, err
	}
	return payment, nil
}

// Get returns the payment with the given id, or ErrPaymentNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (db.Payment, error) {
	payment, err := db.New(s.db).GetPayment(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return db.Payment{}, fmt.Errorf("payments: get %s: %w", id, ErrPaymentNotFound)
		}
		return db.Payment{}, fmt.Errorf("payments: get %s: %w", id, err)
	}
	return payment, nil
}

// Cursor is the keyset position for List: everything strictly older than
// (CreatedAt, ID) in newest-first order. A nil *Cursor means the first page.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// List returns up to limit payments newest-first plus the cursor for the next
// page (nil when the last page is reached). It over-fetches one row to detect
// whether a further page exists without a second round trip: if more than limit
// rows come back, the extra is dropped and the next cursor is taken from the last
// kept row.
func (s *Service) List(ctx context.Context, limit int32, after *Cursor) ([]db.Payment, *Cursor, error) {
	q := db.New(s.db)

	var (
		rows []db.Payment
		err  error
	)
	if after == nil {
		rows, err = q.ListPaymentsFirstPage(ctx, limit+1)
	} else {
		rows, err = q.ListPaymentsAfter(ctx, db.ListPaymentsAfterParams{
			AfterCreatedAt: after.CreatedAt,
			AfterID:        after.ID,
			PageLimit:      limit + 1,
		})
	}
	if err != nil {
		return nil, nil, fmt.Errorf("payments: list: %w", err)
	}

	if int32(len(rows)) <= limit {
		return rows, nil, nil
	}
	rows = rows[:limit]
	last := rows[len(rows)-1]
	return rows, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// Cancel reverses a completed payment: it posts a mirror journal entry (debit
// dest, credit source) and flips the payment to canceled, both in one
// transaction, so the money is returned and the record updated together.
//
// It rejects with ErrPaymentNotFound (no such payment) or ErrPaymentNotCancelable
// (already canceled, or a concurrent cancel won the CancelPayment guard). If the
// destination has since spent the funds the reversal cannot balance and
// ErrInsufficientFunds propagates — the correct outcome, not an error to mask.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (db.Payment, error) {
	var canceled db.Payment
	err := s.tx.ExecTx(ctx, func(q db.Querier) error {
		payment, err := q.GetPayment(ctx, id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("payments: cancel %s: %w", id, ErrPaymentNotFound)
			}
			return fmt.Errorf("payments: cancel %s: %w", id, err)
		}
		if payment.Status != "completed" {
			return fmt.Errorf("payments: cancel %s is %s: %w", id, payment.Status, ErrPaymentNotCancelable)
		}

		reversal := ledger.Entry{
			Kind:        "payment.reversal",
			ExternalRef: id.String() + ":reversal",
			Asset:       payment.Asset,
			Lines: []ledger.Line{
				{AccountID: payment.DestAccountID, Direction: ledger.Debit, Amount: payment.Amount},
				{AccountID: payment.SourceAccountID, Direction: ledger.Credit, Amount: payment.Amount},
			},
		}
		je, err := ledger.PostWithin(ctx, q, reversal)
		if err != nil {
			return err
		}

		canceled, err = q.CancelPayment(ctx, db.CancelPaymentParams{
			ID:              id,
			ReversalEntryID: uuid.NullUUID{UUID: je.ID, Valid: true},
		})
		if err != nil {
			// The UPDATE is guarded on status = 'completed'; no row means a
			// concurrent cancel already committed between our read and here.
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("payments: cancel %s lost race: %w", id, ErrPaymentNotCancelable)
			}
			return fmt.Errorf("payments: cancel %s: %w", id, err)
		}
		// Reached only when CancelPayment flipped a completed row to canceled — a
		// concurrent/repeated cancel returns ErrNoRows above and never gets here, so
		// this emits exactly once per real cancellation, in the same tx.
		if err := outbox.Emit(ctx, q, outbox.Event{
			Type:        "payment.canceled",
			AggregateID: id.String(),
			Data: paymentEvent{
				ID:     canceled.ID,
				Asset:  canceled.Asset,
				Amount: canceled.Amount,
				Source: canceled.SourceAccountID,
				Dest:   canceled.DestAccountID,
				Status: canceled.Status,
			},
		}); err != nil {
			return err
		}
		// F9: record the cancellation in the audit log as the last write in this tx
		// (fail-closed — a failed append rolls the status flip back).
		return audit.Append(ctx, q, audit.Entry{
			Actor:         "system:payments",
			Action:        "payment.canceled",
			AggregateType: "payment",
			AggregateID:   id.String(),
			Data: paymentEvent{
				ID:     canceled.ID,
				Asset:  canceled.Asset,
				Amount: canceled.Amount,
				Source: canceled.SourceAccountID,
				Dest:   canceled.DestAccountID,
				Status: canceled.Status,
			},
		})
	})
	if err != nil {
		return db.Payment{}, err
	}
	return canceled, nil
}
