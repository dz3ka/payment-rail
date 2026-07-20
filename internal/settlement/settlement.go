// Package settlement turns on-chain confirmation events into ledger truth. It
// sits between the chain watcher (internal/chain/evm) and the double-entry
// ledger (internal/ledger): the Recorder writes the payment↔tx-hash link at
// submit time, and the Sink consumes the watcher's Status stream to post the
// clearing entries that move a provisional credit into (or back out of) the
// onchain_settlement house account as a transaction confirms or reorgs.
//
// Like payments, this package never stores a balance and never bypasses
// double-entry: it rides the ledger's PostWithin seam so the journal entry and
// the settlement-row status flip commit in one transaction. The settlement row's
// status guard makes redelivery of the same watcher event a no-op (the ledger's
// UNIQUE(kind, external_ref) is a second-line backstop).
//
// Recovery scope (M3 slice 2): the watcher's tracking set is seeded once at
// startup from persisted pending/settled rows, so an effect dropped by a sink
// error is only re-attempted after a restart re-seeds — there is no in-process
// retry, and the watcher does not re-emit a Status it already emitted. Two
// consequences are deferred to slice 3: (1) a settled tx orphaned and never
// re-mined while the watcher is DOWN is not reversed on restart, because the
// re-seed via Track resets it to pending with no block anchor; (2) a transient
// sink failure stalls that settlement until a restart. Money stays conservatively
// safe in both (no double-post; the destination keeps its provisional credit).
package settlement

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/outbox"
)

// settlementEvent is the small, stable body carried in a settlement.* outbox
// envelope's "data" field. It is keyed for consumers by tx_hash (the envelope's
// aggregate_id) and payment_id; asset/amount are populated when the transition
// resolved them (settle/reverse) and omitted when it did not (finalize).
type settlementEvent struct {
	PaymentID uuid.UUID `json:"payment_id"`
	TxHash    string    `json:"tx_hash"`
	Asset     string    `json:"asset,omitempty"`
	Amount    int64     `json:"amount,omitempty"`
}

// houseAccountName is the well-known name of the clearing account every
// settlement entry balances against — the counterparty that holds funds while
// they are on-chain (per asset, resolved by GetAccountByNameAndAsset).
const houseAccountName = "onchain_settlement"

// Recorder persists the payment↔tx-hash link at submit time so the Sink can
// later resolve a confirmed transaction back to the payment it settles. It
// writes through a db.Querier (the production pool or the in-memory fake), so a
// single INSERT needs no transaction of its own.
type Recorder struct {
	q db.Querier
}

// NewRecorder wires a Recorder over a Querier (db.New(pool) in production).
func NewRecorder(q db.Querier) *Recorder {
	return &Recorder{q: q}
}

// Link records that txHash is the on-chain transaction settling paymentID. It is
// idempotent for a re-link of the SAME payment: InsertSettlement is ON CONFLICT
// (tx_hash) DO NOTHING RETURNING, so a duplicate returns sql.ErrNoRows, on which
// Link reads back the existing row and returns nil only if it already points at
// paymentID. A tx_hash already bound to a DIFFERENT payment is rejected — never
// reported as a successful link — so an operator can't silently mis-bind a
// broadcast transaction. Any other error wraps and propagates.
func (r *Recorder) Link(ctx context.Context, paymentID uuid.UUID, txHash string) error {
	_, err := r.q.InsertSettlement(ctx, db.InsertSettlementParams{
		PaymentID: paymentID,
		TxHash:    txHash,
	})
	if err == nil {
		return nil // freshly linked
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("settlement: link payment %s to tx %s: %w", paymentID, txHash, err)
	}
	// Conflict: the tx is already linked. Idempotent only if to the same payment.
	existing, gerr := r.q.GetSettlementByTxHash(ctx, txHash)
	if gerr != nil {
		return fmt.Errorf("settlement: verify existing link for tx %s: %w", txHash, gerr)
	}
	if existing.PaymentID != paymentID {
		return fmt.Errorf("settlement: tx %s already linked to payment %s, refusing to relink to %s",
			txHash, existing.PaymentID, paymentID)
	}
	return nil // already linked to this payment; idempotent no-op
}

// Sink drives ledger effects from the watcher's Status stream. It implements
// evm.StatusSink, so the watcher dispatches every transition to OnStatus without
// evm ever importing this package (the dependency inversion that keeps evm
// chain-only). A returned error is logged and swallowed by the watcher's Run
// loop; the settlement row's status keeps redelivery idempotent, but a dropped
// effect is only re-attempted after a restart re-seeds tracking (see the package
// doc's recovery-scope note — in-process retry is a slice-3 follow-up).
type Sink struct {
	tx  ledger.Store
	log *slog.Logger
}

var _ evm.StatusSink = (*Sink)(nil)

// NewSink wires a Sink over the ledger's transactor. A nil logger falls back to
// slog.Default() (mirrors NewService / NewWatcher).
func NewSink(tx ledger.Store, log *slog.Logger) *Sink {
	if log == nil {
		log = slog.Default()
	}
	return &Sink{tx: tx, log: log}
}

// OnStatus applies one watcher transition to the ledger. It acts on the three
// effecting phases — Confirmed (settle), Reorged (reverse), and Finalized
// (finalize) — and returns nil immediately for every other phase without
// touching the database.
//
// Settle and reverse run inside one shared ExecTx so the journal entry and the
// settlement-row status flip commit atomically: the transaction resolves the
// tracked settlement (an untracked tx_hash is not an error, just nil), then the
// payment it settles and the per-asset clearing account, and hands off to settle
// or reverse. Finalize is a pure status transition — settle already moved the
// money — so it posts no journal entry and runs its own single-statement ExecTx
// without resolving the payment or clearing account.
func (s *Sink) OnStatus(ctx context.Context, st evm.Status) error {
	switch st.Phase {
	case evm.PhaseConfirmed, evm.PhaseReorged:
		// resolved and dispatched through the shared ExecTx below
	case evm.PhaseFinalized:
		return s.finalize(ctx, st)
	default:
		return nil // pending/mined carry no ledger effect
	}

	txHash := string(st.TxHash)
	return s.tx.ExecTx(ctx, func(q db.Querier) error {
		sett, err := q.GetSettlementByTxHash(ctx, txHash)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // not a tx we track; nothing to settle
			}
			return fmt.Errorf("settlement: lookup tx %s: %w", txHash, err)
		}
		pay, err := q.GetPayment(ctx, sett.PaymentID)
		if err != nil {
			return fmt.Errorf("settlement: get payment %s: %w", sett.PaymentID, err)
		}
		house, err := q.GetAccountByNameAndAsset(ctx, db.GetAccountByNameAndAssetParams{
			Name:  houseAccountName,
			Asset: pay.Asset,
		})
		if err != nil {
			return fmt.Errorf("settlement: resolve %s clearing account for %s: %w", houseAccountName, pay.Asset, err)
		}

		if st.Phase == evm.PhaseConfirmed {
			return s.settle(ctx, q, sett, pay, house, st)
		}
		return s.reverse(ctx, q, sett, pay, house, st)
	})
}

// settle posts the clearing entry for a confirmed transaction — debit the
// destination (releasing the provisional credit), credit the house — then flips
// the row to settled. Idempotency is anchored on the row status guard below, not
// on the ledger's UNIQUE(kind, external_ref): a redelivered confirm is caught by
// the guard before any post. If the destination has since spent the funds,
// ErrInsufficientFunds propagates — the correct outcome, exactly as
// payments.Cancel documents.
func (s *Sink) settle(ctx context.Context, q db.Querier, sett db.Settlement, pay db.Payment, house db.Account, st evm.Status) error {
	// Idempotency is anchored on the row status, checked before the post
	// (mirroring payments.Cancel's status guard). PostWithin validates funds
	// before the ledger's unique constraint, so a redelivered confirm on an
	// already-settled row would trip the balance check — the provisional credit
	// is already released — not the benign duplicate path. Short-circuiting here
	// is what makes redelivery a clean no-op.
	if sett.Status == "settled" {
		return nil
	}

	entry := ledger.Entry{
		Kind:        "settlement.settle",
		ExternalRef: fmt.Sprintf("settle:%s:%s", sett.PaymentID, st.BlockHash),
		Asset:       pay.Asset,
		Lines: []ledger.Line{
			{AccountID: pay.DestAccountID, Direction: ledger.Debit, Amount: pay.Amount},
			{AccountID: house.ID, Direction: ledger.Credit, Amount: pay.Amount},
		},
	}

	je, err := ledger.PostWithin(ctx, q, entry)
	if err != nil {
		// The row-status guard above already short-circuits every reachable
		// redelivery before we post, so a duplicate external_ref is not expected
		// here — and under real Postgres a unique violation aborts the whole
		// transaction, so there is no "benign continue": the following Mark would
		// fail with in_failed_sql_transaction. Any post error therefore rolls the
		// tx back and propagates.
		s.logResult(ctx, "settle", pay, st, err)
		return err
	}
	settleEntryID := uuid.NullUUID{UUID: je.ID, Valid: true}

	if _, err := q.MarkSettlementSettled(ctx, db.MarkSettlementSettledParams{
		TxHash:        string(st.TxHash),
		SettleEntryID: settleEntryID,
		// Anchor the block this settle observed so a later finality/reorg check
		// has the provenance to compare against (MarkSettlementReorged NULLs it
		// again if the tx leaves the canonical chain).
		SettledBlockHash:   sql.NullString{String: st.BlockHash, Valid: true},
		SettledBlockNumber: sql.NullInt64{Int64: int64(st.BlockNumber), Valid: true},
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // already settled; idempotent no-op
		}
		s.logResult(ctx, "settle", pay, st, err)
		return fmt.Errorf("settlement: mark settled %s: %w", st.TxHash, err)
	}

	// Reached only when MarkSettlementSettled actually flipped the row (a
	// redelivered confirm short-circuits at the status guard above, and a
	// concurrent settle returns ErrNoRows), so this emits exactly once per real
	// settlement, in the same tx. Keyed by tx_hash so every lifecycle event for
	// this tx shares an aggregate id.
	if err := outbox.Emit(ctx, q, outbox.Event{
		Type:        "settlement.confirmed",
		AggregateID: string(st.TxHash),
		Data:        settlementEvent{PaymentID: sett.PaymentID, TxHash: string(st.TxHash), Asset: pay.Asset, Amount: pay.Amount},
	}); err != nil {
		return err
	}

	s.logResult(ctx, "settle", pay, st, nil)
	return nil
}

// reverse posts the mirror of settle for a reorged transaction — debit the
// house, credit the destination (restoring the provisional credit) — then flips
// the row to reorged. The row-status guard (only a settled row reverses) makes a
// redelivered reorg a no-op. A distinct block hash yields a distinct
// external_ref, so a re-mine at a new block settles again cleanly.
func (s *Sink) reverse(ctx context.Context, q db.Querier, sett db.Settlement, pay db.Payment, house db.Account, st evm.Status) error {
	// Only a settled tx can reorg; a still-pending or already-reorged row is a
	// no-op. As with settle, this status guard — not the post — is the
	// idempotency anchor: re-posting the reversal on an already-reorged row would
	// trip PostWithin's balance check, not the benign duplicate path.
	if sett.Status != "settled" {
		return nil
	}

	entry := ledger.Entry{
		Kind:        "settlement.reversal",
		ExternalRef: fmt.Sprintf("reverse:%s:%s", sett.PaymentID, st.BlockHash),
		Asset:       pay.Asset,
		Lines: []ledger.Line{
			{AccountID: house.ID, Direction: ledger.Debit, Amount: pay.Amount},
			{AccountID: pay.DestAccountID, Direction: ledger.Credit, Amount: pay.Amount},
		},
	}

	if _, err := ledger.PostWithin(ctx, q, entry); err != nil {
		// Same reasoning as settle: the status guard short-circuits reachable
		// redelivery before we post, and a Postgres unique violation aborts the
		// tx, so any post error rolls back and propagates rather than "continuing".
		s.logResult(ctx, "reverse", pay, st, err)
		return err
	}

	if _, err := q.MarkSettlementReorged(ctx, string(st.TxHash)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // already reorged; idempotent no-op
		}
		s.logResult(ctx, "reverse", pay, st, err)
		return fmt.Errorf("settlement: mark reorged %s: %w", st.TxHash, err)
	}

	// Reached only when MarkSettlementReorged actually flipped a settled row (the
	// status guard makes a redelivered reorg a no-op, and a concurrent reorg returns
	// ErrNoRows), so this emits exactly once per real reversal, in the same tx.
	if err := outbox.Emit(ctx, q, outbox.Event{
		Type:        "settlement.reorged",
		AggregateID: string(st.TxHash),
		Data:        settlementEvent{PaymentID: sett.PaymentID, TxHash: string(st.TxHash), Asset: pay.Asset, Amount: pay.Amount},
	}); err != nil {
		return err
	}

	s.logResult(ctx, "reverse", pay, st, nil)
	return nil
}

// finalize records that a settled tx is now buried past finality — deep enough
// that a reversing reorg is treated as impossible. It is a pure status transition
// with NO ledger effect: settle already moved the money into the house account, so
// there is nothing to post. The single guarded UPDATE (status 'settled' →
// 'finalized') runs in its own ExecTx; a redelivered Finalized on an
// already-finalized (or no-longer-settled) row matches no row and comes back as
// sql.ErrNoRows, which is the benign idempotent no-op — exactly the convention
// settle/reverse use for their guarded Marks.
func (s *Sink) finalize(ctx context.Context, st evm.Status) error {
	txHash := string(st.TxHash)
	return s.tx.ExecTx(ctx, func(q db.Querier) error {
		sett, err := q.MarkSettlementFinalized(ctx, txHash)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil // already finalized or not settled; idempotent no-op
			}
			return fmt.Errorf("settlement: mark finalized %s: %w", txHash, err)
		}
		// Mirror logResult's redacted attr shape (identifiers and public chain data
		// only, never amounts). finalize resolves no payment, so it logs against the
		// tx directly rather than through logResult's payment-keyed helper.
		s.log.InfoContext(ctx, "settlement.finalized",
			"tx_hash", txHash,
			"block_hash", st.BlockHash,
			"status", sett.Status,
		)
		// Inside the row-updated branch only: a redelivered/no-longer-settled finalize
		// returns ErrNoRows above and never reaches here, so this emits exactly once
		// per real finalization, in the same tx. finalize resolves no payment, so the
		// body carries tx_hash + payment_id (from the marked row) without asset/amount.
		return outbox.Emit(ctx, q, outbox.Event{
			Type:        "settlement.finalized",
			AggregateID: txHash,
			Data:        settlementEvent{PaymentID: sett.PaymentID, TxHash: txHash},
		})
	})
}

// logResult emits one structured record per settlement outcome. It mirrors the
// ledger's logResult discipline: only identifiers and public chain data are
// logged — payment_id, tx_hash, block_hash, asset — never amounts, which the
// ledger's sentinel messages carry and logs deliberately withhold. A successful
// reversal is a warn (a settled tx left the chain); a successful settle is info;
// an insufficient-funds reject is a warn (a correct-but-notable outcome); any
// other failure is an error.
func (s *Sink) logResult(ctx context.Context, op string, pay db.Payment, st evm.Status, err error) {
	attrs := []any{
		"payment_id", pay.ID,
		"tx_hash", string(st.TxHash),
		"block_hash", st.BlockHash,
		"asset", pay.Asset,
	}
	switch {
	case errors.Is(err, ledger.ErrInsufficientFunds):
		s.log.WarnContext(ctx, "settlement."+op+" rejected: insufficient funds", attrs...)
	case err != nil:
		s.log.ErrorContext(ctx, "settlement."+op+" failed", attrs...)
	case op == "reverse":
		s.log.WarnContext(ctx, "settlement.reversed", attrs...)
	default:
		s.log.InfoContext(ctx, "settlement.settled", attrs...)
	}
}
