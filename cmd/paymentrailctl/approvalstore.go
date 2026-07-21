package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/audit"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/policy"
)

// proposeAuditData and approveAuditData are the F9 audit payloads for the two
// four-eyes operator actions. They carry only identity fields already present on
// the intent/approval — no new plumbing — so `audit verify`/browsing can tell the
// coherent story of who proposed and who approved each payment.
type proposeAuditData struct {
	Proposer  string `json:"proposer"`
	PaymentID string `json:"payment_id,omitempty"`
	To        string `json:"to"`
	Amount    string `json:"amount"`
	Asset     string `json:"asset"`
}

type approveAuditData struct {
	Approver   string `json:"approver"`
	ApprovalID string `json:"approval_id"`
	PaymentID  string `json:"payment_id,omitempty"`
}

// Store-state sentinels for the four-eyes approval lifecycle. Per the razor these
// live in the composition root — NOT in internal/policy — because they describe
// the STORE's state (row missing, row no longer pending), not a policy decision;
// the policy sentinels (ErrUnknownApprover/ErrSelfApproval) travel out of the
// decide callback unchanged and are matched separately.
var (
	// ErrApprovalNotFound means no approval row exists for the given id.
	ErrApprovalNotFound = errors.New("approval: not found")
	// ErrAlreadyApproved means the row is no longer pending: already approved,
	// broadcast, or claimed by a concurrent approver that won the row lock.
	ErrAlreadyApproved = errors.New("approval: not pending")
)

// pgApprovalStore is the composition-root Postgres impl backing the four-eyes
// commands. It mirrors pgVelocityStore: a bare *sql.DB whose pool lifecycle
// belongs to the caller, with each method owning its own transaction boundary and
// delegating all SQL to db.Queries. Claim runs the guarded status transition under
// a SELECT ... FOR UPDATE row lock so a concurrent approver blocks rather than
// racing past the pending check.
type pgApprovalStore struct {
	db *sql.DB
}

// newApprovalStore wraps an already-open *sql.DB; pool lifecycle (Open/Close)
// belongs to the caller that constructed the handle.
func newApprovalStore(sqlDB *sql.DB) *pgApprovalStore {
	return &pgApprovalStore{db: sqlDB}
}

// Propose parks a proposed payment as a pending approval attributed to proposer,
// returning the generated approval id. The insert and its F9 audit append share
// ONE transaction (mirroring Claim's BeginTx/defer Rollback/db.New(tx)/commit
// idiom) so a failed audit append rolls the proposal back — fail-closed, no parked
// payment without its audit trail.
func (s *pgApprovalStore) Propose(ctx context.Context, proposer string, in policy.Intent) (string, error) {
	// Fail closed on an amount Postgres BIGINT can't hold: storing it would either
	// overflow or silently truncate, and an approval must replay the EXACT amount.
	// The value itself is not surfaced (it may be sensitive); only that it overflows.
	if !in.Amount.IsInt64() {
		return "", fmt.Errorf("approval: amount exceeds int64 range for key %s", in.KeyID)
	}

	var paymentID uuid.NullUUID
	if in.PaymentID != "" {
		parsed, err := uuid.Parse(in.PaymentID)
		if err != nil {
			return "", fmt.Errorf("approval: payment id %q is not a valid uuid: %w", in.PaymentID, err)
		}
		paymentID = uuid.NullUUID{UUID: parsed, Valid: true}
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("approval: begin tx: %w", err)
	}
	// Rollback is a no-op after a successful commit; the error is ignored to mirror
	// Claim's idiom (a benign ErrTxDone on the already-committed path).
	defer func() { _ = tx.Rollback() }()

	q := db.New(tx)

	id, err := q.InsertPaymentApproval(ctx, db.InsertPaymentApprovalParams{
		ToAddress: in.To,
		Amount:    in.Amount.Int64(),
		Asset:     in.Asset,
		KeyID:     in.KeyID,
		PaymentID: paymentID,
		Proposer:  proposer,
	})
	if err != nil {
		return "", fmt.Errorf("approval: insert: %w", err)
	}

	// F9: record the operator's propose in the hash-chained audit log in the SAME
	// tx as the insert. The aggregate is the authorized payment so this ties to the
	// eventual settlement; when no payment id is linked (--payment-id omitted) fall
	// back to the approval id so the action is still anchored to a real aggregate.
	aggregateID := in.PaymentID
	if aggregateID == "" {
		aggregateID = id.String()
	}
	if err := audit.Append(ctx, q, audit.Entry{
		Actor:         proposer,
		Action:        "operator.propose",
		AggregateType: "payment",
		AggregateID:   aggregateID,
		Data: proposeAuditData{
			Proposer:  proposer,
			PaymentID: in.PaymentID,
			To:        in.To,
			Amount:    in.Amount.String(),
			Asset:     in.Asset,
		},
	}); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("approval: commit tx: %w", err)
	}
	return id.String(), nil
}

// Claim atomically approves a pending approval and returns the frozen intent to
// broadcast. In ONE transaction it locks the row (FOR UPDATE), reconstructs the
// PendingApproval, and hands it to decide (the four-eyes authorization check). If
// decide returns non-nil the error propagates UNCHANGED so the caller's errors.Is
// on the policy sentinels still works, and the deferred rollback discards the read.
// A missing row is ErrApprovalNotFound; a non-pending row (or a lost race where the
// guarded UPDATE matches nothing) is ErrAlreadyApproved.
func (s *pgApprovalStore) Claim(ctx context.Context, approvalID, approver string, decide func(policy.PendingApproval) error) (policy.Intent, error) {
	id, err := uuid.Parse(approvalID)
	if err != nil {
		return policy.Intent{}, fmt.Errorf("approval: id %q is not a valid uuid: %w", approvalID, err)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return policy.Intent{}, fmt.Errorf("approval: begin tx: %w", err)
	}
	// Rollback is a no-op after a successful commit; the error is ignored to mirror
	// the SQLStore idiom (a benign ErrTxDone on the already-committed path).
	defer func() { _ = tx.Rollback() }()

	q := db.New(tx)

	row, err := q.GetApprovalForUpdate(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return policy.Intent{}, ErrApprovalNotFound
		}
		return policy.Intent{}, fmt.Errorf("approval: load: %w", err)
	}

	// Reconstruct the frozen intent exactly as it was parked; the amount round-trips
	// through int64 (guarded at Propose), the payment id maps back from NULL to "".
	paymentID := ""
	if row.PaymentID.Valid {
		paymentID = row.PaymentID.UUID.String()
	}
	intent := policy.Intent{
		To:        row.ToAddress,
		Asset:     row.Asset,
		KeyID:     row.KeyID,
		PaymentID: paymentID,
		Amount:    big.NewInt(row.Amount),
	}
	pa := policy.PendingApproval{
		ID:       row.ID.String(),
		Proposer: row.Proposer,
		Status:   row.Status,
		Intent:   intent,
	}

	if row.Status != "pending" {
		return policy.Intent{}, ErrAlreadyApproved
	}

	if err := decide(pa); err != nil {
		return policy.Intent{}, err // unchanged: policy sentinels must survive errors.Is
	}

	rows, err := q.MarkApprovalApproved(ctx, db.MarkApprovalApprovedParams{
		ID:       id,
		Approver: sql.NullString{String: approver, Valid: true},
	})
	if err != nil {
		return policy.Intent{}, fmt.Errorf("approval: mark approved: %w", err)
	}
	if rows == 0 {
		// The guarded UPDATE matched nothing: a concurrent approver claimed it
		// between our locked read and write (should not happen under FOR UPDATE, but
		// the guard is definitive). Fail closed as an already-claimed approval.
		return policy.Intent{}, ErrAlreadyApproved
	}

	// F9: record the operator's approval in the hash-chained audit log on the SAME
	// tx-bound q, AFTER the row is marked approved and BEFORE commit, so a failed
	// append rolls the claim back with it (fail-closed). Same aggregate as the
	// eventual settlement — the authorized payment id, falling back to the approval
	// id when the proposal carried no payment id.
	aggregateID := paymentID
	if aggregateID == "" {
		aggregateID = row.ID.String()
	}
	if err := audit.Append(ctx, q, audit.Entry{
		Actor:         approver,
		Action:        "operator.approve",
		AggregateType: "payment",
		AggregateID:   aggregateID,
		Data: approveAuditData{
			Approver:   approver,
			ApprovalID: row.ID.String(),
			PaymentID:  paymentID,
		},
	}); err != nil {
		return policy.Intent{}, err
	}

	if err := tx.Commit(); err != nil {
		return policy.Intent{}, fmt.Errorf("approval: commit tx: %w", err)
	}
	return intent, nil
}

// MarkBroadcast records the tx hash against an approved approval. It is guarded on
// status = 'approved' AND tx_hash IS NULL, so a double-broadcast matches no row; a
// zero rows-affected is surfaced as an operator-facing error (the approval is not
// in the approved state, or a hash was already recorded).
func (s *pgApprovalStore) MarkBroadcast(ctx context.Context, approvalID, txHash string) error {
	id, err := uuid.Parse(approvalID)
	if err != nil {
		return fmt.Errorf("approval: id %q is not a valid uuid: %w", approvalID, err)
	}
	rows, err := db.New(s.db).MarkApprovalBroadcast(ctx, db.MarkApprovalBroadcastParams{
		ID:     id,
		TxHash: sql.NullString{String: txHash, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("approval: mark broadcast: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("approval: %s is not in the approved state or a tx hash was already recorded", approvalID)
	}
	return nil
}

// Reopen reverts a claimed-but-never-broadcast approval back to pending so a
// pre-send broadcast failure can be retried by re-claiming it. It is guarded on
// status = 'approved' AND tx_hash IS NULL, so once a broadcast has landed the
// UPDATE matches no row and a sent payment can never be resurrected; a zero
// rows-affected is surfaced as an operator-facing error (the row was not in the
// reopenable state — e.g. already broadcast) rather than masked.
func (s *pgApprovalStore) Reopen(ctx context.Context, approvalID string) error {
	id, err := uuid.Parse(approvalID)
	if err != nil {
		return fmt.Errorf("approval: id %q is not a valid uuid: %w", approvalID, err)
	}
	rows, err := db.New(s.db).ReopenApproval(ctx, id)
	if err != nil {
		return fmt.Errorf("approval: reopen: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("approval: %s is not in the reopenable state (approved with no tx hash) — it may already be broadcast", approvalID)
	}
	return nil
}
