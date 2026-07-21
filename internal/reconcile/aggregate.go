// Package reconcile builds the M6 proof-of-reserves report: it folds the ledger's
// expectation (finalized + confirmed settlement value, cursored out of a large
// settlements table) against on-chain treasury balances and user liabilities.
// Like internal/audit it depends only on the stdlib and internal/db, and its core
// is a set of pure functions over a narrow querier seam so the report logic is
// testable without a database.
package reconcile

import (
	"context"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/google/uuid"
)

// AssetSums is the ledger's per-asset expectation, split by finality, in an
// asset's base (minor) units. FinalizedMinor is settlement value the ledger
// treats as final; ConfirmedMinor is confirmed-but-not-yet-final ("settled")
// value — funds already on-chain that the report bridges rather than flags.
type AssetSums struct {
	FinalizedMinor int64
	ConfirmedMinor int64
}

// settlementQuerier is the narrow seam AggregateSettlements pages over: the two
// keyset queries from db/query/reconcile.sql and nothing else. A hermetic fake
// stands in for *db.Queries in tests.
type settlementQuerier interface {
	ListSettlementsForReconcileFirstPage(ctx context.Context, limit int32) ([]db.ListSettlementsForReconcileFirstPageRow, error)
	ListSettlementsForReconcileAfter(ctx context.Context, arg db.ListSettlementsForReconcileAfterParams) ([]db.ListSettlementsForReconcileAfterRow, error)
}

// AggregateSettlements pages the settlements table by keyset (created_at, id),
// oldest-first, accumulating per-asset sums: 'finalized' rows add to
// FinalizedMinor, 'settled' rows add to ConfirmedMinor. It fetches the first page,
// then repeatedly continues past the last row's (created_at, id) cursor until a
// page comes back short (fewer than pageSize rows), which means the table end was
// reached. Keyset (not OFFSET) keeps each page a single index range scan and is
// stable under concurrent inserts on a large table.
func AggregateSettlements(ctx context.Context, q settlementQuerier, pageSize int32) (map[string]AssetSums, error) {
	sums := make(map[string]AssetSums)

	first, err := q.ListSettlementsForReconcileFirstPage(ctx, pageSize)
	if err != nil {
		return nil, err
	}

	var cursorAt time.Time
	var cursorID uuid.UUID
	n := len(first)
	for _, r := range first {
		accumulate(sums, r.Status, r.Asset, r.Amount)
		cursorAt, cursorID = r.CreatedAt, r.ID
	}

	// A short (or empty) first page already covered the whole table.
	for n == int(pageSize) {
		page, err := q.ListSettlementsForReconcileAfter(ctx, db.ListSettlementsForReconcileAfterParams{
			AfterCreatedAt: cursorAt,
			AfterID:        cursorID,
			PageLimit:      pageSize,
		})
		if err != nil {
			return nil, err
		}
		n = len(page)
		for _, r := range page {
			accumulate(sums, r.Status, r.Asset, r.Amount)
			cursorAt, cursorID = r.CreatedAt, r.ID
		}
	}

	return sums, nil
}

// accumulate folds one settlement row into the per-asset sums by finality.
// Statuses other than 'finalized'/'settled' never reach here (the query filters
// to those two), so anything else is silently ignored rather than misclassified.
func accumulate(sums map[string]AssetSums, status, asset string, amount int64) {
	s := sums[asset]
	switch status {
	case "finalized":
		s.FinalizedMinor += amount
	case "settled":
		s.ConfirmedMinor += amount
	default:
		return
	}
	sums[asset] = s
}
