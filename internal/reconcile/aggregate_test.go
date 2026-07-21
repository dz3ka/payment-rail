package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/google/uuid"
)

// fakeQuerier is an in-memory settlementQuerier backed by a pre-sorted slice of
// rows. It reproduces the DB's keyset contract exactly: FirstPage returns the
// oldest `limit` rows; After returns the oldest `PageLimit` rows strictly past
// the (created_at, id) cursor. Rows MUST be supplied already sorted by
// (created_at, id) ascending, mirroring the ORDER BY in the query. calls counts
// invocations so a test can assert paging terminates.
type fakeQuerier struct {
	rows  []db.ListSettlementsForReconcileFirstPageRow
	calls int
	err   error
}

func (f *fakeQuerier) ListSettlementsForReconcileFirstPage(_ context.Context, limit int32) ([]db.ListSettlementsForReconcileFirstPageRow, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.take(0, limit), nil
}

func (f *fakeQuerier) ListSettlementsForReconcileAfter(_ context.Context, arg db.ListSettlementsForReconcileAfterParams) ([]db.ListSettlementsForReconcileAfterRow, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	// Find the first row strictly after the cursor tuple.
	start := len(f.rows)
	for i, r := range f.rows {
		if after(r.CreatedAt, r.ID, arg.AfterCreatedAt, arg.AfterID) {
			start = i
			break
		}
	}
	first := f.take(start, arg.PageLimit)
	out := make([]db.ListSettlementsForReconcileAfterRow, len(first))
	for i, r := range first {
		out[i] = db.ListSettlementsForReconcileAfterRow(r)
	}
	return out, nil
}

func (f *fakeQuerier) take(start int, limit int32) []db.ListSettlementsForReconcileFirstPageRow {
	end := start + int(limit)
	if end > len(f.rows) {
		end = len(f.rows)
	}
	if start > len(f.rows) {
		start = len(f.rows)
	}
	return append([]db.ListSettlementsForReconcileFirstPageRow(nil), f.rows[start:end]...)
}

// after reports whether (ac, aid) sorts strictly after (bc, bid) under the
// (created_at, id) keyset ordering.
func after(ac time.Time, aid uuid.UUID, bc time.Time, bid uuid.UUID) bool {
	if ac.After(bc) {
		return true
	}
	if ac.Before(bc) {
		return false
	}
	return uuidGreater(aid, bid)
}

func uuidGreater(a, b uuid.UUID) bool {
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return false
}

// row builds a settlement row at the given ordinal (drives both created_at and a
// deterministic, monotonically-increasing uuid so the keyset order is stable).
func row(ord int, status, asset string, amount int64) db.ListSettlementsForReconcileFirstPageRow {
	var id uuid.UUID
	id[15] = byte(ord)
	return db.ListSettlementsForReconcileFirstPageRow{
		ID:        id,
		CreatedAt: time.Unix(int64(ord), 0).UTC(),
		Status:    status,
		Asset:     asset,
		Amount:    amount,
	}
}

func TestAggregateSettlements_MultiPageMultiAsset(t *testing.T) {
	// 5 rows, pageSize 2 => pages of 2,2,1 (last short page ends the loop),
	// exercising the keyset continuation twice. Two assets, mixed statuses.
	rows := []db.ListSettlementsForReconcileFirstPageRow{
		row(1, "finalized", "USDC", 100),
		row(2, "settled", "USDC", 30),
		row(3, "finalized", "USDT", 500),
		row(4, "settled", "USDC", 7),
		row(5, "finalized", "USDC", 900),
	}
	f := &fakeQuerier{rows: rows}

	got, err := AggregateSettlements(context.Background(), f, 2)
	if err != nil {
		t.Fatalf("AggregateSettlements: %v", err)
	}

	if usdc := got["USDC"]; usdc.FinalizedMinor != 1000 || usdc.ConfirmedMinor != 37 {
		t.Errorf("USDC = %+v, want {FinalizedMinor:1000 ConfirmedMinor:37}", usdc)
	}
	if usdt := got["USDT"]; usdt.FinalizedMinor != 500 || usdt.ConfirmedMinor != 0 {
		t.Errorf("USDT = %+v, want {FinalizedMinor:500 ConfirmedMinor:0}", usdt)
	}
	// 5 rows, page size 2: FirstPage + After + After(short) = 3 calls. The
	// short final page (1 < 2) terminates the loop; assert we didn't spin.
	if f.calls != 3 {
		t.Errorf("querier calls = %d, want 3 (paging must advance and terminate)", f.calls)
	}
}

func TestAggregateSettlements_ExactMultipleTerminates(t *testing.T) {
	// 4 rows, pageSize 2 => full pages 2,2 then an empty page ends the loop.
	rows := []db.ListSettlementsForReconcileFirstPageRow{
		row(1, "settled", "USDC", 1),
		row(2, "settled", "USDC", 2),
		row(3, "settled", "USDC", 4),
		row(4, "settled", "USDC", 8),
	}
	f := &fakeQuerier{rows: rows}
	got, err := AggregateSettlements(context.Background(), f, 2)
	if err != nil {
		t.Fatalf("AggregateSettlements: %v", err)
	}
	if got["USDC"].ConfirmedMinor != 15 {
		t.Errorf("USDC ConfirmedMinor = %d, want 15", got["USDC"].ConfirmedMinor)
	}
	// FirstPage(2) + After(2) + After(0 empty) = 3 calls.
	if f.calls != 3 {
		t.Errorf("querier calls = %d, want 3", f.calls)
	}
}

func TestAggregateSettlements_Empty(t *testing.T) {
	f := &fakeQuerier{}
	got, err := AggregateSettlements(context.Background(), f, 10)
	if err != nil {
		t.Fatalf("AggregateSettlements: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d assets, want 0", len(got))
	}
	if f.calls != 1 {
		t.Errorf("querier calls = %d, want 1 (single short first page)", f.calls)
	}
}

func TestAggregateSettlements_PropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	f := &fakeQuerier{err: sentinel}
	if _, err := AggregateSettlements(context.Background(), f, 10); !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want %v", err, sentinel)
	}
}
