package ledger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

// quietLogger discards output so tests stay silent but still exercise the
// logging paths.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// twoLineEntry builds a balanced debit/credit transfer of amount between two
// accounts (debit src, credit dst).
func transfer(kind, ref, asset string, src, dst uuid.UUID, amount int64) Entry {
	return Entry{
		Kind:        kind,
		ExternalRef: ref,
		Asset:       asset,
		Lines: []Line{
			{AccountID: src, Direction: Debit, Amount: amount},
			{AccountID: dst, Direction: Credit, Amount: amount},
		},
	}
}

func TestBalanced(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	tests := []struct {
		name    string
		lines   []Line
		wantErr error
	}{
		{
			name:    "no lines",
			lines:   nil,
			wantErr: ErrInvalidEntry,
		},
		{
			name:    "non-positive amount",
			lines:   []Line{{AccountID: a, Direction: Debit, Amount: 0}, {AccountID: b, Direction: Credit, Amount: 0}},
			wantErr: ErrInvalidEntry,
		},
		{
			name:    "unknown direction",
			lines:   []Line{{AccountID: a, Direction: "sideways", Amount: 5}},
			wantErr: ErrInvalidEntry,
		},
		{
			name:    "lopsided",
			lines:   []Line{{AccountID: a, Direction: Debit, Amount: 5}, {AccountID: b, Direction: Credit, Amount: 4}},
			wantErr: ErrUnbalanced,
		},
		{
			name:    "balanced",
			lines:   []Line{{AccountID: a, Direction: Debit, Amount: 5}, {AccountID: b, Direction: Credit, Amount: 5}},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Balanced(tt.lines)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Balanced() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Balanced() = %v, want errors.Is %v", err, tt.wantErr)
			}
		})
	}
}

func TestApplyToBalances(t *testing.T) {
	a, b := uuid.New(), uuid.New()

	t.Run("does not mutate input", func(t *testing.T) {
		cur := map[uuid.UUID]int64{a: 100, b: 0}
		_, err := ApplyToBalances(cur, []Line{
			{AccountID: a, Direction: Debit, Amount: 40},
			{AccountID: b, Direction: Credit, Amount: 40},
		})
		if err != nil {
			t.Fatalf("ApplyToBalances() = %v, want nil", err)
		}
		if cur[a] != 100 || cur[b] != 0 {
			t.Fatalf("input map mutated: %v", cur)
		}
	})

	t.Run("insufficient funds", func(t *testing.T) {
		cur := map[uuid.UUID]int64{a: 10, b: 0}
		_, err := ApplyToBalances(cur, []Line{
			{AccountID: a, Direction: Debit, Amount: 40},
			{AccountID: b, Direction: Credit, Amount: 40},
		})
		if !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("ApplyToBalances() = %v, want errors.Is ErrInsufficientFunds", err)
		}
	})
}

func TestPostEntry(t *testing.T) {
	ctx := context.Background()

	t.Run("balanced posting moves balances", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		je, err := svc.PostEntry(ctx, transfer("transfer", "ref-1", "USD", src, dst, 30))
		if err != nil {
			t.Fatalf("PostEntry() = %v, want nil", err)
		}
		if je.ExternalRef != "ref-1" {
			t.Fatalf("returned entry ref = %q, want ref-1", je.ExternalRef)
		}
		if bal, _ := q.GetAccountBalance(ctx, src); bal != 70 {
			t.Fatalf("src balance = %d, want 70", bal)
		}
		if bal, _ := q.GetAccountBalance(ctx, dst); bal != 30 {
			t.Fatalf("dst balance = %d, want 30", bal)
		}
	})

	t.Run("unbalanced entry is rejected before any tx work", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		e := Entry{Kind: "transfer", ExternalRef: "ref-2", Asset: "USD", Lines: []Line{
			{AccountID: src, Direction: Debit, Amount: 30},
			{AccountID: dst, Direction: Credit, Amount: 20},
		}}
		_, err := svc.PostEntry(ctx, e)
		if !errors.Is(err, ErrUnbalanced) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrUnbalanced", err)
		}
		if len(q.entries) != 0 {
			t.Fatalf("entries persisted on unbalanced posting: %d", len(q.entries))
		}
	})

	t.Run("overdraw is rejected with no partial write", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 10)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		linesBefore := len(q.lines)
		_, err := svc.PostEntry(ctx, transfer("transfer", "ref-3", "USD", src, dst, 40))
		if !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrInsufficientFunds", err)
		}
		if len(q.entries) != 0 {
			t.Fatalf("journal entry persisted on overdraw: %d", len(q.entries))
		}
		if len(q.lines) != linesBefore {
			t.Fatalf("entry lines persisted on overdraw: %d added", len(q.lines)-linesBefore)
		}
	})

	t.Run("duplicate external_ref maps to ErrDuplicateEntry", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		if _, err := svc.PostEntry(ctx, transfer("transfer", "dup", "USD", src, dst, 10)); err != nil {
			t.Fatalf("first PostEntry() = %v, want nil", err)
		}
		_, err := svc.PostEntry(ctx, transfer("transfer", "dup", "USD", src, dst, 10))
		if !errors.Is(err, ErrDuplicateEntry) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrDuplicateEntry", err)
		}
	})

	t.Run("invalid entry is rejected", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		cases := map[string]Entry{
			"empty lines":     {Kind: "transfer", ExternalRef: "r", Asset: "USD"},
			"missing kind":    {ExternalRef: "r", Asset: "USD", Lines: transfer("k", "r", "USD", src, dst, 5).Lines},
			"missing asset":   {Kind: "transfer", ExternalRef: "r", Lines: transfer("k", "r", "USD", src, dst, 5).Lines},
			"missing ref":     {Kind: "transfer", Asset: "USD", Lines: transfer("k", "r", "USD", src, dst, 5).Lines},
			"zero amount":     {Kind: "transfer", ExternalRef: "r", Asset: "USD", Lines: []Line{{AccountID: src, Direction: Debit, Amount: 0}, {AccountID: dst, Direction: Credit, Amount: 0}}},
			"negative amount": {Kind: "transfer", ExternalRef: "r", Asset: "USD", Lines: []Line{{AccountID: src, Direction: Debit, Amount: -5}, {AccountID: dst, Direction: Credit, Amount: -5}}},
		}
		for name, e := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := svc.PostEntry(ctx, e)
				if !errors.Is(err, ErrInvalidEntry) {
					t.Fatalf("PostEntry() = %v, want errors.Is ErrInvalidEntry", err)
				}
				if len(q.entries) != 0 {
					t.Fatalf("entry persisted for invalid input")
				}
			})
		}
	})

	t.Run("cross-asset posting is rejected with no write", func(t *testing.T) {
		store, q := newFake()
		usd := q.seedAccount("wallet", "USD", 100)
		eur := q.seedAccount("wallet", "EUR", 0)
		svc := NewService(store, quietLogger())

		linesBefore := len(q.lines)
		// A "USD" entry whose credit lands on a EUR account must not post,
		// even though it is perfectly balanced.
		_, err := svc.PostEntry(ctx, transfer("transfer", "ref-fx", "USD", usd, eur, 10))
		if !errors.Is(err, ErrInvalidEntry) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrInvalidEntry", err)
		}
		if len(q.entries) != 0 || len(q.lines) != linesBefore {
			t.Fatalf("cross-asset posting wrote rows: entries=%d lines added=%d", len(q.entries), len(q.lines)-linesBefore)
		}
	})

	t.Run("posting to inactive account is rejected", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		frozen := q.accounts[dst]
		frozen.Status = "frozen"
		q.accounts[dst] = frozen
		svc := NewService(store, quietLogger())

		_, err := svc.PostEntry(ctx, transfer("transfer", "ref-frozen", "USD", src, dst, 10))
		if !errors.Is(err, ErrInvalidEntry) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrInvalidEntry", err)
		}
		if len(q.entries) != 0 {
			t.Fatalf("posting to frozen account persisted an entry")
		}
	})

	t.Run("posting to unknown account is rejected", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		svc := NewService(store, quietLogger())

		ghost := uuid.New() // never seeded, so GetAccountsForUpdate omits it
		_, err := svc.PostEntry(ctx, transfer("transfer", "ref-ghost", "USD", src, ghost, 10))
		if !errors.Is(err, ErrInvalidEntry) {
			t.Fatalf("PostEntry() = %v, want errors.Is ErrInvalidEntry", err)
		}
		if len(q.entries) != 0 {
			t.Fatalf("posting to unknown account persisted an entry")
		}
	})

	t.Run("cancelled context propagates", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, quietLogger())

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := svc.PostEntry(cancelled, transfer("transfer", "ref-cancel", "USD", src, dst, 10))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PostEntry() = %v, want errors.Is context.Canceled", err)
		}
	})

	t.Run("nil logger falls back to default", func(t *testing.T) {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", 100)
		dst := q.seedAccount("wallet", "USD", 0)
		svc := NewService(store, nil)
		if _, err := svc.PostEntry(ctx, transfer("transfer", "ref-nil", "USD", src, dst, 5)); err != nil {
			t.Fatalf("PostEntry() = %v, want nil", err)
		}
	})
}
