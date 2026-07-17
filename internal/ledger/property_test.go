package ledger

import (
	"context"
	"testing"
	"testing/quick"

	"github.com/google/uuid"
)

// Property (a): a posting of equal-and-opposite amounts between two accounts is
// always balanced, for any positive amount.
func TestProperty_BalancedForEqualOpposite(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	f := func(amount int64) bool {
		if amount <= 0 {
			return true // Balanced legitimately rejects non-positive amounts.
		}
		lines := []Line{
			{AccountID: a, Direction: Debit, Amount: amount},
			{AccountID: b, Direction: Credit, Amount: amount},
		}
		return Balanced(lines) == nil
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

// Property (b): whenever ApplyToBalances succeeds, the returned map never holds
// a negative balance.
func TestProperty_ApplyNeverNegativeOnSuccess(t *testing.T) {
	pool := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	f := func(raw []int64) bool {
		lines := make([]Line, 0, len(raw))
		for i, v := range raw {
			acc := pool[i%len(pool)]
			dir := Credit
			amt := v
			if amt < 0 {
				dir = Debit
				amt = -amt
			}
			if amt == 0 {
				amt = 1
			}
			lines = append(lines, Line{AccountID: acc, Direction: dir, Amount: amt})
		}
		next, err := ApplyToBalances(map[uuid.UUID]int64{}, lines)
		if err != nil {
			return true // the negative case is reported as an error, not a map.
		}
		for _, bal := range next {
			if bal < 0 {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

// Property (c): conservation of value. Posting a balanced entry over balances
// that stay non-negative leaves the sum of all balances unchanged.
func TestProperty_PostEntryConservesValue(t *testing.T) {
	const opening = int64(1) << 40 // dwarfs any uint32 amount, so no overdraw.
	f := func(amount uint32) bool {
		store, q := newFake()
		src := q.seedAccount("wallet", "USD", opening)
		dst := q.seedAccount("wallet", "USD", opening)
		svc := NewService(store, quietLogger())

		before := q.totalBalance()
		e := transfer("transfer", uuid.NewString(), "USD", src, dst, int64(amount)+1)
		if _, err := svc.PostEntry(context.Background(), e); err != nil {
			return false
		}
		return q.totalBalance() == before
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}
