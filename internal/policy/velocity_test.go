package policy

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"
)

// fakeEvent is one recorded spend, kept in memory by fakeVelocityStore so tests can
// exercise window expiry without a database.
type fakeEvent struct {
	amount *big.Int
	at     time.Time
}

// fakeVelocityStore is a hermetic VelocityStore: it computes Usage from an in-memory
// slice filtered by [now-window, now], calls decide, and records the event iff decide
// returns nil — mirroring the atomic per-key contract of the real Postgres store.
// opErr, when set, is returned before decide is ever consulted, standing in for an
// operational (non-sentinel) store failure.
type fakeVelocityStore struct {
	events  []fakeEvent
	opErr   error
	charges int // number of Charge invocations, to assert the store is/ isn't consulted
}

func (s *fakeVelocityStore) Charge(_ context.Context, _ string, amount *big.Int,
	window time.Duration, now time.Time, decide func(Usage) error) error {
	s.charges++
	if s.opErr != nil {
		return s.opErr
	}

	cutoff := now.Add(-window)
	usage := Usage{Sum: new(big.Int)}
	for _, e := range s.events {
		// In window when at is within (cutoff, now]; strictly-before events expire.
		if e.at.After(cutoff) && !e.at.After(now) {
			usage.Count++
			usage.Sum.Add(usage.Sum, e.amount)
		}
	}

	if err := decide(usage); err != nil {
		return err // no event recorded on a denied/failed decide
	}
	s.events = append(s.events, fakeEvent{amount: new(big.Int).Set(amount), at: now})
	return nil
}

func TestVelocityCapsEnabled(t *testing.T) {
	tests := []struct {
		name string
		caps VelocityCaps
		want bool
	}{
		{name: "zero value disabled", caps: VelocityCaps{}, want: false},
		{name: "window but no caps disabled", caps: VelocityCaps{Window: time.Minute}, want: false},
		{name: "count cap needs window", caps: VelocityCaps{MaxCount: 3}, want: false},
		{name: "window plus count enabled", caps: VelocityCaps{Window: time.Minute, MaxCount: 3}, want: true},
		{name: "window plus amount enabled", caps: VelocityCaps{Window: time.Minute, MaxAmount: big.NewInt(10)}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.caps.Enabled(); got != tc.want {
				t.Errorf("Enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestChargeDisabledNoOp(t *testing.T) {
	// A nil receiver is a valid disabled limiter and must not panic or dial.
	t.Run("nil limiter", func(t *testing.T) {
		var lim *VelocityLimiter
		if err := lim.Charge(context.Background(), "k", big.NewInt(5)); err != nil {
			t.Fatalf("Charge on nil limiter = %v, want nil", err)
		}
	})

	// Disabled caps must not consult the store at all (no round-trip on the hot path).
	tests := []struct {
		name string
		caps VelocityCaps
	}{
		{name: "zero-value caps", caps: VelocityCaps{}},
		{name: "positive window no caps", caps: VelocityCaps{Window: time.Minute}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeVelocityStore{}
			lim := NewVelocityLimiter(store, tc.caps)
			if err := lim.Charge(context.Background(), "k", big.NewInt(5)); err != nil {
				t.Fatalf("Charge on disabled limiter = %v, want nil", err)
			}
			if store.charges != 0 {
				t.Errorf("store consulted %d times on disabled limiter, want 0", store.charges)
			}
		})
	}
}

func TestChargeCountCap(t *testing.T) {
	store := &fakeVelocityStore{}
	lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 2})
	ctx := context.Background()

	// 1st and 2nd charges are within the count cap of 2.
	if err := lim.Charge(ctx, "k", big.NewInt(1)); err != nil {
		t.Fatalf("charge 1 = %v, want nil", err)
	}
	if err := lim.Charge(ctx, "k", big.NewInt(1)); err != nil {
		t.Fatalf("charge 2 = %v, want nil", err)
	}
	// 3rd charge would make count 3 > cap 2: denied.
	err := lim.Charge(ctx, "k", big.NewInt(1))
	if !errors.Is(err, ErrVelocityExceeded) {
		t.Fatalf("charge 3 = %v, want ErrVelocityExceeded", err)
	}
	// The denied charge recorded no event: still exactly 2 in memory.
	if len(store.events) != 2 {
		t.Errorf("recorded %d events after a denial, want 2 (no insert on deny)", len(store.events))
	}
}

func TestChargeAmountCap(t *testing.T) {
	store := &fakeVelocityStore{}
	lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxAmount: big.NewInt(100)})
	ctx := context.Background()

	if err := lim.Charge(ctx, "k", big.NewInt(60)); err != nil {
		t.Fatalf("charge 60 = %v, want nil", err)
	}
	// 60 + 60 = 120 > cap 100: denied, no event recorded.
	err := lim.Charge(ctx, "k", big.NewInt(60))
	if !errors.Is(err, ErrVelocityExceeded) {
		t.Fatalf("charge over amount cap = %v, want ErrVelocityExceeded", err)
	}
	if len(store.events) != 1 {
		t.Errorf("recorded %d events, want 1", len(store.events))
	}
	// A charge exactly reaching the cap (60 + 40 = 100) is allowed (> is the breach).
	if err := lim.Charge(ctx, "k", big.NewInt(40)); err != nil {
		t.Fatalf("charge to exactly cap = %v, want nil", err)
	}
	if len(store.events) != 2 {
		t.Errorf("recorded %d events, want 2", len(store.events))
	}
}

func TestChargeCombinedCaps(t *testing.T) {
	// Amount is roomy but the count cap bites.
	t.Run("count breached amount ok", func(t *testing.T) {
		store := &fakeVelocityStore{}
		lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 1, MaxAmount: big.NewInt(1000)})
		if err := lim.Charge(context.Background(), "k", big.NewInt(1)); err != nil {
			t.Fatalf("charge 1 = %v", err)
		}
		if err := lim.Charge(context.Background(), "k", big.NewInt(1)); !errors.Is(err, ErrVelocityExceeded) {
			t.Fatalf("charge 2 = %v, want ErrVelocityExceeded", err)
		}
	})
	// Count is roomy but the amount cap bites.
	t.Run("amount breached count ok", func(t *testing.T) {
		store := &fakeVelocityStore{}
		lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 100, MaxAmount: big.NewInt(50)})
		if err := lim.Charge(context.Background(), "k", big.NewInt(51)); !errors.Is(err, ErrVelocityExceeded) {
			t.Fatalf("charge 51 = %v, want ErrVelocityExceeded", err)
		}
	})
}

func TestChargeStaleEventsExpire(t *testing.T) {
	// Seed an old event outside the window, then a fresh one; only the fresh one counts.
	store := &fakeVelocityStore{events: []fakeEvent{
		{amount: big.NewInt(90), at: time.Now().Add(-2 * time.Hour)},
	}}
	lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 1, MaxAmount: big.NewInt(100)})

	// The stale 90 is outside the 1-minute window, so a fresh 90 sees count 0, sum 0.
	if err := lim.Charge(context.Background(), "k", big.NewInt(90)); err != nil {
		t.Fatalf("charge with only-stale history = %v, want nil", err)
	}
}

func TestChargeOperationalErrorPropagates(t *testing.T) {
	opErr := errors.New("store: connection refused")
	store := &fakeVelocityStore{opErr: opErr}
	lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 1})

	err := lim.Charge(context.Background(), "k", big.NewInt(1))
	if !errors.Is(err, opErr) {
		t.Fatalf("Charge = %v, want the store's operational error", err)
	}
	// Fail-closed but distinguishable: an operational error is NOT the sentinel.
	if errors.Is(err, ErrVelocityExceeded) {
		t.Errorf("operational error must not be ErrVelocityExceeded: %v", err)
	}
}

func TestChargeBadAmount(t *testing.T) {
	tests := []struct {
		name   string
		amount *big.Int
	}{
		{name: "nil amount", amount: nil},
		{name: "zero amount", amount: big.NewInt(0)},
		{name: "negative amount", amount: big.NewInt(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeVelocityStore{}
			lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 1})
			err := lim.Charge(context.Background(), "k", tc.amount)
			if err == nil {
				t.Fatal("Charge with bad amount = nil, want operational error")
			}
			// Fail-closed operational error, not the sentinel.
			if errors.Is(err, ErrVelocityExceeded) {
				t.Errorf("bad-amount error must not be ErrVelocityExceeded: %v", err)
			}
			if store.charges != 0 {
				t.Errorf("store consulted %d times on bad amount, want 0", store.charges)
			}
		})
	}
}

func TestChargeAllowRecordsExactlyOneEvent(t *testing.T) {
	store := &fakeVelocityStore{}
	lim := NewVelocityLimiter(store, VelocityCaps{Window: time.Minute, MaxCount: 10, MaxAmount: big.NewInt(1000)})
	if err := lim.Charge(context.Background(), "k", big.NewInt(7)); err != nil {
		t.Fatalf("Charge = %v, want nil", err)
	}
	if len(store.events) != 1 {
		t.Fatalf("recorded %d events on a single allow, want exactly 1", len(store.events))
	}
	if store.events[0].amount.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("recorded amount = %s, want 7", store.events[0].amount)
	}
}
