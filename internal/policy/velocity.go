package policy

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// ErrVelocityExceeded is returned (wrapped) when a charge would breach a configured
// velocity cap. Callers distinguish it from operational failures with errors.Is and
// fail closed on either. Mirrors ErrDenied.
var ErrVelocityExceeded = errors.New("policy: velocity limit exceeded")

// VelocityCaps configures a sliding-window velocity limit for a signing key (PRD
// F8b). The zero value is disabled. Window must be > 0 to enable enforcement;
// each individual cap is optional (MaxCount 0 / MaxAmount nil = unlimited).
type VelocityCaps struct {
	Window    time.Duration // rolling window width; <=0 disables the limiter
	MaxCount  uint64        // max transfers in the window; 0 = unlimited
	MaxAmount *big.Int      // max summed amount in the window; nil = unlimited
}

// Enabled reports whether the caps enable enforcement: the window is positive AND
// at least one cap is set. A positive window with no caps enforces nothing, so it
// stays disabled (no store round-trip on the hot path).
func (c VelocityCaps) Enabled() bool {
	return c.Window > 0 && (c.MaxCount > 0 || c.MaxAmount != nil)
}

// Usage is the in-window consumption a VelocityStore reports back to the decide
// callback, observed under the store's per-key lock, BEFORE the pending charge is
// counted.
type Usage struct {
	Count uint64
	Sum   *big.Int // never nil; zero when no events in window
}

// VelocityStore persists and evaluates per-key spend events. Charge MUST, atomically
// for keyID: acquire a per-key lock, compute the in-window Usage over [now-window, now],
// invoke decide(usage), and ONLY if decide returns nil, record one event(amount, now)
// before committing. If decide returns a non-nil error, no event is recorded and that
// error propagates out of Charge unchanged (so ErrVelocityExceeded survives errors.Is).
// The concrete Postgres impl lives in the composition root; this package stays db-free.
type VelocityStore interface {
	Charge(ctx context.Context, keyID string, amount *big.Int, window time.Duration,
		now time.Time, decide func(Usage) error) error
}

// VelocityLimiter enforces VelocityCaps against a VelocityStore. Construct it with
// NewVelocityLimiter; a nil *VelocityLimiter is a valid disabled limiter.
type VelocityLimiter struct {
	store VelocityStore
	caps  VelocityCaps
}

// NewVelocityLimiter binds a store to a set of caps. Passing disabled caps yields a
// limiter whose Charge is a no-op; the store is never consulted in that case.
func NewVelocityLimiter(store VelocityStore, caps VelocityCaps) *VelocityLimiter {
	return &VelocityLimiter{store: store, caps: caps}
}

// Charge enforces the caps for a single pending transfer of amount by keyID at the
// current time. It delegates to the store, supplying a decide closure that returns a
// %w-wrapped ErrVelocityExceeded if adding this transfer would breach MaxCount or
// MaxAmount; on a breach no event is recorded (the store rolls back on the returned
// error). A disabled limiter (or a nil receiver) is a no-op returning nil — no store
// call, no dial. amount must be > 0; a nil or non-positive amount is a fail-closed
// operational error (NOT the sentinel) so callers still block but can tell it apart.
func (l *VelocityLimiter) Charge(ctx context.Context, keyID string, amount *big.Int) error {
	if l == nil || !l.caps.Enabled() {
		return nil
	}
	if amount == nil || amount.Sign() <= 0 {
		return fmt.Errorf("policy: velocity charge for %q requires amount > 0", keyID)
	}

	now := time.Now()
	decide := func(usage Usage) error {
		// usage.Count+1 could in principle overflow uint64, but a real key would
		// need ~1.8e19 in-window transfers to reach that; not a practical concern.
		if l.caps.MaxCount > 0 && usage.Count+1 > l.caps.MaxCount {
			return fmt.Errorf("velocity count %d+1 > cap %d: %w", usage.Count, l.caps.MaxCount, ErrVelocityExceeded)
		}
		if l.caps.MaxAmount != nil {
			// Log hygiene: the message never carries the amount or running sum,
			// only the fact of the breach. Amounts are never logged.
			total := new(big.Int).Add(usage.Sum, amount)
			if total.Cmp(l.caps.MaxAmount) > 0 {
				return fmt.Errorf("velocity amount over cap: %w", ErrVelocityExceeded)
			}
		}
		return nil
	}

	return l.store.Charge(ctx, keyID, amount, l.caps.Window, now, decide)
}
