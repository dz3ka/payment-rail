package webhook

import "time"

// backoff returns the delay before the next attempt after `attempts` failures
// (attempts >= 1): min(backoffBase * 2^(attempts-1), backoffCap).
//
// The doubling is done in a loop that clamps to backoffCap the instant it is
// reached, so the running value can never overflow int64: backoffCap (1h) is far
// below math.MaxInt64 nanoseconds, and we return before doubling past it. This
// keeps every in-range value exact while making arbitrarily large attempt counts
// safe.
func backoff(attempts int32) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := backoffBase
	for i := int32(1); i < attempts; i++ {
		d *= 2
		if d >= backoffCap {
			return backoffCap
		}
	}
	return d
}
