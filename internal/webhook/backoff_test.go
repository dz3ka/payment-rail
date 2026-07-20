package webhook

import (
	"testing"
	"time"
)

func TestBackoffDoubling(t *testing.T) {
	cases := []struct {
		attempts int32
		want     time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
	}
	for _, c := range cases {
		if got := backoff(c.attempts); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.attempts, got, c.want)
		}
	}
}

func TestBackoffClampsAtCap(t *testing.T) {
	// 2s * 2^(attempts-1) exceeds 1h once attempts is large; must clamp exactly.
	for _, attempts := range []int32{12, 20, 100} {
		if got := backoff(attempts); got != backoffCap {
			t.Errorf("backoff(%d) = %v, want cap %v", attempts, got, backoffCap)
		}
	}
}

func TestBackoffNoOverflowAtLargeAttempts(t *testing.T) {
	// A naive base<<(attempts-1) would overflow int64 here; the loop must return
	// the cap without wrapping to a negative or tiny duration.
	if got := backoff(1_000_000); got != backoffCap {
		t.Fatalf("backoff(1e6) = %v, want cap %v (overflow?)", got, backoffCap)
	}
}
