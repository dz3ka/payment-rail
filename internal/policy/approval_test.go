package policy

import (
	"errors"
	"math/big"
	"testing"
)

func TestApprovalGateRequired(t *testing.T) {
	tests := []struct {
		name      string
		threshold *big.Int
		amount    *big.Int
		want      bool
	}{
		{name: "disabled nil threshold", threshold: nil, amount: big.NewInt(1_000_000), want: false},
		{name: "disabled zero threshold", threshold: big.NewInt(0), amount: big.NewInt(1_000_000), want: false},
		{name: "disabled negative threshold", threshold: big.NewInt(-1), amount: big.NewInt(1_000_000), want: false},
		{name: "below threshold", threshold: big.NewInt(100), amount: big.NewInt(99), want: false},
		{name: "exactly at threshold gated", threshold: big.NewInt(100), amount: big.NewInt(100), want: true},
		{name: "above threshold", threshold: big.NewInt(100), amount: big.NewInt(101), want: true},
		{name: "nil amount not required", threshold: big.NewInt(100), amount: nil, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := NewApprovalGate(tc.threshold, nil)
			if got := g.Required(tc.amount); got != tc.want {
				t.Errorf("Required(%v) = %v, want %v", tc.amount, got, tc.want)
			}
		})
	}

	// A nil receiver is a valid disabled gate and must not panic.
	t.Run("nil gate disabled", func(t *testing.T) {
		var g *ApprovalGate
		if g.Required(big.NewInt(1_000_000)) {
			t.Error("Required on nil gate = true, want false")
		}
	})
}

func TestApprovalGateKnownApprover(t *testing.T) {
	g := NewApprovalGate(big.NewInt(100), []string{"alice", "bob"})

	t.Run("member", func(t *testing.T) {
		if !g.KnownApprover("alice") {
			t.Error("KnownApprover(alice) = false, want true")
		}
	})
	t.Run("non member", func(t *testing.T) {
		if g.KnownApprover("mallory") {
			t.Error("KnownApprover(mallory) = true, want false")
		}
	})
	t.Run("empty allowlist knows no one", func(t *testing.T) {
		empty := NewApprovalGate(big.NewInt(100), nil)
		if empty.KnownApprover("alice") {
			t.Error("KnownApprover on empty allowlist = true, want false")
		}
	})
	t.Run("nil gate knows no one", func(t *testing.T) {
		var nilGate *ApprovalGate
		if nilGate.KnownApprover("alice") {
			t.Error("KnownApprover on nil gate = true, want false")
		}
	})
}

func TestApprovalGateAuthorize(t *testing.T) {
	g := NewApprovalGate(big.NewInt(100), []string{"alice", "bob"})

	tests := []struct {
		name     string
		proposer string
		approver string
		wantErr  error // nil => allowed
	}{
		{name: "distinct known approver allowed", proposer: "alice", approver: "bob", wantErr: nil},
		{name: "self approval both known", proposer: "alice", approver: "alice", wantErr: ErrSelfApproval},
		{name: "unknown approver", proposer: "alice", approver: "mallory", wantErr: ErrUnknownApprover},
		// Edge: unknown AND self — unknown-check runs first, so ErrUnknownApprover wins.
		{name: "unknown and self precedence", proposer: "mallory", approver: "mallory", wantErr: ErrUnknownApprover},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := g.Authorize(tc.proposer, tc.approver)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Authorize(%q, %q) = %v, want nil", tc.proposer, tc.approver, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Authorize(%q, %q) = %v, want %v", tc.proposer, tc.approver, err, tc.wantErr)
			}
		})
	}
}
