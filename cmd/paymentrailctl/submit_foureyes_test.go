package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/dz3ka/payment-rail/internal/policy"
)

// These tests prove the four-eyes submit gating branch (PRD F8c) that decides
// park-vs-broadcast. They are fully hermetic (no Postgres, no chain/signer dial):
// every case asserts a reject/fail-fast branch that returns BEFORE the park path
// opens its Postgres pool, so no *_TEST_DSN gate is needed. Each drives runSubmit
// with a valid --to/--amount and the four-eyes env via t.Setenv (auto-cleaned);
// PAYMENT_RAIL_POLICY_DENYLIST is left empty so screening is disabled and the
// first decision reached is the gate.
//
// The successful park path is deliberately NOT exercised here — it needs a live
// Postgres and is covered by the integration test + e2e.

// foureyesRecipient is an arbitrary well-formed destination that clears the
// IsHexAddress guard, so the only thing that can fail is the gate logic.
const foureyesRecipient = "0x00000000000000000000000000000000000000AB"

// TestSubmit_FourEyes_MissingProposer proves an at/above-threshold payment with
// no --proposer is rejected at the gate — before any Postgres pool is opened —
// with an error naming the required proposer. The threshold (1000) is set with a
// non-empty approver allowlist so buildApprovalGate succeeds and gate.Required
// fires on the 1500 amount.
func TestSubmit_FourEyes_MissingProposer(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", "")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD", "1000")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVERS", "alice,bob")

	err := runSubmit([]string{"--to", foureyesRecipient, "--amount", "1500"})
	if err == nil {
		t.Fatal("runSubmit() = nil, want an error for an at-threshold payment with no --proposer")
	}
	if !strings.Contains(err.Error(), "proposer is required") {
		t.Fatalf("runSubmit() error = %v, want it to report a required proposer", err)
	}
}

// TestSubmit_FourEyes_ProposerNotAllowlisted proves an at/above-threshold payment
// whose --proposer is not in PAYMENT_RAIL_POLICY_APPROVERS is rejected at the gate
// (a parked payment must never be one no valid pair of eyes could clear), again
// before any Postgres dial.
func TestSubmit_FourEyes_ProposerNotAllowlisted(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", "")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD", "1000")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVERS", "alice,bob")

	err := runSubmit([]string{
		"--to", foureyesRecipient,
		"--amount", "1500",
		"--proposer", "mallory", // not in {alice,bob}
	})
	if err == nil {
		t.Fatal("runSubmit() = nil, want an error for a proposer outside the allowlist")
	}
	if !strings.Contains(err.Error(), "not in the approver allowlist") {
		t.Fatalf("runSubmit() error = %v, want it to report the proposer is not allowlisted", err)
	}
}

// TestSubmit_FourEyes_ThresholdSetButNoApprovers proves the buildApprovalGate
// coherence fail-fast surfaces from runSubmit: a threshold set with an EMPTY
// approver allowlist would make every gated payment un-approvable, so the command
// aborts (fail closed) before reaching the park path. This asserts the config
// incoherence is caught, not a downstream DB/chain failure.
func TestSubmit_FourEyes_ThresholdSetButNoApprovers(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_POLICY_DENYLIST", "")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVAL_THRESHOLD", "1000")
	t.Setenv("PAYMENT_RAIL_POLICY_APPROVERS", "")

	err := runSubmit([]string{"--to", foureyesRecipient, "--amount", "1500"})
	if err == nil {
		t.Fatal("runSubmit() = nil, want the buildApprovalGate coherence fail-fast")
	}
	// A denial would be ErrDenied; this is a config incoherence, not a screen result.
	if errors.Is(err, policy.ErrDenied) {
		t.Fatalf("runSubmit() error = %v, want a config error, not ErrDenied", err)
	}
	if !strings.Contains(err.Error(), "PAYMENT_RAIL_POLICY_APPROVERS is empty") {
		t.Fatalf("runSubmit() error = %v, want it to report the empty approver allowlist", err)
	}
}
