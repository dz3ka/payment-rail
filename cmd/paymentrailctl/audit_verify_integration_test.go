package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/audit"
	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/google/uuid"
)

// TestAuditVerify_Integration is the headline end-to-end proof for the operator
// chain-integrity verifier (PRD F9). It runs the SAME code path runAuditVerify
// drives — a real ScanAuditChain over live Postgres feeding the pure audit.Verify —
// and exercises the four operator-facing claims: a valid chain verifies, the head
// hash anchors (and a wrong anchor is caught), an in-place edit to a historical row
// is detected as a hash_mismatch, and the append-only trigger blocks a plain UPDATE.
//
// It is robust to a NON-empty starting chain (earlier WP4 tests append rows), and it
// leaves the chain VALID at the end: the one row it tampers is a throwaway tail row
// it appended, which it removes (trigger-disabled) after asserting detection.
func TestAuditVerify_Integration(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// A final safety net: whatever this test does, make sure the append-only trigger
	// is re-enabled so it can never leave the shared dev DB unprotected.
	t.Cleanup(func() {
		_, _ = sqlDB.Exec(`ALTER TABLE audit_log ENABLE TRIGGER USER`)
	})

	// Append a few real rows so the chain has a non-trivial tail regardless of what
	// prior tests left behind. Each Append needs the chain advisory lock, which is
	// xact-scoped, so run each inside its own committed transaction.
	for i := 0; i < 3; i++ {
		appendAuditRow(ctx, t, sqlDB, "verify-it-"+uuid.NewString())
	}

	// --- Good-chain path: the exact composition runAuditVerify uses. ---
	rows, err := db.New(sqlDB).ScanAuditChain(ctx)
	if err != nil {
		t.Fatalf("ScanAuditChain: %v", err)
	}
	res, err := audit.Verify(rows)
	if err != nil {
		t.Fatalf("Verify(good chain) = %v, want nil", err)
	}
	if !res.OK {
		t.Fatalf("Verify(good chain).OK = false, want true")
	}
	if res.Count == 0 {
		t.Fatalf("Verify(good chain).Count = 0, want >0")
	}
	if len(res.HeadHash) == 0 {
		t.Fatalf("Verify(good chain).HeadHash is empty, want the head entry_hash")
	}

	// --- Head-anchor path: the head hash pins the tail. ---
	headHash := res.HeadHash
	if _, err := audit.Verify(rows, audit.WithExpectedHead(headHash)); err != nil {
		t.Fatalf("Verify(correct head anchor) = %v, want nil", err)
	}
	wrongHash := make([]byte, len(headHash))
	copy(wrongHash, headHash)
	wrongHash[0] ^= 0xFF // flip a bit so it can't equal the real head
	_, err = audit.Verify(rows, audit.WithExpectedHead(wrongHash))
	var anchorErr *audit.TamperError
	if !errors.As(err, &anchorErr) {
		t.Fatalf("Verify(wrong head anchor) error = %v, want *audit.TamperError", err)
	}
	if anchorErr.Kind != audit.KindHeadMismatch {
		t.Fatalf("Verify(wrong head anchor).Kind = %q, want %q", anchorErr.Kind, audit.KindHeadMismatch)
	}

	// --- Tamper-DETECTION path (the core claim). ---
	// Append a fresh throwaway tail row, edit it in place through a trigger-disable
	// (a normal UPDATE is blocked by WP1's append-only trigger), and assert Verify
	// catches the hash_mismatch. Then trigger-disable-delete that one row to restore
	// a valid chain, so the shared dev log is left intact for other tests / verify.
	tamperSeq := appendAuditRow(ctx, t, sqlDB, "tamper-target-"+uuid.NewString())

	if _, err := sqlDB.Exec(`ALTER TABLE audit_log DISABLE TRIGGER USER`); err != nil {
		t.Fatalf("disable trigger: %v", err)
	}
	if _, err := sqlDB.Exec(`UPDATE audit_log SET actor='tampered' WHERE seq=$1`, tamperSeq); err != nil {
		t.Fatalf("tamper UPDATE: %v", err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE audit_log ENABLE TRIGGER USER`); err != nil {
		t.Fatalf("re-enable trigger after tamper: %v", err)
	}

	tamperedRows, err := db.New(sqlDB).ScanAuditChain(ctx)
	if err != nil {
		t.Fatalf("ScanAuditChain(after tamper): %v", err)
	}
	_, err = audit.Verify(tamperedRows)
	var tamperErr *audit.TamperError
	if !errors.As(err, &tamperErr) {
		t.Fatalf("Verify(tampered chain) error = %v, want *audit.TamperError", err)
	}
	if tamperErr.Kind != audit.KindHashMismatch {
		t.Fatalf("Verify(tampered chain).Kind = %q, want %q", tamperErr.Kind, audit.KindHashMismatch)
	}
	if tamperErr.Seq != tamperSeq {
		t.Fatalf("Verify(tampered chain).Seq = %d, want %d", tamperErr.Seq, tamperSeq)
	}

	// Restore: trigger-disable-delete the tampered tail row so the chain is valid
	// again (it was the head, so removing it re-exposes a clean prefix).
	if _, err := sqlDB.Exec(`ALTER TABLE audit_log DISABLE TRIGGER USER`); err != nil {
		t.Fatalf("disable trigger for restore: %v", err)
	}
	if _, err := sqlDB.Exec(`DELETE FROM audit_log WHERE seq=$1`, tamperSeq); err != nil {
		t.Fatalf("delete tampered row: %v", err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE audit_log ENABLE TRIGGER USER`); err != nil {
		t.Fatalf("re-enable trigger after restore: %v", err)
	}

	// The chain must be VALID again after restore, so other tests / a real `audit
	// verify` run stay green.
	restored, err := db.New(sqlDB).ScanAuditChain(ctx)
	if err != nil {
		t.Fatalf("ScanAuditChain(after restore): %v", err)
	}
	if r, err := audit.Verify(restored); err != nil || !r.OK {
		t.Fatalf("Verify(restored chain) = (%+v, %v), want OK chain and nil error", r, err)
	}
}

// TestAuditVerify_AppendOnlyTriggerBlocksMutation proves WP1's append-only guard is
// live: a plain UPDATE/DELETE/TRUNCATE against audit_log is rejected by the trigger
// with an append-only message, so the chain cannot be edited through the normal
// write path the verifier defends.
func TestAuditVerify_AppendOnlyTriggerBlocksMutation(t *testing.T) {
	dsn := getTestDSN(t)
	ctx := context.Background()

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	for _, stmt := range []string{
		`UPDATE audit_log SET actor='hacked'`,
		`DELETE FROM audit_log`,
		`TRUNCATE audit_log`,
	} {
		_, err := sqlDB.Exec(stmt)
		if err == nil {
			t.Fatalf("%q succeeded, want append-only rejection", stmt)
		}
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "append-only") && !strings.Contains(msg, "append only") {
			t.Fatalf("%q error = %v, want it to mention append-only", stmt, err)
		}
	}
}

// appendAuditRow commits one audit.Append inside its own transaction (the chain
// advisory lock is xact-scoped) and returns the seq assigned to the new head.
func appendAuditRow(ctx context.Context, t *testing.T, sqlDB *sql.DB, actor string) int64 {
	t.Helper()
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := audit.Append(ctx, db.New(tx), audit.Entry{
		Actor:         actor,
		Action:        "audit.verify.test",
		AggregateType: "test",
		AggregateID:   uuid.NewString(),
		Data:          map[string]string{"note": "verify integration"},
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("audit.Append: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit append: %v", err)
	}

	head, err := db.New(sqlDB).GetAuditHead(ctx)
	if err != nil {
		t.Fatalf("GetAuditHead: %v", err)
	}
	return head.Seq
}

// getTestDSN gates every case on a live-Postgres DSN, matching the repo's other
// integration tests.
func getTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PAYMENT_RAIL_TEST_DSN")
	if dsn == "" {
		t.Skip("set PAYMENT_RAIL_TEST_DSN to run the audit-verify integration test")
	}
	return dsn
}
