package audit

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dz3ka/payment-rail/internal/db"
)

// buildChain appends n well-formed entries and returns the resulting rows. Every
// tamper test starts from a chain Verify accepts, then corrupts one thing.
func buildChain(t *testing.T, n int) []db.AuditLog {
	t.Helper()
	f := &fakeChain{}
	for i := 0; i < n; i++ {
		e := sampleEntry()
		e.AggregateID = "pay_" + string(rune('a'+i))
		if err := Append(context.Background(), f, e); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	rows, _ := f.ScanAuditChain(context.Background())
	if _, err := Verify(rows); err != nil {
		t.Fatalf("freshly built chain failed Verify: %v", err)
	}
	return rows
}

// clone deep-copies the slice so a mutation in one test can't leak into another.
func clone(rows []db.AuditLog) []db.AuditLog {
	out := make([]db.AuditLog, len(rows))
	copy(out, rows)
	for i := range out {
		out[i].PrevHash = bytes.Clone(rows[i].PrevHash)
		out[i].EntryHash = bytes.Clone(rows[i].EntryHash)
		out[i].Payload = bytes.Clone(rows[i].Payload)
	}
	return out
}

// assertTamper asserts Verify returned a *TamperError of exactly wantKind.
func assertTamper(t *testing.T, res Result, err error, wantKind string) {
	t.Helper()
	if res.OK {
		t.Errorf("Result.OK = true, want false on tamper")
	}
	var te *TamperError
	if !errors.As(err, &te) {
		t.Fatalf("error = %v, want *TamperError", err)
	}
	if te.Kind != wantKind {
		t.Fatalf("tamper Kind = %q, want %q", te.Kind, wantKind)
	}
}

func TestVerifyEmptyChainOK(t *testing.T) {
	res, err := Verify(nil)
	if err != nil {
		t.Fatalf("Verify(nil): %v", err)
	}
	if !res.OK || res.Count != 0 {
		t.Errorf("Verify(nil) = %+v, want OK count=0", res)
	}
}

func TestVerifyDetectsTamper(t *testing.T) {
	t.Run("interior row deleted is gap", func(t *testing.T) {
		rows := clone(buildChain(t, 4))
		tampered := append(rows[:1:1], rows[2:]...) // drop seq 2
		res, err := Verify(tampered)
		assertTamper(t, res, err, KindGap)
	})

	t.Run("rows reordered", func(t *testing.T) {
		rows := clone(buildChain(t, 4))
		rows[1], rows[2] = rows[2], rows[1] // swap seq 2 and 3
		res, err := Verify(rows)
		// Swapped seqs surface as a gap (seq 3 where 2 was expected).
		var te *TamperError
		if !errors.As(err, &te) || (te.Kind != KindGap && te.Kind != KindBrokenLink) {
			t.Fatalf("reorder err = %v, want gap or broken_link", err)
		}
		if res.OK {
			t.Error("reordered chain reported OK")
		}
	})

	t.Run("prev_hash relinked is broken_link", func(t *testing.T) {
		rows := clone(buildChain(t, 3))
		rows[2].PrevHash[0] ^= 0xff // break the link into row 3
		res, err := Verify(rows)
		assertTamper(t, res, err, KindBrokenLink)
	})

	t.Run("forged payload byte is hash_mismatch", func(t *testing.T) {
		rows := clone(buildChain(t, 3))
		rows[1].Payload[0] ^= 0xff
		res, err := Verify(rows)
		assertTamper(t, res, err, KindHashMismatch)
	})

	t.Run("forged actor column is hash_mismatch", func(t *testing.T) {
		rows := clone(buildChain(t, 3))
		rows[1].Actor = "mallory"
		res, err := Verify(rows)
		assertTamper(t, res, err, KindHashMismatch)
	})

	t.Run("first row prev not genesis is broken_link", func(t *testing.T) {
		rows := clone(buildChain(t, 3))
		rows[0].PrevHash[0] ^= 0xff // row 1 no longer anchored to genesis
		res, err := Verify(rows)
		assertTamper(t, res, err, KindBrokenLink)
	})

	t.Run("first row seq not 1 is broken_link", func(t *testing.T) {
		rows := clone(buildChain(t, 3))
		trimmed := rows[1:] // now starts at seq 2
		res, err := Verify(trimmed)
		assertTamper(t, res, err, KindBrokenLink)
	})
}

func TestVerifyAnchor(t *testing.T) {
	full := clone(buildChain(t, 5))
	fullHead := full[len(full)-1].EntryHash

	t.Run("tail truncated no opt is OK", func(t *testing.T) {
		truncated := clone(full)[:3]
		res, err := Verify(truncated)
		if err != nil || !res.OK {
			t.Fatalf("truncated chain without anchor: res=%+v err=%v", res, err)
		}
	})

	t.Run("tail truncated with expected head is head_mismatch", func(t *testing.T) {
		truncated := clone(full)[:3]
		res, err := Verify(truncated, WithExpectedHead(fullHead))
		assertTamper(t, res, err, KindHeadMismatch)
	})

	t.Run("re-genesis shorter self-consistent chain with expected head is head_mismatch", func(t *testing.T) {
		// A completely rebuilt shorter chain: internally valid, but its head differs
		// from the anchor an operator recorded, so truncation-by-rebuild is caught.
		rebuilt := buildChain(t, 2)
		res, err := Verify(rebuilt, WithExpectedHead(fullHead))
		assertTamper(t, res, err, KindHeadMismatch)
	})

	t.Run("true head anchors clean", func(t *testing.T) {
		res, err := Verify(clone(full), WithExpectedHead(fullHead))
		if err != nil || !res.OK {
			t.Fatalf("anchor with true head: res=%+v err=%v", res, err)
		}
	})

	t.Run("empty chain with expected head is head_mismatch", func(t *testing.T) {
		res, err := Verify(nil, WithExpectedHead(fullHead))
		assertTamper(t, res, err, KindHeadMismatch)
	})
}
