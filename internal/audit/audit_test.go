package audit

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
)

// fakeChain is an in-memory, append-only model of the audit_log table. Only the
// four audit methods carry real behavior; every other Querier method is inherited
// from the embedded nil interface and panics if called, which is the signal that a
// test exercised a path it shouldn't. It mirrors the DB contract exactly: the lock
// is a no-op, GetAuditHead returns the tail or sql.ErrNoRows on an empty chain,
// InsertAuditEntry appends, and ScanAuditChain returns the rows seq-ascending.
type fakeChain struct {
	db.Querier
	rows []db.AuditLog
}

func (f *fakeChain) AcquireAuditChainLock(context.Context, int64) error { return nil }

func (f *fakeChain) GetAuditHead(context.Context) (db.GetAuditHeadRow, error) {
	if len(f.rows) == 0 {
		return db.GetAuditHeadRow{}, sql.ErrNoRows
	}
	last := f.rows[len(f.rows)-1]
	return db.GetAuditHeadRow{Seq: last.Seq, EntryHash: last.EntryHash}, nil
}

func (f *fakeChain) InsertAuditEntry(_ context.Context, p db.InsertAuditEntryParams) error {
	f.rows = append(f.rows, db.AuditLog(p))
	return nil
}

func (f *fakeChain) ScanAuditChain(context.Context) ([]db.AuditLog, error) {
	return f.rows, nil
}

// sampleEntry is a fully-populated entry with a fixed timestamp for deterministic
// hashing across tests.
func sampleEntry() Entry {
	return Entry{
		Actor:         "alice",
		Action:        "payment.submitted",
		AggregateType: "payment",
		AggregateID:   "pay_123",
		OccurredAt:    time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC),
		Data:          map[string]any{"amount": 100, "asset": "USDC"},
	}
}

func TestCanonicalHashDeterminism(t *testing.T) {
	base := func() []byte {
		return canonical(1, "alice", "submit", "payment", "pay_1", 1_700_000_000_000_000, []byte(`{"a":1}`))
	}
	h := func(b []byte) []byte { return entryHash(genesisPrevHash, b) }

	// Identical inputs must hash identically.
	if !bytes.Equal(h(base()), h(base())) {
		t.Fatal("same inputs produced different hashes")
	}

	// Each single-field mutation must change the hash — the canonical form binds
	// every field into the preimage.
	mutations := map[string][]byte{
		"seq":            canonical(2, "alice", "submit", "payment", "pay_1", 1_700_000_000_000_000, []byte(`{"a":1}`)),
		"actor":          canonical(1, "bob", "submit", "payment", "pay_1", 1_700_000_000_000_000, []byte(`{"a":1}`)),
		"action":         canonical(1, "alice", "cancel", "payment", "pay_1", 1_700_000_000_000_000, []byte(`{"a":1}`)),
		"aggregate_type": canonical(1, "alice", "submit", "settlement", "pay_1", 1_700_000_000_000_000, []byte(`{"a":1}`)),
		"aggregate_id":   canonical(1, "alice", "submit", "payment", "pay_2", 1_700_000_000_000_000, []byte(`{"a":1}`)),
		"occurredMicros": canonical(1, "alice", "submit", "payment", "pay_1", 1_700_000_000_000_001, []byte(`{"a":1}`)),
		"payload":        canonical(1, "alice", "submit", "payment", "pay_1", 1_700_000_000_000_000, []byte(`{"a":2}`)),
	}
	want := h(base())
	for name, mutated := range mutations {
		if bytes.Equal(want, h(mutated)) {
			t.Errorf("mutating %s did not change the hash", name)
		}
	}

	// prev_hash is fed into entryHash directly; changing it must change the result.
	other := make([]byte, 32)
	other[0] = 0xff
	if bytes.Equal(entryHash(genesisPrevHash, base()), entryHash(other, base())) {
		t.Error("changing prev_hash did not change the entry hash")
	}
}

func TestCanonicalLengthPrefixCollisionResistance(t *testing.T) {
	// Without length framing these would concatenate to the same bytes.
	a := canonical(1, "ab", "c", "payment", "id", 0, nil)
	b := canonical(1, "a", "bc", "payment", "id", 0, nil)
	if bytes.Equal(a, b) {
		t.Fatal("length-prefix framing failed: (ab,c) and (a,bc) collided")
	}
	if bytes.Equal(entryHash(genesisPrevHash, a), entryHash(genesisPrevHash, b)) {
		t.Error("hashes of framed (ab,c) and (a,bc) collided")
	}
}

func TestAppendGenesis(t *testing.T) {
	f := &fakeChain{}
	if err := Append(context.Background(), f, sampleEntry()); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(f.rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(f.rows))
	}
	row := f.rows[0]
	if row.Seq != 1 {
		t.Errorf("first seq = %d, want 1", row.Seq)
	}
	if !bytes.Equal(row.PrevHash, genesisPrevHash) {
		t.Errorf("first prev_hash = %x, want 32 zero bytes", row.PrevHash)
	}
	if len(row.EntryHash) != 32 {
		t.Errorf("entry_hash len = %d, want 32", len(row.EntryHash))
	}
}

func TestAppendZeroTimeStamped(t *testing.T) {
	f := &fakeChain{}
	e := sampleEntry()
	e.OccurredAt = time.Time{} // zero => Append stamps now, truncated to micros
	if err := Append(context.Background(), f, e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got := f.rows[0].OccurredAt
	if got.IsZero() {
		t.Fatal("zero OccurredAt was not stamped")
	}
	if !got.Equal(got.Truncate(time.Microsecond)) {
		t.Errorf("stamped time %v not truncated to microseconds", got)
	}
}

func TestAppendRoundTripVerifies(t *testing.T) {
	f := &fakeChain{}
	const n = 5
	for i := 0; i < n; i++ {
		e := sampleEntry()
		e.AggregateID = "pay_" + string(rune('a'+i))
		if err := Append(context.Background(), f, e); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	rows, _ := f.ScanAuditChain(context.Background())
	res, err := Verify(rows)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !res.OK || res.Count != n || res.HeadSeq != n {
		t.Errorf("Verify = %+v, want OK count=%d headSeq=%d", res, n, n)
	}
	// The recorded head hash must anchor cleanly.
	if _, err := Verify(rows, WithExpectedHead(res.HeadHash)); err != nil {
		t.Errorf("anchor with true head failed: %v", err)
	}
}

func TestAppendSequentialLinks(t *testing.T) {
	f := &fakeChain{}
	for i := 0; i < 3; i++ {
		if err := Append(context.Background(), f, sampleEntry()); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	for i := 1; i < len(f.rows); i++ {
		if f.rows[i].Seq != f.rows[i-1].Seq+1 {
			t.Errorf("row %d seq = %d, not contiguous", i, f.rows[i].Seq)
		}
		if !bytes.Equal(f.rows[i].PrevHash, f.rows[i-1].EntryHash) {
			t.Errorf("row %d prev_hash does not link to predecessor entry_hash", i)
		}
	}
}
