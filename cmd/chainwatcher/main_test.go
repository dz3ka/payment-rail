package main

import (
	"database/sql"
	"testing"

	"github.com/dz3ka/payment-rail/internal/chain"
	"github.com/dz3ka/payment-rail/internal/db"
)

// spyTracker records which watcher entry point seed chose for a row, so the
// branch logic can be asserted without a live *evm.Watcher or an RPC node.
type spyTracker struct {
	tracked  []chain.TxHash
	resumed  []chain.TxHash
	lastHash string
	lastNum  uint64
}

func (s *spyTracker) Track(tx chain.TxHash) error {
	s.tracked = append(s.tracked, tx)
	return nil
}

func (s *spyTracker) Resume(tx chain.TxHash, blockHash string, blockNumber uint64) error {
	s.resumed = append(s.resumed, tx)
	s.lastHash = blockHash
	s.lastNum = blockNumber
	return nil
}

func TestSeedResumesSettledRowWithAnchor(t *testing.T) {
	spy := &spyTracker{}
	row := db.Settlement{
		TxHash:             "0xabc",
		Status:             "settled",
		SettledBlockHash:   sql.NullString{String: "0xblock", Valid: true},
		SettledBlockNumber: sql.NullInt64{Int64: 42, Valid: true},
	}

	if err := seed(spy, row); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if len(spy.resumed) != 1 || spy.resumed[0] != chain.TxHash("0xabc") {
		t.Fatalf("want one Resume of 0xabc, got tracked=%v resumed=%v", spy.tracked, spy.resumed)
	}
	if len(spy.tracked) != 0 {
		t.Fatalf("want no Track, got %v", spy.tracked)
	}
	if spy.lastHash != "0xblock" || spy.lastNum != 42 {
		t.Fatalf("Resume anchor = (%q, %d), want (0xblock, 42)", spy.lastHash, spy.lastNum)
	}
}

func TestSeedTracksPendingRow(t *testing.T) {
	spy := &spyTracker{}
	row := db.Settlement{TxHash: "0xdef", Status: "pending"}

	if err := seed(spy, row); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if len(spy.tracked) != 1 || spy.tracked[0] != chain.TxHash("0xdef") {
		t.Fatalf("want one Track of 0xdef, got tracked=%v resumed=%v", spy.tracked, spy.resumed)
	}
	if len(spy.resumed) != 0 {
		t.Fatalf("want no Resume, got %v", spy.resumed)
	}
}

// A legacy row persisted as settled before the anchor columns existed carries a
// NULL SettledBlockHash. It must fall to the pending Track path, not Resume with
// a zero anchor — tracking a settled tx afresh is money-safe, resuming a bogus
// anchor is not.
func TestSeedTracksSettledRowWithNullAnchor(t *testing.T) {
	spy := &spyTracker{}
	row := db.Settlement{
		TxHash:           "0xghi",
		Status:           "settled",
		SettledBlockHash: sql.NullString{Valid: false},
	}

	if err := seed(spy, row); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if len(spy.tracked) != 1 || spy.tracked[0] != chain.TxHash("0xghi") {
		t.Fatalf("want one Track of 0xghi, got tracked=%v resumed=%v", spy.tracked, spy.resumed)
	}
	if len(spy.resumed) != 0 {
		t.Fatalf("want no Resume, got %v", spy.resumed)
	}
}
