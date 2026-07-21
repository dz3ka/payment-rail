package audit

import (
	"bytes"
	"fmt"

	"github.com/dz3ka/payment-rail/internal/db"
)

// The four ways a chain can be broken. Kind is stable and asserted on by callers
// (and tests) instead of the human-readable message.
const (
	// KindGap: a seq is missing or out of order — a row was deleted or rows reordered.
	KindGap = "gap"
	// KindBrokenLink: a row's prev_hash does not equal the previous row's entry_hash,
	// or the first row's prev_hash is not genesis — the links were re-stitched.
	KindBrokenLink = "broken_link"
	// KindHashMismatch: a row's stored entry_hash does not match the recomputed hash
	// of its own fields — some column (payload, actor, …) was edited in place.
	KindHashMismatch = "hash_mismatch"
	// KindHeadMismatch: the walk was internally consistent but the head hash does not
	// match a caller-supplied anchor — catches tail-truncation / re-genesis that a
	// self-consistent shorter chain would otherwise hide.
	KindHeadMismatch = "head_mismatch"
)

// TamperError reports the first integrity violation found, at row Seq, of the given
// Kind. Seq is the seq of the offending row (or the expected head for a head
// mismatch), so an operator can jump straight to it.
type TamperError struct {
	Seq  int64
	Kind string
}

func (e *TamperError) Error() string {
	return fmt.Sprintf("audit: chain tamper at seq %d: %s", e.Seq, e.Kind)
}

// Result summarizes a verified chain. OK is true only when the whole walk (and any
// anchor) passed. HeadHash is the last row's entry_hash — the value a caller should
// persist elsewhere and later feed back via WithExpectedHead to pin the tail.
type Result struct {
	Count    int64
	HeadSeq  int64
	HeadHash []byte
	OK       bool
}

type verifyConfig struct {
	expectHead    []byte
	hasExpectHead bool
}

// VerifyOpt configures an optional anchor check applied after the structural walk.
type VerifyOpt func(*verifyConfig)

// WithExpectedHead anchors verification to a known head hash. After a clean walk,
// if the chain's head hash differs from hash (or the chain is empty when a head was
// expected), Verify returns a KindHeadMismatch TamperError. This is the ONLY anchor:
// an expected-count is redundant because seq is inside every entry's preimage, so
// the head hash already pins the chain's prefix length.
func WithExpectedHead(hash []byte) VerifyOpt {
	return func(c *verifyConfig) {
		c.expectHead = hash
		c.hasExpectHead = true
	}
}

// Verify is a PURE function (no ctx, no db) over rows the caller obtained via
// db.ScanAuditChain, which guarantees ORDER BY seq ASC. It confirms the chain is
// contiguous from genesis, that every link matches, and that every row's stored
// hash equals a fresh recompute of its fields; then it applies any anchor opt. A
// fresh (empty) chain is valid. The first violation stops the walk and is returned
// as a *TamperError; a healthy chain returns Result{OK: true} and a nil error.
func Verify(rows []db.AuditLog, opts ...VerifyOpt) (Result, error) {
	var cfg verifyConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if len(rows) == 0 {
		res := Result{Count: 0, OK: true}
		// An empty chain fails the anchor only if a specific head was expected.
		if cfg.hasExpectHead && len(cfg.expectHead) > 0 {
			res.OK = false
			return res, &TamperError{Seq: 0, Kind: KindHeadMismatch}
		}
		return res, nil
	}

	// Row 1 must be the genesis row: seq 1 with the all-zero prev_hash. If it is not,
	// the chain was re-anchored (a new row spliced in front, or the real head cut).
	first := rows[0]
	if first.Seq != 1 || !bytes.Equal(first.PrevHash, genesisPrevHash) {
		return Result{}, &TamperError{Seq: first.Seq, Kind: KindBrokenLink}
	}

	var prevSeq int64
	var prevEntryHash []byte
	for i, row := range rows {
		if i > 0 {
			if row.Seq != prevSeq+1 {
				return Result{}, &TamperError{Seq: row.Seq, Kind: KindGap}
			}
			if !bytes.Equal(row.PrevHash, prevEntryHash) {
				return Result{}, &TamperError{Seq: row.Seq, Kind: KindBrokenLink}
			}
		}

		// Recompute the row's hash from its own fields and its stored prev_hash.
		// UnixMicro() reconstructs the same integer Append truncated to before hashing.
		pre := canonical(row.Seq, row.Actor, row.Action, row.AggregateType, row.AggregateID, row.OccurredAt.UTC().UnixMicro(), row.Payload)
		want := entryHash(row.PrevHash, pre)
		if !bytes.Equal(row.EntryHash, want) {
			return Result{}, &TamperError{Seq: row.Seq, Kind: KindHashMismatch}
		}

		prevSeq = row.Seq
		prevEntryHash = row.EntryHash
	}

	last := rows[len(rows)-1]
	res := Result{
		Count:    int64(len(rows)),
		HeadSeq:  last.Seq,
		HeadHash: last.EntryHash,
		OK:       true,
	}

	if cfg.hasExpectHead && !bytes.Equal(cfg.expectHead, res.HeadHash) {
		res.OK = false
		return res, &TamperError{Seq: res.HeadSeq, Kind: KindHeadMismatch}
	}
	return res, nil
}
