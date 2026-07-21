// Package audit is the append-only, hash-chained audit log (PRD F9). Each entry
// commits inside the caller's transaction and links to its predecessor by hash,
// so any later deletion, reordering, or field edit breaks the chain and is caught
// by Verify. It mirrors internal/outbox: stdlib + internal/db only, a single
// Append(ctx, q, ...) that does one q.Insert... call whose error propagates so the
// surrounding ExecTx rolls back on failure.
package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dz3ka/payment-rail/internal/db"
)

// chainAdvisoryKey is the SINGLE GLOBAL key every appender serializes the chain
// head on. UNLIKE velocity's per-key fnv64a(keyID) values (see
// cmd/paymentrailctl/velocitystore.go, advisoryKey), the audit chain has exactly
// one head, so all writers must contend on one deterministic well-known constant.
//
// The literal below is fnv64a("payment-rail.audit.chain.v1") reinterpreted as
// int64 (the lock space is the full 64-bit range; the sign is immaterial). It is
// pinned as a literal rather than computed at init so the value is greppable and
// stable. A collision with some velocity key would only make those two locks
// over-serialize — harmless for correctness, at worst slightly slower.
const chainAdvisoryKey int64 = 8516588576188130913

// genesisPrevHash is the prev_hash of the very first row (seq 1): 32 zero bytes.
// It anchors the chain so a verifier can prove row 1 was not itself replaced.
var genesisPrevHash = make([]byte, 32)

// Entry is the caller-facing record to append. Data is the event-specific payload
// (any JSON-marshalable value) and becomes the canonical `payload` bytes. If
// OccurredAt is the zero time, Append stamps time.Now().UTC().
type Entry struct {
	Actor         string
	Action        string
	AggregateType string
	AggregateID   string
	OccurredAt    time.Time
	Data          any
}

// Append hash-chains e onto the audit log through q, committing in whatever
// transaction q belongs to. It marshals Data, takes the chain advisory lock, reads
// the head, computes seq/prev_hash/entry_hash, and inserts one row. Every failure
// is wrapped %w and propagated so the surrounding transaction rolls back — the log
// fails closed, never silently dropping an entry.
//
// OccurredAt is truncated to microseconds before both hashing and storing: Postgres
// TIMESTAMPTZ is microsecond-precision, so truncating here makes the store→read
// round-trip byte-exact and prevents Verify from reporting a false tamper on the
// sub-microsecond digits Postgres would have dropped anyway.
func Append(ctx context.Context, q db.Querier, e Entry) error {
	occurred := e.OccurredAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	occurred = occurred.Truncate(time.Microsecond)

	payload, err := json.Marshal(e.Data)
	if err != nil {
		return fmt.Errorf("audit: marshal payload: %w", err)
	}

	// Serialize the chain head. The advisory lock is xact-scoped, so it releases at
	// commit/rollback — no explicit unlock, and concurrent appenders can't both read
	// the same head and fork the chain by assigning the same seq / prev_hash.
	if err := q.AcquireAuditChainLock(ctx, chainAdvisoryKey); err != nil {
		return fmt.Errorf("audit: acquire chain lock: %w", err)
	}

	var (
		seq  int64
		prev []byte
	)
	head, err := q.GetAuditHead(ctx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		seq, prev = 1, genesisPrevHash
	case err != nil:
		return fmt.Errorf("audit: read head: %w", err)
	default:
		seq, prev = head.Seq+1, head.EntryHash
	}

	pre := canonical(seq, e.Actor, e.Action, e.AggregateType, e.AggregateID, occurred.UnixMicro(), payload)
	hash := entryHash(prev, pre)

	if err := q.InsertAuditEntry(ctx, db.InsertAuditEntryParams{
		Seq:           seq,
		PrevHash:      prev,
		EntryHash:     hash,
		Actor:         e.Actor,
		Action:        e.Action,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		OccurredAt:    occurred,
		Payload:       payload,
	}); err != nil {
		return fmt.Errorf("audit: insert entry seq %d: %w", seq, err)
	}
	return nil
}

// canonical serializes one entry's fields into the deterministic, unambiguous
// preimage that gets hashed. It is the chain's CANONICAL WIRE FORM: it must never
// change without a version bump, because every stored entry_hash was computed over
// exactly these bytes and Verify recomputes them.
//
// Wire layout (all integers big-endian):
//
//	seq             : 8 bytes int64
//	occurredMicros  : 8 bytes int64
//	len(actor)      : 8 bytes uint64, then actor bytes
//	len(action)     : 8 bytes uint64, then action bytes
//	len(aggType)    : 8 bytes uint64, then aggType bytes
//	len(aggID)      : 8 bytes uint64, then aggID bytes
//	len(payload)    : 8 bytes uint64, then payload bytes
//
// The length prefix on every variable-length field makes boundaries unambiguous:
// without it, actor="ab"+action="c" and actor="a"+action="bc" would serialize to
// the same bytes — a trivial forgery. With the prefix their frames differ, so
// their hashes differ.
func canonical(seq int64, actor, action, aggType, aggID string, occurredMicros int64, payload []byte) []byte {
	var buf bytes.Buffer
	var scratch [8]byte

	writeUint64 := func(v uint64) {
		binary.BigEndian.PutUint64(scratch[:], v)
		buf.Write(scratch[:])
	}
	writeField := func(b []byte) {
		writeUint64(uint64(len(b)))
		buf.Write(b)
	}

	writeUint64(uint64(seq))
	writeUint64(uint64(occurredMicros))
	writeField([]byte(actor))
	writeField([]byte(action))
	writeField([]byte(aggType))
	writeField([]byte(aggID))
	writeField(payload)

	return buf.Bytes()
}

// entryHash chains a row: sha256(prevHash || canonicalBytes). Feeding the
// predecessor's hash into every entry is what makes the log tamper-evident — any
// edit to an earlier row changes its entry_hash, which is the next row's prev_hash,
// cascading a mismatch all the way to the head.
func entryHash(prevHash, canonicalBytes []byte) []byte {
	h := sha256.New()
	h.Write(prevHash)
	h.Write(canonicalBytes)
	return h.Sum(nil)
}
