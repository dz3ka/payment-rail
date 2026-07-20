package outbox

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// fakeQuerier is the outboxQuerier spy: it replays a scripted ClaimUnsentOutbox
// result, captures the ids MarkOutboxSent was called with, and supports error
// injection on either call so drainBatch's control flow can be asserted without a
// database. It records whether MarkOutboxSent was reached at all — the empty and
// publish-error paths must never call it.
type fakeQuerier struct {
	claimRows []db.Outbox
	claimErr  error

	markErr    error
	marked     bool
	markedIDs  []uuid.UUID
	claimCalls int
}

func (f *fakeQuerier) ClaimUnsentOutbox(_ context.Context, _ int32) ([]db.Outbox, error) {
	f.claimCalls++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	return f.claimRows, nil
}

func (f *fakeQuerier) MarkOutboxSent(_ context.Context, ids []uuid.UUID) (int64, error) {
	f.marked = true
	f.markedIDs = ids
	if f.markErr != nil {
		return 0, f.markErr
	}
	return int64(len(ids)), nil
}

// fakeProducer captures the batch handed to Publish and can inject an error so the
// rows-stay-unsent path is exercised. published stays nil until Publish is called,
// so tests distinguish "not called" from "called with empty batch".
type fakeProducer struct {
	err       error
	calls     int
	published []Message
}

func (f *fakeProducer) Publish(_ context.Context, msgs []Message) error {
	f.calls++
	f.published = msgs
	return f.err
}

func outboxRow(aggregateID, payload string) db.Outbox {
	return db.Outbox{
		ID:          uuid.New(),
		AggregateID: aggregateID,
		Payload:     []byte(payload),
	}
}

func TestDrainBatchOldestFirstMapping(t *testing.T) {
	rows := []db.Outbox{
		outboxRow("agg-1", `{"n":1}`),
		outboxRow("agg-2", `{"n":2}`),
		outboxRow("agg-3", `{"n":3}`),
	}
	q := &fakeQuerier{claimRows: rows}
	p := &fakeProducer{}

	n, err := drainBatch(context.Background(), q, p, 10)
	if err != nil {
		t.Fatalf("drainBatch: %v", err)
	}
	if n != 3 {
		t.Fatalf("published = %d, want 3", n)
	}

	if len(p.published) != 3 {
		t.Fatalf("Publish got %d messages, want 3", len(p.published))
	}
	for i, row := range rows {
		if string(p.published[i].Key) != row.AggregateID {
			t.Errorf("msg[%d].Key = %q, want %q", i, p.published[i].Key, row.AggregateID)
		}
		if string(p.published[i].Value) != string(row.Payload) {
			t.Errorf("msg[%d].Value = %q, want %q", i, p.published[i].Value, row.Payload)
		}
	}

	if !q.marked {
		t.Fatalf("MarkOutboxSent not called")
	}
	if len(q.markedIDs) != 3 {
		t.Fatalf("marked %d ids, want 3", len(q.markedIDs))
	}
	for i, row := range rows {
		if q.markedIDs[i] != row.ID {
			t.Errorf("markedIDs[%d] = %v, want %v", i, q.markedIDs[i], row.ID)
		}
	}
}

func TestDrainBatchEmptyNoOp(t *testing.T) {
	q := &fakeQuerier{claimRows: nil}
	p := &fakeProducer{}

	n, err := drainBatch(context.Background(), q, p, 10)
	if err != nil {
		t.Fatalf("drainBatch: %v", err)
	}
	if n != 0 {
		t.Fatalf("published = %d, want 0", n)
	}
	if p.calls != 0 {
		t.Errorf("Publish called %d times, want 0 on empty batch", p.calls)
	}
	if q.marked {
		t.Errorf("MarkOutboxSent called on empty batch, want no call")
	}
}

func TestDrainBatchPublishErrorLeavesUnsent(t *testing.T) {
	publishErr := errors.New("broker unreachable")
	q := &fakeQuerier{claimRows: []db.Outbox{outboxRow("agg-1", `{"n":1}`)}}
	p := &fakeProducer{err: publishErr}

	n, err := drainBatch(context.Background(), q, p, 10)
	if !errors.Is(err, publishErr) {
		t.Fatalf("err = %v, want %v", err, publishErr)
	}
	if n != 0 {
		t.Fatalf("published = %d, want 0 on publish error", n)
	}
	if q.marked {
		t.Errorf("MarkOutboxSent called after publish error; rows must stay unsent")
	}
}

func TestDrainBatchClaimError(t *testing.T) {
	claimErr := errors.New("db down")
	q := &fakeQuerier{claimErr: claimErr}
	p := &fakeProducer{}

	n, err := drainBatch(context.Background(), q, p, 10)
	if !errors.Is(err, claimErr) {
		t.Fatalf("err = %v, want %v", err, claimErr)
	}
	if n != 0 {
		t.Fatalf("published = %d, want 0 on claim error", n)
	}
	if p.calls != 0 {
		t.Errorf("Publish called %d times after claim error, want 0", p.calls)
	}
	if q.marked {
		t.Errorf("MarkOutboxSent called after claim error, want no call")
	}
}

// fakeTransactor runs fn against a fixed Querier and counts invocations, standing
// in for *ledger.SQLStore so Run's loop is exercised without a database.
type fakeTransactor struct {
	q     db.Querier
	calls int
}

func (f *fakeTransactor) ExecTx(_ context.Context, fn func(q db.Querier) error) error {
	f.calls++
	return fn(f.q)
}

// stubQuerier embeds db.Querier (all methods nil) and overrides only the two the
// relay uses, so it satisfies the full interface while a publish error is injected.
type stubQuerier struct {
	db.Querier
	rows []db.Outbox
}

func (s *stubQuerier) ClaimUnsentOutbox(_ context.Context, _ int32) ([]db.Outbox, error) {
	return s.rows, nil
}

func (s *stubQuerier) MarkOutboxSent(_ context.Context, ids []uuid.UUID) (int64, error) {
	return int64(len(ids)), nil
}

// TestRunContinuesAfterPublishError proves a publish error inside a tick logs and
// continues rather than aborting Run, and that a cancelled context yields a clean
// nil return. The producer errors on every tick, so if Run aborted on the error the
// test would see a non-nil return instead of the ctx-cancel nil.
func TestRunContinuesAfterPublishError(t *testing.T) {
	q := &stubQuerier{rows: []db.Outbox{outboxRow("agg-1", `{"n":1}`)}}
	store := &fakeTransactor{q: q}
	prod := &fakeProducer{err: errors.New("broker down")}

	ctx, cancel := context.WithCancel(context.Background())
	relay := NewRelay(store, prod, 10, time.Millisecond, slog.New(slog.NewTextHandler(nopWriter{}, nil)))

	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	// Let a few ticks fire (each returns a publish error that must not abort Run),
	// then cancel and require a clean nil return.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on ctx cancel despite publish errors", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	if store.calls == 0 {
		t.Fatal("ExecTx never invoked; ticker did not fire")
	}
	if prod.calls == 0 {
		t.Fatal("Publish never invoked; drain did not run")
	}
}

// nopWriter discards log output so the Run test stays quiet.
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
