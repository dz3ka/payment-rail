package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// captureQuerier is a db.Querier that records the single InsertOutboxEventParams
// Emit hands it. The embedded nil interface promotes every other method so the
// struct satisfies db.Querier without spelling them out; none are called here, so
// a stray call would nil-panic loudly rather than pass silently.
type captureQuerier struct {
	db.Querier
	got    db.InsertOutboxEventParams
	called int
	err    error
}

func (c *captureQuerier) InsertOutboxEvent(_ context.Context, arg db.InsertOutboxEventParams) error {
	c.called++
	c.got = arg
	return c.err
}

// orderedKeys returns the top-level object keys of b in document order, so a test
// can assert the envelope's field order — which map-based unmarshaling would lose.
func orderedKeys(t *testing.T, b []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		t.Fatalf("payload is not a JSON object: tok=%v err=%v", tok, err)
	}
	var keys []string
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			t.Fatalf("read key token: %v", err)
		}
		keys = append(keys, kt.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatalf("skip value for %q: %v", kt, err)
		}
	}
	return keys
}

func TestEmit_EnvelopeShape(t *testing.T) {
	cq := &captureQuerier{}
	e := Event{
		Type:        "payment.created",
		AggregateID: "agg-123",
		Data:        map[string]any{"foo": "bar"},
	}
	if err := Emit(context.Background(), cq, e); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if cq.called != 1 {
		t.Fatalf("InsertOutboxEvent called %d times, want 1", cq.called)
	}

	// The row's scalar columns mirror the Event.
	if cq.got.EventType != "payment.created" {
		t.Errorf("EventType = %q, want payment.created", cq.got.EventType)
	}
	if cq.got.AggregateID != "agg-123" {
		t.Errorf("AggregateID = %q, want agg-123", cq.got.AggregateID)
	}

	// The payload is the versioned envelope with exactly seven fields, in order.
	wantKeys := []string{"id", "type", "aggregate_type", "aggregate_id", "occurred_at", "schema_version", "data"}
	gotKeys := orderedKeys(t, cq.got.Payload)
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("payload keys = %v, want %v", gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("payload key[%d] = %q, want %q (full=%v)", i, gotKeys[i], wantKeys[i], gotKeys)
		}
	}

	var env struct {
		ID            string `json:"id"`
		Type          string `json:"type"`
		AggregateType string `json:"aggregate_type"`
		AggregateID   string `json:"aggregate_id"`
		SchemaVersion int    `json:"schema_version"`
	}
	if err := json.Unmarshal(cq.got.Payload, &env); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", env.SchemaVersion)
	}
	if env.AggregateType != "payment" {
		t.Errorf("aggregate_type = %q, want payment", env.AggregateType)
	}
	if env.Type != "payment.created" || env.AggregateID != "agg-123" {
		t.Errorf("type/aggregate_id = %q/%q, want payment.created/agg-123", env.Type, env.AggregateID)
	}
	// id is a valid uuid and equals the row's ID (so relay and payload agree).
	id, err := uuid.Parse(env.ID)
	if err != nil {
		t.Fatalf("payload id %q is not a uuid: %v", env.ID, err)
	}
	if id != cq.got.ID {
		t.Errorf("payload id %s != params ID %s", id, cq.got.ID)
	}
}

func TestEmit_AggregateTypeDerivedFromType(t *testing.T) {
	cases := map[string]string{
		"payment.created":      "payment",
		"settlement.finalized": "settlement",
	}
	for eventType, wantAgg := range cases {
		cq := &captureQuerier{}
		if err := Emit(context.Background(), cq, Event{Type: eventType, AggregateID: "x"}); err != nil {
			t.Fatalf("Emit(%s): %v", eventType, err)
		}
		var env struct {
			AggregateType string `json:"aggregate_type"`
		}
		if err := json.Unmarshal(cq.got.Payload, &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if env.AggregateType != wantAgg {
			t.Errorf("aggregate_type for %q = %q, want %q", eventType, env.AggregateType, wantAgg)
		}
	}
}

func TestParseEnvelope_RoundTrip(t *testing.T) {
	// A known payload as it would appear on the wire, fields in schema order.
	const payload = `{"id":"11111111-1111-1111-1111-111111111111","type":"payment.created","aggregate_type":"payment","aggregate_id":"agg-123","occurred_at":"2026-07-20T12:00:00Z","schema_version":1,"data":{"foo":"bar"}}`

	got, err := ParseEnvelope([]byte(payload))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if got.ID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("ID = %q", got.ID)
	}
	if got.Type != "payment.created" {
		t.Errorf("Type = %q, want payment.created", got.Type)
	}
	if got.AggregateType != "payment" {
		t.Errorf("AggregateType = %q, want payment", got.AggregateType)
	}
	if got.AggregateID != "agg-123" {
		t.Errorf("AggregateID = %q, want agg-123", got.AggregateID)
	}
	wantTime := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	if !got.OccurredAt.Equal(wantTime) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, wantTime)
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", got.SchemaVersion)
	}
	data, ok := got.Data.(map[string]any)
	if !ok || data["foo"] != "bar" {
		t.Errorf("Data = %v, want map with foo=bar", got.Data)
	}

	// Re-marshaling the parsed Envelope preserves the on-the-wire field order,
	// so ParseEnvelope reads the same schema Emit writes.
	reMarshaled, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	wantKeys := []string{"id", "type", "aggregate_type", "aggregate_id", "occurred_at", "schema_version", "data"}
	gotKeys := orderedKeys(t, reMarshaled)
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("re-marshaled keys = %v, want %v", gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("re-marshaled key[%d] = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

func TestParseEnvelope_InvalidJSON(t *testing.T) {
	if _, err := ParseEnvelope([]byte("not json")); err == nil {
		t.Fatal("ParseEnvelope(invalid) = nil error, want a wrapped error")
	}
}

func TestEmit_InsertErrorWraps(t *testing.T) {
	sentinel := errors.New("boom")
	cq := &captureQuerier{err: sentinel}
	err := Emit(context.Background(), cq, Event{Type: "payment.created", AggregateID: "x"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Emit err = %v, want it to wrap sentinel", err)
	}
}
