package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/outbox"
)

// fanOutSpy captures the params FanOutDelivery was called with and can inject an
// error, so Handle's parse -> fan-out flow is asserted without a database. called
// stays false until FanOutDelivery is reached — the poison paths must never call.
type fanOutSpy struct {
	called bool
	arg    db.FanOutDeliveryParams
	err    error
	n      int64
}

func (s *fanOutSpy) FanOutDelivery(_ context.Context, arg db.FanOutDeliveryParams) (int64, error) {
	s.called = true
	s.arg = arg
	return s.n, s.err
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func envelopeValue(t *testing.T, id, typ string) []byte {
	t.Helper()
	env := outbox.Envelope{
		ID:            id,
		Type:          typ,
		AggregateType: "payment",
		AggregateID:   "agg-1",
		SchemaVersion: 1,
		Data:          map[string]any{"amount": 100},
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

func TestHandleFansOutValidEnvelope(t *testing.T) {
	id := uuid.New()
	value := envelopeValue(t, id.String(), "payment.settled")
	spy := &fanOutSpy{n: 2}

	if err := Handle(context.Background(), spy, value, quietLogger()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !spy.called {
		t.Fatal("FanOutDelivery not called")
	}
	if spy.arg.EventID != id {
		t.Errorf("EventID = %v, want %v", spy.arg.EventID, id)
	}
	if spy.arg.EventType != "payment.settled" {
		t.Errorf("EventType = %q, want payment.settled", spy.arg.EventType)
	}
	if string(spy.arg.Payload) != string(value) {
		t.Errorf("Payload = %q, want raw value %q", spy.arg.Payload, value)
	}
}

func TestHandleMalformedJSONIsPoison(t *testing.T) {
	spy := &fanOutSpy{}
	err := Handle(context.Background(), spy, []byte(`{not json`), quietLogger())
	if !errors.Is(err, ErrPoisonMessage) {
		t.Fatalf("err = %v, want poison", err)
	}
	if spy.called {
		t.Error("FanOutDelivery called on malformed JSON, want skip")
	}
}

func TestHandleBadEventIDIsPoison(t *testing.T) {
	value := envelopeValue(t, "not-a-uuid", "payment.settled")
	spy := &fanOutSpy{}
	err := Handle(context.Background(), spy, value, quietLogger())
	if !errors.Is(err, ErrPoisonMessage) {
		t.Fatalf("err = %v, want poison", err)
	}
	if spy.called {
		t.Error("FanOutDelivery called with bad event id, want skip")
	}
}

func TestHandleDBErrorIsTransient(t *testing.T) {
	dbErr := errors.New("db down")
	value := envelopeValue(t, uuid.New().String(), "payment.settled")
	spy := &fanOutSpy{err: dbErr}

	err := Handle(context.Background(), spy, value, quietLogger())
	if err == nil {
		t.Fatal("Handle returned nil on DB error, want error")
	}
	if errors.Is(err, ErrPoisonMessage) {
		t.Fatal("DB error classified as poison; loop would wrongly commit offset")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("err = %v, want to wrap %v", err, dbErr)
	}
}
