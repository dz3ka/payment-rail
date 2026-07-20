package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
)

// markSpy records which terminal method the worker called and with what params,
// so drainOnce's outcome branch is asserted without a database. Exactly one of
// the mark methods should fire per delivered row.
type markSpy struct {
	claimRows []db.ClaimDueDeliveriesRow
	claimErr  error

	succeeded *db.MarkDeliverySucceededParams
	retried   *db.MarkDeliveryRetryParams
	dead      *db.MarkDeliveryDeadLetteredParams
	markErr   error
}

func (m *markSpy) ClaimDueDeliveries(_ context.Context, _ db.ClaimDueDeliveriesParams) ([]db.ClaimDueDeliveriesRow, error) {
	if m.claimErr != nil {
		return nil, m.claimErr
	}
	return m.claimRows, nil
}

func (m *markSpy) MarkDeliverySucceeded(_ context.Context, arg db.MarkDeliverySucceededParams) error {
	m.succeeded = &arg
	return m.markErr
}

func (m *markSpy) MarkDeliveryRetry(_ context.Context, arg db.MarkDeliveryRetryParams) error {
	m.retried = &arg
	return m.markErr
}

func (m *markSpy) MarkDeliveryDeadLettered(_ context.Context, arg db.MarkDeliveryDeadLetteredParams) error {
	m.dead = &arg
	return m.markErr
}

// fakeSender scripts one send result and captures the signature/args passed in.
type fakeSender struct {
	res     SendResult
	err     error
	gotSig  string
	gotBody []byte
	gotURL  string
	gotEvt  string
	gotAtt  int
	calls   int
}

func (f *fakeSender) Send(_ context.Context, url string, body []byte, sig, eventID string, attempt int) (SendResult, error) {
	f.calls++
	f.gotURL = url
	f.gotBody = body
	f.gotSig = sig
	f.gotEvt = eventID
	f.gotAtt = attempt
	return f.res, f.err
}

func dueRow(attempts int32, secret, payload string) db.ClaimDueDeliveriesRow {
	return db.ClaimDueDeliveriesRow{
		ID:            uuid.New(),
		EventID:       uuid.New(),
		Attempts:      attempts,
		Payload:       []byte(payload),
		Url:           "https://sub.example/hook",
		SigningSecret: secret,
	}
}

func fixedNow() func() time.Time {
	t := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return t }
}

func newTestWorker(q workerQuerier, s Sender, log *slog.Logger) *Worker {
	w := NewWorker(q, s, time.Second, log)
	w.now = fixedNow()
	return w
}

func TestDrainOnceSuccess(t *testing.T) {
	row := dueRow(0, "whsec", `{"id":"evt"}`)
	q := &markSpy{claimRows: []db.ClaimDueDeliveriesRow{row}}
	s := &fakeSender{res: SendResult{StatusCode: 200}}
	w := newTestWorker(q, s, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := w.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if q.succeeded == nil {
		t.Fatal("MarkDeliverySucceeded not called")
	}
	if q.retried != nil || q.dead != nil {
		t.Fatal("retry/dead-letter called on success")
	}
	if q.succeeded.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", q.succeeded.Attempts)
	}
	if !q.succeeded.LastStatusCode.Valid || q.succeeded.LastStatusCode.Int32 != 200 {
		t.Errorf("LastStatusCode = %+v, want valid 200", q.succeeded.LastStatusCode)
	}
}

func TestDrainOnceRetryOnServerError(t *testing.T) {
	row := dueRow(2, "whsec", `{"id":"evt"}`)
	q := &markSpy{claimRows: []db.ClaimDueDeliveriesRow{row}}
	s := &fakeSender{res: SendResult{StatusCode: 500}}
	w := newTestWorker(q, s, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := w.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if q.retried == nil {
		t.Fatal("MarkDeliveryRetry not called")
	}
	if q.dead != nil {
		t.Fatal("dead-lettered below threshold")
	}
	if q.retried.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", q.retried.Attempts)
	}
	wantNext := w.now().Add(backoff(3))
	if !q.retried.NextAttemptAt.Equal(wantNext) {
		t.Errorf("NextAttemptAt = %v, want %v", q.retried.NextAttemptAt, wantNext)
	}
	if !q.retried.LastStatusCode.Valid || q.retried.LastStatusCode.Int32 != 500 {
		t.Errorf("LastStatusCode = %+v, want valid 500", q.retried.LastStatusCode)
	}
	if !q.retried.LastError.Valid || q.retried.LastError.String == "" {
		t.Errorf("LastError = %+v, want set", q.retried.LastError)
	}
}

func TestDrainOnceDeadLetterOnTransportErrorAtThreshold(t *testing.T) {
	// newAttempts = maxAttempts triggers dead-letter; transport error => no code.
	row := dueRow(maxAttempts-1, "whsec", `{"id":"evt"}`)
	q := &markSpy{claimRows: []db.ClaimDueDeliveriesRow{row}}
	s := &fakeSender{err: errors.New("connection refused")}
	w := newTestWorker(q, s, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := w.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if q.dead == nil {
		t.Fatal("MarkDeliveryDeadLettered not called")
	}
	if q.retried != nil || q.succeeded != nil {
		t.Fatal("retry/success called at dead-letter threshold")
	}
	if q.dead.Attempts != maxAttempts {
		t.Errorf("Attempts = %d, want %d", q.dead.Attempts, maxAttempts)
	}
	if q.dead.LastStatusCode.Valid {
		t.Errorf("LastStatusCode.Valid = true on transport error, want false")
	}
	if !q.dead.LastError.Valid {
		t.Error("LastError not set on transport failure")
	}
}

func TestDrainOnceClaimErrorPropagates(t *testing.T) {
	claimErr := errors.New("db down")
	q := &markSpy{claimErr: claimErr}
	s := &fakeSender{}
	w := newTestWorker(q, s, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := w.drainOnce(context.Background()); !errors.Is(err, claimErr) {
		t.Fatalf("err = %v, want %v", err, claimErr)
	}
	if s.calls != 0 {
		t.Error("Sender called despite claim error")
	}
}

// TestDrainOnceSignatureVerifies proves the signature handed to the Sender is
// well-formed (t=<int>,v1=<hex>) and recomputes to the same HMAC over the exact
// payload the row carried, using the row's secret.
func TestDrainOnceSignatureVerifies(t *testing.T) {
	secret := "whsec_verify"
	payload := `{"id":"evt-sig"}`
	row := dueRow(0, secret, payload)
	q := &markSpy{claimRows: []db.ClaimDueDeliveriesRow{row}}
	s := &fakeSender{res: SendResult{StatusCode: 200}}
	w := newTestWorker(q, s, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := w.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}

	tPart, v1 := parseSig(t, s.gotSig)
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", tPart)
	mac.Write([]byte(payload))
	want := hex.EncodeToString(mac.Sum(nil))
	if v1 != want {
		t.Errorf("v1 = %q, want %q", v1, want)
	}
	if tPart != w.now().Unix() {
		t.Errorf("t = %d, want %d", tPart, w.now().Unix())
	}
	if string(s.gotBody) != payload {
		t.Errorf("body = %q, want %q", s.gotBody, payload)
	}
	if s.gotAtt != 1 {
		t.Errorf("attempt = %d, want 1", s.gotAtt)
	}
	if s.gotEvt != row.EventID.String() {
		t.Errorf("eventID = %q, want %q", s.gotEvt, row.EventID)
	}
}

func parseSig(t *testing.T, sig string) (int64, string) {
	t.Helper()
	parts := strings.Split(sig, ",")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") || !strings.HasPrefix(parts[1], "v1=") {
		t.Fatalf("malformed signature %q", sig)
	}
	ts, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "t="), 10, 64)
	if err != nil {
		t.Fatalf("bad t in signature %q: %v", sig, err)
	}
	v1 := strings.TrimPrefix(parts[1], "v1=")
	if _, err := hex.DecodeString(v1); err != nil {
		t.Fatalf("v1 not hex in %q: %v", sig, err)
	}
	return ts, v1
}

// TestDrainOnceNeverLogsSecretOrPayload runs a drain through a real JSON slog
// handler and asserts neither the signing secret nor the payload body ever
// appears in the log output, on the retry path (which logs the most detail).
func TestDrainOnceNeverLogsSecretOrPayload(t *testing.T) {
	const (
		secret  = "whsec_super_secret_value"
		payload = `{"id":"evt","card":"4242424242424242"}`
	)
	row := dueRow(1, secret, payload)
	q := &markSpy{claimRows: []db.ClaimDueDeliveriesRow{row}}
	s := &fakeSender{res: SendResult{StatusCode: 500}}

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := newTestWorker(q, s, log)

	if err := w.drainOnce(context.Background()); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	out := buf.String()
	if out == "" {
		t.Fatal("expected some log output on retry path")
	}
	if strings.Contains(out, secret) {
		t.Error("log leaked the signing secret")
	}
	if strings.Contains(out, "4242424242424242") || strings.Contains(out, payload) {
		t.Error("log leaked the payload body")
	}
}
