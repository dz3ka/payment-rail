package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"
)

// maxRequestBodyBytes caps the create request body. The DTO is a few hundred
// bytes; 64 KiB is generous headroom while denying an attacker the ability to
// stream an unbounded body into memory before validation runs.
const maxRequestBodyBytes = 64 << 10

// withIdempotency enforces at-most-once semantics for create (ADR-0005). It
// requires an Idempotency-Key, hashes the canonical request, and uses the store
// to claim the key: a fresh key runs the handler once and caches its response;
// a repeat key either replays the cached response, reports the original still
// in-flight (409), or rejects a body that no longer matches (422).
func (s *server) withIdempotency(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
			return
		}

		// Buffer the body: it is needed both to hash the request and to hand to
		// the handler, and a request body is a one-shot reader. MaxBytesReader
		// caps it so a hostile client cannot OOM the process before validation.
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the maximum allowed size")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_request", "could not read request body")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		// Canonical request = method \n path \n raw-body. Same key with the same
		// canonical request is a retry; same key with a different one is a misuse.
		sum := sha256.Sum256([]byte(r.Method + "\n" + r.URL.Path + "\n" + string(body)))
		hash := sum[:]

		res, err := s.idem.Begin(ctx, key, hash)
		if err != nil {
			s.log.ErrorContext(ctx, "idempotency begin failed", "err", err)
			writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			return
		}

		if !res.Fresh {
			s.replay(w, res.Existing.Status, res.Existing.RequestHash, hash,
				res.Existing.ResponseStatus.Int32, res.Existing.ResponseBody)
			return
		}

		// Fresh claim: run the handler into a recorder so nothing reaches the
		// client until we have decided whether to cache or release the key.
		rec := newResponseRecorder()

		// If the handler panics mid-flight, release the claim during unwind so a
		// crash does not wedge the key in_flight until the 24h sweep; the panic
		// then propagates to the server's own recovery as a 500. A separate
		// context is used because r's context may already be canceled by then.
		completed := false
		defer func() {
			if !completed {
				if delErr := s.idem.Delete(context.WithoutCancel(ctx), key); delErr != nil {
					s.log.ErrorContext(ctx, "idempotency release after panic failed", "err", delErr, "key", key)
				}
			}
		}()
		next(rec, r)
		completed = true

		if rec.status >= 200 && rec.status < 300 {
			if err := s.idem.Complete(ctx, key, rec.status, rec.body.Bytes(), paymentIDFrom(rec.body.Bytes())); err != nil {
				// The payment already committed but its response was not cached, so
				// the key stays in_flight: same-key retries get 409 until the sweeper
				// reaps it after the TTL (fails safe — no double charge in-window).
				// The fully-robust fix is to write the idempotency completion inside
				// the payment's own transaction; deferred as a follow-up (ADR-0005).
				s.log.ErrorContext(ctx, "idempotency complete failed", "err", err, "key", key)
			}
		} else if err := s.idem.Delete(ctx, key); err != nil {
			// Release the claim so a transient failure does not wedge the key
			// in_flight and block every retry.
			s.log.ErrorContext(ctx, "idempotency delete failed", "err", err, "key", key)
		}

		rec.flushTo(w)
	}
}

// replay resolves a repeat of an already-claimed key.
func (s *server) replay(w http.ResponseWriter, status string, storedHash, hash []byte, respStatus int32, respBody []byte) {
	if status == "in_flight" {
		writeError(w, http.StatusConflict, "request_in_flight", "a request with this idempotency key is still in progress")
		return
	}
	if !bytes.Equal(storedHash, hash) {
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"idempotency key was already used with a different request")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(respStatus))
	_, _ = w.Write(respBody)
}

// paymentIDFrom extracts the created payment's id from a captured success body
// so it can be stored alongside the cached response. A body without an id (a
// non-payment 2xx) yields uuid.Nil, which Complete treats as "no payment".
func paymentIDFrom(body []byte) uuid.UUID {
	var envelope struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return uuid.Nil
	}
	return envelope.ID
}

// responseRecorder buffers a handler's status and body so the idempotency
// middleware can inspect and cache the outcome before committing it to the wire.
type responseRecorder struct {
	status      int
	body        *bytes.Buffer
	header      http.Header
	wroteHeader bool
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{status: http.StatusOK, body: &bytes.Buffer{}, header: make(http.Header)}
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(b)
}

// flushTo copies the buffered response to the real writer.
func (r *responseRecorder) flushTo(w http.ResponseWriter) {
	for k, vs := range r.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body.Bytes())
}
