package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/dz3ka/payment-rail/internal/ledger"
	"github.com/dz3ka/payment-rail/internal/payments"
)

// errMalformedCursor marks a cursor token the API did not issue.
var errMalformedCursor = errors.New("malformed cursor")

// errorEnvelope is the single error shape every failure returns:
// {"error":{"code":"...","message":"..."}}. Messages are generic by design —
// amounts, balances, and account internals never appear in them; details go to
// the server log instead.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeJSON encodes v as the response body with the given status. A late
// encoding error (client already gone) is unrecoverable and dropped.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends a structured error envelope.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message}})
}

// writeServiceError maps a payments/ledger sentinel to its HTTP status and a
// safe, generic message. It is the single place service errors cross into HTTP,
// so the mapping lives in one auditable switch. Unexpected and invariant-
// violation errors are logged with detail and returned as an opaque 500.
func (s *server) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	switch {
	case errors.Is(err, ledger.ErrInvalidEntry):
		writeError(w, http.StatusBadRequest, "invalid_request", "the payment request is invalid")
	case errors.Is(err, ledger.ErrInsufficientFunds):
		// 422 Unprocessable Entity: the request is well-formed and understood, but
		// the source account lacks the funds to satisfy it. Chosen over 409
		// Conflict, which implies a resource-state clash the client can resolve by
		// retrying — an overdraw is not resolved by retrying the same request.
		writeError(w, http.StatusUnprocessableEntity, "insufficient_funds", "insufficient funds for this payment")
	case errors.Is(err, ledger.ErrDuplicateEntry):
		writeError(w, http.StatusConflict, "duplicate_payment", "a conflicting payment already exists")
	case errors.Is(err, payments.ErrPaymentNotFound):
		writeError(w, http.StatusNotFound, "not_found", "payment not found")
	case errors.Is(err, payments.ErrPaymentNotCancelable):
		writeError(w, http.StatusConflict, "not_cancelable", "payment cannot be canceled")
	case errors.Is(err, ledger.ErrUnbalanced):
		// Invariant violation: a posted entry must always balance. If this ever
		// surfaces, the ledger has a bug — log it loudly and return an opaque 500.
		s.log.ErrorContext(ctx, "ledger invariant violated: unbalanced entry reached the API", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	default:
		s.log.ErrorContext(ctx, "unexpected service error", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}
