package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/payments"
)

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

// handleCreate validates and creates a payment. It runs inside the idempotency
// middleware, so its response is what gets cached under the Idempotency-Key.
// Input is validated fully before touching the service, so bad requests never
// open a ledger transaction.
func (s *server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body is not valid JSON")
		return
	}
	if req.SourceAccountID == uuid.Nil || req.DestAccountID == uuid.Nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "source_account_id and dest_account_id are required")
		return
	}
	if strings.TrimSpace(req.Asset) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "asset is required")
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "amount must be positive")
		return
	}

	payment, err := s.svc.Create(r.Context(), payments.CreateInput{
		SourceAccountID: req.SourceAccountID,
		DestAccountID:   req.DestAccountID,
		Asset:           req.Asset,
		Amount:          req.Amount,
	})
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toPaymentResponse(payment))
}

// handleGet returns one payment by id.
func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "id is not a valid uuid")
		return
	}
	payment, err := s.svc.Get(r.Context(), id)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPaymentResponse(payment))
}

// handleList returns a keyset page of payments, newest first, plus an opaque
// cursor for the next page.
func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	limit := int32(defaultListLimit)
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be an integer")
			return
		}
		limit = clampLimit(n)
	}

	var cursor *payments.Cursor
	if token := r.URL.Query().Get("cursor"); token != "" {
		c, err := decodeCursor(token)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "cursor is malformed")
			return
		}
		cursor = &c
	}

	page, next, err := s.svc.List(r.Context(), limit, cursor)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}

	resp := listResponse{Data: make([]paymentResponse, 0, len(page))}
	for _, p := range page {
		resp.Data = append(resp.Data, toPaymentResponse(p))
	}
	if next != nil {
		resp.NextCursor = encodeCursor(*next)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCancel reverses a completed payment.
func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "id is not a valid uuid")
		return
	}
	payment, err := s.svc.Cancel(r.Context(), id)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPaymentResponse(payment))
}

// clampLimit bounds a requested page size to [1, maxListLimit] rather than
// rejecting out-of-range values, so clients can pass a large limit and get the
// server's maximum.
func clampLimit(n int) int32 {
	if n < 1 {
		return 1
	}
	if n > maxListLimit {
		return maxListLimit
	}
	return int32(n)
}
