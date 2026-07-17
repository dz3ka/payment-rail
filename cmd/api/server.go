package main

import (
	"encoding/base64"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dz3ka/payment-rail/internal/db"
	"github.com/dz3ka/payment-rail/internal/payments"
)

// server holds the API's dependencies and hangs the HTTP handlers off them.
type server struct {
	svc  *payments.Service
	idem *payments.IdempotencyStore
	log  *slog.Logger
}

func newServer(svc *payments.Service, idem *payments.IdempotencyStore, log *slog.Logger) *server {
	return &server{svc: svc, idem: idem, log: log}
}

// routes wires the four payment endpoints. It uses Go 1.22 method+path patterns
// so routing and method matching are the mux's job, not each handler's. Create
// is wrapped in the idempotency middleware; the read/cancel routes are not, per
// ADR-0005 (only create mutates money on a client-driven key).
func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", s.withIdempotency(s.handleCreate))
	mux.HandleFunc("GET /v1/payments/{id}", s.handleGet)
	mux.HandleFunc("GET /v1/payments", s.handleList)
	mux.HandleFunc("POST /v1/payments/{id}/cancel", s.handleCancel)
	return mux
}

// createRequest is the wire shape of a create body. uuid.UUID rejects malformed
// strings at decode time (400); absent ids decode to uuid.Nil and are caught by
// validation.
type createRequest struct {
	SourceAccountID uuid.UUID `json:"source_account_id"`
	DestAccountID   uuid.UUID `json:"dest_account_id"`
	Asset           string    `json:"asset"`
	Amount          int64     `json:"amount"`
}

// paymentResponse is the wire representation of a payment. It is deliberately
// separate from db.Payment so storage columns never leak to clients and nullable
// columns become omitempty pointers rather than {Valid,...} wrappers.
type paymentResponse struct {
	ID              uuid.UUID  `json:"id"`
	Status          string     `json:"status"`
	Asset           string     `json:"asset"`
	Amount          int64      `json:"amount"`
	SourceAccountID uuid.UUID  `json:"source_account_id"`
	DestAccountID   uuid.UUID  `json:"dest_account_id"`
	JournalEntryID  uuid.UUID  `json:"journal_entry_id"`
	ReversalEntryID *uuid.UUID `json:"reversal_entry_id,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	CanceledAt      *time.Time `json:"canceled_at,omitempty"`
}

// listResponse is a page of payments plus the opaque cursor for the next page.
// NextCursor is omitted on the last page.
type listResponse struct {
	Data       []paymentResponse `json:"data"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

func toPaymentResponse(p db.Payment) paymentResponse {
	resp := paymentResponse{
		ID:              p.ID,
		Status:          p.Status,
		Asset:           p.Asset,
		Amount:          p.Amount,
		SourceAccountID: p.SourceAccountID,
		DestAccountID:   p.DestAccountID,
		JournalEntryID:  p.JournalEntryID,
		CreatedAt:       p.CreatedAt,
	}
	if p.ReversalEntryID.Valid {
		id := p.ReversalEntryID.UUID
		resp.ReversalEntryID = &id
	}
	if p.CanceledAt.Valid {
		t := p.CanceledAt.Time
		resp.CanceledAt = &t
	}
	return resp
}

// encodeCursor renders a keyset position as an opaque base64url token. The
// "<rfc3339nano>|<uuid>" payload is an implementation detail clients must not
// parse; base64url keeps it URL-safe.
func encodeCursor(c payments.Cursor) string {
	raw := c.CreatedAt.Format(time.RFC3339Nano) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor, returning an error for any token the API
// did not issue so a malformed cursor becomes a 400, not a panic or empty page.
func decodeCursor(token string) (payments.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return payments.Cursor{}, err
	}
	at, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return payments.Cursor{}, errMalformedCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return payments.Cursor{}, err
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return payments.Cursor{}, err
	}
	return payments.Cursor{CreatedAt: createdAt, ID: uid}, nil
}
