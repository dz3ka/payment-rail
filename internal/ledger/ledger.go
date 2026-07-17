// Package ledger is Conduit's double-entry accounting domain. It owns the rules
// that make money movement trustworthy — postings must balance, no account may
// go negative, and each external reference posts at most once — and it drives
// those rules through a narrow transactor seam so the same logic runs against a
// real *sql.DB in production and an in-memory fake in tests.
//
// The production Store implementation (a *sql.DB-backed transactor) lives in
// store_sql.go; the rest of this package is domain logic plus the interfaces it
// depends on, so the same orchestration runs against Postgres or the in-memory
// fake in tests.
package ledger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/dz3ka/payment-rail/internal/db"
)

// Direction is the side of the ledger a line posts to. Modeling it as a named
// string type (rather than a bare string) documents intent and lets Balanced
// reject any value that is neither Debit nor Credit.
type Direction string

const (
	Debit  Direction = "debit"
	Credit Direction = "credit"
)

// Line is a single leg of a posting: an amount, in minor units (e.g. cents),
// moved into or out of one account. Amount must be strictly positive; the
// Direction carries the sign.
type Line struct {
	AccountID uuid.UUID
	Direction Direction
	Amount    int64
}

// Entry is a whole double-entry posting: its metadata plus the lines that must
// balance. ExternalRef is the idempotency key — the (Kind, ExternalRef) pair is
// unique, so retrying the same logical event never double-posts.
type Entry struct {
	Kind        string
	ExternalRef string
	Asset       string
	Lines       []Line
}

// Sentinel errors. Callers match these with errors.Is; return sites wrap them
// with %w so the underlying cause and human-readable context travel together.
var (
	// ErrUnbalanced means debits != credits — a violation of the core
	// double-entry invariant, logged loudly because it should never happen.
	ErrUnbalanced = errors.New("ledger: entry is unbalanced")
	// ErrInsufficientFunds means a posting would drive an account negative.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	// ErrDuplicateEntry means the (kind, external_ref) pair already posted.
	ErrDuplicateEntry = errors.New("ledger: duplicate entry")
	// ErrInvalidEntry means the entry is malformed: no lines, a non-positive
	// amount, an unknown direction, or an empty kind/asset/external_ref.
	ErrInvalidEntry = errors.New("ledger: invalid entry")
)

// Store is the transaction-boundary seam — the central M1 learning objective.
//
// ExecTx runs fn inside a single database transaction: the production impl
// (WP5) will BEGIN a tx, build a db.Querier bound to it, COMMIT if fn returns
// nil, and ROLLBACK if fn returns an error (or panics). Because the domain only
// ever sees this interface, PostEntry's orchestration is identical whether it
// runs against Postgres or the in-memory fake in ledger's tests. The Querier
// handed to fn is valid only for the duration of that call.
type Store interface {
	ExecTx(ctx context.Context, fn func(q db.Querier) error) error
}

// Service posts journal entries through a Store, enforcing the ledger's
// invariants around each transaction.
type Service struct {
	store Store
	log   *slog.Logger
}

// NewService builds a Service. A nil logger falls back to slog.Default() so
// callers that don't care about logging need not construct one.
func NewService(store Store, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, log: log}
}

// PostEntry validates a posting and, if it holds, commits it atomically:
// it locks the touched accounts, checks that none would go negative, then
// writes the journal entry and its lines — all inside one transaction, so
// either every row lands or none does.
//
// On failure it returns a wrapped sentinel (ErrInvalidEntry, ErrUnbalanced,
// ErrInsufficientFunds, ErrDuplicateEntry) that callers match with errors.Is.
func (s *Service) PostEntry(ctx context.Context, e Entry) (db.JournalEntry, error) {
	// Validate shape before opening a transaction — cheap rejects stay cheap.
	// PostWithin re-validates once the tx is open; this keeps the fast path from
	// ever touching the database.
	if err := e.validate(); err != nil {
		s.logResult(ctx, e, err)
		return db.JournalEntry{}, err
	}

	// The lock → check → apply → insert sequence runs inside one transaction, so
	// either every row lands or none does. PostWithin is the shareable primitive.
	var created db.JournalEntry
	err := s.store.ExecTx(ctx, func(q db.Querier) error {
		var err error
		created, err = PostWithin(ctx, q, e)
		return err
	})

	s.logResult(ctx, e, err)
	if err != nil {
		return db.JournalEntry{}, err
	}
	return created, nil
}

// PostWithin posts a validated entry using the supplied transactional Querier,
// WITHOUT opening its own transaction, so callers that must commit extra rows
// atomically with the journal entry (e.g. the payments service) can share one
// tx: they call this inside their own ExecTx closure and write their rows on the
// same q. It performs the lock → check → apply → insert-entry → insert-lines
// sequence and returns the created JournalEntry. It re-validates defensively so
// it is safe to call directly, not only via Service.PostEntry.
func PostWithin(ctx context.Context, q db.Querier, e Entry) (db.JournalEntry, error) {
	if err := e.validate(); err != nil {
		return db.JournalEntry{}, err
	}

	// Deterministic, de-duplicated lock set. The fixed order (by id) is what
	// prevents deadlocks between concurrent transfers that share accounts.
	ids := accountIDs(e.Lines)

	// Lock the rows for the lifetime of the transaction, then validate them:
	// every touched account must exist, be active, and hold the entry's asset.
	// Balanced() is asset-blind, so this is the only guard stopping a single
	// posting from moving value across currencies or into a frozen/absent
	// account.
	accounts, err := q.GetAccountsForUpdate(ctx, ids)
	if err != nil {
		return db.JournalEntry{}, fmt.Errorf("ledger: lock accounts: %w", err)
	}
	if err := checkAccounts(accounts, ids, e.Asset); err != nil {
		return db.JournalEntry{}, err
	}

	// Read current balances under lock and project the posting onto them;
	// ApplyToBalances aborts the tx if any account goes negative.
	cur := make(map[uuid.UUID]int64, len(ids))
	for _, id := range ids {
		bal, err := q.GetAccountBalance(ctx, id)
		if err != nil {
			return db.JournalEntry{}, fmt.Errorf("ledger: read balance for %s: %w", id, err)
		}
		cur[id] = bal
	}
	if _, err := ApplyToBalances(cur, e.Lines); err != nil {
		return db.JournalEntry{}, err
	}

	// Persist the entry, then one row per line, capturing the result.
	je, err := q.InsertJournalEntry(ctx, db.InsertJournalEntryParams{
		Kind:        e.Kind,
		ExternalRef: e.ExternalRef,
		Asset:       e.Asset,
	})
	if err != nil {
		return db.JournalEntry{}, mapInsertError(err)
	}
	for _, l := range e.Lines {
		if _, err := q.InsertEntryLine(ctx, db.InsertEntryLineParams{
			EntryID:   je.ID,
			AccountID: l.AccountID,
			Direction: string(l.Direction),
			Amount:    l.Amount,
		}); err != nil {
			return db.JournalEntry{}, fmt.Errorf("ledger: insert line: %w", err)
		}
	}
	return je, nil
}

// validate checks the entry's own fields and delegates the balancing rules to
// the pure Balanced function.
func (e Entry) validate() error {
	if e.Kind == "" || e.Asset == "" || e.ExternalRef == "" {
		return fmt.Errorf("ledger: kind, asset and external_ref are required: %w", ErrInvalidEntry)
	}
	return Balanced(e.Lines)
}

// accountIDs returns the distinct account IDs touched by lines, sorted so the
// production FOR UPDATE lock is always acquired in the same order.
func accountIDs(lines []Line) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(lines))
	ids := make([]uuid.UUID, 0, len(lines))
	for _, l := range lines {
		if _, ok := seen[l.AccountID]; ok {
			continue
		}
		seen[l.AccountID] = struct{}{}
		ids = append(ids, l.AccountID)
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	return ids
}

// checkAccounts verifies every account the entry touches was locked, is active,
// and is denominated in the entry's asset. A missing id (GetAccountsForUpdate
// returns only rows that exist), an inactive status, or an asset mismatch all
// mean the posting is malformed, not that a balance is short — so they wrap
// ErrInvalidEntry. Identifiers (not amounts) appear in the message, which is safe
// to surface. ids is the sorted, de-duplicated lock set from accountIDs.
func checkAccounts(accounts []db.Account, ids []uuid.UUID, asset string) error {
	byID := make(map[uuid.UUID]db.Account, len(accounts))
	for _, a := range accounts {
		byID[a.ID] = a
	}
	for _, id := range ids {
		a, ok := byID[id]
		if !ok {
			return fmt.Errorf("ledger: account %s does not exist: %w", id, ErrInvalidEntry)
		}
		if a.Status != "active" {
			return fmt.Errorf("ledger: account %s is %s, not active: %w", id, a.Status, ErrInvalidEntry)
		}
		if a.Asset != asset {
			return fmt.Errorf("ledger: account %s asset %q != entry asset %q: %w", id, a.Asset, asset, ErrInvalidEntry)
		}
	}
	return nil
}

// mapInsertError translates a Postgres unique-violation on (kind, external_ref)
// into ErrDuplicateEntry. errors.As unwraps the chain to find a *pq.Error, so
// this stays correct even once the driver error is wrapped by a real Store.
func mapInsertError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return fmt.Errorf("ledger: entry already posted: %w", ErrDuplicateEntry)
	}
	return fmt.Errorf("ledger: insert journal entry: %w", err)
}

// logResult emits one structured record per posting outcome. Amounts are never
// logged (they are sensitive), and neither is the raw err — the wrapped
// sentinels embed monetary values in their messages (e.g. "balance -5 < 0",
// "debits 30 != credits 20"), so attaching err.Error() would leak exactly what
// we withhold above. The failure category is carried by the message + level
// instead: expected business rejects are info/warn, a broken double-entry
// invariant is an error. Callers that need the detail get the returned error;
// operators correlate by external_ref.
func (s *Service) logResult(ctx context.Context, e Entry, err error) {
	attrs := []any{
		"kind", e.Kind,
		"external_ref", e.ExternalRef,
		"asset", e.Asset,
		"line_count", len(e.Lines),
	}
	switch {
	case err == nil:
		s.log.InfoContext(ctx, "journal entry posted", attrs...)
	case errors.Is(err, ErrInvalidEntry):
		s.log.InfoContext(ctx, "journal entry rejected: invalid", attrs...)
	case errors.Is(err, ErrInsufficientFunds):
		s.log.WarnContext(ctx, "journal entry rejected: insufficient funds", attrs...)
	case errors.Is(err, ErrDuplicateEntry):
		s.log.WarnContext(ctx, "journal entry rejected: duplicate", attrs...)
	case errors.Is(err, ErrUnbalanced):
		s.log.ErrorContext(ctx, "journal entry unbalanced: invariant violation", attrs...)
	default:
		s.log.ErrorContext(ctx, "journal entry failed", attrs...)
	}
}
