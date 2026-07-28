package evm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// chainReader is the read-only seam the watcher polls the chain through. It
// names only the three reads the watcher makes — a receipt lookup, a header
// lookup by height, and the head number — so the watcher depends on an interface
// it owns rather than on the concrete *ethclient.Client. The method set is a
// strict subset of go-ethereum's client surface: both *ethclient.Client and the
// simulated backend's Client() satisfy it (asserted in watcher_test.go), so the
// same watcher runs against a live node and a hermetic in-memory chain unchanged.
type chainReader interface {
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	BlockNumber(ctx context.Context) (uint64, error)
}

// Phase is a tracked transaction's lifecycle position as the watcher observes it.
type Phase int

const (
	// PhasePending: broadcast but not yet found in a mined block.
	PhasePending Phase = iota
	// PhaseMined: found in a block, but with fewer than the required confirmations.
	PhaseMined
	// PhaseConfirmed: buried under at least the configured confirmation depth.
	// No longer terminal (M3 slice 2): the anchor is re-verified every pass so a
	// deep reorg can still reverse a confirmed tx into PhaseReorged.
	PhaseConfirmed
	// PhaseReorged: the block it was mined in is no longer canonical. Non-terminal:
	// the tracked entry resets to pending semantics so a re-mine is observed fresh
	// (the reverse+reapply cycle M3 slice 2 requires).
	PhaseReorged
	// PhaseFinalized: buried at least the configured finality depth under the head,
	// deep enough that a reversing reorg is treated as impossible. Terminal: the
	// entry is evicted from the tracked map once this is surfaced, bounding the
	// growth Confirmed's non-terminal re-verification would otherwise leave unchecked.
	PhaseFinalized
)

// String renders the phase as a stable, lower-case label for logs and callers.
func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "pending"
	case PhaseMined:
		return "mined"
	case PhaseConfirmed:
		return "confirmed"
	case PhaseReorged:
		return "reorged"
	case PhaseFinalized:
		return "finalized"
	default:
		return "unknown"
	}
}

// Status is the surfaced observation for one tracked transaction at one poll. It
// is deliberately go-ethereum-free — a 0x-hex string and plain uints keyed by the
// neutral chain.TxHash — so a caller consuming watcher output never imports
// go-ethereum types.
type Status struct {
	TxHash      chain.TxHash
	Phase       Phase
	BlockHash   string // 0x-hex; empty while pending
	BlockNumber uint64 // 0 while pending
	Depth       uint64 // confirmations incl. the mined block; 0 while pending
}

// tracked is the watcher's per-transaction state. blockHash/blockNumber record
// the block the tx was mined in (the anchor a reorg is detected against);
// lastEmittedPhase/lastEmittedDepth dedupe emits so a Status is surfaced only on
// a genuine transition, never once per tick at an unchanged depth.
type tracked struct {
	phase       Phase
	blockHash   common.Hash
	blockNumber uint64

	lastEmittedPhase Phase
	lastEmittedDepth uint64
}

// StatusSink receives each Status the watcher emits. Implemented by callers
// (e.g. cmd/chainwatcher) so evm never imports the ledger/settlement layer — the
// dependency inversion that keeps this package chain-only. A sink error is logged
// and swallowed by Run, never fatal: the settlement row's status is the recovery
// anchor, so the watcher keeps polling.
type StatusSink interface {
	OnStatus(ctx context.Context, s Status) error
}

// Watcher polls a chainReader for the confirmation status of transactions it has
// been asked to Track. It is safe for concurrent use: its only mutable state is
// the tracked map, guarded by mu (the repo's concurrency idiom, per the
// nonceAllocator). The mutex is never held across an RPC call — poll snapshots
// the keys under the lock, does its network I/O unlocked, then re-acquires to
// mutate — so a slow node never stalls a concurrent Track.
type Watcher struct {
	reader        chainReader
	depth         uint64
	finalityDepth uint64
	interval      time.Duration
	log           *slog.Logger

	mu      sync.Mutex
	tracked map[chain.TxHash]*tracked
}

// NewWatcher validates its knobs and wires the watcher. depth is the confirmation
// threshold N and must be >= 1 (zero confirmations is not a threshold);
// finalityDepth is the deeper threshold at which a confirmed tx is treated as
// irreversible and evicted, and must exceed depth (a finality that fires at or
// before confirmation would evict a tx a reorg could still reverse); interval is
// the poll cadence and must be > 0. All three are operator-supplied, so they are a
// real trust boundary and fail loudly here rather than producing a watcher that
// spins or never confirms. A nil logger falls back to slog.Default() (mirrors
// NewAdapter / NewServer).
func NewWatcher(reader chainReader, depth, finalityDepth uint64, interval time.Duration, log *slog.Logger) (*Watcher, error) {
	if reader == nil {
		return nil, errors.New("evm: watcher reader is required")
	}
	if depth == 0 {
		return nil, errors.New("evm: watcher confirmation depth must be positive")
	}
	if finalityDepth <= depth {
		return nil, errors.New("evm: watcher finality depth must exceed confirmation depth")
	}
	if interval <= 0 {
		return nil, errors.New("evm: watcher poll interval must be positive")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Watcher{
		reader:        reader,
		depth:         depth,
		finalityDepth: finalityDepth,
		interval:      interval,
		log:           log,
		tracked:       make(map[chain.TxHash]*tracked),
	}, nil
}

// Track registers a transaction for observation. The hash is caller-supplied, so
// it is validated at the boundary (a non-empty 0x-hex 32-byte hash) before it
// ever reaches an RPC call. Duplicate tracks are idempotent: re-tracking a hash
// already in flight is a no-op, not a reset of its observed phase.
func (w *Watcher) Track(tx chain.TxHash) error {
	if !isHexTxHash(string(tx)) {
		return fmt.Errorf("evm: track transaction hash is not a valid 0x-hex hash: %w", chain.ErrInvalidIntent)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.tracked[tx]; ok {
		return nil
	}
	w.tracked[tx] = &tracked{phase: PhasePending}
	return nil
}

// Resume re-registers a transaction that a prior process already observed settled,
// seeding it as a Confirmed anchor rather than a fresh Pending track. This is the
// crash-recovery path: a settlement row persisted as settled at (blockHash,
// blockNumber) is re-tracked on restart so an in-flight reorg is still caught,
// without re-emitting the settle that already landed. Seeding lastEmittedPhase =
// PhaseConfirmed / lastEmittedDepth = w.depth means the next canonical poll dedupes
// (no duplicate settle), while a divergent anchor still surfaces as Reorged. Both
// hashes are caller-supplied — a recovered DB row is a trust boundary — so both are
// validated before either reaches the map or an RPC call. Idempotent like Track:
// re-resuming an already-tracked key is a no-op, never a reset of live state.
func (w *Watcher) Resume(tx chain.TxHash, blockHash string, blockNumber uint64) error {
	if !isHexTxHash(string(tx)) {
		return fmt.Errorf("evm: resume transaction hash is not a valid 0x-hex hash: %w", chain.ErrInvalidIntent)
	}
	if !isHexTxHash(blockHash) {
		return fmt.Errorf("evm: resume block hash is not a valid 0x-hex hash: %w", chain.ErrInvalidIntent)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.tracked[tx]; ok {
		return nil
	}
	w.tracked[tx] = &tracked{
		phase:            PhaseConfirmed,
		blockHash:        common.HexToHash(blockHash),
		blockNumber:      blockNumber,
		lastEmittedPhase: PhaseConfirmed,
		lastEmittedDepth: w.depth,
	}
	return nil
}

// Run polls on a ticker until the context is cancelled, logging one redacted line
// per transition surfaced by poll and dispatching each to the sink. It owns no
// channel: poll returns the transitions of the pass and Run logs then forwards
// them inline. A nil sink is log-only — exactly the slice-1 behavior. A sink error
// is logged at error level and does NOT stop the loop: the watcher must keep
// observing (the settlement row is the recovery anchor for a dropped status). It
// returns nil on ctx.Done — a cancelled watcher is a clean shutdown, not an error.
func (w *Watcher) Run(ctx context.Context, sink StatusSink) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, s := range w.poll(ctx) {
				w.logStatus(ctx, s)
				if sink == nil {
					continue
				}
				if err := sink.OnStatus(ctx, s); err != nil {
					w.log.ErrorContext(ctx, "watcher: status sink failed",
						"tx_hash", string(s.TxHash), "phase", s.Phase.String(), "error", err)
					// A failed Confirmed delivery must be retried, or the settlement
					// row never learns it confirmed. Roll the entry back to Mined and
					// clear its emit dedupe so the next poll's Mined branch re-reads the
					// receipt+header and re-emits Confirmed — no extra RPC beyond the
					// normal tick. Guarded on phase == PhaseConfirmed so a poll that
					// already advanced the entry (e.g. to Reorged) is never clobbered.
					// Other failed phases (Reorged, Finalized) need no rollback: their
					// recovery anchor is the persisted row, re-derived on the next pass.
					if s.Phase == PhaseConfirmed {
						w.mu.Lock()
						if t, ok := w.tracked[s.TxHash]; ok && t.phase == PhaseConfirmed {
							t.phase = PhaseMined
							t.lastEmittedPhase = PhasePending // zero value: force re-emit
							t.lastEmittedDepth = 0
						}
						w.mu.Unlock()
					}
				}
			}
		}
	}
}

// poll runs exactly one synchronous pass and returns the transitions observed
// this pass, in order. It is the deterministically-tested seam: no timers, no
// goroutines, no wall clock — a test drives the lifecycle by calling it directly
// with a scripted reader. A transient RPC error (anything but ethereum.NotFound
// on a receipt/header lookup) is logged and skipped, never mistaken for a reorg;
// the tx is left untouched and retried next pass.
func (w *Watcher) poll(ctx context.Context) []Status {
	head, err := w.reader.BlockNumber(ctx)
	if err != nil {
		// Cannot price confirmations without the head; skip the whole pass rather
		// than advance anything against a stale number.
		w.log.WarnContext(ctx, "watcher: head query failed", "error", RedactRPCError(err))
		return nil
	}

	// Snapshot every tracked key under the lock, then do all RPC OUTSIDE it —
	// holding the mutex across network I/O would serialize Track and pin every
	// caller behind the slowest node round-trip (mirrors the adapter pricing gas
	// outside the nonce lock). Confirmed entries are observed too: since M3 slice 2
	// Confirmed is non-terminal, its anchor is re-verified each pass so a deep
	// reorg can still reverse it. The tracked set is bounded by the finality-depth
	// eviction in the Confirmed branch: once an anchor is buried >= finalityDepth
	// deep it is surfaced as PhaseFinalized and deleted.
	w.mu.Lock()
	keys := make([]chain.TxHash, 0, len(w.tracked))
	for tx := range w.tracked {
		keys = append(keys, tx)
	}
	w.mu.Unlock()

	var emitted []Status

	for _, tx := range keys {
		w.mu.Lock()
		t, ok := w.tracked[tx]
		var phase Phase
		var bHash common.Hash
		var bNum uint64
		if ok {
			phase, bHash, bNum = t.phase, t.blockHash, t.blockNumber
		}
		w.mu.Unlock()
		if !ok {
			continue
		}
		hash := common.HexToHash(string(tx))

		switch phase {
		case PhasePending:
			receipt, err := w.reader.TransactionReceipt(ctx, hash)
			if err != nil {
				if !errors.Is(err, ethereum.NotFound) {
					w.log.WarnContext(ctx, "watcher: receipt query failed", "tx_hash", string(tx), "error", RedactRPCError(err))
				}
				continue // NotFound or transient: stay pending, retry next tick
			}
			if receipt == nil {
				continue
			}
			h := receipt.BlockNumber.Uint64()
			depth := confirmations(head, h)
			if depth == 0 {
				depth = 1 // mined ⇒ at least one confirmation, even if head lags
			}
			w.mu.Lock()
			t.blockHash = receipt.BlockHash
			t.blockNumber = h
			if depth >= w.depth {
				// Already buried deep enough on first sighting (N=1, or a backlog
				// catch-up where head ran far ahead): confirm in this same pass
				// rather than making the caller wait a further tick at depth N+1.
				t.phase = PhaseConfirmed
				w.emit(t, tx, PhaseConfirmed, receipt.BlockHash, h, depth, &emitted)
				w.mu.Unlock()
				continue
			}
			t.phase = PhaseMined
			w.emit(t, tx, PhaseMined, receipt.BlockHash, h, depth, &emitted)
			w.mu.Unlock()

		case PhaseMined:
			receipt, err := w.reader.TransactionReceipt(ctx, hash)
			if err != nil {
				if errors.Is(err, ethereum.NotFound) {
					// The receipt is gone: the block it was in is no longer canonical.
					w.mu.Lock()
					w.reverse(t, tx, bHash, bNum, &emitted)
					w.mu.Unlock()
					continue
				}
				w.log.WarnContext(ctx, "watcher: receipt query failed", "tx_hash", string(tx), "error", RedactRPCError(err))
				continue // transient: a transport fault is NOT a reorg
			}
			if receipt == nil {
				continue
			}
			hdr, err := w.reader.HeaderByNumber(ctx, new(big.Int).SetUint64(bNum))
			if err != nil {
				w.log.WarnContext(ctx, "watcher: header query failed", "tx_hash", string(tx), "error", RedactRPCError(err))
				continue // transient: NOT a reorg
			}
			if hdr == nil {
				continue
			}
			if hdr.Hash() != bHash {
				// The canonical block at our recorded height is a different block:
				// the tx was re-organized onto another chain.
				w.mu.Lock()
				w.reverse(t, tx, bHash, bNum, &emitted)
				w.mu.Unlock()
				continue
			}
			if head < bNum {
				continue // head lagging behind the receipt; advance nothing this pass
			}
			depth := head - bNum + 1
			if depth >= w.depth {
				w.mu.Lock()
				t.phase = PhaseConfirmed
				w.emit(t, tx, PhaseConfirmed, bHash, bNum, depth, &emitted)
				w.mu.Unlock()
				continue
			}
			// Still mined, deeper than before: emit only if the depth actually grew.
			w.mu.Lock()
			w.emit(t, tx, PhaseMined, bHash, bNum, depth, &emitted)
			w.mu.Unlock()

		case PhaseConfirmed:
			// Confirmed is non-terminal: re-verify the anchor every pass so a deep
			// reorg can still reverse a confirmed tx. Steady state emits nothing —
			// the confirmation was already surfaced, and depth climbing further is
			// not a new transition — so only positive evidence the tx left the
			// canonical chain (a vanished receipt or a divergent canonical header)
			// emits again, as Reorged. A transient read failure is never that
			// evidence: it is logged and skipped, preserving the ADR-0028 invariant
			// that a transport fault never manufactures a Reorged.
			receipt, err := w.reader.TransactionReceipt(ctx, hash)
			if err != nil {
				if errors.Is(err, ethereum.NotFound) {
					w.mu.Lock()
					w.reverse(t, tx, bHash, bNum, &emitted)
					w.mu.Unlock()
					continue
				}
				w.log.WarnContext(ctx, "watcher: receipt query failed", "tx_hash", string(tx), "error", RedactRPCError(err))
				continue // transient: a transport fault is NOT a reorg
			}
			if receipt == nil {
				continue
			}
			hdr, err := w.reader.HeaderByNumber(ctx, new(big.Int).SetUint64(bNum))
			if err != nil {
				w.log.WarnContext(ctx, "watcher: header query failed", "tx_hash", string(tx), "error", RedactRPCError(err))
				continue // transient: NOT a reorg
			}
			if hdr == nil {
				continue
			}
			if hdr.Hash() != bHash {
				w.mu.Lock()
				w.reverse(t, tx, bHash, bNum, &emitted)
				w.mu.Unlock()
				continue
			}
			// Anchor still canonical: the confirmation holds. Once it is buried at
			// least finalityDepth deep, a reversing reorg is treated as impossible —
			// surface a terminal PhaseFinalized and evict the entry so the tracked set
			// stays bounded (the growth slice-2 left as a documented follow-up). Only
			// reachable on the canonical path: a divergent header already reversed and
			// continued above, so finality never races a reorg. head >= bNum guards the
			// depth subtraction against a transiently lagging head.
			if head >= bNum && head-bNum+1 >= w.finalityDepth {
				w.mu.Lock()
				w.emit(t, tx, PhaseFinalized, bHash, bNum, head-bNum+1, &emitted)
				delete(w.tracked, tx)
				w.mu.Unlock()
			}
		}
	}

	return emitted
}

// reverse records a reorg of a tracked tx: it emits Reorged against the old anchor
// then resets the entry to Pending semantics — clearing the block anchor and (via
// emit) leaving lastEmittedPhase=PhaseReorged / lastEmittedDepth=0 — so a
// subsequent re-mine is observed fresh as Mined→Confirmed. This is what makes
// Confirmed/Reorged non-terminal: the tx keeps being watched across a
// reverse+reapply cycle instead of being dropped from the tracked map. Callers
// hold w.mu. It is only ever reached on positive evidence the tx left the
// canonical chain (a vanished receipt or a divergent canonical header), never on a
// transient read failure — preserving the ADR-0028 no-reorg-on-transport-fault
// invariant.
func (w *Watcher) reverse(t *tracked, tx chain.TxHash, bHash common.Hash, bNum uint64, out *[]Status) {
	w.emit(t, tx, PhaseReorged, bHash, bNum, 0, out)
	t.phase = PhasePending
	t.blockHash = common.Hash{}
	t.blockNumber = 0
}

// emit appends a Status only when it represents a real transition — a phase
// change or a strictly deeper confirmation than the last one surfaced for this
// tx — deduping the repeated observations a steady poll produces at an unchanged
// depth. Callers hold w.mu.
func (w *Watcher) emit(t *tracked, tx chain.TxHash, phase Phase, bHash common.Hash, bNum, depth uint64, out *[]Status) {
	if phase == t.lastEmittedPhase && depth <= t.lastEmittedDepth {
		return
	}
	t.lastEmittedPhase = phase
	t.lastEmittedDepth = depth
	blockHash := ""
	if bNum != 0 {
		blockHash = bHash.Hex()
	}
	*out = append(*out, Status{
		TxHash:      tx,
		Phase:       phase,
		BlockHash:   blockHash,
		BlockNumber: bNum,
		Depth:       depth,
	})
}

// logStatus emits one structured line per transition. Every field here is public
// chain data — a phase label, the tx hash, the block it landed in, its depth —
// so there is nothing to redact; it carries no amount, recipient, or key material
// (mirrors the adapter's logResult discipline). A reorg is a warn; everything
// else is info.
func (w *Watcher) logStatus(ctx context.Context, s Status) {
	attrs := []any{
		"phase", s.Phase.String(),
		"tx_hash", string(s.TxHash),
		"block_hash", s.BlockHash,
		"block_number", s.BlockNumber,
		"depth", s.Depth,
	}
	switch s.Phase {
	case PhaseReorged:
		w.log.WarnContext(ctx, "watcher: transaction reorged", attrs...)
	case PhaseConfirmed:
		w.log.InfoContext(ctx, "watcher: transaction confirmed", attrs...)
	default:
		w.log.InfoContext(ctx, "watcher: transaction observed", attrs...)
	}
}

// confirmations is the depth of a tx mined at height mined given a chain head:
// the mined block itself counts as one confirmation. It returns 0 when the head
// lags behind the receipt (a transiently stale head), so callers can choose to
// hold rather than advance.
func confirmations(head, mined uint64) uint64 {
	if head < mined {
		return 0
	}
	return head - mined + 1
}

// RedactRPCError renders an RPC transport error safe to log. go-ethereum's
// HTTP(S) transport wraps failures in *url.Error, whose Error() embeds the full
// request URL — and managed-node endpoints (Infura, Alchemy, …) carry the API
// key inside that URL's path or query. Logging err.Error() raw on a routine
// node-unreachable tick would leak that key. We keep the operation and the
// underlying cause but reduce the URL to scheme://host, so the secret never
// reaches the logs. Non-URL errors pass through unchanged.
func RedactRPCError(err error) string {
	if err == nil {
		return ""
	}
	var uerr *url.Error
	if errors.As(err, &uerr) {
		endpoint := uerr.URL
		if u, perr := url.Parse(uerr.URL); perr == nil && u.Host != "" {
			endpoint = u.Scheme + "://" + u.Host // drop userinfo, path, and query
		}
		return fmt.Sprintf("%s %s: %v", uerr.Op, endpoint, uerr.Err)
	}
	return err.Error()
}

// isHexTxHash reports whether s is a 0x-prefixed 32-byte (64 hex digit)
// transaction hash. It is the Track boundary check — the same shape common.Hash
// round-trips — so a malformed hash never reaches an RPC call.
func isHexTxHash(s string) bool {
	if len(s) != 66 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return false
	}
	_, err := hex.DecodeString(s[2:])
	return err == nil
}
