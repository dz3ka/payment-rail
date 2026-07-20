package evm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"github.com/dz3ka/payment-rail/internal/chain"
)

// Compile-time proof both the live JSON-RPC client and the simulated backend's
// client satisfy the read-only chainReader seam. If go-ethereum changes a
// signature this fails to compile and we fix the interface — we never cast.
var (
	_ chainReader = (*ethclient.Client)(nil)
	_ chainReader = (simulated.Client)(nil)
)

// testTxHash is a well-formed 0x-hex 32-byte hash the unit tests track.
var testTxHash = chain.TxHash("0x" + strings.Repeat("ab", 32))

// testLogger keeps the deterministic unit tests quiet.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeReader is a scripted chainReader. blockNumber is a plain field a test
// advances between poll calls (no wall clock); receiptFn/headerFn are closures so
// a test sequences pending→mined→confirmed and mined→reorg exactly — the
// fakeSigner closure idiom from fakes_test.go, one seam per chain read.
type fakeReader struct {
	blockNumber    uint64
	blockNumberErr error

	receiptFn func(ctx context.Context, hash common.Hash) (*types.Receipt, error)
	headerFn  func(ctx context.Context, number *big.Int) (*types.Header, error)
}

func newFakeReader() *fakeReader {
	return &fakeReader{
		receiptFn: notFoundReceipt,
		headerFn:  func(context.Context, *big.Int) (*types.Header, error) { return nil, ethereum.NotFound },
	}
}

func (f *fakeReader) BlockNumber(context.Context) (uint64, error) {
	return f.blockNumber, f.blockNumberErr
}

func (f *fakeReader) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	return f.receiptFn(ctx, hash)
}

func (f *fakeReader) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return f.headerFn(ctx, number)
}

// notFoundReceipt is the "not mined yet / gone" receipt outcome.
func notFoundReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return nil, ethereum.NotFound
}

// foundReceipt returns a receipt placing the tx in the given block.
func foundReceipt(blockNum uint64, blockHash common.Hash) func(context.Context, common.Hash) (*types.Receipt, error) {
	return func(context.Context, common.Hash) (*types.Receipt, error) {
		return &types.Receipt{BlockNumber: new(big.Int).SetUint64(blockNum), BlockHash: blockHash}, nil
	}
}

// staticHeader always returns hdr — the canonical block at the queried height.
func staticHeader(hdr *types.Header) func(context.Context, *big.Int) (*types.Header, error) {
	return func(context.Context, *big.Int) (*types.Header, error) { return hdr, nil }
}

// makeHeader builds a header whose Hash() is distinct per tag, so a test can make
// the canonical block at a height "change" by swapping the tag.
func makeHeader(num uint64, tag byte) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(num), Extra: []byte{tag}}
}

// requireStatuses asserts the pass surfaced exactly the given phases in order.
func requirePhases(t *testing.T, got []Status, want ...Phase) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("poll emitted %d statuses %v, want %d %v", len(got), phasesOf(got), len(want), want)
	}
	for i, w := range want {
		if got[i].Phase != w {
			t.Fatalf("status[%d].Phase = %s, want %s", i, got[i].Phase, w)
		}
	}
}

func phasesOf(ss []Status) []Phase {
	out := make([]Phase, len(ss))
	for i, s := range ss {
		out[i] = s.Phase
	}
	return out
}

func TestWatcherDrivesPendingToConfirmed(t *testing.T) {
	ctx := context.Background()
	r := newFakeReader()
	w, err := NewWatcher(r, 3, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	hdr := makeHeader(10, 'a')
	blockHash := hdr.Hash()

	// Pass 1: not yet mined — pending, no emit.
	r.blockNumber = 10
	requirePhases(t, w.poll(ctx)) // empty

	// Pass 2: mined at block 10, head 10 ⇒ depth 1 ⇒ Mined.
	r.receiptFn = foundReceipt(10, blockHash)
	r.headerFn = staticHeader(hdr)
	got := w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].Depth != 1 || got[0].BlockNumber != 10 || got[0].BlockHash != blockHash.Hex() {
		t.Fatalf("mined status = %+v, want depth 1 block 10 hash %s", got[0], blockHash.Hex())
	}

	// Pass 3: head 11 ⇒ depth 2 ⇒ Mined progress.
	r.blockNumber = 11
	got = w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].Depth != 2 {
		t.Fatalf("progress depth = %d, want 2", got[0].Depth)
	}

	// Pass 4: head 12 ⇒ depth 3 == N ⇒ Confirmed.
	r.blockNumber = 12
	got = w.poll(ctx)
	requirePhases(t, got, PhaseConfirmed)
	if got[0].Depth != 3 {
		t.Fatalf("confirmed depth = %d, want 3", got[0].Depth)
	}

	// Pass 5: terminal ⇒ dropped ⇒ silent.
	requirePhases(t, w.poll(ctx))
}

func TestWatcherConfirmationDepthConfigurable(t *testing.T) {
	ctx := context.Background()
	hdr := makeHeader(5, 'a')
	blockHash := hdr.Hash()

	// driveToConfirm mines the tx at block 5 then advances the head one block per
	// poll, returning the head at which Confirmed was surfaced.
	driveToConfirm := func(t *testing.T, depth uint64) uint64 {
		t.Helper()
		r := newFakeReader()
		r.receiptFn = foundReceipt(5, blockHash)
		r.headerFn = staticHeader(hdr)
		w, err := NewWatcher(r, depth, time.Second, testLogger())
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Track(testTxHash); err != nil {
			t.Fatalf("Track: %v", err)
		}
		for head := uint64(5); head < 100; head++ {
			r.blockNumber = head
			for _, s := range w.poll(ctx) {
				if s.Phase == PhaseConfirmed {
					if s.Depth != depth {
						t.Fatalf("N=%d confirmed at depth %d, want %d", depth, s.Depth, depth)
					}
					return head
				}
			}
		}
		t.Fatalf("N=%d never confirmed", depth)
		return 0
	}

	head1 := driveToConfirm(t, 1)
	head3 := driveToConfirm(t, 3)
	// N=1 confirms as soon as the mined block is head (block 5); N=3 needs two more.
	if head1 != 5 {
		t.Fatalf("N=1 confirmed at head %d, want 5", head1)
	}
	if head3 != 7 {
		t.Fatalf("N=3 confirmed at head %d, want 7", head3)
	}
	if head3 <= head1 {
		t.Fatalf("expected the deeper threshold to confirm later: N=1 at %d, N=3 at %d", head1, head3)
	}
}

func TestWatcherEmitsMinedProgressAsDepthClimbs(t *testing.T) {
	ctx := context.Background()
	r := newFakeReader()
	hdr := makeHeader(10, 'a')
	r.receiptFn = foundReceipt(10, hdr.Hash())
	r.headerFn = staticHeader(hdr)
	w, err := NewWatcher(r, 100, time.Second, testLogger()) // N high so it never confirms
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	// Mined at depth 1.
	r.blockNumber = 10
	got := w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].Depth != 1 {
		t.Fatalf("first depth = %d, want 1", got[0].Depth)
	}

	// Same head again ⇒ unchanged depth ⇒ deduped, no emit.
	requirePhases(t, w.poll(ctx))

	// Head advances ⇒ depth 2 ⇒ one emit.
	r.blockNumber = 11
	got = w.poll(ctx)
	requirePhases(t, got, PhaseMined)
	if got[0].Depth != 2 {
		t.Fatalf("second depth = %d, want 2", got[0].Depth)
	}

	// Same head again ⇒ deduped once more.
	requirePhases(t, w.poll(ctx))
}

func TestWatcherFlagsReorgOnReceiptDisappearance(t *testing.T) {
	ctx := context.Background()
	r := newFakeReader()
	hdr := makeHeader(10, 'a')
	r.receiptFn = foundReceipt(10, hdr.Hash())
	r.headerFn = staticHeader(hdr)
	w, err := NewWatcher(r, 5, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	r.blockNumber = 10
	requirePhases(t, w.poll(ctx), PhaseMined)

	// The receipt vanishes: the mined block dropped out of the canonical chain.
	r.receiptFn = notFoundReceipt
	requirePhases(t, w.poll(ctx), PhaseReorged)

	// Terminal: no further emits, even if a receipt reappears.
	r.receiptFn = foundReceipt(10, hdr.Hash())
	requirePhases(t, w.poll(ctx))
}

func TestWatcherFlagsReorgOnCanonicalHashChange(t *testing.T) {
	ctx := context.Background()
	r := newFakeReader()
	original := makeHeader(10, 'a')
	r.receiptFn = foundReceipt(10, original.Hash())
	r.headerFn = staticHeader(original)
	w, err := NewWatcher(r, 5, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	r.blockNumber = 10
	requirePhases(t, w.poll(ctx), PhaseMined)

	// The canonical block at height 10 is now a different block (receipt still
	// reports the old block hash, but the chain has moved).
	r.headerFn = staticHeader(makeHeader(10, 'b'))
	requirePhases(t, w.poll(ctx), PhaseReorged)

	// Terminal.
	requirePhases(t, w.poll(ctx))
}

func TestWatcherIgnoresTransientRPCError(t *testing.T) {
	ctx := context.Background()
	r := newFakeReader()
	hdr := makeHeader(10, 'a')
	r.receiptFn = foundReceipt(10, hdr.Hash())
	r.headerFn = staticHeader(hdr)
	w, err := NewWatcher(r, 2, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	r.blockNumber = 10
	requirePhases(t, w.poll(ctx), PhaseMined)

	boom := errors.New("rpc: connection reset")

	// A receipt transport error must NOT flag a reorg — the tx is unchanged.
	r.receiptFn = func(context.Context, common.Hash) (*types.Receipt, error) { return nil, boom }
	requirePhases(t, w.poll(ctx))

	// A header transport error likewise leaves the tx untouched.
	r.receiptFn = foundReceipt(10, hdr.Hash())
	r.headerFn = func(context.Context, *big.Int) (*types.Header, error) { return nil, boom }
	requirePhases(t, w.poll(ctx))

	// Recovered: the chain moved on and the tx confirms as if the blips never happened.
	r.headerFn = staticHeader(hdr)
	r.blockNumber = 11 // depth 2 == N
	requirePhases(t, w.poll(ctx), PhaseConfirmed)
}

func TestWatcherRunReturnsNilOnContextCancel(t *testing.T) {
	w, err := NewWatcher(newFakeReader(), 1, time.Millisecond, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after context cancel")
	}
}

func TestRedactRPCErrorStripsEndpointURL(t *testing.T) {
	secret := "SUPERSECRETAPIKEY123"
	// The shape go-ethereum's HTTP transport actually returns: a *url.Error whose
	// URL carries the API key in the path.
	urlErr := &url.Error{
		Op:  "Post",
		URL: "https://mainnet.example.com/v3/" + secret,
		Err: errors.New("dial tcp: lookup mainnet.example.com: no such host"),
	}
	got := RedactRPCError(urlErr)
	if strings.Contains(got, secret) {
		t.Fatalf("RedactRPCError leaked the API key: %q", got)
	}
	if !strings.Contains(got, "mainnet.example.com") {
		t.Errorf("RedactRPCError dropped the host (needed for triage): %q", got)
	}

	// Key hidden in the query string is stripped too.
	q := &url.Error{Op: "Post", URL: "https://rpc.example.com/?key=" + secret, Err: errors.New("timeout")}
	if got := RedactRPCError(q); strings.Contains(got, secret) {
		t.Fatalf("RedactRPCError leaked a query-string key: %q", got)
	}

	// A plain (non-URL) error passes through untouched.
	plain := errors.New("rpc: connection reset")
	if got := RedactRPCError(plain); got != plain.Error() {
		t.Errorf("RedactRPCError mangled a plain error: got %q, want %q", got, plain.Error())
	}
	if RedactRPCError(nil) != "" {
		t.Error("RedactRPCError(nil) should be empty")
	}
}

func TestNewWatcherValidatesConfig(t *testing.T) {
	if _, err := NewWatcher(nil, 1, time.Second, testLogger()); err == nil {
		t.Error("nil reader accepted, want error")
	}
	if _, err := NewWatcher(newFakeReader(), 0, time.Second, testLogger()); err == nil {
		t.Error("zero depth accepted, want error")
	}
	if _, err := NewWatcher(newFakeReader(), 1, 0, testLogger()); err == nil {
		t.Error("zero interval accepted, want error")
	}
}

func TestWatcherTrackValidatesAndIsIdempotent(t *testing.T) {
	w, err := NewWatcher(newFakeReader(), 1, time.Second, testLogger())
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	for _, bad := range []chain.TxHash{"", "0x", "not-hex", "0x123", chain.TxHash("0x" + strings.Repeat("zz", 32))} {
		if err := w.Track(bad); !errors.Is(err, chain.ErrInvalidIntent) {
			t.Errorf("Track(%q) err = %v, want ErrInvalidIntent", bad, err)
		}
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track valid: %v", err)
	}
	if err := w.Track(testTxHash); err != nil {
		t.Fatalf("Track duplicate should be idempotent, got %v", err)
	}
}
