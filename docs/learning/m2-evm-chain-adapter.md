# M2 — The EVM chain adapter: a lock that spans the network, ports across a process boundary, and arithmetic that refuses to wrap

> Scope: this lesson is about *turning a chain-neutral payment intent into a signed,
> broadcast EIP-1559 ERC-20 transfer* without ever holding a key and without ever
> handing out a duplicate nonce. The headline achievements are (1) a
> **concurrency-safe, chain-authoritative nonce allocator** whose `sync.Mutex` is held
> across the *entire* `{query pending → sign → broadcast → advance}` critical section,
> committing the high-water only on success so a failed broadcast can never open a gap;
> (2) a **ports-and-adapters seam that reaches across a process boundary** — the adapter
> depends on its own `evm.Signer` interface with its own DTOs, and the proto↔domain
> mapping lives alone in the composition root; (3) **`*big.Int` discipline** — the fee
> cap is copied at construction and every fee is freshly allocated, because Go's
> `*big.Int` is a *mutable pointer*, not a value; and (4) an **overflow-safe gas buffer**
> using `math/bits.Mul64`, because a hostile RPC estimate near 2⁶⁴ could otherwise wrap
> a `uint64` multiply into a small number that slips under the cap. This builds directly
> on the M2 signer lesson — the nonce lock is the same `{check → act → commit}` shape as
> `spendBucket.charge`, and the `*big.Int` copy is the same hazard `deepCopy` guards.
> Skim `m2-isolated-grpc-signer.md` if that pattern isn't yet reflexive.

## 1. What we built

M2 slice 2 is the component that actually moves value on-chain. `internal/chain` defines
a deliberately tiny, dependency-free **port** — `chain.Adapter`, one method `Submit(ctx,
PaymentIntent) (TxHash, error)` — expressed in terms every chain can honor (a key id, an
asset symbol, a recipient string, an amount in smallest units). `internal/chain/evm` is
the first (and so far only) implementation. Given a `PaymentIntent{Asset:"USDC", To:...,
Amount:...}`, the EVM adapter validates it, packs a 68-byte ERC-20 `transfer(address,
uint256)` calldata, prices gas and fees under hard operator caps, allocates a
chain-authoritative nonce, asks the *isolated signer* (from slice 1) to sign the
`DynamicFeeTx` over gRPC, and broadcasts the resulting RLP bytes to a JSON-RPC node. The
whole thing is driven by a new `paymentrailctl submit --to ... --amount ...` CLI.

The spine of the design is the same domain/transport split the signer used, now applied
one layer out. The `evm` package is **proto-free and ethclient-concrete-free**: it talks
to the signer through an `evm.Signer` interface it owns, and to the chain through an
`ethRPC` interface it owns. Neither the generated gRPC types nor a concrete
`*ethclient.Client` appear anywhere in the adapter's logic. The composition root
(`cmd/paymentrailctl`) is the only place that knows both dialects: `signerclient.go`
implements `evm.Signer` by mapping the adapter's DTOs onto the `signerpb` wire and back,
and `submit.go` dials the real signer and the real node and wires them in.

The part to study hardest is the **nonce allocator**, because nonces are the one place
where Ethereum forces strict per-sender serialization and a newcomer's instinct —
"just ask the node for the next nonce" — produces duplicate-nonce collisions under any
concurrency. The allocator holds a single mutex across the network calls that sign and
broadcast, takes `max(localHighWater, PendingNonceAt)`, and advances the high-water only
on a committed success. The gate test `TestWithNonceConcurrentUniqueIncreasing` fires 50
concurrent allocations for one sender and asserts unique, gap-free, strictly increasing
nonces under `-race`; the full-wire e2e proves the same property across real gRPC and a
real in-process EVM.

## 2. The design decision

### Decision A: the nonce lock spans the network, and the high-water commits only on success

**The problem.** Every Ethereum account has a nonce: a strictly sequential per-sender
counter starting at 0, and the chain will only include transaction *n* after it has
included *n−1*. Miss a number and every later transaction for that account is stuck
("wedged") in the mempool until the gap is filled. Send two transactions with the *same*
nonce and only one can win; the other is rejected (or, worse, silently replaces the
first). So allocation must guarantee two properties simultaneously: **uniqueness** (no
two live transactions share a nonce) and **gap-freedom** (no allocated-but-unused nonce
strands the ones behind it). Both must hold under concurrent `Submit` calls for the same
sender.

**The chosen approach — one mutex, held across the whole `{query → sign → broadcast →
advance}` section, advancing only on success.** `withNonce` locks, reads
`PendingNonceAt`, takes the max with a local per-sender high-water, runs the caller's
`fn(nonce)` (which signs *and* broadcasts) still holding the lock, and only if `fn`
returns nil does it set `next[from] = nonce + 1`. This is the exact `{check → act →
commit}` shape as the signer's `spendBucket.charge`, and for the same reason: the whole
sequence is one indivisible critical section per sender.

```go
func (n *nonceAllocator) withNonce(ctx context.Context, from common.Address, fn func(nonce uint64) error) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	pending, err := n.rpc.PendingNonceAt(ctx, from)
	if err != nil {
		return fmt.Errorf("evm: query pending nonce: %w", ErrNonceUnavailable)
	}
	nonce := pending
	if hw := n.next[from]; hw > nonce {
		nonce = hw
	}
	if err := fn(nonce); err != nil {
		return err // commit nothing: the nonce stays free for the next caller
	}
	n.next[from] = nonce + 1
	return nil
}
```

Two design decisions inside this deserve a name. First, `nonce = max(localHighWater,
PendingNonceAt)` makes the allocator **chain-authoritative and self-healing**:
`PendingNonceAt` is the node's count of transactions it has *seen* (mined + pending), so
it's correct across a process restart (the map starts empty; the first allocation
re-seeds from the chain) and if a nonce is consumed out of band. The local high-water
covers the window the node *hasn't* caught up on yet — the moment between "we broadcast
nonce N" and "the txpool reports nonce N+1 as pending." Second, **commit-on-success**:
advancing only when `fn` returns nil means a failed sign or broadcast leaves the nonce
free, so the next caller reuses it. That's what keeps the allocation gap-free by
construction. Restart re-seeds from the chain, so the in-memory map is fail-safe, not a
durability requirement.

**Alternative 1: return `PendingNonceAt` alone, no lock, no high-water.** This is the
newcomer default and it's broken under concurrency. Two goroutines call `PendingNonceAt`
back-to-back *before either has broadcast*; the txpool hasn't seen the first transaction
yet, so both read the same value, both sign nonce N, and one loses. The high-water exists
precisely to bridge the observability gap between "we've committed to N" and "the node
reports N as pending."

**Alternative 2: advance the high-water at allocation time (optimistic), release the lock
before signing.** This fixes uniqueness but breaks gap-freedom and liveness. If you bump
`next[from]` the instant you hand out a nonce and *then* the broadcast fails, you've
burned that nonce: nothing will ever occupy it, and every later transaction for the
account wedges behind the hole. You'd need a compensating "return the nonce" path with
its own races. Holding the lock across the broadcast and committing only on success gives
you both properties with no compensation logic.

**The cost, stated honestly.** Holding the lock across the signer round-trip means one
slow signer call *serializes every other submit for that sender*. That's a real liveness
tax — but it's inherent, not incidental: nonces are serial per sender by definition, so
you cannot parallelize same-sender submission without reintroducing the collision. Note
what the code does *not* serialize: different senders never contend (the map is keyed by
address), and gas pricing happens *outside* the lock in `Submit` (step 3) because it
doesn't depend on the nonce and mustn't hold up other senders' allocations. Releasing the
lock earlier — say, right after reading the nonce — would reintroduce Alternative 1's
race, so "the lock spans the network calls" is load-bearing, not lazy.

### Decision B: ports and adapters *across a process boundary* — duplicate the boundary DTO

**The problem.** The adapter needs to call the signer, which lives in another process
behind gRPC. The tempting shortcut is to have the adapter speak `signerpb` (the generated
types) directly, or import `internal/signer`'s domain types. Both couple the adapter to
something it shouldn't own.

**The chosen approach — the adapter defines its own `evm.Signer` interface with its own
`SignerRequest`/`SignedTx` DTOs; the composition root maps them to the wire.** The `evm`
package declares:

```go
type SignerRequest struct {
	KeyID                                     string
	ChainID, Nonce, GasLimit                  uint64
	To                                        common.Address
	Value, MaxFeePerGas, MaxPriorityFeePerGas *big.Int
	Data                                      []byte
}
type Signer interface {
	Sign(ctx context.Context, req SignerRequest) (SignedTx, error)
}
```

The production implementation — `signerClient` in `cmd/paymentrailctl/signerclient.go` —
is the *only* code that imports both `evm` and `signerpb`. This mirrors exactly how
slice 1 kept `internal/signer` proto-free with its adapter in `cmd/signer`: the same rule,
applied to the *client* side. The adapter is a gRPC *client* here, and coupling a client
to the server's internal domain types (or dragging generated proto into a domain package)
is precisely the entanglement the port prevents.

**Why duplicate a struct instead of sharing one?** Because the alternative is worse. If
`evm.SignerRequest` *were* `signer.SignRequest`, then `internal/chain/evm` would import
`internal/signer` — a package that loads private keys — just to name a request shape. If
it were `signerpb.SignTransactionRequest`, then the domain logic would be pinned to a wire
encoding (addresses as `[]byte`, amounts as `[]byte`) and to protobuf's regeneration
cycle. The DTO is small and its cost is a few lines of mapping in one file; the coupling
you'd trade it for is structural and permanent. This is "duplication is cheaper than the
wrong abstraction" applied at a boundary — the boundary DTO is a *feature*, not debt.

There's a second, subtler payoff visible in the full-wire test: because the adapter owns
its port, the test can stand up the *real* `signerClient` against an in-memory bufconn
gRPC server that mirrors the real domain, and drive the *real* adapter through it. The
seam is what makes the whole stack testable hermetically without a network.

### Decision C: uint256 crosses the wire as big-endian bytes, not a decimal string

**The problem.** Ethereum amounts are `uint256` — they do not fit any Go integer, so
they're `*big.Int` in the domain. But protobuf has no 256-bit type. How do you carry a
`*big.Int` across the wire without losing a single wei at a money boundary?

**The chosen approach — canonical minimal big-endian bytes, `(*big.Int).Bytes()` on the
way out and `new(big.Int).SetBytes` on the way back.** In `signerclient.go`:

```go
Value:        req.Value.Bytes(),        // minimal big-endian
MaxFeePerGas: req.MaxFeePerGas.Bytes(), // minimal big-endian
To:           req.To.Bytes(),           // exactly 20 bytes
```

`(*big.Int).Bytes()` returns the *minimal* big-endian encoding (no leading zeros); the
signer's boundary does `new(big.Int).SetBytes(b)`, the exact inverse, so a value
round-trips octet-for-octet. A zero value encodes as an *empty slice*, which `SetBytes`
reads back as `0` — a small edge that both sides agree on. `common.Address.Bytes()` is
always exactly 20 bytes, which is why the server boundary length-checks `len(To) == 20`
before the lossy `BytesToAddress`.

**Why not a decimal string?** Two parties parsing/formatting a decimal string is *two
opportunities to disagree* — leading zeros, sign handling, radix, locale, an accidental
`float` somewhere in the chain. Bytes are the value's own representation: there is nothing
to parse and nothing to round. At a precision-sensitive money boundary you want the
encoding with the fewest degrees of freedom. The full-wire e2e is the proof that the
round-trip is exact: it signs a real transaction over real gRPC and recovers the sender
from the mined transaction — if a single byte of any amount had shifted, the signature
wouldn't verify and `types.Sender` would recover the wrong address.

### Decision D: own your `*big.Int` — copy at construction, allocate fresh on every use

**The problem.** Go's `*big.Int` is a *pointer to a mutable struct*, not a value. If you
store a caller's `*big.Int` and the caller later mutates it (`x.Add(x, ...)`), your stored
"constant" moves under you. At a boundary that enforces a *cap*, that's a way to defeat
the cap after the fact.

**The chosen approach — defensive copy of the cap at construction; fresh allocation for
every derived fee.** `NewAdapter` copies the fee cap so the adapter owns it:

```go
// Own the fee cap: copy it so a later mutation of the caller's *big.Int cannot
// move the ceiling every Submit prices against.
cfg.MaxFeePerGasCapWei = new(big.Int).Set(cfg.MaxFeePerGasCapWei)
```

And `gasEstimate` never mutates the values the RPC handed it — the header's base fee and
the suggested tip are read-only inputs, so `maxFee` is a fresh allocation:

```go
// maxFee = baseFee*headroom + tip, all freshly allocated so we never mutate
// the header's base fee or the tip the RPC handed us.
maxFee := new(big.Int).Mul(header.BaseFee, big.NewInt(baseFeeHeadroom))
maxFee.Add(maxFee, tip)
```

`new(big.Int).Mul(a, b)` computes `a*b` into a *new* receiver, touching neither `a` nor
`b`. Had this been written `header.BaseFee.Mul(header.BaseFee, ...)`, it would have
*rewritten the header's base fee in place* — corrupting a value go-ethereum may cache or
reuse. This is the same discipline the signer's `deepCopy` applies at its key-holding
boundary; here it protects a cap and a shared library value rather than a spend budget.

**Where copies are load-bearing vs wasteful.** The cap copy is load-bearing: it's stored
long-term and priced against on every `Submit`, so it must not alias caller state.
Allocating `maxFee` fresh is load-bearing: it must not clobber `header.BaseFee`. But you
would *not* copy a `*big.Int` you only read once and discard — that's cargo-culting.
The rule is "copy when you retain, or when you'd otherwise mutate a value you don't own,"
not "copy always."

### Decision E: refuse to let a gas multiply wrap — `bits.Mul64` at an untrusted boundary

**The problem.** The gas estimate comes from the RPC node, which is *untrusted input*.
The adapter inflates it by 25% (`estimate * 125 / 100`) to give the transaction headroom
to mine. But `estimate` is a `uint64`, and a hostile or buggy node could return a value
near 2⁶⁴ whose product with 125 *wraps modulo 2⁶⁴* to a small number — one that would slip
under `GasLimitCap` and get signed as a badly under-gassed transaction (which reverts
out-of-gas and still costs money).

**The chosen approach — compute the full 128-bit product with `math/bits.Mul64` and
reject if the high word is non-zero.**

```go
hi, lo := bits.Mul64(estimate, gasLimitBufferPct)
if hi != 0 {
	return gasParams{}, fmt.Errorf("evm: gas estimate %d too large to buffer: %w", estimate, ErrGasCapExceeded)
}
gasLimit := lo / 100
if gasLimit > cfg.GasLimitCap {
	return gasParams{}, fmt.Errorf("evm: buffered gas limit %d exceeds cap %d: %w", gasLimit, cfg.GasLimitCap, ErrGasCapExceeded)
}
```

`bits.Mul64(x, y)` returns the 128-bit product split into `(hi, lo)` — the high and low 64
bits. A non-zero `hi` means the true product doesn't fit in 64 bits, i.e. the estimate is
absurdly large; that's a cap rejection, not silent truncation. The test
`TestGasEstimateBufferOverflowRejected` pins this with `estimate =
3_689_348_814_741_927_124`, whose naive `*125` folds to roughly 21001 gas — comfortably
under the cap and catastrophically under-gassed — and asserts the overflow-safe path
rejects it instead.

**Alternative: do the math in `*big.Int`.** It would be correct, but it allocates on every
price for a quantity that legitimately fits in 64 bits 100% of the time. `bits.Mul64` is
the idiomatic zero-allocation guard for exactly this "multiply two `uint64`s and detect
overflow" case; reaching for `big.Int` here would be using a bulldozer to set a
mousetrap. The lesson generalizes: **`uint64` overflow is a real bug class at any trust
boundary that multiplies attacker-influenced numbers**, and `math/bits` is Go's answer.

### Decision F (briefly): the `ethRPC` seam gives hermetic CI with a real EVM

The adapter depends on `ethRPC`, a six-method interface naming exactly the calls it makes,
not on `*ethclient.Client`. The payoff is asserted in one place:

```go
var (
	_ ethRPC = (*ethclient.Client)(nil)     // the live JSON-RPC client
	_ ethRPC = (simulated.Client)(nil)      // go-ethereum's in-memory chain
)
```

Because go-ethereum's `ethclient/simulated` backend exposes a `Client` with the same
method set, the *same adapter code* runs unchanged against a live node in production and
an in-process EVM in tests. That's what lets the e2e mine real transactions, recover real
senders, and verify real calldata with no network, no Postgres, and no env gate.

## 3. Language deep-dive

### 3a. `withNonce`: a callback run *inside* the lock, and Go's map zero value

The signature `func(ctx, from, fn func(nonce uint64) error) error` is the same
"caller-supplied critical section" idiom as the signer's `charge`. The adapter passes a
closure that captures the signer, the RPC, and the gas params, and does the sign+broadcast
*inside the lock*:

```go
err = a.nonces.withNonce(ctx, a.cfg.From, func(nonce uint64) error {
	signed, signErr := a.signer.Sign(ctx, SignerRequest{ ... Nonce: nonce, ... })
	if signErr != nil {
		return fmt.Errorf("evm: signer declined: %w", chain.ErrSignerRejected)
	}
	// ... unmarshal + SendTransaction ...
	txHash = chain.TxHash(signed.TxHash.Hex())
	return nil
})
```

Line by line: `n.mu.Lock()` followed by `defer n.mu.Unlock()` guarantees the lock is
released on *every* return path — the RPC-error return, the `fn`-error return, and the
success return — which is the whole reason `defer` exists (no goroutine can leave the
critical section locked, even on a panic). Note `if hw := n.next[from]; hw > nonce` relies
on a **Go map zero value**: reading a missing key returns the value type's zero (`0` for
`uint64`), so a first-ever allocation for a sender reads `hw == 0`, which never dominates a
real `PendingNonceAt` — the map needs no initialization-per-sender. This is unlike a
Java `HashMap` (returns `null`, NPE risk) or a Python `dict` (raises `KeyError`); Go's
"missing key → zero value" is exactly what makes the high-water logic clean.

The closure captures `txHash` and `gasLimit` from the enclosing `Submit` by *reference*
(Go closures close over variables, not values), which is how the deferred `logResult` and
the final `return txHash` see what the closure set. That's intentional and safe here
because the closure runs synchronously inside `withNonce` before `Submit` reads them —
but it's the same capture-by-reference that bites people who spawn goroutines in a loop.

### 3b. `common.Address` is a value array, so `==` and `common.Address{}` just work

The validation in `Submit` and `NewAdapter` leans on a Go fact that surprises newcomers:

```go
if cfg.From == (common.Address{}) { ... }           // NewAdapter: zero-address guard
recipient := common.HexToAddress(intent.To)
if recipient == (common.Address{}) { ... }          // Submit: reject the zero address
if signed.From != a.cfg.From { ... }                // abort before broadcast
```

`common.Address` is defined as `[20]byte` — a fixed-size **array**, which in Go is a
*value type*, not a reference. That means `==` compares all 20 bytes structurally (arrays
are comparable when their element type is), and `common.Address{}` is the zero value: 20
zero bytes, i.e. the Ethereum zero address. No `.Equals()` method, no `bytes.Equal`, no
allocation. Contrast a Go **slice** (`[]byte`), which is *not* comparable with `==` (you'd
get a compile error) and whose zero value is `nil` — that's why calldata (`[]byte`) is
compared with `bytes.Equal` in the tests but addresses are compared with `==`. The
`signed.From != a.cfg.From` check is a genuine safety gate: the signer returns the sender
its signature recovers to, and if the wrong key signed, the adapter aborts *before*
`SendTransaction`, so a mis-signed transaction never reaches the wire.

### 3c. `FillBytes` writes a fixed-width word; `Bytes` writes a minimal one

`packERC20Transfer` builds the 68-byte calldata by hand, and the choice of `*big.Int`
method is deliberate:

```go
data := make([]byte, erc20TransferCalldataLen) // 68 bytes, zeroed
copy(data[0:4], erc20TransferSelector[:])       // 4-byte selector
copy(data[16:36], to.Bytes())                   // address in low 20 of the 32-byte word
amount.FillBytes(data[36:68])                   // amount, big-endian, fixed 32-byte word
```

`make([]byte, 68)` gives 68 *zeroed* bytes (Go zeroes all allocations — there is no
uninitialized memory), which is why the high 12 bytes of the address word are already
correct without touching them: the address is right-aligned by copying its 20 bytes into
`data[16:36]` and leaving `data[4:16]` zero. Then the method choice: **`FillBytes(buf)`
writes the value big-endian into `buf` at a *fixed* width, left-padding with zeros and
panicking only if it doesn't fit** — perfect for a fixed 32-byte ABI word. That's the
opposite of `Bytes()`, which returns the *minimal* encoding (used on the wire in 3c above,
where the receiver re-pads). The `amount.BitLen() > uint256Bits` guard three lines up is
what rules out the `FillBytes` panic: a value that fits in 256 bits fits in the 32-byte
word by definition, so the panic is provably unreachable — the guard converts a would-be
panic into a clean `chain.ErrInvalidIntent` at the boundary. Note the ABI encoding is
hand-rolled to *exactly 68 bytes* on purpose: the isolated signer allowlists that one
calldata length, so the adapter builds to the shape the signer will accept.

### 3d. `errors.Is` reads *through* wrapping to drive both control flow and observability

The adapter wraps neutral sentinels with `%w` at every return, then reads them back two
ways. `submitOutcome` turns a wrapped error into a stable label for the structured log,
and `logResult` chooses a log *level* from the same sentinels:

```go
case errors.Is(err, ErrGasCapExceeded), errors.Is(err, ErrFeeCapExceeded):
	a.log.WarnContext(ctx, "submit rejected: cap exceeded", attrs...)
case errors.Is(err, chain.ErrSignerRejected):
	a.log.WarnContext(ctx, "submit rejected: signer declined", attrs...)
default:
	a.log.ErrorContext(ctx, "submit failed", attrs...)
```

`errors.Is(err, target)` walks the `%w` chain comparing each link to `target`, so it finds
`chain.ErrSignerRejected` even though the actual error is `fmt.Errorf("evm: signer
declined: %w", chain.ErrSignerRejected)` — the human context ("evm: signer declined")
rides along for the log message while the *identity* survives for matching. This is why
the port can promise "callers match `chain.ErrBroadcast` with `errors.Is`" and the adapter
can *also* wrap EVM-specific sentinels (`ErrGasCapExceeded`) alongside: a caller speaking
the neutral port matches the neutral sentinel; an EVM-aware caller can distinguish
gas-cap from fee-cap. Note the deliberate level split — client-caused rejections (invalid
intent, cap exceeded, signer declined) are `Info`/`Warn`; only an *unexpected* fault falls
to the `default` `Error` arm, so the error log stays signal, not noise. This is the same
`logResult` discipline the signer used, and it never logs the amount, recipient, or raw
bytes.

## 4. What would break

- **Duplicate-nonce collision (the newcomer bug).** Return `PendingNonceAt` without the
  lock-and-high-water and two concurrent submits get the same nonce; one transaction is
  silently dropped or replaced. The `-race` gate and the concurrent-e2e exist to catch
  exactly this. Avoided by holding the lock across sign+broadcast and taking `max(hw,
  pending)`.

- **A wedged account from a burned nonce.** Advance the high-water at allocation time and
  a failed broadcast leaves a permanent hole; every later transaction for the sender
  stalls. Avoided by commit-on-success — `TestWithNonceErrorReusesNonce` pins that a
  failed `fn` reuses the nonce (`{10, 10, 11}`).

- **A leaked lock on a panic or early return.** Without `defer n.mu.Unlock()`, an RPC
  error return (or a panic in the signer call) would leave the mutex held and deadlock
  every future submit for *all* senders. `defer` guarantees release on every path.

- **Silent gas overflow.** A naive `estimate*125/100` wraps a near-2⁶⁴ estimate to a tiny
  number that passes the cap and signs an under-gassed, reverting transaction. `bits.Mul64`
  + `hi != 0` rejects it; the test constructs the exact wrapping value.

- **A moved cap or a corrupted base fee.** Storing the caller's `*big.Int` cap lets a later
  caller mutation raise the ceiling; mutating `header.BaseFee` in place corrupts a shared
  go-ethereum value. `new(big.Int).Set` at construction and fresh allocation for `maxFee`
  close both.

- **A mis-signed transaction on the wire.** If the signer returns a signature that
  recovers to the wrong sender (wrong key), broadcasting it burns a nonce and moves value
  from the wrong account. The `signed.From != a.cfg.From` check aborts *before*
  `SendTransaction`.

- **A pre-EIP-1559 chain silently underpriced.** A `nil` base fee (legacy header) would
  make the `DynamicFeeTx` fee math meaningless. The adapter refuses with `ErrGasEstimation`
  rather than sign a transaction that can't be priced.

## 5. Compared to what you know

- **The nonce lock is a per-key `synchronized` block that spans I/O.** In Java you might
  reach for `AtomicLong.getAndIncrement()` for a counter — but that's exactly
  Alternative 2 (optimistic advance) and it burns nonces on failure. The correct analogue
  is a `synchronized(sender)` block (or a `ReentrantLock` per sender) held across the
  network call, releasing only after the commit decision. The surprising part for most
  engineers isn't the lock, it's that **the lock deliberately spans the RPC** — the
  opposite of the usual "never hold a lock across I/O" advice, because here the I/O *is*
  the thing being serialized.

- **The port/adapter split is hexagonal architecture / dependency inversion**, familiar
  from any DDD codebase. The twist is that Go's interfaces are *implicitly* satisfied:
  `signerClient` never says "implements `evm.Signer`" — it just has the right method, and
  the `var _ evm.Signer = (*signerClient)(nil)` line is a *voluntary* compile-time
  assertion, not a `implements` keyword. The interface is declared on the *consumer* side
  ("accept interfaces, return structs"), which is the inverse of Java where the interface
  usually ships with the provider.

- **`*big.Int` is like Java `BigInteger` — except `BigInteger` is immutable and `*big.Int`
  is not.** Every `BigInteger` operation returns a new object; you can share references
  freely. Go's `*big.Int` methods mutate the *receiver* (`z.Add(x, y)` writes into `z`),
  so sharing a reference is a footgun. This is the single biggest `*big.Int` gotcha for a
  Java/C# engineer, and it's why the copy-and-allocate discipline exists. Python's `int`
  is immutable and arbitrary-precision, so the same code in Python would have no hazard at
  all — the bug is specifically a Go/`*big.Int` phenomenon.

- **`bits.Mul64` is `Math.multiplyHigh` (Java 9+) / `__int128` (C).** The concept — get
  the high word of a widening multiply to detect overflow — is identical; Go just packages
  it as a stdlib function with a clean `(hi, lo)` return.

## 6. Gotchas & idioms

- **Map zero value, not `KeyError`.** `n.next[from]` on a missing key returns `0`, no
  check needed. Relied on for the first allocation per sender.

- **Array `==` vs slice `bytes.Equal`.** `common.Address` (a `[20]byte` array) is
  comparable with `==` and has a usable zero value `common.Address{}`; `[]byte` calldata is
  *not* `==`-comparable (compile error) and its zero value is `nil`. Use `==` for
  addresses/hashes, `bytes.Equal` for byte slices.

- **`Bytes()` is minimal, `FillBytes(buf)` is fixed-width.** On the wire you want minimal
  (the receiver re-pads via `SetBytes`); in an ABI word you want fixed-width. Mixing them
  up gives you either a malformed calldata word or a wire value with spurious leading
  zeros.

- **A zero `*big.Int` encodes to an empty slice.** `big.NewInt(0).Bytes()` is `[]byte{}`,
  and `SetBytes([]byte{})` is `0`. Both boundaries agree, but it's a surprising edge if you
  assume "one byte per value."

- **`grpc.NewClient` is lazy.** Dialing the signer doesn't connect; a bad address surfaces
  on the first RPC, not at `NewClient`. That's why `submit.go` can construct the client
  before it knows the signer is reachable — and why `VerifyChainID` (a real RPC) is the
  first thing that actually talks to anything.

- **Gas knobs are `const`, caps are config.** The 25% buffer and 2× base-fee headroom are
  *safety margins* (constants), while `GasLimitCap`/`MaxFeePerGasCapWei` are *operator
  policy* (config). Conflating the two — making the buffer tunable — would invite an
  operator to disable a safety margin by accident.

## 7. Check yourself

1. `withNonce` holds the mutex across the signer round-trip. Standard advice is "never
   hold a lock across I/O." Why is that advice wrong here, and what specifically breaks if
   you release the lock right after reading `PendingNonceAt`?
2. The high-water advances only when `fn` returns nil. Walk through what the mined nonces
   look like if a broadcast fails on the *second* of three sequential submits, under both
   this design and an "advance-at-allocation" design.
3. Why does the wire use `(*big.Int).Bytes()` (minimal) but the calldata use
   `FillBytes` (fixed 32-byte)? What would go wrong if you swapped them?
4. `NewAdapter` does `cfg.MaxFeePerGasCapWei = new(big.Int).Set(cfg.MaxFeePerGasCapWei)`.
   Construct the concrete sequence of caller calls that would defeat the fee cap *without*
   this line.
5. Construct a `uint64` `estimate` for which naive `estimate*125/100` produces a value
   under a 300000 gas cap even though the true buffered value is astronomically larger.
   (Hint: you want `estimate*125 mod 2⁶⁴` to be small.)

<details>
<summary>Answers</summary>

1. The I/O *is* the critical section: the thing being serialized is "sign-and-broadcast
   for this sender," and a nonce is only safe to reuse once you know the previous one has
   been broadcast. Release the lock after reading `PendingNonceAt` and a second goroutine
   reads the same pending value before the first has broadcast → both sign the same nonce →
   collision (Alternative 1). The high-water can't save you if it's read/updated outside
   the lock.
2. This design: submit#1 gets nonce N and commits (advance to N+1); submit#2 gets N+1, its
   broadcast fails, high-water stays at N+1; submit#3 *reuses* N+1 and commits. Mined
   nonces: N, N+1 — gap-free. Advance-at-allocation: #1→N, #2→N+1 (fails, but already
   advanced), #3→N+2. Mined: N, N+2 — a permanent hole at N+1 wedges everything from N+2
   onward until N+1 is filled by hand.
3. The wire is symmetric: the sender minimizes, the receiver re-pads with `SetBytes`, so
   minimal is fine and avoids sending 20 leading zero bytes per value. The calldata is a
   *fixed* 32-byte ABI word with no re-padding step, so it must be written full-width;
   `Bytes()` there would produce a short, misaligned word. Swap them and you'd send a
   spuriously zero-padded wire value (harmless but wrong) and build a malformed calldata
   word (broken — the EVM reads a fixed 32-byte slot).
4. `cap := big.NewInt(100)`, pass `evm.Config{MaxFeePerGasCapWei: cap}` to `NewAdapter`,
   then later `cap.SetInt64(1_000_000_000)`. Without the copy, the adapter's stored cap
   *is* `cap`, so the ceiling every `Submit` prices against just jumped to 1 gwei. The
   `Set` copy severs the alias so the adapter's cap is immune to the caller's later
   mutation.
5. `estimate = 3_689_348_814_741_927_124` (from the test). `estimate * 125 = 461168601...`
   which exceeds 2⁶⁴; the low 64 bits are ~2_100_100, and `/100` ≈ 21001 — under a 300000
   cap. `bits.Mul64` returns `hi != 0`, so the overflow-safe path rejects it instead of
   signing a 21001-gas transaction that reverts out-of-gas.

</details>

## 8. Further reading

- [Go blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) — `%w`,
  `errors.Is`/`errors.As`, and the wrapping model the adapter and port lean on.
- [`math/bits` package docs](https://pkg.go.dev/math/bits#Mul64) — `Mul64` and the rest of
  the overflow-aware primitives.
- [`math/big` package docs](https://pkg.go.dev/math/big#Int) — note that the methods
  mutate the receiver; read `Set`, `Bytes`, and `FillBytes` specifically.
- [EIP-1559: Fee market change](https://eips.ethereum.org/EIPS/eip-1559) — where base fee,
  priority tip, and max fee per gas come from, and the 12.5%-per-block base-fee dynamics
  the 2× headroom absorbs.
- [go-ethereum `ethclient/simulated`](https://pkg.go.dev/github.com/ethereum/go-ethereum/ethclient/simulated) —
  the in-process backend that makes the hermetic e2e possible.
