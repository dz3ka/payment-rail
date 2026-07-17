# M2 — The isolated gRPC signer: a proto→domain adapter, a per-key spend-limit mutex, and defensive copies at a key-holding trust boundary

> Scope: this lesson is about *standing up the first gRPC service in the repo* and
> *writing a security-critical domain that holds private keys*. The headline
> achievements are (1) a network-isolated signer whose stable core (`internal/signer`)
> knows nothing about protobuf — a thin adapter in `cmd/signer` maps the wire to the
> domain and back, and translates domain error sentinels into gRPC status codes; (2) a
> per-key cumulative spend limiter whose `{check → sign → commit}` sequence is *one*
> critical section under a per-key `sync.Mutex`, proven overspend-free under `-race`;
> and (3) a defensive-copy discipline at the trust boundary that closes a TOCTOU gap
> created by Go's mutable `*big.Int` and shared slices. Along the way: EIP-1559 signing
> delegated to go-ethereum, a calldata allowlist, and secret hygiene (a `0600`
> permission gate and an error that is deliberately *not* wrapped). This builds on the
> M1 lessons for the error-sentinel and `logResult` disciplines — skim those if
> `errors.Is`/`%w` is not yet reflexive.

## 1. What we built

M2 slice 1 splits signing into its own process. `cmd/signer` is a gRPC server exposing
one method, `SignTransaction`, defined by `proto/paymentrail/signer/v1/signer.proto`. A
caller hands over a fully-specified EIP-1559 transaction (key id, chain, nonce, gas,
destination, amounts, calldata) and receives back the broadcast-ready RLP bytes, the
transaction hash, and the recovered sender. Private key material never crosses the wire
and never leaves the process; the service binds to loopback only, because slice 1 has
no mTLS or caller auth yet (ADR-0009), so the blast radius is bounded by the network
boundary plus a per-key spend cap rather than by authentication.

The design's spine is a hard separation between *domain* and *transport*. The
`internal/signer` package is the domain: it loads keys from a permission-checked,
secret-free manifest (`keyring.go`), validates every request field as untrusted input
(`policy.go`), enforces a per-key cumulative spend limit concurrency-safely
(`limiter.go`), and produces a signed transaction via go-ethereum (`signer.go`).
Crucially it *never imports the generated protobuf code*. The `cmd/signer/server.go`
adapter is the transport: it converts proto bytes into domain types (`common.Address`,
`*big.Int`), calls `signer.Sign`, and maps the domain's sentinel errors onto gRPC
status codes. The domain is the stable core; the wire format is replaceable around it.

The part to study hardest is the concurrency and the trust-boundary discipline, because
this is the one service where a bug moves money or leaks a key. The spend limiter uses
one `sync.Mutex` per key and holds it across the *entire* check-sign-commit sequence, so
two concurrent requests on the same key can never both see budget, both sign, and
together overspend — the milestone's gate,
`TestSpendBucket_ConcurrentOneKeyOnlyKFit`, proves it under `-race` (100 goroutines,
only 7 charges fit, exactly 7 succeed, committed total is exactly `7×amt`). And because
Go's `*big.Int` is *mutable* and slices are shared references, `Sign` takes a `deepCopy`
of the request at the boundary so a caller can't mutate an amount in the window between
the limit check and signing.

## 2. The design decision

### Decision A: a pb-agnostic domain with a thin proto↔domain adapter around it

**The problem.** A signer needs a network API, and gRPC is the obvious choice for an
internal service-to-service call. But the generated protobuf types (`[]byte` for
addresses and amounts, `uint64` for chain/nonce) are a *wire* shape, not a *domain*
shape, and the code that actually holds keys and enforces policy must not be entangled
with a particular serialization or transport.

**The chosen approach — the domain never imports the generated code; a boundary adapter
does all translation.** The `.proto` file is compiled to a `SignerServiceServer`
interface in `internal/signerpb`; `cmd/signer/server.go` implements that interface, and
its `SignTransaction` method does exactly three things: convert the proto request to a
`signer.SignRequest` (`toDomainRequest`), call `s.signer.Sign`, and map the result (or a
domain sentinel error) back to the wire. The domain package's doc comment states the rule
outright (`signer.go:1`): "this package never imports the generated protobuf code: the
domain is the stable core, the transport is replaceable around it." The conversion is not
a formality — `common.BytesToAddress` *silently pads or truncates* and `big.Int.SetBytes`
*silently accepts oversized input*, so the adapter validates the *byte length* of every
field before converting (`server.go:78`), rejecting a 19-byte address or a 33-byte
uint256 with `codes.InvalidArgument` rather than letting a malformed value slip past as a
"valid-but-wrong" one.

The error mapping is its own small state machine (`mapSignError`, `server.go:126`): each
domain sentinel maps to one gRPC code — `ErrUnknownKey`→`NotFound`,
`ErrChainMismatch`/`ErrMalformedTx`→`InvalidArgument`,
`ErrSpendLimitExceeded`→`ResourceExhausted`, and *anything unrecognized*→`Internal`. The
`default` arm is the safety net: an unexpected error is never leaked verbatim to the
client (it might carry detail), it becomes a generic `Internal`.

**Alternative 1: let the domain speak protobuf directly** (methods take
`*signerpb.SignTransactionRequest`). Rejected. It welds the security core to a wire
format: every proto regeneration risks a domain change, the domain becomes untestable
without constructing proto messages, and swapping transport (say, an in-process call from
a future co-located service) would drag protobuf along. The adapter costs ~40 lines and
buys a domain that tests with plain structs.

**Alternative 2: connect-go** instead of vanilla gRPC. Rejected in ADR-0008 for a
*learning* reason as much as a technical one: Connect swaps gRPC's stub/wire surface for
its own, which would obscure the "gRPC in Go" objective this milestone exists to teach.
Vanilla gRPC keeps the generated `ServiceDesc`/handler machinery visible.

**Toolchain note (ADR-0008).** Codegen is `buf`, pinned in `go.mod` via `tool`
directives and run as `go tool buf generate`. `buf` bundles a pure-Go protobuf
compiler, so a bare checkout regenerates stubs with only the Go toolchain on `PATH` — no
system `protoc`, no Homebrew — mirroring the sqlc-via-Make story. `buf.gen.yaml` even
invokes the plugins as `local: [go, tool, protoc-gen-go]`, so the plugins themselves are
go.mod-pinned. This is the hermetic-build discipline: everything that generates code is
reproducible from `go.mod` alone.

### Decision B: one `sync.Mutex` per key, and `{check → sign → commit}` is ONE critical section

**The problem.** Each key has a cumulative spend ceiling (ADR-0009). The invariant is:
*for each key, the sum of charged amounts across successful signs never exceeds the
key's limit.* Under concurrency this is a classic check-then-act race: two requests could
both read "80 spent of 100", both decide their 40 fits, both sign, and leave 160 spent.

**The chosen approach — colocate a lock with each key and hold it across the whole
sequence.** Each `keyEntry` owns a `*spendBucket` (`keyring.go:33`), and `spendBucket`
holds a `sync.Mutex`, an immutable `limit`, and a mutable `spent` (`limiter.go:19`). The
key *is* the serialization point: a lookup returns both the signing material and the lock
that guards its budget. `charge` takes the signing work as a *callback* and runs it under
the lock (`limiter.go:51`):

```go
func (b *spendBucket) charge(amount *big.Int, sign func() (SignedTx, error)) (SignedTx, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	next := new(big.Int).Add(b.spent, amount)
	if next.Cmp(b.limit) > 0 {
		return SignedTx{}, fmt.Errorf("signer: charge would exceed the key's spend limit: %w", ErrSpendLimitExceeded)
	}
	tx, err := sign()
	if err != nil {
		return SignedTx{}, err // commit nothing: a failed sign must not advance the counter
	}
	b.spent = next
	return tx, nil
}
```

Three properties fall out. (1) **Check and commit are atomic** because they share the
lock, so the two-requests-both-see-80 race is impossible; the second request blocks on
`b.mu.Lock()` until the first has committed to 120, then correctly sees no room. (2)
**Signing is inside the section**, not before or after, so the amount that was checked is
the amount that gets signed — no window in between. (3) **A failed sign advances
nothing**: the counter is bumped only on the success path (`b.spent = next` runs only
after `sign()` returns nil), so a go-ethereum error or a cancelled context can't consume
budget. Requests on *different* keys never contend, because they hold different buckets'
locks.

The `-race` gate `TestSpendBucket_ConcurrentOneKeyOnlyKFit` (`limiter_test.go:58`) fires
100 goroutines at one key sized for 7 charges: exactly 7 succeed and `b.spent` is exactly
`7×amt`. `TestSpendBucket_Property_AdmittedNeverExceedLimit` (`limiter_test.go:92`) is the
`testing/quick` property form: for any random sequence of charges, admitted sum ≤ limit
*and* admitted sum == committed total.

**Alternative 1: an atomic counter** (`atomic.AddInt64` / a CAS loop). Rejected in
ADR-0009. A uint256 total does not fit in `int64`; it needs `big.Int`, and `big.Int` is a
multi-word struct you cannot update atomically — you would need a mutex around it anyway.
The lock-free version is a mirage here.

**Alternative 2: a rolling per-time-window cap** (e.g. "N wei per hour"). Rejected: a
wall-clock window makes tests non-deterministic (you would need a fake clock everywhere)
and adds no security this milestone needs. A cumulative-over-process-lifetime cap is
simpler and its restart behavior is *fail-safe* — see Gotchas.

### Decision C: defensive copy at the boundary, because `*big.Int` and slices are mutable references

**The problem.** `SignRequest` carries three `*big.Int` amounts and a `Data []byte`
(`signer.go:32`). Both are *reference-like*: the caller still holds the same underlying
memory after passing the request. Even though `Sign` receives `r SignRequest` by value —
which copies the *scalar* fields — the pointers and the slice header still alias the
caller's data. That opens a TOCTOU hole: validate a small `Value`, then, in the window
before the bytes are actually signed, the caller mutates `*Value` to something huge, and
the signer charges the small amount but signs the large transfer.

**The chosen approach — clone every reference field into signer-owned memory up front.**
`Sign` calls `r = r.deepCopy()` as its first real act (`signer.go:107`):

```go
func (r SignRequest) deepCopy() SignRequest {
	c := r                                        // copies scalars; pointers/slice still alias
	c.Value = copyBig(r.Value)                    // fresh big.Int
	c.MaxFeePerGas = copyBig(r.MaxFeePerGas)
	c.MaxPriorityFeePerGas = copyBig(r.MaxPriorityFeePerGas)
	if r.Data != nil {
		c.Data = bytes.Clone(r.Data)              // fresh backing array
	}
	return c
}
```

After `deepCopy`, the value the limiter checks and the bytes go-ethereum signs are both
signer-owned; nothing the caller retains can change them. `TestSign_DefensiveCopy_
CallerCannotMutateChargedAmount` (`signer_test.go:127`) proves it: it signs, then does
`req.Value.SetInt64(0)`, and asserts the committed `spent` is unchanged. This is Effective
Java's "make defensive copies of mutable parameters," elevated from a hygiene nicety to a
*security control* because the boundary holds keys.

**Alternative: trust the caller / copy lazily.** Rejected. In a same-process call the
caller is another goroutine that legitimately still owns its request; the signer cannot
assume the request is frozen. Copying once, eagerly, at the boundary is cheaper to reason
about than auditing every downstream read for aliasing.

### Decision D: EIP-1559 signing is delegated to go-ethereum; policy is an allowlist

**The problem.** Producing a valid signed transaction means RLP-encoding an EIP-1559
typed transaction and hashing it under the exact EIP-1559/EIP-155 rules. Hand-rolling any
of that is how you produce a subtly-invalid or replayable signature.

**The chosen approach — map the validated request onto `types.DynamicFeeTx` and let the
London signer do the crypto** (`signTx`, `signer.go:151`). The domain builds the tx,
calls `types.SignTx(tx, types.NewLondonSigner(chainID), key.privateKey)`, and marshals
the result — it never touches RLP or the signing hash itself. Two policy checks make this
a *payment* signer rather than a general one:

- **Chain binding is replay protection** (`policy.go:57`). A key carries exactly one
  `chainID`, and the request's `chain_id` must equal it. EIP-1559 embeds the chain id in
  the signed payload, so a signature is only replay-safe on its own chain; rejecting a
  mismatch (`ErrChainMismatch`) stops a caller from getting a signature valid on a chain
  the key owner never intended.
- **The calldata allowlist fixes what is being spent** (`chargedAmount`, `policy.go:117`).
  Only two shapes sign: empty calldata (a native ETH transfer, charged `Value`), or an
  *exact* ERC-20 `transfer(address,uint256)` call — selector `0xa9059cbb`, total length
  68 bytes, and `Value == 0` (a token transfer moves no ETH) — charged the decoded token
  amount. Anything else is `ErrMalformedTx`. An isolated signer signs only payments it can
  *price*; it refuses arbitrary contract calls it cannot account for.

**Alternative: build and sign transactions by hand** (or with a minimal RLP encoder).
Rejected on principle: go-ethereum is the most battle-tested code in this stack, and the
signing hash is exactly the code you least want a hand-rolled bug in. The domain's job is
*policy*, not *crypto primitives*.

### Decision E: secret hygiene — a permission gate, and an error deliberately left unwrapped

**The problem.** This process reads raw private keys from disk. Two leak vectors dominate:
a key file readable by other users, and key bytes echoed into a log or error string.

**The chosen approach — fail closed on permissions, and never let key material reach a
message.** `loadKeyFile` (`keyring.go:106`) stats the file and rejects any file with a
group- or world- bit set via `perm & 0o077 != 0`; only owner-only modes (`0600`, `0400`)
pass. Then it parses the hex — and if parsing fails, it *does not wrap* go-ethereum's
error:

```go
priv, err := crypto.HexToECDSA(hexKey)
if err != nil {
	// Deliberately do NOT wrap err: go-ethereum's parse errors can include
	// fragments of the input, and the input is the private key.
	return nil, common.Address{}, fmt.Errorf("key_file %s does not contain a valid hex private key", path)
}
```

This is the one place in the codebase that breaks the "always wrap with `%w`" habit *on
purpose*, and the comment says why: the wrapped error could carry a fragment of the key.
The whole load path also fails closed — a duplicate `key_id`, an unparseable
`spend_limit`, a missing or over-permissioned file, or malformed hex aborts the entire
`LoadKeyring` rather than silently dropping one key (`keyring.go:61`), because a signer
that started with a key missing would fail *open* (rejecting requests it should serve, or
worse, serving with an unexpected key set).

**Alternative: an encrypted keystore** (the go-ethereum `keystore` format). Rejected in
ADR-0009 because its unlock passphrase just becomes a *new* env secret — the exact thing
the "no secret in env or repo" NFR forbids. A `0600` hex file plus a committed,
secret-free manifest keeps custody reproducible from local files + committed config, with
`.gitignore` covering `*.key`/`*.pem`/`.env`.

## 3. Language deep-dive

### 3a. `charge`: a callback run *inside* the lock, and why signing can't be pulled out

The concurrency core is that `charge` takes `sign func() (SignedTx, error)` as a
*parameter* and calls it while holding `b.mu`. The alternative a newcomer reaches for —
lock, check, unlock, sign, lock, commit, unlock — reintroduces the exact race the mutex
was meant to kill:

```go
// WRONG — do not do this:
b.mu.Lock(); ok := next.Cmp(b.limit) <= 0; b.mu.Unlock()
if !ok { return ..., ErrSpendLimitExceeded }
tx, err := sign()                      // <-- another goroutine can check-and-commit here
b.mu.Lock(); b.spent = next; b.mu.Unlock()
```

Between the unlock and the re-lock, a second goroutine can pass its own check against the
*stale* `spent`, and both commit. Holding the lock across `sign()` is what makes
check-sign-commit indivisible. The cost is real: the lock is held for the whole (CPU-bound,
fast) signing operation, so requests *on the same key* are fully serialized. That is
acceptable here precisely because signing is fast and a single key is not a
high-throughput hot path — and it is the correct trade: correctness (never overspend) over
per-key parallelism. Different keys still sign in parallel, because the lock lives on the
bucket, not on the ring.

Passing the work as a closure — rather than, say, returning a "reservation" token the
caller must later commit or cancel — keeps the commit/rollback decision *inside* the
guarded section, so there is no way for a caller to forget to release a reservation and
wedge the counter. Note the LIFO cleanup: `defer b.mu.Unlock()` unlocks on *every* return
path, including the `ErrSpendLimitExceeded` early return and a panicking `sign()`, so the
mutex is never left held.

### 3b. `deepCopy` and `copyBig`: what a value receiver does and does not copy

`deepCopy` has a value receiver `(r SignRequest)`, so `r` is already a shallow copy of the
argument. The line `c := r` copies it *again*, shallowly. Understanding what that does
requires Go's copy semantics: assigning a struct copies every field *by value*, but for a
pointer field the "value" is the pointer, and for a slice field the "value" is the
`{ptr, len, cap}` header — so `c.Value` and `r.Value` are two pointers to the *same*
`big.Int`, and `c.Data` and `r.Data` are two headers over the *same* backing array. The
scalar fields (`KeyID`, `ChainID`, `Nonce`, `GasLimit`, `To`) are genuinely independent
after `c := r`, because they are copied bit-for-bit — `To` in particular is a
`[20]byte` *array*, a value type, so it is deep-copied for free.

That is why `deepCopy` only has to fix the reference fields:

```go
func copyBig(x *big.Int) *big.Int {
	if x == nil {
		return nil        // preserve nil so validation can still see "missing" vs "zero"
	}
	return new(big.Int).Set(x)
}
```

`new(big.Int).Set(x)` allocates a fresh `big.Int` and copies `x`'s magnitude into it, so
the result shares no memory with `x`. The `nil` guard is load-bearing: `validate`
distinguishes a *missing* amount (`f.v == nil` → `"value is required"`) from a *zero*
amount, so `copyBig` must not turn a nil into a `big.Int(0)`. `bytes.Clone(r.Data)`
(from Go 1.20) does the slice equivalent — a fresh backing array — and is itself
nil-preserving, but the explicit `if r.Data != nil` keeps a nil `Data` as nil for the
same reason. This is the general Go rule: **structs copy; pointers, slices, and maps
share** — so a "deep" copy means walking exactly the reference fields.

### 3c. The gRPC adapter: implicit interface satisfaction and forward-compatible embedding

`Server` satisfies the generated `signerpb.SignerServiceServer` interface with no
`implements` keyword — Go interface satisfaction is structural, so defining
`SignTransaction(context.Context, *SignTransactionRequest) (*SignTransactionResponse,
error)` *is* satisfaction. But the generated interface has a second, unexported method,
`mustEmbedUnimplementedSignerServiceServer()`, which the adapter can't spell — so it
**embeds** the generated stub (`server.go:23`):

```go
type Server struct {
	signerpb.UnimplementedSignerServiceServer // embedded by VALUE, per the generated NOTE
	signer *signer.Signer
	log    *slog.Logger
}
```

Embedding promotes the stub's methods onto `Server`, including
`mustEmbedUnimplementedSignerServiceServer` and a default `SignTransaction` that returns
`codes.Unimplemented` — which our real `SignTransaction` then *overrides* (a method
defined directly on `Server` shadows the promoted one). This is Go's mechanism for
gRPC forward compatibility: if the proto later gains a method this adapter hasn't
implemented, the code still compiles and that method returns `Unimplemented` instead of
breaking the build. The generated code's own comment insists on embedding *by value, not
pointer* (`signer_grpc.pb.go:77`), to avoid a nil-pointer dereference if an unimplemented
method is ever called — and `RegisterSignerServiceServer` asserts this at registration
time.

The adapter method itself stays disciplined about what leaves the process
(`SignTransaction`, `server.go:47`): every request produces exactly one structured log
line via `logResult`, carrying the gRPC code and the non-secret `key_id` — never an
amount, limit, sender, or the raw transaction. The response returns
`signed.From.Hex()` (a public, EIP-55-checksummed address derived from the *public* key),
which is safe to expose.

### 3d. Boundary length checks before lossy conversions

`toDomainRequest` exists because two go-ethereum/`big.Int` conversions are silently lossy,
and the trust boundary must catch the loss before it becomes a valid-but-wrong value:

```go
if len(req.GetTo()) != 20 {
	return signer.SignRequest{}, status.Error(codes.InvalidArgument, "malformed transaction: destination address must be 20 bytes")
}
// ...
func toUint256(b []byte, field string) (*big.Int, error) {
	if len(b) > 32 {
		return nil, status.Errorf(codes.InvalidArgument, "malformed transaction: %s must be at most 32 bytes", field)
	}
	return new(big.Int).SetBytes(b), nil // empty/nil -> non-nil big.Int(0)
}
```

`common.BytesToAddress` right-aligns its input into a 20-byte array, silently truncating a
33-byte input or zero-padding a 19-byte one — so a caller who sends 19 bytes would get a
*different, valid* address than intended. Checking `len == 20` first turns that into an
explicit rejection. Likewise `big.Int.SetBytes` happily accepts a 33-byte big-endian
number, which is not a uint256; the adapter rejects `len > 32`. And note the deliberate
asymmetry: `SetBytes` always returns a **non-nil** `*big.Int` (empty input → `0`), so the
domain never receives a nil amount *from this adapter* — yet `validate` still nil-checks
the amounts (`policy.go:80`), because the domain must be correct independent of who calls
it (tests construct `SignRequest` directly). Boundary validation and domain validation are
defense-in-depth layers, not one substituting for the other.

### 3e. Serve-in-a-goroutine with a buffered error channel, then bounded `GracefulStop`

`run` (`cmd/signer/main.go:32`) is the gRPC analogue of the M0 HTTP graceful-shutdown
skeleton. It binds the listener synchronously (so a bad address fails *here*, not
asynchronously), then serves in a goroutine feeding a buffered channel:

```go
errCh := make(chan error, 1)
go func() {
	if err := grpcServer.Serve(lis); err != nil {
		errCh <- err
	}
}()
select {
case err := <-errCh:
	return fmt.Errorf("signer: serve: %w", err)
case <-ctx.Done():
}
```

The channel is **buffered to 1** on purpose: on the clean-shutdown path `GracefulStop`
makes `Serve` return `nil`, so nothing is ever sent — but if `Serve` failed *after* we had
already taken the `ctx.Done()` branch, an unbuffered send would block forever and leak the
goroutine. Buffer-of-1 lets that late send complete and the goroutine exit. The `select`
races an early serve error against shutdown: whichever happens first wins.

Shutdown itself is bounded because `grpcServer.GracefulStop()` takes *no context* and
blocks until in-flight RPCs drain — a slow client could wedge it forever. So `run` races
the graceful stop against the configured `ShutdownTimeout` and falls back to a hard
`Stop()`:

```go
stopped := make(chan struct{})
go func() { grpcServer.GracefulStop(); close(stopped) }()
select {
case <-stopped:
case <-time.After(cfg.ShutdownTimeout):
	grpcServer.Stop()
	<-stopped
}
```

`Stop()` forcibly closes connections; the trailing `<-stopped` still waits for the
`GracefulStop` goroutine to observe the closure and finish, so the function doesn't return
while that goroutine is live. This is the same "bound the drain, then force it" shape as
`http.Server.Shutdown` with a timeout context, adapted to gRPC's context-less API.

## 4. What would break

- **Overspend under concurrency.** The whole point of Decision B. Proven absent by
  `TestSpendBucket_ConcurrentOneKeyOnlyKFit` under `-race`. A newcomer who checks the
  counter, releases the lock, signs, and re-locks to commit would ship an intermittent
  overspend that only appears under load — the classic check-then-act race.
- **A failed sign consuming budget.** `charge` bumps `spent` only after `sign()` returns
  nil, so a go-ethereum error or a cancelled request leaves the counter untouched
  (`TestSpendBucket_FailedSignDoesNotCommit`, `TestSign_SuccessAdvancesCounter_
  FailureDoesNot`). Bumping before signing would leak budget on every failure and
  eventually lock a key out below its real limit.
- **TOCTOU on the charged amount.** Without `deepCopy`, a caller sharing the request's
  `*big.Int` could mutate an amount after the limit check but before signing — charge
  small, sign large. `TestSign_DefensiveCopy_CallerCannotMutateChargedAmount` guards it.
- **A malformed value smuggled past the domain.** `common.BytesToAddress`/`SetBytes`
  silently pad/truncate; the adapter's length checks turn a 19-byte address or 33-byte
  uint256 into an explicit `InvalidArgument` (`TestSignTransaction_ErrorMapping` cases
  "to wrong length", "value exceeds 32 bytes").
- **Cross-chain replay.** The `chain_id == key.chainID` check refuses to produce a
  signature valid on a chain the key owner didn't target.
- **Contract creation / arbitrary calls from a payment signer.** The zero-address guard
  rejects `to == common.Address{}` (contract creation), and the calldata allowlist rejects
  anything that isn't a native transfer or an exact ERC-20 `transfer`.
- **Key material in a log or error.** The `0600` permission gate refuses a group/world-
  readable key file, `HexToECDSA`'s error is *not* wrapped, and no amount/limit/sender/raw
  tx is ever logged — only the gRPC code and the opaque `key_id`.
- **A hostile oversized request.** `grpc.MaxRecvMsgSize(64<<10)` caps the receive size far
  below gRPC's 4 MB default (a real request's largest field is ≤68-byte calldata), so an
  attacker can't force a large allocation *plus its defensive copy* before validation runs.
- **A partially-loaded keyring.** `LoadKeyring` fails closed — one bad entry aborts the
  whole load — rather than silently starting with a missing key.

## 5. Compared to what you know

- **The generated `SignerServiceServer` interface + `Unimplemented…` embedding** is Java
  gRPC's `SignerServiceGrpc.SignerServiceImplBase` (a generated base class whose methods
  return `UNIMPLEMENTED` until you override them) — except Go uses *embedding
  (composition)* rather than inheritance, and interface satisfaction is structural, so
  `Server` never names the interface it implements.
- **`mapSignError`** is a gRPC status-mapping interceptor or a Spring
  `@ExceptionHandler`/`@GrpcAdvice`: a single place that turns internal error types into
  wire status codes, with a catch-all that refuses to leak unexpected detail. The `errors.Is`
  chain-walk is how Go matches a sentinel through wrapping — like `catch (SpecificException
  e)` matching a cause, but explicit.
- **`*big.Int` is like Java `BigInteger`/JS `BigInt` — except it is MUTABLE.** That single
  difference is why `deepCopy`, `copyBig`, and `newSpendBucket`'s `new(big.Int).Set(limit)`
  all exist. In Java/JS, `BigInteger`/`BigInt` are immutable, so aliasing one is harmless;
  in Go, sharing a `*big.Int` means a mutation is visible to every holder. Treat Go's
  `big.Int` like a mutable `StringBuilder`, not an immutable `String`.
- **The per-key mutex** is lock striping (`ConcurrentHashMap`'s segment locks, Guava's
  `Striped<Lock>`) with the key itself as the stripe — or an actor-per-key that serializes
  messages for one entity. It bounds contention to same-key traffic.
- **The defensive `deepCopy`** is Effective Java Item 50 ("make defensive copies") — the
  copy-the-mutable-`Date`-in-the-constructor rule — but here it defends a *security*
  invariant (charge == sign), not just encapsulation.
- **Delegating signing to go-ethereum** is "use BouncyCastle / the platform's crypto, never
  hand-roll." The domain owns *policy*; the library owns the primitive.
- **Loopback-only binding as the slice-1 auth story** is network segmentation standing in
  for authn — the same reason you bind a debug endpoint or an admin socket to `127.0.0.1`
  until real auth lands.

## 6. Gotchas & idioms

- **`big.Int` is mutable and its zero value is a real 0.** `var x big.Int` is a valid zero,
  but copying a `big.Int` *by value* shares its internal magnitude slice — a footgun — so
  the idiom is always `*big.Int` (`signer.go:27` says exactly this). Every place that
  stores an externally-supplied `big.Int` first does `new(big.Int).Set(x)` to own a private
  copy: `copyBig`, `newSpendBucket`, `chargedAmount`, and `charge`'s `next`.
- **`common.Address{}` — the zero value — is a *meaningful, dangerous* address.** It is the
  zero address: in a transaction's `to` it means contract creation; as a recipient it is a
  burn. So the domain treats the zero value as an explicit reject (`policy.go:65`), not as
  "unset." Zero values in Go often carry meaning; here the meaning is "do not sign this."
- **`SetBytes` never returns nil.** Empty/nil input decodes to `big.Int(0)`, so the adapter
  can't hand the domain a nil amount — but the domain still nil-checks for callers that
  build requests directly. Don't collapse two validation layers into one.
- **Embed the `Unimplemented…` stub by value, not pointer.** The generated NOTE
  (`signer_grpc.pb.go:77`) is not decoration: a pointer embed left nil dereferences when an
  unimplemented method is invoked.
- **Not wrapping an error is sometimes correct.** `HexToECDSA`'s error is deliberately
  returned as a fixed string, breaking the repo's `%w` habit, because the wrapped detail
  could echo key bytes (`keyring.go:132`). "Always wrap with `%w`" is a default, not a law.
- **Octal permission masks.** `perm & 0o077 != 0` tests every group/world bit at once; the
  `0o` prefix (Go 1.13+) is the modern octal literal — don't write it as bare `077`.
- **The spend counter is in-memory and resets on restart — and that's the fail-safe
  direction.** A restart zeroes `spent`, which can only *lower* effective usage toward the
  limit, never raise the ceiling. Persisting the counter would add a way to *corrupt* it
  upward; ADR-0009 chooses "forget past spend" precisely because a lost counter can never
  overspend.
- **The listener binds before `Serve`.** `net.Listen` runs synchronously so a bad
  `SignerGRPCAddr` fails in `run`'s error return, not inside the serve goroutine where it
  would be awkward to surface; `lis.Addr()` then reports the concrete port (useful when the
  configured port is `0`).
- **`buf` and its plugins are go.mod-pinned.** `go tool buf generate` needs only `go` on
  `PATH`; there is no system `protoc` to drift. Regenerate with `make proto`.

## 7. Check yourself

1. Rewrite `charge` to release the lock during `sign()` and re-acquire it to commit.
   Construct a concrete two-goroutine interleaving that overspends, and explain why the
   real code (lock held across `sign()`) makes it impossible.
2. `deepCopy` starts with `c := r`. List which `SignRequest` fields are already safe after
   that line and which still alias the caller, and say *why* for each (tie it to Go's
   struct/pointer/slice copy rules). Why must `copyBig` preserve `nil`?
3. Go's `big.Int` is mutable; Java's `BigInteger` is not. Describe a *single-threaded* bug
   that omitting `deepCopy` would allow here, then a *concurrent* one. Would either exist if
   `SignRequest.Value` were an immutable type?
4. `loadKeyFile` returns a fixed string instead of `fmt.Errorf("...: %w", err)` when
   `HexToECDSA` fails. What exactly could leak if it wrapped the error, and why is this the
   *one* place the `%w` habit is wrong?
5. `toDomainRequest` checks `len(req.GetTo()) != 20` before calling
   `common.BytesToAddress`. What does `BytesToAddress` do with 19 bytes, and what concrete
   attack does the length check stop? Why does `toUint256` reject `>32` bytes but accept
   `0`?
6. `Server` embeds `signerpb.UnimplementedSignerServiceServer` by value. What breaks at
   compile time if you remove the embed entirely? What breaks at *runtime* if you embed it
   by pointer and leave it nil? Why does gRPC design it this way?

<details>
<summary>Answers</summary>

1. Interleaving: both goroutines lock, read `spent == 80` (limit 100), each computes
   `next == 120 <= ... ` — wait, size it so both fit: `spent == 80`, limit 200, each charge
   40. G1 checks (120 ≤ 200), unlocks, signs. G2 checks against the *stale* 80 (120 ≤ 200),
   unlocks, signs. G1 commits `spent = 120`; G2 commits `spent = 120` too (its `next` was
   also computed from 80), so two 40-charges leave `spent == 120` instead of 160 — or, with
   both reading 80 and each adding to *their own* `next`, the last writer wins and budget is
   silently lost/overspent depending on ordering. Holding the lock across `sign()` forces
   G2 to block until G1 has committed 120, so G2 then reads 120 and correctly computes 160.
   The check and commit must be indivisible *with the sign between them*.
2. Safe after `c := r`: `KeyID` (string header is copied and the backing bytes are
   immutable), `ChainID`/`Nonce`/`GasLimit` (scalars, copied bit-for-bit), and `To` (a
   `[20]byte` *array*, a value type, deep-copied by assignment). Still aliasing: `Value`,
   `MaxFeePerGas`, `MaxPriorityFeePerGas` (pointers — the copy duplicates the pointer, not
   the pointee) and `Data` (a slice header — copied header, shared backing array). `copyBig`
   preserves `nil` because `validate` distinguishes a missing amount (`== nil` → "required")
   from a zero amount; turning nil into `big.Int(0)` would defeat that check.
3. Single-threaded: caller reuses one `*big.Int`, passes it as `Value`, then after `Sign`
   returns mutates it and passes the *same struct* again for a second sign — without the
   copy the limiter's stored `next`/history could alias it (the code avoids this by only
   ever storing fresh `big.Int`s, but the request-level copy is the belt-and-braces).
   Concurrent: another goroutine mutates `*Value` in the window between the check and the
   sign, so charge≠sign. Neither exists if `Value` were immutable — an immutable
   `BigInteger`-like type makes aliasing harmless, which is precisely why Java/JS don't need
   this dance.
4. `HexToECDSA` parses the file's hex, and its error can include a fragment of the input it
   choked on — and the input *is* the private key. Wrapping with `%w` would fold that
   fragment into an error that may be logged or returned to a caller. It is the one place
   the habit is wrong because it is the one error whose *cause value* is derived directly
   from secret bytes; everywhere else the cause is safe (a DB error, a field name).
5. `BytesToAddress` right-aligns into a 20-byte array, so 19 bytes gets zero-*padded on the
   left* into a different-but-valid address (and 21+ bytes truncates from the left). Without
   the length check a caller could send a short/long blob and get a signature for an address
   they didn't specify — funds to the wrong destination, silently. `toUint256` rejects `>32`
   because a 33-byte big-endian number isn't a uint256 (it would silently become a huge but
   "valid" `big.Int`); it accepts `0` bytes because empty→`big.Int(0)` is a legitimate zero
   value and `SetBytes` never returns nil, so the domain still gets a non-nil pointer.
6. Removing the embed: compile error — `Server` no longer has
   `mustEmbedUnimplementedSignerServiceServer()`, an unexported method of the
   `SignerServiceServer` interface that only the generated stub can provide, so `Server` no
   longer satisfies the interface. Pointer embed left nil: calling any *unimplemented*
   method dereferences the nil embedded pointer and panics at runtime;
   `RegisterSignerServiceServer` even probes for this at registration. gRPC designs it so
   that adding a method to the service doesn't break existing servers — they inherit an
   `Unimplemented` default and keep compiling — which is forward compatibility bought with
   embedding.

</details>

## 8. Further reading

- [gRPC — Basics tutorial (Go)](https://grpc.io/docs/languages/go/basics/) — the generated
  `ServiceServer` interface, the `Unimplemented…` embed, and server wiring this service
  follows.
- [Go blog — Arrays, slices (and strings): the mechanics of 'append'](https://go.dev/blog/slices)
  — slice headers as `{ptr, len, cap}`, exactly why `bytes.Clone` is needed for a real copy.
- [`math/big` package docs](https://pkg.go.dev/math/big#Int) — that `Int` methods *mutate
  the receiver* and the `Set`/`SetBytes` conventions the copy discipline relies on.
- [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559) and go-ethereum
  [`core/types` `DynamicFeeTx`/`NewLondonSigner`](https://pkg.go.dev/github.com/ethereum/go-ethereum/core/types)
  — the dynamic-fee transaction and London signer the domain delegates the crypto to.
