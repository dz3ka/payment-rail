# M0 — The Repo Skeleton: layout, thin entrypoints, and graceful shutdown

> Scope: this lesson is about *idiomatic Go structure*, not payments. We built the
> load-bearing skeleton — module layout, a shared bootstrap, config loading, build
> stamping, and the repo's canonical test shape. Everything in M1+ hangs off these
> seams, so it's worth understanding them precisely.

## 1. What we built

M0 is the empty airframe of Payment Rail. There is no HTTP server, no gRPC, no database
yet — and deliberately so. What exists is the *structure* that the next seven
milestones will bolt real components onto: a single Go module (`github.com/dz3ka/payment-rail`),
six binaries under `cmd/` (`api`, `ledger`, `signer`, `chainwatcher`, `webhookd`, and
the `paymentrailctl` operator CLI), and three shared private packages under `internal/`
(`config`, `service`, `version`).

Each of the six `main.go` files is intentionally about 20 lines. They do essentially
nothing except hand a closure — "the real work" — to a shared `service.Run(name, fn)`
bootstrap. That bootstrap loads config, builds a structured JSON logger, installs a
signal handler that turns SIGINT/SIGTERM into context cancellation, calls your work
function, and translates its return value into a process exit code. The `version`
package holds build metadata that the `Makefile` stamps in at link time, and it's
split into a testable shape. `config` reads the environment and returns *wrapped
errors* instead of panicking.

Alongside the code sit the non-code deliverables that make this a real project:
CI (`.github/workflows/ci.yml`), lint config (`.golangci.yml`), a docker-compose dev
stack, and the docs — C4 diagrams, three ADRs, and a threat model. This lesson focuses
on the Go; the ADRs (especially ADR-0001) carry the architectural "why."

## 2. The design decision — one module, many binaries, one bootstrap

**The problem.** Payment Rail is five services plus a CLI, and one of those services — the
`signer` — holds private keys and must be *network-isolatable* from everything else.
Every arrow between services is a trust and failure boundary. We need a structure that
makes those boundaries real (separately deployable, separately network-scoped
processes) without the operational tax of running a solo learning project like a
20-team enterprise.

**The chosen approach: single module, monorepo, `cmd/` + `internal/`, one binary per
service.** Standard Go layout. Each service is its own `package main` under
`cmd/<svc>/`; all the private implementation lives under `internal/`. Because each
service compiles to a *separate binary*, it can be scheduled, scaled, and firewalled
independently — the signer can run on its own isolated host with no inbound route from
the public internet.

**Alternatives weighed (from ADR-0001):**

- **Multi-repo / multi-module (one repo per service).** This mirrors how production
  teams often draw boundaries, and it forces API versioning to be explicit. But at a
  solo/learning scale it buys nothing except cross-repo version friction: a change that
  touches `api` and `ledger` together becomes two PRs in two repos with a version bump
  in between. The monorepo keeps one CI pipeline and lets cross-service refactors land
  atomically in a single commit. Rejected.
- **One binary with sub-commands** (`payment-rail api`, `payment-rail signer`, …, à la
  `kubectl`). This is the simplest thing to run — one artifact, one image. But it
  *collapses the signer's trust boundary*: if the signer's key-handling code is
  compiled into the same binary as the public API, you can no longer network-isolate
  it, because it's the same process image with the same attack surface. That directly
  defeats a core security goal, so it was rejected. This is the sharpest trade-off in
  M0 — "simpler to operate" lost to "the security model requires separate processes."
- **The bootstrap itself** could have been copy-pasted into each `main`. Instead we
  extracted `internal/service.Run`. Six copies of signal handling and logger setup is
  six places to get graceful shutdown subtly wrong. One shared seam is the DRY choice —
  and, as we'll see, it's shaped as a *dependency-injection seam* (`RunFunc`) rather
  than a rigid framework.

The pattern name for the `service.Run(name, RunFunc)` shape is **inversion of control /
dependency injection via a function seam**: the framework owns the lifecycle (setup,
signal handling, teardown, exit codes), and the caller injects only the domain-specific
middle. If you've used a web framework's `app.Run(handler)`, this is the same idea at
the process level.

## 3. Language deep-dive

### 3.1 Why `internal/` is a compiler-enforced boundary

```go
module github.com/dz3ka/payment-rail
```

```go
import (
	"github.com/dz3ka/payment-rail/internal/config"
	"github.com/dz3ka/payment-rail/internal/service"
)
```

`internal/` is not a naming convention — it's a rule the Go *compiler* enforces. Any
package whose import path contains a path element named `internal` can only be imported
by code rooted at the parent of that `internal` directory. Here, everything under
`github.com/dz3ka/payment-rail/internal/...` is importable only by code under
`github.com/dz3ka/payment-rail/...`. If someone vendors this module and tries to
`import "github.com/dz3ka/payment-rail/internal/config"` from their own project, the build
*fails*.

This gives you real encapsulation at the module boundary — the equivalent of everything
being package-private-to-the-module by default. Compare: Java's `public`/`package-private`
operates per-package and a determined consumer can still reach in; Go's `internal/`
makes "this is my private implementation, not your API surface" a hard compile error.
For a library you'd expose a curated `pkg/` (or top-level) API and hide the rest in
`internal/`. Payment Rail has *no* public API surface — it's an application, not a library —
so essentially everything lives in `internal/`, and `cmd/` holds only the `main`
packages. That's the idiomatic layout for an app: `cmd/` = entrypoints, `internal/` =
the private guts.

One more subtlety: the module path in `go.mod` (`github.com/dz3ka/payment-rail`) is the
*import prefix*, independent of where the repo is checked out on disk. The imports above
resolve against that prefix, not against a relative filesystem path — Go has no
relative imports.

### 3.2 The thin `main` + injected `RunFunc` seam

```go
// internal/service/service.go
type RunFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) error

func Run(name string, run RunFunc) {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "service", name, "err", err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(cfg.LogLevel),
	})).With("service", name, "version", version.Version)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "env", cfg.Env, "build", version.String())
	if err := run(ctx, cfg, log); err != nil {
		log.Error("service exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}
```

```go
// cmd/signer/main.go
func main() {
	service.Run("signer", func(ctx context.Context, cfg config.Config, log *slog.Logger) error {
		log.Info("signer ready (M0 skeleton: no keys, no signing yet)")
		<-ctx.Done()
		return nil
	})
}
```

`RunFunc` is a **named function type**. In Go, functions are first-class values, and
giving a signature a name lets you pass "a unit of service work" around as a value the
way you'd pass a `Runnable` or a `Func<Context, Config, Logger, Error>` in Java, or an
`async fn` pointer in Rust. The `main` in `cmd/signer` constructs one inline as a
closure and hands it to `Run`. That's the whole DI seam: `Run` owns the *ceremony*
(config, logging, signals, exit codes), the closure owns the *work*.

Line by line in `Run`:

- `config.Load()` returns `(Config, error)` — the ubiquitous Go two-value pattern. On
  error we log and `os.Exit(1)`. Note we use the *package-level* `slog.Error` here,
  because our configured `log` doesn't exist yet (its level comes from `cfg`). That's a
  deliberate ordering constraint.
- `slog.New(slog.NewJSONHandler(...))` builds a structured logger. `.With("service",
  name, "version", ...)` returns a *child logger* that stamps those fields onto every
  subsequent line — so you never have to repeat them. `slog` is Go 1.21+'s standard
  structured-logging package; before it, everyone reached for `zap`/`logrus`.
- `signal.NotifyContext(...)` — the graceful-shutdown core, covered next.
- `defer stop()` — `stop` releases the signal-handling resources registered by
  `NotifyContext`. `defer` schedules it to run when `Run` returns, no matter which path
  we exit by. (Caveat: `os.Exit` does *not* run deferred functions — more in §6.)
- `run(ctx, cfg, log)` invokes the injected work and inspects its returned `error`. A
  non-nil error becomes exit code 1; nil means "stopped cleanly."

And the signer's closure shows the contract from the callee side: it does its (trivial,
for M0) startup, then `<-ctx.Done()` **blocks** until the context is cancelled. When a
signal arrives, `ctx.Done()`'s channel closes, the receive unblocks, and the function
returns `nil`. That's the entire lifecycle a real server will slot into: start
listeners, then block on `ctx.Done()`, then tear down.

### 3.3 `signal.NotifyContext` — signals as context cancellation

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
```

This is the single most instructive line in M0. `context.Context` is Go's standard
mechanism for propagating cancellation and deadlines across API boundaries and
goroutines — think of it as a cancellation token that flows *downward* through every
call. `signal.NotifyContext` derives a child context from `context.Background()` that is
**automatically cancelled when the process receives one of the listed signals** (here
Ctrl-C / `SIGINT` and the orchestrator's `SIGTERM`).

The elegance: shutdown becomes just another `ctx.Done()` — the *same* channel every
timeout, deadline, and parent-cancellation uses. A deeply nested database call that
already respects `ctx` will unwind on Ctrl-C for free, with no signal-specific plumbing.
The alternative — an old-style `make(chan os.Signal)` + `signal.Notify(ch, ...)` +
`<-ch` in `main`, then manually fanning a shutdown out to every goroutine — works but
forces you to invent your own cancellation-propagation scheme. `NotifyContext` folds
signals into the one Go already has.

The comment on the line captures a real operational nicety: the context cancels on the
*first* signal only. A *second* SIGINT is left to the OS default (hard kill), so if a
service wedges during graceful shutdown, an impatient operator can still Ctrl-C twice to
force it down. You get graceful-by-default without losing the escape hatch.

`stop()` (via `defer`) unregisters the signal handler and frees its resources. Not
strictly required right before process exit, but it's the correct, leak-free idiom and
matters if this were ever called from a longer-lived context.

### 3.4 Splitting `format` from `String` for testability

```go
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

func String() string {
	return format(Version, Commit, BuildDate)
}

// format is separated from String so it can be tested without mutating the
// package-level build vars.
func format(version, commit, buildDate string) string {
	return fmt.Sprintf("%s (commit %s, built %s)", version, commit, buildDate)
}
```

`Version`, `Commit`, and `BuildDate` are package-level `var`s (not `const`) — they *must*
be vars, because they're overwritten at link time (§3.6). `String()` reads those globals
and formats them; `format(...)` is an unexported (lowercase) pure function that takes its
inputs as *parameters*.

Why the split? If the only formatter were `String()`, a test could exercise it only by
mutating the package globals `Version = "v1.2.3"` etc. before calling — which is a
shared-mutable-state trap: it's not safe under `t.Parallel()`, and if the test forgets to
restore them it poisons other tests in the package. By extracting the pure `format`
function, the *logic* is testable with plain inputs and zero global mutation. This is a
tiny instance of a big idea: **push side-effecting state (globals, clocks, I/O) to the
edges and keep a pure, easily-testable core.** `format` stays lowercase because it's an
internal helper — nothing outside `version` should call it; `String()` is the public
face (and, by naming it `String()`, `version` satisfies `fmt.Stringer`, so a `version`
value prints nicely with `%v`).

### 3.5 The table-driven test — the repo's canonical shape

```go
func TestFormat(t *testing.T) {
	tests := []struct {
		name    string
		version string
		commit  string
		date    string
		want    string
	}{
		{name: "dev defaults", version: "dev", commit: "none", date: "unknown",
			want: "dev (commit none, built unknown)"},
		{name: "tagged release", version: "v1.2.3", commit: "abc1234", date: "2026-07-16T00:00:00Z",
			want: "v1.2.3 (commit abc1234, built 2026-07-16T00:00:00Z)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := format(tt.version, tt.commit, tt.date); got != tt.want {
				t.Errorf("format(%q, %q, %q) = %q, want %q",
					tt.version, tt.commit, tt.date, got, tt.want)
			}
		})
	}
}
```

This is the idiomatic Go test pattern, and M0 enshrines it as the shape all future tests
should follow. A slice of anonymous-struct cases, each with a `name` and its inputs/`want`,
looped over and run via `t.Run(tt.name, ...)`. The payoffs:

- **`t.Run` creates a named subtest**, so a failure reports as `TestFormat/tagged_release`
  — you see *which case* broke without decoding line numbers, and you can re-run just that
  case with `go test -run 'TestFormat/tagged_release'`.
- **Adding a case is adding a struct literal**, not copy-pasting a whole test function.
  The assertion logic is written once.
- Go has no built-in assertion library by design; `if got != want { t.Errorf(...) }` is
  the whole vocabulary. `t.Errorf` records a failure but keeps going (so multiple cases
  still all run); `t.Fatalf` (seen in `config_test.go`) stops the current test immediately —
  use it when continuing would panic (e.g. after an unexpected error where later lines
  would dereference a zero value).

### 3.6 `-ldflags -X` stamps build metadata into vars

```makefile
VPKG    := github.com/dz3ka/payment-rail/internal/version
LDFLAGS := -s -w \
	-X '$(VPKG).Version=$(VERSION)' \
	-X '$(VPKG).Commit=$(COMMIT)' \
	-X '$(VPKG).BuildDate=$(BUILD_DATE)'

$(BINARIES):
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$@ ./cmd/$@
```

`go build -ldflags "-X 'importpath.Var=value'"` tells the **linker** to overwrite the
initial value of a *string variable* at link time. So `version.Version`, which reads
`"dev"` in source, becomes whatever `git describe --tags --always --dirty` produced in the
build — without touching the source or reading a file at runtime. The metadata is baked
into the binary. (`-s -w` additionally strip the symbol table and DWARF debug info to
shrink the binary; unrelated to `-X` but standard for release builds.)

Two hard constraints this explains:
- The vars *must be package-level `var string`* — `-X` cannot set a `const` (constants
  are compile-time, resolved before linking) and cannot set non-string types.
- At `go run`, `go test`, or plain `go build` (no ldflags), the vars keep their source
  defaults `"dev" / "none" / "unknown"`. That's why the test's "dev defaults" case exists —
  it pins the *un-stamped* behavior. This is Go's standard answer to "how do I get the git
  SHA into `--version` output" that Java/Node solve with a generated properties/JSON file
  read at startup; Go bakes it into the binary at link time instead.

### 3.7 Wrapped errors instead of panics in `config.Load`

```go
if v := os.Getenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
	secs, err := strconv.Atoi(v)
	if err != nil {
		return Config{}, fmt.Errorf("config: parse PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS %q: %w", v, err)
	}
	cfg.ShutdownTimeout = time.Duration(secs) * time.Second
}
```

`config.Load` returns `(Config, error)` and, on a bad value, returns a *wrapped* error
rather than panicking. The `%w` verb in `fmt.Errorf` is the key: it wraps the underlying
`strconv.Atoi` error inside a new one that adds context ("which env var, what raw value"),
while preserving the original in the chain. Callers can later interrogate the chain with
`errors.Is(err, target)` (does any error in the chain equal this sentinel?) or
`errors.As(err, &target)` (is any error in the chain of this concrete type? — bind it).
`%w` is thus strictly more powerful than the plain `%v`/`%s`, which would flatten the
error to a string and sever the chain.

Why return instead of panic? Returning keeps the *failure path in the caller's hands* —
`service.Run` decides to log-and-`os.Exit(1)`. A panic would unwind the stack with a
scary trace and couple every caller to that policy. The Go norm: reserve `panic` for
truly unrecoverable programmer errors (broken invariants), and return `error` for
expected, handleable conditions like "operator typed a non-number." Note also
`return Config{}, err` — Go has no exceptions, so you must return *something* for the
value slot; the zero-value `Config{}` is the conventional "ignore me, check the error
first" placeholder.

### 3.8 `t.Setenv` and the no-parallel constraint

```go
func TestLoadDefaults(t *testing.T) {
	t.Setenv("PAYMENT_RAIL_ENV", "")
	t.Setenv("PAYMENT_RAIL_LOG_LEVEL", "")
	t.Setenv("PAYMENT_RAIL_SHUTDOWN_TIMEOUT_SECONDS", "")
	cfg, err := Load()
	// ...
}
```

`t.Setenv` sets an environment variable *for the duration of the test* and
**automatically restores** the previous value when the test ends — no manual
`defer os.Setenv(old)` cleanup. It's used here to force the vars empty so `getEnv` falls
through to its documented defaults regardless of what's in the ambient shell — making the
test hermetic.

The catch worth internalizing: **`t.Setenv` forbids `t.Parallel()` in the same test**
(it panics if you try). Process environment is global mutable state shared across all
goroutines; if two parallel tests each mutated and restored it, they'd race and clobber
each other. Go's testing package makes that mistake impossible by refusing the
combination. So: env-dependent tests are serial by nature — which is fine, they're fast.
The table-driven `TestFormat` (pure function, no env) *could* run parallel; these config
tests deliberately cannot.

## 4. What would break (and the newcomer bugs avoided)

- **Leaked signal handler / no graceful shutdown.** A newcomer often writes
  `<-someChan` in `main` and calls `os.Exit(0)` on a signal, killing in-flight work
  instantly. `signal.NotifyContext` + blocking on `ctx.Done()` gives every downstream
  goroutine a chance to unwind. `defer stop()` avoids leaking the handler registration.
- **The double-signal wedge.** Cancelling only on the *first* signal and leaving the
  second to the OS means a hung shutdown is still killable. Trapping *all* signals forever
  would make a wedged service unkillable without `kill -9`.
- **Global-mutation test poisoning.** Testing `String()` by assigning to `version.Version`
  would break under parallelism and leak state into sibling tests. Extracting `format`
  sidesteps it entirely.
- **Non-hermetic config tests.** Reading the real ambient environment would make
  `TestLoadDefaults` pass or fail depending on the developer's shell. `t.Setenv` pins it.
- **`const` build vars.** If `Version` had been a `const`, `-ldflags -X` would *silently
  do nothing* (the linker can't touch constants) and every binary would report `dev`. The
  code correctly uses `var`.
- **String-flattened errors.** Formatting the parse error with `%v` instead of `%w` would
  compile and look fine, but any future `errors.Is/As` check would fail to see through the
  wrapper. `%w` keeps the chain intact.
- **`os.Exit` skips `defer`.** `Run` calls `os.Exit(1)` on the error paths — and `os.Exit`
  does **not** run deferred functions. Here that's acceptable (`stop()` cleanup right
  before process death is moot, and the OS reclaims everything), but it's a classic
  footgun: any cleanup that *matters* (flushing a buffer, committing a file) must not sit
  behind a `defer` in a function that can `os.Exit`. Keep `os.Exit` at the very top of
  `main`/`Run`, never buried where real deferred cleanup is pending.

## 5. Compared to what you already know

- **`internal/`** ≈ module-wide "package-private," but *compiler-enforced* across the
  module boundary — stronger than Java package-private (which is per-package and
  reflectively bypassable) and more like a Rust crate's private items, except keyed on the
  directory name `internal` rather than `pub` visibility.
- **`cmd/` + `internal/`** ≈ a Maven/Gradle multi-module build where `cmd/*` are the
  `application` modules with `main` classes and `internal/*` are the `library` modules —
  but it's all one Go module, so no version graph between them.
- **`service.Run(name, RunFunc)`** ≈ a framework's `app.run(handler)` / Spring Boot's
  `SpringApplication.run` — inversion of control at the process level. `RunFunc` ≈ a
  Java `@FunctionalInterface` / a Rust `Fn(...) -> Result<(), E>` you pass as an argument.
- **`context.Context`** ≈ a `CancellationToken` (C#) or an `AbortSignal` (JS) threaded
  through every call — but idiomatic Go passes it *explicitly as the first parameter*
  everywhere, rather than stashing it in thread-locals/async-context. `signal.NotifyContext`
  ≈ wiring `Ctrl-C` / `SIGTERM` into that token automatically.
- **`(T, error)` returns** ≈ Rust's `Result<T, E>`, but *unchecked by the compiler* — Go
  won't force you to handle the error (the linter and `errcheck` do). `%w` + `errors.Is/As`
  ≈ exception `getCause()` chains / Rust's `source()` and `anyhow` context — you attach
  context while preserving the root cause.
- **Table-driven tests** ≈ JUnit `@ParameterizedTest` / `@MethodSource`, but hand-rolled
  with a plain slice and `t.Run`, and with no assertion DSL (no AssertJ/Hamcrest —
  `if got != want { t.Errorf }` is the idiom).
- Where the analogy *breaks*: there's no exceptions, no DI container, no reflection-based
  wiring, and no relative imports. Go leans on explicitness — pass the context, return the
  error, name the type — over framework magic.

## 6. Gotchas & idioms specific to this diff

- **`os.Exit` bypasses `defer`.** Covered above — the reason `Run` keeps its `os.Exit`
  calls shallow.
- **`slog.Error` (package-level) vs `log.Error` (child).** The config-load failure uses
  the package-level logger because the configured one doesn't exist yet. Ordering matters:
  you can't log at the configured level before you've loaded the config that sets it.
- **Zero values as sentinels.** `return Config{}, err` returns a fully-formed zero `Config`,
  not `nil` — structs aren't nilable. The contract is "if err != nil, ignore the value."
- **`With(...)` returns a new logger.** `slog`'s `With` doesn't mutate; it returns a child.
  Idiomatic structured logging = build a context-stamped child once, pass it down.
- **Named function type vs inline signature.** `type RunFunc func(...)` reads far better at
  call sites and in docs than repeating the raw four-parameter signature everywhere.
- **`t.Setenv` ⇒ no `t.Parallel()`.** Structural, enforced by a panic. Env-touching tests
  are serial.
- **`%q` in test/error messages.** `%q` quotes strings, so an empty or whitespace value
  shows as `""` — invaluable when the bug *is* an empty string.

## 7. Check yourself

1. Change `version.Version` from `var` to `const` (mentally). Which build path breaks, and
   what would every binary then report for its version — and *silently* or with an error?
2. Why does `service.Run` block on `<-ctx.Done()` inside the injected closure rather than
   `Run` itself owning the block? What flexibility does putting it in the closure buy M1's
   HTTP server?
3. `TestFormat` can safely call `t.Parallel()`, but `TestLoadDefaults` cannot. State the
   exact reason in terms of shared mutable state.
4. Rewrite the `strconv.Atoi` error line using `%v` instead of `%w`. What still works, and
   what specifically stops working for a future caller?
5. An operator sends SIGTERM, the service hangs flushing a queue for 30s, and they lose
   patience. What does a second Ctrl-C do here, and which line makes that possible?

<details>
<summary>Answers</summary>

1. The `-ldflags -X` build path breaks: the linker cannot overwrite a `const` (it's
   resolved at compile time, before linking). It fails *silently* — no error — and every
   binary reports the source default `dev (commit none, built unknown)`, because the `-X`
   flags are simply ignored for constants.
2. `Run` owns the *lifecycle* (setup/teardown/exit codes); the closure owns the *work*,
   and "how to block until shutdown" is part of the work. M1's HTTP server will start
   `http.Server.ListenAndServe()` in a goroutine, then block on `ctx.Done()`, then call
   `server.Shutdown(shutdownCtx)` — a teardown sequence unique to that service. If `Run`
   hard-coded the block, it couldn't run service-specific shutdown logic.
3. `t.Setenv` mutates process-global environment state shared by all goroutines. Two
   parallel tests would race on it and clobber each other's restore. `format` is a pure
   function of its arguments with no shared state, so parallel runs can't interfere. Go
   enforces this by panicking if you call `t.Parallel()` after `t.Setenv`.
4. With `%v` the error message string is identical, so logging looks the same. What breaks:
   the wrapper no longer *contains* the underlying `strconv.NumError`, so
   `errors.Is(err, strconv.ErrSyntax)` and `errors.As(err, &numErr)` return false/fail —
   the causal chain is severed. `%w` preserves it.
5. The context cancels on the *first* SIGINT/SIGTERM only; the second signal is not trapped,
   so it hits the OS default action and hard-kills the process. `signal.NotifyContext(...,
   syscall.SIGINT, syscall.SIGTERM)` — by registering the handler for exactly one cancellation
   and not re-arming — is what leaves the second signal to the OS.

</details>

## 8. Further reading

- [Go — `signal.NotifyContext`](https://pkg.go.dev/os/signal#NotifyContext) and the
  [`context` package](https://pkg.go.dev/context) docs.
- [Go Blog — Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) (`%w`,
  `errors.Is`, `errors.As`).
- [Go Wiki — Table-driven tests](https://go.dev/wiki/TableDrivenTests) and
  [`testing.T.Setenv`](https://pkg.go.dev/testing#T.Setenv).
- [`cmd/link` documentation](https://pkg.go.dev/cmd/link) for the `-X` and `-s -w` linker
  flags, and [`log/slog`](https://pkg.go.dev/log/slog) for structured logging.
