# ADR-0030: The front door is a contract — fixed README order, no process vocabulary, every claim verified

- **Status:** Accepted. Binds `README.md` (and any future top-level reader-facing doc);
  explicitly does **not** bind `docs/adr/` or `docs/learning/`. Supersedes none.
- **Date:** 2026-08-05
- **Deciders:** Bogdan Dzekic

## Context

`README.md` is the only document in this repository that is read by people who have not
decided yet whether to read anything else, and the only one that is *executed* rather than
merely read. It had drifted into the shape a README acquires when it grows alongside the
code: sections in the order they happened to be written, capabilities described as the
increments that produced them, and a handful of assertions that were true when typed and
silently stopped being true afterwards — a CI badge pointing at a branch the workflow no
longer triggered on being the clearest case.

Nothing compiles a README. `go vet` will not tell you the binary inventory is off by one,
and no test fails when a `make` target named in a code fence stops existing. The decay is
therefore invisible from inside the repo and only visible to the reader — the one person
who cannot fix it.

payment-rail is also one of three sibling portfolio repositories. A reader who evaluates
one of them and then opens another should not have to re-learn where anything is.

## Decision

Front-door documentation is treated as an interface with three obligations.

1. **A fixed section order, shared across the sibling repositories.**

   H1 + italic tagline → two badges (CI, license) → blockquote disclaimer → **Status**
   ("Feature-complete." followed by capabilities prose) → **Features** → **Architecture** →
   **Quickstart** (prerequisites stated before the first command) → domain-specific sections
   → **Repository layout** → documentation pointers → **Scope & limitations** → **License**
   last.

   The order is an answer to a fifteen-second question: *is this worth my time?* It
   front-loads what the thing is and what state it is in, puts the runnable proof in the
   upper half, and defers what the thing deliberately does **not** do to the end — where an
   honest limitations list reads as engineering judgement rather than as a warning label. A
   repo may realise the documentation-pointer slot inside the layout block when the tree
   already names the files, as this one does (`README.md:105-117`); the slot's *position*
   is the convention, not its markup.

2. **No development-process vocabulary and no internal milestone or phase codes on the
   front door.** Features are described as capabilities the software *has*, never as
   increments that were *delivered*. "M5 added a policy engine" becomes "screens payments
   through a policy / velocity / four-eyes engine". Milestone codes carry meaning only for
   whoever held the schedule; to everyone else they are noise, and — together with
   process terminology — they are the single clearest tell that a document is an internal
   working artifact rather than a released project.

   The exception is narrow and deliberate: where the delivery increment *is* the subject
   matter, the codes stay. `docs/adr/` and `docs/learning/` are organised by increment by
   design — an ADR's whole value is being pinned to the moment and the constraints under
   which the decision was made — and 36 files across those two trees carry M0–M7 codes
   today. This ADR binds the front door, not the archive.

3. **Every factual claim and every command is verified against the tree before it ships.**
   Not reviewed for plausibility — executed or resolved. The standing checklist, each item
   of which has been wrong here at least once:

   - the CI badge's `?branch=` parameter matches **both** the real default branch **and** a
     branch the workflow actually triggers on (these are two different facts; the badge was
     `?branch=master` while `ci.yml` triggered on `main`, so it reported nothing);
   - the stated toolchain version matches `go.mod` (README says Go 1.26+, `go.mod:3` says
     `go 1.26.0`);
   - inventories match reality — "Seven binaries" against seven `cmd/*/` directories;
   - every documented `make` target resolves (`make -n up build test down test-chaos`);
   - every relative link and in-page anchor resolves against the working tree.

## Alternatives considered

- **Leave the order to taste, per repo.** Cheapest, and defensible for a single repo. It
  loses the property that actually pays: three repos a reader can navigate identically.
  The order above is not claimed to be optimal — only to be one order, everywhere.
- **Keep milestone codes but add a legend.** Considered because the codes are genuinely
  useful shorthand internally. Rejected: a legend asks the reader to load a private
  vocabulary before reading a feature list, which is a worse tax than the one it removes.
- **Enforce the conventions with a linter or a docs test.** A CI step could resolve links,
  run `make -n` on fenced targets, and diff the README's Go version against `go.mod`.
  Attractive and probably correct eventually. Rejected for now on scope: the checks that
  are mechanical (links, targets, versions) are the ones that are also cheap to eyeball,
  while the checks that carry the value (does the Status prose describe capabilities or
  increments?) are not mechanisable. Building the easy half would buy false confidence
  in the hard half.
- **Fold the limitations into the Status section**, so the caveats arrive with the claims.
  Honest, but it makes the first screen defensive; a reader deciding in fifteen seconds
  would meet the disclaimers before the capabilities.

## Consequences

- The section order and the vocabulary rule are **conventions, not tooling** — nothing
  fails if they are broken. They are enforced at review, and a reviewer of any README
  change is expected to walk the part-3 checklist rather than trust the diff.
- Feature prose is now written twice in different registers: capability-shaped on the front
  door, increment-shaped in `docs/learning/`. That duplication is accepted; the two
  audiences want genuinely different documents.
- Adding a binary, renaming a `make` target, changing the default branch, or bumping the
  toolchain in `go.mod` are now **README-affecting changes**. The binary count and the
  toolchain version are the two claims most likely to rot first.
- The badge lesson generalises: any URL with a branch, tag or ref embedded in it encodes an
  assumption about repository configuration, and repository configuration changes outside
  the repository. Those are re-checked, not inherited.
- Follow-up left open deliberately: if the checklist is repeatedly missed at review, revisit
  the rejected linter alternative for the mechanical half of it.
