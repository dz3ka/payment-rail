# Ops — Calling somebody else's workflow: a per-PR preview environment, and the review pass that deleted three quarters of it

> Scope: this lesson is about **being the consumer half of a contract that lives in another
> repository**. payment-rail gained two files — `deploy/paymentstack.yaml` (a committed
> `PaymentStack` manifest used as the per-PR preview template) and
> `.github/workflows/preview-environment.yml` (a `pull_request`-triggered caller of
> `dz3ka/bosun/.github/workflows/preview-environment.yml`). Fifty-seven lines of YAML and
> thirty-four lines of YAML, most of it comments. There is no Go here, so there is no Go
> deep-dive; the transferable content is elsewhere and it is worth more than the line count
> suggests: **how you read a producer's source to derive your own defaults**, what a
> plan-*challenging* review pass catches that a plan-*writing* design pass structurally
> cannot, why the minimal declarative manifest beats the complete-looking one, and where
> GitHub's reusable-workflow trust boundary actually sits. The decision record is
> `docs/adr/0029-preview-environment-caller-and-template.md`; this lesson is the reasoning
> behind it.

## 1. What we built

bosun — the sibling platform repo at `/home/dzeka/prd-projects/bosun` — owns a Kubernetes
operator with three CRDs (`ManagedApp`, `PaymentStack`, `PreviewEnvironment`) and a reusable
GitHub Actions workflow that stands up a preview environment for a pull request. The
mechanism: a caller repo commits a `PaymentStack` manifest, the reusable workflow checks out
both repos, runs bosun's `cmd/preview-manifest` renderer to wrap that manifest's `spec` into a
`PreviewEnvironment` stamped with the PR's identity, and pipes the result into `kubectl apply`.
The operator then creates the child `PaymentStack`, which creates a `ManagedApp` per app, a
CloudNativePG `Cluster` for the database, and a Redpanda `Topic` per topic — and deletes the
lot when the TTL expires.

payment-rail is the *caller*. Its whole job in this round is to supply two things: the
template (what the stack looks like) and the trigger (when and with what arguments to invoke
bosun's workflow). Everything else — naming, rendering, applying, TTL enforcement, teardown —
belongs to the other repo and is deliberately not re-implemented here.

The honest scope is narrower than "preview environments work now." payment-rail **builds no
container images**: there is no Dockerfile anywhere in the repo, and `Makefile:22-23` produces
native binaries into `./bin`. And bosun's `hack/preview-up.sh` **creates no namespace and no
Redpanda cluster** — `spec.messaging.clusterRef` must name a broker that already exists in the
target namespace. So on a real cluster this preview has two known reds by construction: the
`api` pod sits in `ImagePullBackOff`, and the Topic child never goes Ready. What the round
actually delivers is **contract agreement**: the manifest matches the CRD schema, the caller's
inputs match the callee's declared `workflow_call` signature, and the whole thing renders. That
is a real deliverable — it is the half of the work that fails at 3am if you skip it — but
calling it "a running preview" would be a lie, and the manifest's comments say so in the file
rather than in a wiki nobody reads.

## 2. The design decision

### Decision A: one app in the template, not four — and *why the review was right* matters more than *that* it was right

**The problem.** `PaymentStackSpec.Apps` has `MinItems=1`, `MaxItems=10`. payment-rail has
seven binaries under `cmd/`: `api`, `chainwatcher`, `ledger`, `outboxrelay`, `paymentrailctl`,
`signer`, `webhookd`. Which of them belong in a preview template?

The architect's plan said four: `api`, `chainwatcher`, `outboxrelay`, `webhookd`, each with a
placeholder port for the three that don't listen on anything. The stated rationale was that
the topic in `spec.messaging.topics` **needs a producer** — a topic with nobody writing to it
is a half-declared stack.

**The chosen approach: exactly one app, `api`.** The review pass (`razor`) cut three apps, and
the reason it gave is the interesting part: *the rationale was factually false about the CRD.*
Read `PaymentStackSpec` (`bosun/api/v1alpha1/paymentstack_types.go:12-31`) and there is **no
link whatsoever** between `spec.apps` and `spec.messaging.topics`. They are two independent
lists; `paymentstack_objects.go` builds one `Topic` object per entry of `spec.messaging.topics`
and never consults `spec.apps` while doing it. The operator creates the topic whether or not
anything runs. So the justification for the extra three apps did not survive contact with the
producer's source code.

Once that rationale is gone, what's left is arithmetic on the actual consequences. With no
images published, each extra app buys exactly one unschedulable `Deployment` (its pod stuck in
`ImagePullBackOff`) and one `Service` with no endpoints. Three of those is not "more complete,"
it is three more red objects for a reviewer to triage past. And the remaining two binaries are
excluded on their own facts: `cmd/ledger` runs **in-process** under `api` (ADR-0006), so
listing it would create a second copy of something that isn't a separate process; `cmd/signer`
defaults to a loopback bind address (`internal/config/config.go:87`) and nothing in this
template overrides it, so a Service in front of it would point at a port nothing outside the
pod's netns can reach.

**Alternative 1 — ship all four as planned.** Rejected because its premise was false and its
cost was three permanently-red children. The general form of this trap is worth naming: *a
design justification that describes a dependency the schema does not encode.* "X needs Y" is a
claim about the producer's behaviour, and the only place to check it is the producer's code.

**Alternative 2 — ship zero apps and only the database + topics.** Not available: `MinItems=1`
on `Apps` is a schema-level refusal, and the CRD's doc comment is explicit that a stack missing
one of its three parts "is a different kind of thing, not a smaller stack."

**Alternative 3 — ship all seven, `paymentrailctl` and all.** Fails on the same
unschedulable-children argument, plus `paymentrailctl` is a CLI: a `Deployment` of a program
that exits immediately is a `CrashLoopBackOff` by design.

The decision is explicitly **re-decided when payment-rail publishes images**, and the manifest
says that in a comment at the point of decision. That is the difference between a minimal
template and a lazy one: the minimal one records the condition under which it stops being
right.

### Decision B: a placeholder image that *cannot* be mistaken for a release

```yaml
image: ghcr.io/dz3ka/payment-rail/api:0.0.0-unpublished
```

Two independent choices are packed into that string.

**The tag.** `0.0.0-unpublished` is not a semver anyone will ever cut, and it reads as broken to
a human scanning `kubectl get pods -o wide`. Alternatives considered: `latest` (looks
legitimate, and would silently pull *something* the day an unrelated image lands at that
coordinate), or omitting the tag entirely (illegal — `Image` is `MinLength=1` and the operator
concatenates a tag on override, so a tagless reference would produce a malformed one).

**The path form**, `payment-rail/api` rather than a shared `payment-rail` repository, is the
non-obvious one, and it is a *forward-compatibility* choice made against the producer's
override semantics. From `bosun/internal/preview/manifest.go:244-269`:

```go
for _, override := range overrides {
	matched := 0
	for i := range template.Apps {
		if imageName(template.Apps[i].Image) != override.name {
			continue
		}
		template.Apps[i].Image = override.name + ":" + override.tag
		matched++
	}
	if matched == 0 { /* error */ }
}
```

`--image name=tag` matches by **image name**, not by app name — deliberately, mirroring the
kustomize images transformer. One override therefore rewrites *every* app whose image name
matches. With a shared placeholder like `ghcr.io/dz3ka/payment-rail:0.0.0-unpublished` across
future apps, `images: payment-rail=<sha>` would rewrite all of them at once and there would be
no way to pin one app to a different build. The per-binary path keeps each app individually
addressable. This costs nothing today (there is one app) and is unfixable-in-place later
without editing every app entry, which is the profile of a decision worth making early.

Note also `matched == 0` is an **error**, not a silent no-op. That is the producer refusing to
let a caller's typo produce a green pipeline that reviewed the wrong image — the same instinct
as the multi-line `images` refusal in the workflow. Good contracts fail on ambiguity.

### Decision C: one shared namespace, because the callee gives you no choice

```yaml
namespace: payment-rail-previews
```

The tempting design is a namespace per PR: perfect isolation, trivial teardown, no name
collisions. It is unavailable, and the reason is a property of the callee, not a preference.

`hack/preview-up.sh` runs exactly one mutating command — `kubectl apply --namespace "${namespace}" -f -`
— and never creates a namespace. And `PaymentStackMessaging.ClusterRef` is documented as "the
name of the streaming cluster, **in this namespace**". So a per-PR namespace would need, per
PR: a namespace, a CloudNativePG operator-visible Postgres, and a running Redpanda cluster,
all provisioned by something that does not exist in either repo. Shared namespace it is.

The cost is real and is written down rather than discovered later: **concurrent previews
collide.** Object *names* are already PR-scoped — bosun's `preview.Name` derives
`trunc(slug(repo),30)-sha256(repo)[:8]-pr-N`, so PR 41's `PreviewEnvironment` and PR 42's are
distinct objects. But the *topic name inside the broker* is `payment-rail.events` for both,
and `webhookd`'s consumer group is a constant in payment-rail's code. Two live previews would
share a topic and steal each other's messages. The callee exposes no topic-name override, so
this is a property of the contract; the fix, when it bites, is a bosun-side input, not a
payment-rail-side workaround.

### Decision D: `ttl-minutes: 480`, derived from two facts in the callee's source

The callee defaults `ttl-minutes` to 60. Taking the default would have been the obvious move.
It is wrong, and seeing why requires reading two unrelated files in the other repo.

Fact one, `bosun/internal/controller/previewenvironment_objects.go:76`:

```go
func expiresAt(env *platformv1alpha1.PreviewEnvironment) time.Time {
	return env.CreationTimestamp.Add(time.Duration(*env.Spec.TTLMinutes) * time.Minute)
}
```

Expiry is anchored to the object's **creation** timestamp — the moment of the *first* apply.

Fact two, `bosun/internal/preview/name.go:42-56`: the environment's name is a pure function of
`(repository, pullRequest)` and **the head SHA is deliberately excluded**, "which is what makes
CI's apply an update of one environment rather than a new one per commit."

Put together: every push to a PR re-applies the *same* object, which means `CreationTimestamp`
never moves, which means **a re-apply does not extend the TTL**. With the default, a preview
created when the PR was opened is deleted 60 minutes later regardless of how much pushing
happens, mid-review, with a green pipeline and no error anywhere. 480 minutes is one working
day — long enough that the review finishes first, short enough that a forgotten PR doesn't
hold a database overnight. (The CRD caps TTL at 10080 minutes, one week.)

This is the lesson-shaped part: **the two facts that combine into the bug live in two different
files in a repo you don't own, and neither one is wrong on its own.** Stable naming is a
feature. Creation-anchored expiry is a feature. Their composition is a footgun, and the only
way to find it before it fires is to read the producer's code rather than its README.

### Decision E: per-PR `concurrency` with `cancel-in-progress`

```yaml
concurrency:
  group: preview-environment-${{ github.event.pull_request.number }}
  cancel-in-progress: true
```

Push twice in quick succession and two runs race to apply the *same* object name (same repo,
same PR ⇒ same `preview.Name`). Whichever finishes last wins, and there is no ordering
guarantee — the older run can overtake the newer one. The `PreviewEnvironment` carries
`platform.bosun.dev/head-sha` as an annotation, so the loser writing last leaves the object
*claiming to be a commit it is not*. The environment doesn't just lag; it lies, and everything
downstream that reads that annotation to answer "what am I looking at" lies with it.

The group key is the PR number, so concurrency is scoped exactly as narrowly as the collision
is: two different PRs apply two different objects and must not block each other.

`cancel-in-progress: true` is only safe because of a property of the callee — its final step is

```yaml
- name: remove kubeconfig
  if: always()
```

A cancellation is not a normal completion, and a cleanup step guarded by success would be
skipped, leaving a live cluster credential on a runner that may be shared. `if: always()` runs
on cancellation too. Had the callee cleaned up only on success, the correct caller-side choice
would flip to `cancel-in-progress: false` — **the safety of your concurrency policy is a
function of the callee's cleanup guarantees**, which is not something the input signature tells
you.

### Decision F: no fork gate — a deliberate omission, with a named trigger

A common pattern is `if: github.event.pull_request.head.repo.full_name == github.repository`,
skipping the job for fork PRs. It was considered and deliberately not added.

The trigger here is `pull_request`, **not** `pull_request_target`. For a fork PR under
`pull_request`, GitHub does not expose repository secrets: `${{ secrets.PREVIEW_KUBECONFIG }}`
evaluates to the empty string. The job therefore starts, writes an empty kubeconfig, and fails
at the first `kubectl` call. A fork gate would convert that red X into a skip — cosmetically
nicer, functionally identical, and one more conditional to maintain. The trigger to add it is
recorded: **the first fork PR against this repo.** Until then it would be a guard against a
case that has never occurred.

The choice of `pull_request` over `pull_request_target` is the load-bearing security decision
and deserves its own sentence. `pull_request_target` runs with the *base* repo's secrets while
the PR's code is fetched — and the callee here checks out the caller's tree and runs the Go
toolchain in a workspace beside a live kubeconfig. That combination is the textbook
privilege-escalation shape for Actions. bosun's own workflow comments show it has thought hard
about the same boundary (root checkout ordering, `GOWORK=off`, the `.caller` dot prefix); the
caller's contribution to that defence is simply choosing the trigger that never hands a fork
the secret in the first place.

### Decision G: `@main` on the callee reference

```yaml
uses: dz3ka/bosun/.github/workflows/preview-environment.yml@main
```

Two runs a week apart can execute different bosun code. That is a genuine trade-off, taken
knowingly: both repos are the same author's, they evolve together, and pinning to a SHA would
mean a manual bump in payment-rail every time bosun's preview tooling changes — the classic way
a consumer drifts a year behind and nobody notices until it breaks.

What makes `@main` tolerable is that the callee is **self-consistent within a run**: it checks
itself out at `${{ github.job_workflow_sha }}`, the commit the workflow file itself was read
from, so the tooling that runs is the tooling that was called. There is no window in which the
YAML comes from one commit and `hack/preview-up.sh` from another.

For a **third-party** reusable workflow the calculus inverts completely: `@main` on somebody
else's repo means an arbitrary future commit of theirs executes with your secrets. Pin those to
a full commit SHA. The rule is not "always pin" or "never pin" — it is *pin across a trust
boundary, float within one*.

### Decision H: verification without the target environment

Nothing about this change could be end-to-end tested on this machine: there is no cluster, no
CI secret, and no `actionlint`, `yq`, `kubeconform` or `kind` installed. Two checks were run
instead, chosen for the two classes of failure that would otherwise surface only in
production.

**Local render** through bosun's `cmd/preview-manifest`, pointed at the committed template.
This exercises `LoadTemplate`, which decodes **strictly** (`Strict: true` on the apimachinery
serializer — unknown *and duplicated* fields rejected) and checks the kind. A misspelled
`storagesize`, a stray `replicaCount`, a duplicate key: all rejected here, at a place where the
error names the file. Without it, the same mistakes travel to an API server that rejects them
much later with nothing left to point at.

**A hand-written input-name subset check** — assert that every key under the caller's `with:`
appears among the callee's declared `workflow_call` inputs. This is the class GitHub only
reports **at dispatch time**: an undeclared input is an "invalid input" error when the workflow
is *called*, not when it is *linted*, so a typo in `template-path` sits green in the repo until
the first PR opens. Reproducing that check locally costs ten lines and moves the failure from
"first real run" to "before commit".

What was *not* verified, stated plainly rather than glossed: no cluster ever saw this, no
secret was ever read, and the first real run is still the first real run. That's the correct
level of confidence to claim, and writing it into the ADR's Consequences section is what stops
the next person from assuming otherwise.

## 3. Contract deep-dive — reading the two files line by line

There is no Go in this diff, so this section does for the YAML what the Go lessons do for
snippets: walk the constructs that carry meaning and explain the machinery underneath.

### 3a. `uses:` at job level — a typed function call across a repo boundary

```yaml
jobs:
  preview:
    uses: dz3ka/bosun/.github/workflows/preview-environment.yml@main
    with:
      pull-request-number: ${{ github.event.pull_request.number }}
      head-sha: ${{ github.event.pull_request.head.sha }}
      ...
    secrets:
      kubeconfig: ${{ secrets.PREVIEW_KUBECONFIG }}
```

A job with `uses:` is a **reusable workflow call**, and it is structurally different from a step
with `uses:` (which is a composite action). The whole job is replaced by the callee's jobs; you
cannot also write `steps:` here, you cannot add a `runs-on` (the callee picks its own runners),
and the only things that cross the boundary are `with:` (inputs), `secrets:` (explicitly named
secrets), and — coming back — the callee's declared `outputs:`.

That is what makes it a **typed call**. The callee's `on.workflow_call.inputs` block is a
signature: names, types (`number`, `string`, `boolean` — no list type exists, which is why
`images` is a comma-separated string with a hand-rolled splitter on the far side), `required`
flags and defaults. GitHub validates the call against it. Pass a key the callee doesn't declare
and the run fails immediately; omit a `required: true` input and likewise.

Two inputs are conspicuously *absent* from the signature, and the callee's header comment
explains both. There is no `repository` input, because inside a called workflow
`github.repository` is already the **caller's** repository — an input for it could only ever
disagree with the checkout that actually happened. And there is no `bosun-ref` input, because
`github.job_workflow_sha` already names the commit the workflow file came from. Both omissions
follow the same rule, and it generalizes far beyond Actions: **do not accept a parameter whose
value the callee can derive, when a wrong value would be silently accepted.** A `repository`
input is a field that can lie; deriving it makes the lie unrepresentable.

### 3b. `secrets:` by name vs `secrets: inherit` — where the trust boundary is

```yaml
    secrets:
      # Never `secrets: inherit` — that ships every secret to a workflow in
      # another repository. PREVIEW_KUBECONFIG must be configured in this
      # repo's settings for the job to run.
      kubeconfig: ${{ secrets.PREVIEW_KUBECONFIG }}
```

`secrets: inherit` is one word shorter and hands the callee **every** secret configured on this
repository and its organization. For a workflow in the same repo that is merely sloppy. For a
workflow in *another* repository it is a delegation of your entire secret store to code you do
not review on every change — and here the callee is at `@main`, so "code you do not review" is
literal.

Naming the single secret makes the blast radius equal to the credential the feature actually
needs. The mental model to carry: this is the difference between passing a scoped token to a
library function and handing it `process.env`. The reason the second is common is that it
always works on the first try.

Note the callee declares `kubeconfig` under `on.workflow_call.secrets` with
`required: true`, and immediately writes it to a file from an **environment variable** rather
than interpolating it into the `run:` line — because a secret spliced into a command *is* the
command, and any `set -x` prints it. That's the producer's half of the same discipline; worth
recognizing so you can tell a careful callee from a careless one before you hand it a
credential.

### 3c. `permissions:` in the caller is a ceiling, not a request

```yaml
permissions:
  contents: read
```

The caller's `GITHUB_TOKEN` permissions bound what the called workflow can get. A reusable
workflow cannot escalate beyond its caller's grant, so declaring the minimum here is a real
control and not decoration. `contents: read` is what the callee's own `actions/checkout` of the
caller's tree needs, and nothing more: no `packages: write`, no `pull-requests: write` (nothing
posts a comment yet), no `id-token: write`.

When the follow-up image-build job lands, it will need `packages: write` — and the right move
is to scope that **at the job level** on the build job, not by widening this top-level block,
so the preview job keeps its read-only token.

### 3d. Omitted fields in the manifest are a *statement*, not an absence

```yaml
    - name: api
      image: ghcr.io/dz3ka/payment-rail/api:0.0.0-unpublished
      port: 8080
      # replicas is omitted: the default of 1 is what a preview wants...
```

`PaymentStackApp.Replicas` and `.Port` are `*int32` with `+kubebuilder:default=1` and
`+kubebuilder:default=8080`. The pointer is what makes "unset" distinguishable from "set to the
zero value" — exactly the same three-state problem Go's `encoding/json` solves with pointer
fields, and the reason `Replicas` documents that "Zero is legal: it scales the application to
nothing without removing it from the stack." A non-pointer `int32` could not tell those apart.

Defaulting happens **in the API server**, on admission. So an omitted field is not missing at
rest; it comes back populated on the next read, which is why bosun's controller dereferences
those pointers without a nil check and pins the assumption with a test
(`TestPaymentStackDefaultsEveryOptionalField`).

Which raises the question the manifest answers explicitly: if the server fills it in anyway,
why write `port: 8080` and not `replicas: 1`?

- `port` is written because the value it must agree with is **a constant in payment-rail's Go
  code** — `cmd/api/main.go:29`, `const listenAddr = ":8080"` — not configuration. Today the
  CRD default happens to match. Pinning it means that if the constant ever moves, the template
  is the place that has to move with it, and the Service stays honest. The field encodes a
  cross-repo agreement, so it is stated.
- `replicas` is omitted because there is no such agreement: 1 is simply what a preview wants,
  and restating a default duplicates a decision that lives correctly in the CRD.

The general rule, and it is the declarative-config rule that most teams get backwards: **state a
field when its value is a fact about your system; omit it when its value is a fact about the
platform's default.** Copy-pasting the full schema with every default spelled out produces a
manifest where nothing stands out as intentional.

There is a sharper version of this under **server-side apply**: a field you write is a field you
*own*, recorded in `metadata.managedFields`. bosun's own controller comments say it outright —
"a field present here is a field bosun owns forever, so the omissions are as much of the
contract as the values." Write `replicas: 1` and you have claimed ownership of replica count
against every other actor (an HPA, an operator, a human) forever.

### 3e. The topic name, and a length budget that quietly runs out at PR 10000

```yaml
      - name: payment-rail.events
        partitions: 1
```

The name is `outbox.DefaultTopic` (`internal/outbox/envelope.go:24`) verbatim, which is the
whole point: the relay publishes there, so a template naming anything else provisions a topic
nobody writes to. But the verbatim choice carries a bound, and the arithmetic is worth doing
because it is the kind that nobody does until the API server refuses a write.

bosun labels each child with `app.kubernetes.io/name`, whose value is capped at **63
characters**, and derives child names by concatenation:

| step | rule | for `dz3ka/payment-rail`, PR number of `d` digits |
|---|---|---|
| environment | `trunc(slug,30) + "-" + digest8 + "-pr-" + N` | `18 + 1 + 8 + 4 + d` = `31 + d` |
| child stack | `<env>--stack` | `38 + d` |
| child topic | `<stack>--payment-rail.events` | `38 + d + 2 + 19` = `59 + d` |

At `d = 4` that is exactly 63. At `d = 5` it is 64 and the API server rejects the Topic. So this
template is sound for PR numbers ≤ 9999 and breaks at 10000.

The instructive bit is *where the budget went wrong*. bosun's `Name` doc comment does compute a
budget — "a label value stops at 63 characters, and the operator writes this name into one
after appending the child stack's `--stack` (7), leaving 56" — and bounds PR numbers at seven
digits on that basis. That budget accounts for the **child**, but the topic is a
**grandchild**, and its suffix spends the remaining 25 characters that the budget never
reserved. A composed naming scheme is only as safe as the *deepest* derivation, and each level
was locally correct.

Shortening the topic here would fix the arithmetic and break the more important property —
template agreeing with `outbox.DefaultTopic` — so the bound is accepted and written down, and
the real fix is recorded as belonging to bosun's name budget. That is the right shape for a
constraint you can't fix from your side: **make it explicit, locate the fix, move on.**

### 3f. `images` omitted, and the multi-line refusal on the far side

```yaml
      # images is omitted, not passed empty: the callee defaults it.
```

`images` is `required: false, default: ''`. Omitting it and passing `''` are equivalent to the
callee today — but they say different things, and only one survives the callee changing its
default. Omission means "I have no opinion, use yours"; `images: ''` means "I insist on empty."
The former is what's meant.

The callee's handling of that input is a small masterclass in boundary hygiene worth studying
even though payment-rail never trips it:

```bash
if [[ "${IMAGES}" == *$'\n'* ]]; then
  echo 'preview-environment: the images input must be one line ...' >&2
  exit 1
fi
image_flags=()
IFS=',' read -ra image_list <<<"${IMAGES}"
for image in ${image_list[@]+"${image_list[@]}"}; do
  if [[ -n "${image}" ]]; then image_flags+=(--image "${image}"); fi
done
```

`read -ra` consumes the **first line only**. A caller writing `images: |` as a YAML block
scalar would get every override after the first silently dropped — a green pipeline reviewing a
stale image, the worst possible failure mode. So the multi-line case is refused explicitly
rather than truncated. Empty elements are skipped (covering both the empty default and a
trailing comma), and `${array[@]+"${array[@]}"}` is the standard incantation for expanding a
possibly-empty array under `set -u`, where a bare `"${arr[@]}"` on an empty array is an unbound
variable error in older bash.

When payment-rail does start publishing images, the caller gains one line —
`images: api=${{ github.event.pull_request.head.sha }}` — and that is the entire change,
because the placeholder was chosen in the name-matched form Decision B describes.

## 4. What would break

Failure modes this design **handles**:

- **Overtaken re-apply writes a stale `head-sha`.** Handled by the per-PR concurrency group with
  `cancel-in-progress`, which is safe only because the callee's kubeconfig removal is
  `if: always()`.
- **Preview deleted mid-review.** Handled by `ttl-minutes: 480`, derived from creation-anchored
  expiry plus SHA-free naming. The default 60 would have looked fine in review and failed an
  hour into the first real PR.
- **Secret exfiltration to another repository.** Handled by naming one secret instead of
  `secrets: inherit`.
- **Fork PR reaching a cluster credential.** Handled by choosing `pull_request` over
  `pull_request_target`. This is the single highest-severity decision in the diff.
- **Typo'd input name.** Caught pre-commit by the input-subset check; otherwise a dispatch-time
  failure discovered by the first PR author.
- **Typo'd manifest field.** Caught by the renderer's strict decode, which rejects unknown *and*
  duplicated keys.
- **A misdirected image override.** The renderer errors when an override matches no app, listing
  what the template does contain.

Mistakes a newcomer to this stack would plausibly have made here:

1. **`secrets: inherit`.** The default reflex, because it always works. It hands every secret in
   the repo to a workflow in another repository at a floating ref.
2. **`pull_request_target` "so previews work for forks."** The exact combination — base-repo
   secrets plus fork-controlled code, with a callee that checks that code out next to a
   kubeconfig — that turns a preview feature into a cluster takeover.
3. **Taking the default TTL.** Requires reading two unrelated files in the callee to know it's
   wrong. Nothing in the input's description says "not extended by re-apply."
4. **Listing all seven binaries** because the template "should describe the system." It
   describes the system correctly and produces six red children in every preview, which trains
   reviewers to ignore red.
5. **Restating every default** (`replicas: 1`, `replicationFactor: 1`, `instances: 1`) to make
   the manifest "explicit". Under server-side apply that is a permanent ownership claim on
   fields you had no opinion about, and it buries the two values that *are* deliberate.
6. **No `concurrency` block**, because pushing twice in a minute feels rare. It is not rare, and
   the resulting failure — an object whose recorded SHA is not its rendered SHA — is invisible
   until someone trusts the annotation.
7. **Renaming the topic to fit the label budget.** Fixes the string length, silently breaks the
   agreement with `outbox.DefaultTopic`, and produces a preview whose broker has a topic the
   application never writes to. Correct move: accept the bound, document it, locate the fix in
   the repo that owns the naming rule.

Known-red by design, and *not* bugs: `ImagePullBackOff` on `api` (no images are published), and
the Topic child never going Ready (nothing provisions the `payment-rail` Redpanda cluster). A
third, subtler one is recorded in the ADR: even *with* images the preview would not be
functional, because `PaymentStackApp` has no env-var field, so `api` would use its default
`localhost:5432` DSN and never find the preview's database. That closure is a bosun-side change,
and knowing it now is what stops someone from spending a day debugging it later.

## 5. Compared to what you know

- **A reusable workflow is a cross-repo function call with a declared signature.** `with:` is the
  parameter list, `on.workflow_call.inputs` is the type declaration, `outputs:` is the return
  value, and `@main` vs `@<sha>` is the dependency-version decision — exactly the semver-range
  vs lockfile trade-off from npm or Cargo. *Where the analogy breaks:* there is no lockfile.
  Nothing records which commit of `@main` you actually ran; only the run log knows. And the
  "function" executes on your runner with the secrets you pass it, so a version bump is a
  code-execution event, not just a behaviour change.

- **`secrets: inherit` vs named secrets** is passing `process.env` to a library versus passing
  one scoped credential — the same reasoning as ambient authority versus capability passing.
  *Where it breaks:* an npm library already runs in your process and could read `process.env`
  anyway; a reusable workflow genuinely cannot see a secret you don't pass, so the boundary is
  real rather than advisory.

- **A CRD with kubebuilder markers is a schema with server-side defaults and constraints**, like
  a SQL table with `DEFAULT`, `CHECK` and `NOT NULL` — plus CEL validation rules
  (`self.all(a, self.exists_one(x, x.name == a.name))` is a uniqueness constraint) enforced at
  admission. *Where it breaks:* server-side apply adds **field ownership**, which SQL has no
  analogue for. In SQL, writing a column its current value is a no-op; in Kubernetes it
  permanently registers you as that field's manager and takes it away from whoever else was
  managing it.

- **`PaymentStack` as a preview template** is a Helm values file, except validated by the API
  server against a real schema rather than by a chart's templating at render time, and inlined
  into the `PreviewEnvironment` rather than referenced. *Where it breaks:* Helm renders on the
  client and the cluster sees only the output; here the *template itself* is a first-class
  typed object, so schema violations are caught by the same machinery that validates the final
  objects.

- **The `--image name=tag` override** is the kustomize `images:` transformer, deliberately — same
  match-by-image-name rule. *Where it breaks:* kustomize silently no-ops on a non-matching
  entry; bosun errors, on the argument that a silent no-op means reviewing the wrong build under
  a green check.

- **`preview.Name` as a pure function of `(repo, PR)`** is the idempotency-key pattern this
  codebase already uses for payments (M1): a deterministic key derived from stable identity so
  that a retry is an update rather than a duplicate. *Where it breaks:* an idempotency key
  usually covers the whole request payload; this one deliberately excludes the head SHA, which
  is exactly what produces the TTL surprise in Decision D. Same pattern, different
  consequence — because here the "duplicate" is a long-lived object with its own clock.

## 6. Gotchas & idioms

- **`github.event.pull_request.number` vs `github.event.number`.** Both exist on a
  `pull_request` event. The former is on the PR object itself and is the one that survives
  copy-pasting the expression into a workflow triggered by something else — where
  `github.event.number` is simply absent and interpolates to the empty string rather than
  failing.

- **`github.job_workflow_sha` only exists inside a called workflow**, and it names the commit the
  *called workflow file* was read from. It pins the callee's internals to itself; it does **not**
  pin your `uses:` reference. Those are different guarantees, and conflating them is how people
  convince themselves `@main` is pinned.

- **An empty secret is a *provided* secret.** `required: true` on a `workflow_call` secret checks
  that the caller passed something, and `${{ secrets.MISSING }}` interpolates to `''`. So a
  missing repository secret does not fail fast at dispatch; it fails deep inside the callee when
  a tool gets an empty file. If you want a fast, legible failure, assert non-emptiness
  explicitly.

- **`cancel-in-progress` interacts with the callee's cleanup guarantees.** Cancellation skips
  steps guarded by `if: success()` (the implicit default). Before enabling it on a job that
  handles credentials, check that the cleanup is `if: always()`.

- **`permissions` at the top level is a ceiling for every job**, including reusable-workflow
  calls. Widening it to satisfy one future job silently widens the token for all of them; scope
  it per job instead.

- **YAML numbers vs strings across the input boundary.** `ttl-minutes` is `type: number`, so
  `480` is correct and `"480"` would be a type mismatch. Meanwhile `2Gi` in the manifest *must*
  be a string, and the CRD's pattern `^[1-9][0-9]*(Mi|Gi)$` rejects a leading zero deliberately:
  `0Gi` parses to a quantity that renders as `0`, which would hand the database operator a
  request for no storage at all.

- **Comments in a manifest are the only documentation that travels with the decision.** Both
  files here are majority comment. That ratio would be absurd in Go, where types and names carry
  meaning; in a declarative manifest, `port: 8080` records the value and nothing about why it is
  stated while `replicas` is not. YAML has no signature to read — so the "why" has to be
  adjacent or it does not exist.

- **`kubectl apply` returning is not the object existing.** The callee re-reads with
  `kubectl get` before reporting, and deliberately does **not** `kubectl wait --for=condition=Ready`,
  because readiness depends on operators the script doesn't own. Knowing what your tooling
  promises — accepted-by-the-API-server, not converged — is the difference between a useful CI
  signal and a timeout on a cluster where everything worked.

## 7. Check yourself

1. The callee defaults `ttl-minutes` to 60 and this caller passes 480. Name the **two**
   independent properties of bosun's implementation that make the default wrong here, and say
   which file each one lives in. Then: what would have to change for the default to become
   correct?

2. The architect's plan justified four apps with "the topic needs a producer." Precisely which
   part of `PaymentStackSpec` would have had to exist for that claim to be true, and what
   observable difference would there be between a stack with 1 app and a stack with 4 (given no
   images are published)?

3. `port: 8080` is written even though the CRD defaults it to 8080; `replicas` is omitted even
   though the template wants 1. Give the rule that distinguishes them, then explain the extra
   consequence that server-side apply attaches to writing a field you didn't need to write.

4. Suppose bosun's `remove kubeconfig` step were guarded by `if: success()` instead of
   `if: always()`. Which line of *payment-rail's* workflow would have to change, and what is the
   concrete risk if it doesn't?

5. Work out the exact PR number at which this template starts failing, and say which *level* of
   the name derivation exhausts the 63-character label budget. Why is shortening
   `payment-rail.events` the wrong fix?

6. A colleague proposes switching the trigger to `pull_request_target` "so preview environments
   work for contributors' forks." Describe the attack this enables, using the specific steps in
   the callee that make it exploitable.

<details>
<summary>Answers</summary>

1. (a) `expiresAt` is anchored to `CreationTimestamp`
   (`bosun/internal/controller/previewenvironment_objects.go:76`), so the clock starts at the
   *first* apply. (b) `preview.Name` (`bosun/internal/preview/name.go`) excludes the head SHA on
   purpose, so every push re-applies the **same** object and never resets that timestamp.
   Together: TTL is measured from the PR's first push and is never extended, so 60 minutes
   deletes the preview mid-review. The default would become correct if bosun recomputed expiry
   on each reconcile from `lastAppliedAt` (or bumped an annotation the controller reads) instead
   of from `CreationTimestamp` — i.e. sliding expiry rather than absolute.

2. There would have to be a reference from a topic (or an app) to the other list — e.g. a
   `producedBy`/`topics` field on `PaymentStackApp`, or a `producer` field on
   `PaymentStackTopic` — that the controller consulted when building Topic objects. There is
   none: `paymentstack_objects.go` builds one Topic per `spec.messaging.topics` entry
   unconditionally. Observable difference between 1 app and 4: identical database, identical
   topic; the 4-app stack additionally has three `ManagedApp`s, three `Deployment`s whose pods
   sit in `ImagePullBackOff`, and three `Service`s with no endpoints — and the stack's aggregate
   Ready condition names one of them as the offender instead of `api`.

3. Rule: state a field when its value is a fact about **your** system that must agree with
   something outside the manifest; omit it when the value is a fact about the **platform's**
   default. `8080` must agree with `const listenAddr = ":8080"` in `cmd/api/main.go`; `1` replica
   agrees with nothing. The server-side-apply consequence: a field present in your applied
   configuration is recorded in `metadata.managedFields` as owned by you, so you have claimed it
   against every other manager (an HPA, an operator, a human `kubectl scale`) until you remove it
   — and removing it from a later apply *deletes* the field rather than releasing it to the
   default.

4. `cancel-in-progress: true` would have to become `false`. Risk otherwise: cancelling an
   in-flight run skips any step guarded by the implicit `if: success()`, so the kubeconfig file
   — a live cluster credential — is left on the runner's temp directory after the job ends, on
   infrastructure that may not be exclusively yours. The cost of `false` is that a superseded run
   completes and may write the older `head-sha`, which is why the callee's `if: always()` is what
   lets you have both.

5. PR **10000** (five digits). Derivation:
   `31 + d` (environment) → `+7` for `--stack` (child PaymentStack) → `+2 + 19` for
   `--payment-rail.events` (grandchild Topic) = `59 + d`; `d = 4` is exactly 63, `d = 5` is 64
   and the `app.kubernetes.io/name` label value is rejected. The level that exhausts the budget
   is the **grandchild** (Topic) — bosun's own `Name` budget only reserved the child's `--stack`
   suffix. Shortening the topic is wrong because the name must equal `outbox.DefaultTopic`
   (`internal/outbox/envelope.go:24`), which the relay publishes to; a shorter name provisions a
   topic nothing writes to, trading a loud future failure for a silent present one. The fix
   belongs in bosun's name budget.

6. `pull_request_target` evaluates the workflow from the **base** branch but runs with the base
   repository's secrets available — including `PREVIEW_KUBECONFIG` — while the callee explicitly
   checks out the pull request's code (`actions/checkout` with `ref: inputs.head-sha`, into
   `.caller`) and runs the Go toolchain in that workspace, with the kubeconfig written to
   `${runner.temp}/kubeconfig` at the same time. An attacker opens a fork PR whose tree
   influences what gets executed — the callee's comments name the concrete vector it defends
   against, a root-level `go.work` redirecting `go run ./cmd/preview-manifest` into the PR's own
   code — and that code runs beside a live cluster credential. bosun mitigates with `GOWORK=off`,
   the workspace-root checkout ordering and the `.caller` dot prefix, but the caller's
   contribution is to never create the condition: under `pull_request`, a fork gets no secret at
   all.

</details>

## 8. Further reading

- [GitHub Docs — Reusing workflows](https://docs.github.com/en/actions/using-workflows/reusing-workflows) —
  `workflow_call` inputs/secrets/outputs, the `secrets: inherit` semantics, and the limits on a
  job that uses `uses:`.
- [GitHub Docs — Security hardening for GitHub Actions](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions) —
  the `pull_request_target` section, script-injection via interpolation, and why third-party
  references get pinned to a full commit SHA.
- [GitHub Docs — Using concurrency](https://docs.github.com/en/actions/using-jobs/using-concurrency) —
  concurrency groups, `cancel-in-progress`, and how cancellation interacts with step conditions.
- [Kubernetes — Server-Side Apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/) —
  field management, `managedFields`, and why an applied field is an owned field.
- [Kubernetes — Labels and Selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set) —
  the 63-character label value limit that sets the name budget in section 3e.
- [The Kubebuilder Book — CRD validation & defaulting markers](https://book.kubebuilder.io/reference/markers/crd-validation) —
  `+kubebuilder:default`, the validation markers, and why optional fields are pointers.
