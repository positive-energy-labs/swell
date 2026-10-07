# swell

Evidence in, moves out. A controller watches a plant, lets evidence pile up as facts, and moves the plant once the evidence crosses a threshold, each move gated by its operator. General over targets: a git repo, The Current's fact log, a Drive folder, PostHog replays. Two packages: `packages/kernel` (`@swell/kernel`: facts, rules, ports, receipts; born in The Current at `8c5c0bb`) and `packages/swell` (the loop as kernel primitives, a git plant, a SQLite historian, a controller with its door and HMI).

**The code is the spec.** Rulings live in `meta(..., "why")` on each primitive and in commit messages. This file holds only what code cannot say.

## Vocabulary

Control theory, all the way down (Kai, 2026-10-07). The words are established so an agent that knows control theory reads the code right the first time. Where control theory has no honest word, the plain one stays: proposal, verdict, brief, cite, peer, door.

| Word            | What it is                                                                                                   |
| --------------- | ------------------------------------------------------------------------------------------------------------ |
| **plant**       | what a loop measures and moves: a repo at a sha, a deployment at a time, a folder                            |
| **controller**  | one process, one historian, many plants: the laptop daemon, or Convex                                        |
| **sample**      | the plant at one instant; instruments measure a sample, never a moving tree                                  |
| **sensor**      | a deterministic instrument: argv at a clean checkout                                                         |
| **observer**    | a model instrument: it estimates what no sensor can measure and distills it to cited signals                 |
| **measurement** | one instrument over one sample, with its denominator; `measured` from a sensor, `estimated` from an observer |
| **signal**      | one thing an instrument saw; never a row, it rides inside a measurement                                      |
| **signature**   | what signals group under, and its tally; never a row, folded from the rollup                                 |
| **threshold**   | when a signature's evidence is enough to move                                                                |
| **hysteresis**  | a dismissal holds until the evidence set grows                                                               |
| **actuator**    | what makes the move: argv in a worktree, handed the brief                                                    |
| **move**        | one loop acting once on one signature; the receipt is its history entry                                      |
| **limit**       | the actuator's rate limit, moves per day                                                                     |
| **mode**        | `manual`: the operator approves each move; `auto`: it applies                                                |
| **operator**    | who approves, and who the loop acts for                                                                      |
| **feedback**    | the path that brings a new sample and the operator's decisions back in                                       |
| **historian**   | the controller's store                                                                                       |
| **HMI**         | the operator's screen                                                                                        |

**The brand is only where a person types it**: the `swell` CLI and package, `swell.config.ts`, `SWELL_*` variables, `~/.swell`. Everything stored uses the domain: fact ids are `control::<name>` (the kernel's `namespace::name` grammar, which also names the table, `control_measurement`), and a URN is `control:<controller>/<table>/<id>` (a scheme, one colon). A product rename never migrates a row.

## Rulings (Kai, 2026-10-07)

- **Identity is where it was written; everything else is a citation.** A fact lives with the controller that wrote it and is never copied. Elsewhere it is a `control::cite` row keyed by URN. A citation points at the origin, not the nearest controller: a merged change is cited by its PR, never by the laptop that proposed it.
- **Convex holds facts about the business; the laptop holds facts about code.** Two controllers, three plants: the laptop daemon over the Pe.Tools and TC repos, Convex over PE's business with Drive and PostHog loops as ports.
- **Moves never span controllers.** Cross-controller is evidence in by URN through the peer port, and a proposal out through the peer's own operator. Distributed receipts would be a second state owner, which is why Temporal was rejected.
- **Operators approve the thing that will run.** A proposal's `apply` is target-typed and opaque to the kernel: a PR ref for git, a command call for The Current, a port call for Drive.
- **One move per signature.** The subject is the signature plus the evidence set, so a new source is a new subject and the kernel's receipt dedupes the rest.
- **Hysteresis is keyed to the evidence set**, not a rate or a clock. A rate wobbles across a floor; a set only grows.
- **Strength is distinct sources, never a count.** Counts are gauges. The default threshold: two sources agree, or a known signature was seen in every run of the window, over at least three runs. `rate` is `seen / runs`, 0..1, where `seen` counts measurements, never signals; `hits` (signals) stays for a configured `threshold`.
- **The limit counts attempts, and a failed actuator keeps its work.** `limit.perDay` counts the kernel's `kernel::attempt` rows since the UTC day began, so a failed propose or a crashed actuator spends the day. A loop rule gives up after two attempts and its lease outlasts the actuator's timeout. A failed actuator's changes are committed as `swell: wip ...` on the move branch with its output in `<work>/logs`; the next attempt resumes that branch and reads the failure as `previous`, and a finished move is never made twice.
- **Observers are their own list.** They must declare a closed `vocabulary` (an outsider is `new:` and needs a second source), are budgeted by `every.commits`, default to a thirty-minute timeout, and write `estimated` measurements. The actuator's brief is the evidence, never the number. An observer's model is never the actuator's with shared context (unenforced; a convention).
- **A failed measurement is an error row, never a zero.** An instrument's output is decoded, so garbage is a failed measurement.
- **An estimate is irreplaceable.** It cost money and cannot be recomputed. Measurements could be, but the rollup hangs on them, so both are rows.
- **The controller's identity.** A daemon is not a person. It writes `by: controller:<name>`; a loop reads a peer as the operator who enabled it, so `via` is the rule's enabler.
- **The controller's own tree is never the plant, and apply is push-only.** Instruments, actuators and `apply` run in worktrees under the work dir; no operation touches the plant root's working tree, HEAD or index. `apply` with no PR fetches, squashes onto `<remote>/<ref>` in its own worktree and pushes without force; a rejected push fails and a later sweep retries it against a fresh fetch. An auto loop needs a remote and `defineControl` refuses one without.
- **Every commit the controller makes is its own.** `user.name=swell`, whatever identity the machine has or lacks.
- **The kernel is swell's** (`@swell/kernel`, Kai 2026-10-07). It carries no domain's words: a domain names its own graph layers (`Graph.declare(..., layers)`), and The Current passes its four. A consumer vendors it as a workspace package (The Current: `pnpm vendor:kernel`, which records the swell commit), never as a `file:` or registry dependency: the kernel ships TypeScript source, and Node will not strip types under `node_modules`. A change goes to swell first and comes back through the script; a vendored copy is never patched.

## Proof

`pnpm verify` is the one definition of done: typecheck, lint, format, every test. Lanes:

- **deterministic**: `packages/swell/test/control.test.ts`, twelve scenarios over the memory store and the historian, plus `defineControl` and `week`: hysteresis, the honest rate and its three-run floor, the attempt-counting limit and the two-attempt give-up. The memory store is the semantic spec; the historian must agree.
- **controller**: `packages/swell/test/controller.test.ts`, a real git plant against a bare origin: sample, measurement, move, auto verdict, squash and push to origin with the plant root untouched (a side branch with staged, unstaged and untracked work), a push rejected by a hook and applied on a later tick, a failed and a timed-out actuator that leave a wip commit and a log, a resume that reads `previous`, an actuator that is not rerun when propose fails, a quiet second tick, a hung and a malformed sensor, the HMI template, the peer door under a token, and the decide verb (401, 400, 422).
- **unproven**: the `gh` PR path (`--gh`), the peer port between two real controllers, an observer, the HMI's script in a browser, and any run on the laptop.

## Run

```sh
swell once  --config <plant>/swell.config.ts [--config <other>/swell.config.ts]   # feedback, sweep, drain, exit
swell serve --config ... --port 4747 --period 60 [--gh]                          # sample every period and serve the HMI
swell view  --config ... --plant <id>                                             # the HMI's JSON
```

Every verb takes `--token` or `SWELL_TOKEN`: the bearer for the peer door and the operator's decide verb. The work dir defaults to `~/.swell/<controller>`, with the historian at `historian.sqlite`. An instrument prints `{ signals, analyzed, excluded, failed }`. An actuator runs in a worktree with `SWELL_BRIEF` pointing at the evidence JSON (with `previous`, why the last attempt failed) and is killed at `timeoutMs` (default one hour); what it leaves changed becomes the move. All of them run asynchronously, so the HMI and the door stay live while an agent works.

The door is one `HttpApi` contract (`door.ts`), `ControllerApi`, that the server and the peer client both derive from: `/facts/:table`, `/tallies/:id`, `/view`, `/health`, `/decide`. A bad limit or an unknown index is a 400, a bad token a 401, a refused verdict a 422.

```ts
// <plant>/swell.config.ts. A type-only import is erased by Node, so the plant installs nothing; the controller validates on load.
import type { ControlSpec } from "swell";
export default {
  plant: { id: "pe-tools", kind: "git", root: ".", ref: "main", remote: "origin" },
  sensors: [{ id: "fallow", run: ["mise", "x", "--", "fallow", "health", "--format", "swell"] }],
  observers: [
    {
      id: "review",
      run: ["claude", "-p", "@review.md"],
      every: { commits: 20 },
      vocabulary: ["dup-code", "dead-export"],
    },
  ],
  actuators: [{ id: "purge", run: ["claude", "-p", "@purge.md"] }],
  loops: [
    {
      id: "purge",
      inputs: ["fallow", "review"],
      actuator: "purge",
      mode: "manual",
      operator: "kai",
      limit: { perDay: 1 },
    },
  ],
} satisfies ControlSpec;
```

## Effect

Audited module by module against the pinned rc.118 (`.artifacts/swarm/*.md`, 2026-10-07). Adopted: `HttpApi` for the door, `Effect.callback` plus `timeoutOrElse` for child processes, `Semaphore` plus `acquireUseRelease` for the historian's transaction, `Schema` decoding at every edge that was a cast, `Data.TaggedError` for plant and peer errors, `effect/cli` for the verbs, `Effect.repeat` with `Schedule.spaced` for the sample period. Rejected: `eventlog`, `workflow`, `cluster`, `sql` and `persistence` as a second state owner beside the facts; `Path` as POSIX-only on Windows; `FileSystem` and `Config` as surface with no bug removed; `Hash.string` as unstable across runs for a persisted key.

## Owed

- A manual loop on a plant with no remote can propose but its accepted move cannot apply (push-only needs a remote); only auto is refused at load.
- The kernel's `onCron` trigger stays for The Current's Convex crons; swell loops have none, because the controller sweeps every rule each period.
- A killed actuator's own children are not killed; `execFile`'s signal reaches one process.
- The HMI's token lives in `localStorage`; the server is loopback-only, so tailnet exposure needs a reverse proxy or a listen flag.
- The laptop's historian is the one unreplicated thing: PRs and verdicts live on GitHub, estimates do not. A nightly copy.
- The Current: dissolve `pi::observed`, `pi::proposal`, `pi::verdict` into `control::` rows; its `apply` is a command call that must run in the verdict's transaction. `pi::link` and `pi::mark` stay as domain facts.
- Pe.Tools: adopt after mise lands; delete `ts/packages/factory`; the factory ledger's sensors (fallow, tokei, gitleaks, guards) become `swell.config.ts` rows.
