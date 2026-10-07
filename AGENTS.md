# swell

Evidence in, moves out. A controller watches a plant, lets evidence pile up as facts, and moves the plant once the evidence crosses a threshold, each move gated by its operator. Git is the one plant kind today (`PlantSpec` is a union of one); The Current's fact log, a Drive folder and PostHog replays are each the next member and an adapter behind the plant port, never a change to the loop. Two packages: `packages/kernel` (`@swell/kernel`: facts, rules, ports, receipts; born in The Current at `8c5c0bb`) and `packages/swell` (the loop as kernel primitives, a git plant, a SQLite historian, a controller with its door and HMI).

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
- **One move per signature per arming** (Kai deferred; ruled 2026-10-07). The subject is the signature, the evidence set and the arming, readable as `dup-code@lint+review#1`, so a new source is a new subject and the kernel's receipt dedupes the rest. An arming closes when its accepted move lands, and the signature re-arms only when an input measures a sample taken after the apply and still sees it (the weekly tallies lag; this does not). It also closes when the move can never land (its apply gives up after five attempts: a conflict, a PR closed unmerged); the next arming's brief carries why as `previous`.
- **Hysteresis is keyed to the evidence set within an arming**, not a rate or a clock. A rate wobbles across a floor; a set only grows. A new arming starts with no dismissals behind it.
- **Strength is distinct sources, never a count.** Counts are gauges. The default threshold: two sources agree, or a known signature was seen in every run of the window, over at least three runs. `rate` is `seen / runs`, 0..1, where `seen` counts measurements, never signals, and a failed measurement is not a run; `hits` (signals) stays for a configured `threshold`. An observer's outsider `new:X` joins `X` when another source saw `X`: that is its second source.
- **The limit counts attempts, and a failed actuator keeps its work.** `limit.perDay` counts the kernel's `kernel::attempt` rows since UTC midnight, so a failed propose or a crashed actuator spends the day; an attempt in flight or given up is never metered twice. A loop rule gives up after two attempts and its lease outlasts the actuator's timeout (the measure rule's, its slowest instrument's). A failed, timed-out or interrupted actuator's changes (and any commits of its own) are kept as `swell: wip ...` on the move branch, named for the subject, with its output in `<work>/logs`; the next attempt resumes that branch however far the plant moved, reads the failure as `previous`, and a finished move is never made twice.
- **Observers are their own list.** They must declare a closed `vocabulary` (an outsider is `new:` and needs a second source), are budgeted by `every.commits`, default to a thirty-minute timeout, and write `estimated` measurements. The actuator's brief is the evidence, never the number. An observer's model is never the actuator's with shared context (unenforced; a convention).
- **A failed measurement is an error row, never a zero.** An instrument's output is decoded, so garbage is a failed measurement.
- **An estimate is irreplaceable.** It cost money and cannot be recomputed. Measurements could be, but the rollup hangs on them, so both are rows.
- **The controller's identity.** A daemon is not a person. It writes `by: controller:<name>`. A loop is switched on for its operator, and the kernel stamps every rule's writes with its enabler: a loop's proposals carry `via: operator:<name>`, the measure and apply rules' carry the controller.
- **An operator is named by their token, never by a request** (Kai deferred; ruled 2026-10-07). Peers and operators hold separate bearers: `--token` opens the read door to a peer and nothing else; `--operators kai=<token>` gives each operator a token, and `/decide` and `/retry` act as the operator it names. An empty or missing token refuses everyone.
- **A child sees only what it is given.** An instrument or actuator gets a base environment (PATH, HOME and the platform's own) plus the names its declaration lists in `env`; never `SWELL_*` or a credential nobody declared. git and gh get the controller's environment minus `SWELL_*`, so agents and credential helpers work. A timeout or an interrupt kills the child's whole tree.
- **The plant's hooks judge the controller's moves** (Kai, 2026-10-07). The finished-move commit, the apply commit and every push run the plant's own hooks, with ten minutes each; a refusal fails the attempt with the hook's words as `previous`, so the agent fixes what the plant objects to. A salvage commit skips hooks: it is the controller's bookkeeping, not a move.
- **With `--gh`, a yes anywhere applies** (Kai, 2026-10-07). A yes on the HMI merges the PR (`gh pr merge --squash`, under GitHub's own reviews and checks; a refusal is a failed apply, retried); a merge on GitHub is read back as the verdict. A PR closed unmerged fails its apply.
- **The controller's own tree is never the plant, and apply is push-only.** Instruments, actuators and `apply` run in worktrees under the work dir; no operation touches the plant root's working tree, HEAD or index. `apply` with no PR fetches, squashes onto `<remote>/<ref>` in its own worktree and pushes without force; a rejected push fails and a later sweep retries it against a fresh fetch. An auto loop needs a remote and `defineControl` refuses one without. One controller holds a work dir at a time (`controller.lock`).
- **Every commit the controller makes is its own.** `user.name=swell`, whatever identity the machine has or lacks.
- **The kernel is swell's** (`@swell/kernel`, Kai 2026-10-07). It carries no domain's words: a domain names its own graph layers (`Graph.declare(..., layers)`, and each actor's `layer`), and The Current passes its four. A consumer vendors it as a workspace package (The Current: `pnpm vendor:kernel`, which records the swell commit), never as a `file:` or registry dependency: the kernel ships TypeScript source, and Node will not strip types under `node_modules`. A change goes to swell first and comes back through the script; a vendored copy is never patched.

## Proof

`pnpm verify` is the one definition of done: typecheck, lint, format, every test. Lanes:

- **kernel**: `packages/kernel/test/kernel.test.ts` and the type test `uses.types.ts`: every failure settles as a receipt (an undeclared write, a refused unique write), a rule writes for its enabler, the memory store as one connection (a read waits for an open transaction; a nested one is a savepoint), a scoped declaration forgotten with its scope, a bad cron refused at boot, and an undeclared port or write is a compile error.
- **deterministic**: `packages/swell/test/control.test.ts`, every scenario over the memory store and the historian, plus a differential property test that the two agree on random measurement histories, `defineControl` (typos refused by path) and the time helpers: hysteresis, re-arm after a landed move, a move that can never land, a retry grant, the honest rate (a failed measurement is not a run), the attempt-counting limit across UTC midnight and with an attempt in flight, the two-attempt give-up, a four-week window that reads four weeks however much history precedes it, and an outsider joining a sensor's signature. The memory store is the semantic spec; the historian must agree.
- **controller**: `packages/swell/test/controller.test.ts`, a real git plant against a bare origin: sample, measurement, move, auto verdict, squash and push with the plant root untouched (a side branch with staged, unstaged and untracked work), a push rejected by a hook and applied later, a squash conflict that names its file and never wedges the next apply, a failed and a timed-out actuator that leave a wip commit and a log, a resume after the plant moved, a timed-out actuator's grandchild killed with it, an actuator not rerun when propose fails, the plant's commit hook refusing a move, an instrument's environment, the work-dir lock, the HMI template, the peer door and the peer client against it, separate peer and operator tokens, an empty token refused, `--gh` against a fake `gh` (a `.cmd` shim on Windows) for an HMI yes and a GitHub merge, one plant's stuck actuator never holding another, and a dead plant that keeps ticking.
- **unproven**: real `gh` against GitHub, the peer port between two controllers in two processes, an observer, the HMI's script in a browser, an 8.3 short work path, an actuator interrupted mid-run (salvage on interrupt), and any run on the laptop.

## Run

```sh
swell once  --config <plant>/swell.config.ts [--config <other>/swell.config.ts]   # feedback, sweep, drain, exit
swell serve --config ... --port 4747 --period 60 [--gh]                          # each plant on its own period; serve the HMI
swell view  --config ... --plant <id>                                             # the HMI's JSON
```

Every verb takes `--token` (or `SWELL_TOKEN`), a peer's bearer for the read door, and `--operators kai=<token>[,name=token]` (or `SWELL_OPERATORS`), each operator's bearer for `/decide` and `/retry`. The work dir defaults to `~/.swell/<controller>`, with the historian at `historian.sqlite` and the lock at `controller.lock`. An instrument prints `{ signals, analyzed, excluded, failed }`; a signature is printable ASCII without `|` or `~`, and anything else is a failed measurement. An actuator runs in a worktree with `SWELL_BRIEF` pointing at the evidence JSON (with `previous`, why the last attempt failed) and is killed, tree and all, at `timeoutMs` (default one hour); what it leaves changed becomes the move. On Windows a `.cmd` shim (`claude`, `gh`) is found through PATHEXT and run under cmd.exe. Each plant ticks on its own fiber, so one plant's agent never holds another's sample or apply, and the HMI and the door stay live throughout.

The door is one `HttpApi` contract (`door.ts`), `ControllerApi`, that the server and the peer client both derive from: `/facts/:table`, `/tallies/:id` (peer token), `/view`, `/health` (open, typed), `/decide`, `/retry` (operator token). A bad limit, an unknown index or an unknown plant is a 400, a bad token a 401, a refused verdict a 422.

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

Pinned exactly to the stable 4.0.2: `effect/http`, `effect/http-api`, `effect/cli`, `effect/process` and `effect/sql` are `@stability unstable` (breaking in a minor). Audited twice: module by module against rc.118 (`.artifacts/swarm/*.md`), then by a swarm review against an `effect@4.0.2` clone (`.artifacts/review/swarm-2026-10-07.md`). The bar is Kai's: use Effect wherever it might remotely make something easier.

Adopted: `HttpApi` for the door, with `HttpApiMiddleware` security for both bearers (the operator's provides `Operator`); `effect/process` `ChildProcess` on the Node spawner for every child (tree kill, async git, an explicit environment); `effect/sql` with `@effect/sql-sqlite-node` as the historian's driver (one connection, savepoints, decoded rows), never a state owner; `Schema` for the whole config, every fact, every edge that was a cast, and `PlantError`; `DateTime` for the tally week and UTC midnight; `Array`, `Record`, `Order` and `Number` for the tally fold; `FileSystem` and `Path` (the Node layers: platform paths) for briefs, logs, worktrees and the lock; `Config` with `effect/cli` flags (Redacted tokens, filters, descriptions); `Crypto` for a cite's content hash; `HttpTraceContext` for the kernel's traceparent; `Context.Service` and scoped `Layer` for the controller, the historian and the process runner; `Effect.fn` spans at every plant step and tick; `Duration` and `Cron` at rule declaration; `@effect/vitest` `it.effect`, `it.live`, `it.prop` and `TestClock`.

Rejected, and still right: `eventlog`, `workflow`, `cluster`, `persistence`, `Migrator`, `KeyValueStore` and `TxRef` as a second owner of state or schema beside the facts; `Schedule` as the retry owner (receipts must survive a restart and the Convex scheduler hop). Reversed: `Path` (the Node layer is the platform's), `FileSystem` and `Config` (each removed a real bug), `sql` as a driver. `Hash.string` is deterministic after all, but it is no persistence format: a subject is now its readable sources, with no hash. Where Effect falls short: its `stat` reports no `ino` past 2^53, which NTFS ids routinely are, and its `realPath` does not expand 8.3 names, so worktree identity compares native realpaths; `homedir` and `hostname` stay on `node:os`.

## Owed

- A manual loop on a plant with no remote can propose but its accepted move cannot apply (push-only needs a remote); only auto is refused at load.
- The kernel's `onCron` trigger stays for The Current's Convex crons; swell loops have none, because the controller sweeps every rule each period.
- The HMI's token lives in `localStorage`; the server is loopback-only, so tailnet exposure needs a reverse proxy or a listen flag.
- The laptop's historian is the one unreplicated thing: PRs and verdicts live on GitHub, estimates do not. A nightly copy (`SqliteClient.backup` is there).
- A no on the HMI does not close the PR, and a re-armed move after a PR that could not land leaves the old PR open.
- The primitive registry is still process-global: a controller's compiled rules are scoped and forgotten when it closes, but a declaration at module load stays for the process.
- Left from the swarm review, judged low: streaming an actuator's output live to its log, a fake spawner for instant timeout tests, running the real-git tests concurrently (they mutate PATH and the environment), branded fact ids, OpenAPI on the door.
- The Current: bump its Effect catalog from rc.118 to 4.0.2; take the kernel's API changes when it vendors (`lease` as a `Duration` instead of `leaseMs`, `GraphLayer` for the meta layer type, `Actor.layer` in the graph, `Job.urn`, tagged `DuplicateId` and `InvalidId`, a row read back after insert); dissolve `pi::observed`, `pi::proposal` and `pi::verdict` into `control::` rows; its `apply` is a command call that must run in the verdict's transaction. `pi::link` and `pi::mark` stay as domain facts.
- Pe.Tools: adopt after mise lands; delete `ts/packages/factory`; the factory ledger's sensors (fallow, tokei, gitleaks, guards) become `swell.config.ts` rows.
