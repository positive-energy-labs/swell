# tide

Evidence in, waves out. A background loop that watches a plant, lets evidence pile up as facts, and changes the plant in gated waves once the evidence clears a bar. General over targets: a git repo, The Current's fact log, a Drive folder, PostHog replays. Two packages: `packages/kernel` (facts, rules, ports, receipts; copied from The Current at `8c5c0bb`, now owned here) and `packages/tide` (the loop as kernel primitives, a git plant, a SQLite store, a host).

**The code is the spec.** Rulings live in `meta(..., "why")` on each primitive and in commit messages. This file holds only what code cannot say.

## Nouns

| Noun      | What it is                                                              | Who mints its id                                      |
| --------- | ----------------------------------------------------------------------- | ----------------------------------------------------- |
| **plant** | what a loop measures: a repo at a sha, a deployment at a time, a folder | the origin, never tide                                |
| **host**  | one process, one store, many plants: a laptop daemon, or Convex         | tide; the one minted name                             |
| **loop**  | one row of a plant's `tide.config.ts`                                   | the plant, as `<plant>/<loop>`                        |
| **fact**  | reading, snapshot, proposal, verdict, observed                          | the host that wrote it, as `tide:<host>/<table>/<id>` |

A signal is never a row: it rides inside a reading and the issues rollup folds it by fingerprint. An issue is a tally, never a row. A wave is one loop acting once on one fingerprint; the receipt is its history entry.

## Rulings (Kai, 2026-10-07)

- **Identity is where it was written; everything else is a citation.** A fact lives with the host that wrote it and is never copied. Elsewhere it is a `tide::observed` row keyed by URN. A citation points at the origin, not the nearest tide: a merged change is cited by its PR, never by the laptop that proposed it.
- **Convex holds facts about the business; the laptop holds facts about code.** Two hosts, three plants: the laptop daemon over the Pe.Tools and TC repos, Convex over PE's business with Drive and PostHog loops as ports.
- **Waves never span tides.** Cross-tide is evidence in by URN through the peer port, and a proposal out through the peer's own gate. Distributed receipts would be a second state owner, which is why Temporal was rejected.
- **Humans approve the thing that will run.** A proposal's `apply` is target-typed and opaque to the kernel: a PR ref for git, a command call for The Current, a port call for Drive. One gate, on the exact apply.
- **One wave per fingerprint.** The subject is the fingerprint plus the evidence set, so a new source is a new subject and the kernel's receipt dedupes the rest. A batch wave is the unreadable merge.
- **A dismissal is keyed to the evidence set**, not a rate or a clock. A rate wobbles across a floor; a set only grows.
- **Strength is distinct sources, never a count.** Counts are gauges. The default bar: two sources agree, or one measured source sees it in every run of the window.
- **Model sensors are admissible** when they distill what no number holds into a cited fact. Conditions: a budget (`every.commits`), fingerprints from a closed `vocabulary` (an outsider is `new:` and needs a second source), the actuator's brief is the evidence and never the number, and the sensor's model is never the actuator's with shared context (unenforced; a convention).
- **A failed reading is an error row, never a zero.** Readings carry their denominator.
- **A model reading is irreplaceable.** It cost money and cannot be recomputed. Deterministic readings could be, but the rollup hangs on them, so both are rows.
- **The host's identity.** A daemon is not a person. It writes `by: host:<name>`; a reading loop reads a peer as the person who enabled it. That person is the authorization, so `via` is the rule's enabler.
- **The host's own tree is never the plant.** Sensors and actuators run in worktrees under the host's work dir. `apply` with no PR merges only into a ref checked out at the plant root, and refuses a dirty tree.
- **Every commit the host makes is the host's.** `user.name=tide`, whatever identity the machine has or lacks.
- **The kernel moved here, not a layer over it.** It has one dependency, `effect`, and nothing of The Current inside. The Current consumes it by `file:` path today and by a mise vendor task later. `Source`, `Layer`, `Audience` and `Agent` are strings; a domain narrows them.

## Proof

`pnpm verify` is the one definition of done: typecheck, lint, format, every test. Lanes:

- **deterministic**: `packages/tide/test/tide.test.ts`, seven scenarios over the memory store and the SQLite store. The memory store is the semantic spec; SQLite must agree.
- **host**: `packages/tide/test/host.test.ts`, a real git plant against a bare origin: snapshot, reading, wave, policy verdict, squash merge, push, a quiet second tick, the page, and the peer door under a token.
- **unproven**: the `gh` PR path (`--gh`), the peer port between two real hosts, a model sensor, and any run on the laptop.

## Run

```sh
tide once  --config <plant>/tide.config.ts [--config <other>/tide.config.ts]   # observe, sweep, drain, exit
tide serve --config ... --port 4747 --every 60 [--gh] [--token <peer token>]  # tick and serve the page
tide view  --config ... --plant <id>                                            # the page's JSON
```

Work dir defaults to `~/.tide/<host>`; the store is `tide.sqlite` there. A sensor is argv run at a clean checkout, printing `{ findings, analyzed, excluded, failed }`. An actuator is argv run in a worktree with `TIDE_BRIEF` pointing at the evidence JSON; what it leaves changed becomes the wave.

```ts
// <plant>/tide.config.ts. A type-only import is erased by Node, so the plant installs nothing; the host validates on load.
import type { TideSpec } from "@tc/tide";
export default {
  plant: { id: "pe-tools", kind: "git", root: ".", ref: "main", remote: "origin" },
  sensors: [
    { id: "fallow", kind: "measured", run: ["mise", "x", "--", "fallow", "health", "--format", "tide"] },
    {
      id: "review",
      kind: "model",
      run: ["claude", "-p", "@review.md"],
      every: { commits: 20 },
      vocabulary: ["dup-code", "dead-export"],
    },
  ],
  actuators: [{ id: "purge", run: ["claude", "-p", "@purge.md"] }],
  loops: [
    {
      id: "purge",
      sense: ["fallow", "review"],
      act: "purge",
      gate: "pr",
      person: "kai",
      budget: { perDay: 1 },
    },
  ],
} satisfies TideSpec;
```

## Owed

- A GitHub remote (`gh` was not logged in on the build machine).
- The kernel's `onCron` trigger is declared and not evaluated: the host sweeps every rule each tick.
- The laptop's store is the one unreplicated thing: PRs and verdicts live on GitHub, model readings do not. A nightly copy.
- The Current: dissolve `pi::observed`, `pi::proposal`, `pi::verdict` into tide's rows; its `Proposal.apply` is a command call. `pi::link` and `pi::mark` stay as domain facts.
- Pe.Tools: adopt after mise lands; delete `ts/packages/factory`; sensors from the factory ledger (fallow, tokei, gitleaks, guards) become `tide.config.ts` rows.
