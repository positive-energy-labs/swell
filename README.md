# swell

**Evidence in, moves out.**

A control loop that watches a system, lets evidence pile up, and proposes a single change when enough of it agrees. You write the parts: sensors that measure, thresholds that decide, actuators that fix. Plain scripts where a number will do, agents where it won't. Swell keeps the books, and nothing moves without your yes.

## In plain words

Most tools react the first time something looks wrong, so you get noise and quick fixes that don't stick. Swell waits until a problem keeps showing up, then suggests one fix. It keeps notes on everything it saw and every answer you gave, so it never asks the same question twice. Nothing changes until someone says yes. It works like a thermostat for your work: it reads, waits, and acts only when the reading says so.

## For developers

One-off fixes fire on the first signal and chase noise. Swell waits for evidence from independent sources to agree, then moves once.

Every fact it writes is append-only and cited. A dismissal holds until new evidence appears, and the next attempt reads your reason. Every move has exactly one history entry.

Moves wait for an operator unless their class is pre-approved, and every actuator has a daily rate limit.

The vocabulary is control theory (plant, sensor, observer, threshold, actuator, operator), because control systems solved this problem long ago.

A sensor is any script that prints JSON, so deterministic checks cost nothing to add. An observer is an agent for what numbers can't hold, held to a fixed vocabulary.

A plant is anything you can sample and change. Git is the one kind today; the next is an adapter behind the same port, never a change to the loop.

```ts
// swell.config.ts at the plant's root
import type { ControlSpec } from "swell";
export default {
  plant: { id: "my-repo", kind: "git", root: ".", ref: "main", remote: "origin" },
  sensors: [{ id: "lint", run: ["node", "scripts/lint-signals.js"] }],
  actuators: [{ id: "fix", run: ["claude", "-p", "@fix.md"] }],
  loops: [
    { id: "tidy", inputs: ["lint"], actuator: "fix", mode: "manual", operator: "you", limit: { perDay: 1 } },
  ],
} satisfies ControlSpec;
```

```sh
swell serve --config swell.config.ts --operators you=<secret>   # sample every 60 s, serve the operator's screen on :4747
```

`AGENTS.md` holds the vocabulary, the rulings, and what is proven. `pnpm verify` is the definition of done.

## For agents

swell: a control loop over an append-only fact kernel. Problem: act once on agreed evidence, never on a first signal. Memory: every measurement, proposal and verdict is a cited fact; a dismissal holds until the evidence set grows (hysteresis), and once a move lands its signature re-arms only if it is seen again. Control: moves wait for the operator in manual mode, and every actuator has a per-day limit. Vocabulary: plant, sensor (deterministic, any argv printing `{signals}`), observer (model, closed vocabulary), measurement, signal, signature, threshold, actuator, move, mode, operator. Generality: a plant is anything that can be sampled and changed; git is the first adapter. Declare a plant in `swell.config.ts`, then run `swell serve`.
