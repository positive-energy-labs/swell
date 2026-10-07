/** Test doubles, off the public surface: `swell/testing`. */
import { Effect, Layer } from "effect";
import {
  type Brief,
  type Changes,
  type Decision,
  type Measured,
  measureFailed,
  Plant,
  PlantError,
  type Sampled,
} from "./plant.ts";

/** A scripted plant: a world the test writes, so failures are chosen rather than random. */
export const fakeWorld = () => ({
  samples: [] as Array<Sampled>,
  measured: new Map<string, Measured>(),
  acts: [] as Array<Brief>,
  changes: null as Changes | null,
  proposed: [] as Array<{ changes: Changes; text: string }>,
  decisions: [] as Array<Decision>,
  applied: [] as Array<string>,
  /** Applies that fail, in order, before one succeeds: a conflict that will not resolve, or a rejected push. */
  applyFails: 0,
  failNext: 0,
  sampleFails: false,
  /** Every `propose` fails, as when `gh` is down: the actuator already ran and spent. */
  proposeFails: false,
  /** How many times the plant was sampled, so a test can see the loop keep ticking. */
  sampled: 0,
  /** Samples per plant id, when one world serves several plants. */
  sampledBy: new Map<string, number>(),
  /** Run inside every `act` after it is recorded: `Effect.never` is an actuator that never comes back. */
  holdAct: Effect.void as Effect.Effect<void>,
});
export type FakeWorld = ReturnType<typeof fakeWorld>;

export const fakePlant = (world: FakeWorld): Layer.Layer<Plant> =>
  Layer.succeed(Plant, {
    sample: (plant) =>
      Effect.sync(() => {
        world.sampled++;
        world.sampledBy.set(plant.id, (world.sampledBy.get(plant.id) ?? 0) + 1);
      }).pipe(
        Effect.andThen(
          world.sampleFails
            ? Effect.fail(new PlantError({ op: `sample ${plant.id}`, message: "remote is down" }))
            : Effect.sync(() => world.samples.at(-1) ?? { sample: "s0", commits: 0, churn: 0 }),
        ),
      ),
    measure: (_plant, instrument, sample) =>
      Effect.sync(
        () =>
          world.measured.get(`${instrument.id}@${sample}`) ??
          measureFailed(`no scripted measurement for ${instrument.id}@${sample}`),
      ),
    act: (_plant, _actuator, brief) =>
      Effect.gen(function* () {
        if (world.failNext > 0) {
          world.failNext--;
          return yield* new PlantError({ op: "act", message: "actuator crashed" });
        }
        world.acts.push(brief);
        yield* world.holdAct;
        return world.changes;
      }),
    propose: (_plant, changes, text) =>
      world.proposeFails
        ? Effect.fail(new PlantError({ op: `propose ${changes.ref}`, message: "gh is down" }))
        : Effect.sync(() => {
            world.proposed.push({ changes, text });
            return {
              apply: JSON.stringify({ ref: changes.ref, head: changes.head }),
              cite: `fake:${changes.ref}`,
            };
          }),
    decisions: (_plant, applies) =>
      Effect.sync(() => world.decisions.filter((d) => applies.includes(d.apply))),
    apply: (_plant, apply) =>
      Effect.gen(function* () {
        if (world.applyFails > 0) {
          world.applyFails--;
          return yield* new PlantError({ op: "apply", message: "squash conflict: a.ts" });
        }
        world.applied.push(apply);
        return `merged:${(JSON.parse(apply) as { head: string }).head}`;
      }),
  });
