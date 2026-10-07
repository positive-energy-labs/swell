import { Port } from "@swell/kernel";
import { Context, Data, Effect, Layer, Schema } from "effect";
import { meta, Signal } from "./facts.ts";

/** Every plant failure, so a controller can skip one dead plant this tick and still sweep the others. */
export class PlantError extends Data.TaggedError("PlantError")<{
  readonly op: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export interface PlantSpec {
  readonly id: string;
  readonly kind: "git";
  /** Absolute path of the tree the controller loaded this plant's config from. Never the controller's own tree. */
  readonly root: string;
  readonly ref: string;
  readonly remote?: string;
}

/** A deterministic instrument: argv run at a clean checkout of the sample, printing one JSON `Measured`. */
export interface SensorSpec {
  readonly id: string;
  readonly run: ReadonlyArray<string>;
  /** A hung sensor is a failed measurement, never a wedged controller. Default ten minutes. */
  readonly timeoutMs?: number;
}

/**
 * A model instrument: it estimates what no sensor can measure (the worst session replays, a grouping of
 * signals) and distills it to cited signals. It costs money and cannot be recomputed, so it is budgeted by
 * `every`, and its signatures come from a closed `vocabulary`: one outside it is `new:` and needs a second
 * source to count.
 */
export interface ObserverSpec {
  readonly id: string;
  readonly run: ReadonlyArray<string>;
  readonly vocabulary: ReadonlyArray<string>;
  /** Run only after this many commits since its last estimate. */
  readonly every?: { readonly commits: number };
  /** Default thirty minutes: an observer is an agent. */
  readonly timeoutMs?: number;
}

/** What one measure call runs: the instrument resolved to argv and a timeout, whichever kind it is. */
export interface Instrument {
  readonly id: string;
  readonly run: ReadonlyArray<string>;
  readonly timeoutMs: number;
}

export interface ActuatorSpec {
  readonly id: string;
  /** argv, run with cwd at a worktree of the sample and `SWELL_BRIEF` set to a JSON file path. */
  readonly run: ReadonlyArray<string>;
  /** Default one hour: an actuator is an agent and the long step. */
  readonly timeoutMs?: number;
}

/** What an instrument prints. Decoded, never cast: a malformed measurement is a failed one, and `failed` keeps it from reading as a zero. */
export const Measured = Schema.Struct({
  signals: Schema.Array(Signal),
  analyzed: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  excluded: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  failed: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  error: Schema.optionalKey(Schema.String),
});
export type Measured = typeof Measured.Type;
export const MeasuredJson = Schema.fromJsonString(Measured);
export const measureFailed = (error: string): Measured => ({
  signals: [],
  analyzed: 0,
  excluded: 0,
  failed: 1,
  error,
});

/** The plant sampled: its id at this instant and how far it moved since the last one. */
export interface Sampled {
  readonly sample: string;
  readonly parent?: string;
  readonly commits: number;
  readonly churn: number;
}

/** What an actuator left behind, before anyone has approved it. */
export interface Changes {
  readonly ref: string;
  readonly head: string;
  readonly summary: string;
}

/** The evidence handed to an actuator. Never the number: the actuator is told what was seen, not what to move. */
export interface Brief {
  readonly loop: string;
  readonly signature: string;
  readonly sample: string;
  readonly sources: ReadonlyArray<string>;
  readonly signals: ReadonlyArray<Signal>;
  /** The text of the last rejection of this signature, so the next attempt reads it. */
  readonly feedback: string;
  /** Why the last attempt on this subject failed, or empty: a retry resumes the work it left rather than starting over. */
  readonly previous: string;
}

export interface Decision {
  readonly apply: string;
  readonly accept: boolean;
  readonly text: string;
  readonly cite: string;
}

/**
 * The plant as a service: sample, measure, act, propose, read decisions, apply. Git is one implementation;
 * the kernel never sees a branch. `apply` strings are target-typed and opaque above this seam.
 */
export interface PlantService {
  readonly sample: (plant: PlantSpec, parent: string | undefined) => Effect.Effect<Sampled, PlantError>;
  readonly measure: (
    plant: PlantSpec,
    instrument: Instrument,
    sample: string,
  ) => Effect.Effect<Measured, PlantError>;
  readonly act: (
    plant: PlantSpec,
    actuator: ActuatorSpec,
    brief: Brief,
  ) => Effect.Effect<Changes | null, PlantError>;
  /** `manual`: put the changes where the operator decides and return a cite there. `auto`: an apply string, no wait. */
  readonly propose: (
    plant: PlantSpec,
    changes: Changes,
    text: string,
    mode: "manual" | "auto",
  ) => Effect.Effect<{ readonly apply: string; readonly cite: string }, PlantError>;
  readonly decisions: (
    plant: PlantSpec,
    applies: ReadonlyArray<string>,
  ) => Effect.Effect<ReadonlyArray<Decision>, PlantError>;
  readonly apply: (plant: PlantSpec, apply: string) => Effect.Effect<string, PlantError>;
}

export class Plant extends Context.Service<Plant, PlantService>()("swell/Plant") {}

/** A scripted plant: a world the test writes, so failures are chosen rather than random. */
export const fakeWorld = () => ({
  samples: [] as Array<Sampled>,
  measured: new Map<string, Measured>(),
  acts: [] as Array<Brief>,
  changes: null as Changes | null,
  proposed: [] as Array<{ changes: Changes; text: string }>,
  decisions: [] as Array<Decision>,
  applied: [] as Array<string>,
  failNext: 0,
  sampleFails: false,
  /** Every `propose` fails, as when `gh` is down: the actuator already ran and spent. */
  proposeFails: false,
});
export type FakeWorld = ReturnType<typeof fakeWorld>;

export const fakePlant = (world: FakeWorld): Layer.Layer<Plant> =>
  Layer.succeed(Plant, {
    sample: (plant) =>
      world.sampleFails
        ? Effect.fail(new PlantError({ op: `sample ${plant.id}`, message: "remote is down" }))
        : Effect.sync(() => world.samples.at(-1) ?? { sample: "s0", commits: 0, churn: 0 }),
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
          return yield* Effect.fail(new PlantError({ op: "act", message: "actuator crashed" }));
        }
        world.acts.push(brief);
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
      Effect.sync(() => {
        world.applied.push(apply);
        return `merged:${(JSON.parse(apply) as { head: string }).head}`;
      }),
  });

const shared = fakeWorld();
/** The fake layer is a shared scripted world; a test makes its own with `fakePlant(fakeWorld())` and resolves the port to it. */
export const PlantPort = Port.make({
  id: "control::plant",
  service: Plant,
  live: Layer.effect(
    Plant,
    Effect.die(new Error("control::plant live layer is provided by the controller, per plant kind")),
  ),
  fake: fakePlant(shared),
  meta: meta(
    "Plant",
    "The thing a controller measures and moves: a repo, a deployment, a folder.",
    "git is one adapter; the core never assumes it",
  ),
});
