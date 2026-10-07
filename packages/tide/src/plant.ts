import { Port } from "@tc/kernel";
import { Context, Data, Effect, Layer, Schema } from "effect";
import { Finding, meta } from "./facts.ts";

/** Every plant failure, so a host can skip one dead plant this tick and still sweep the others. */
export class PlantError extends Data.TaggedError("PlantError")<{
  readonly op: string;
  readonly message: string;
  readonly cause?: unknown;
}> {}

export interface PlantSpec {
  readonly id: string;
  readonly kind: "git";
  /** Absolute path of the tree the host loaded this plant's config from. The plant is never the host's own tree. */
  readonly root: string;
  readonly ref: string;
  readonly remote?: string;
}

export interface SensorSpec {
  readonly id: string;
  readonly kind: "measured" | "model";
  /** argv, run with cwd at a clean checkout of the snapshot; prints one JSON `Sensed` on stdout. */
  readonly run: ReadonlyArray<string>;
  /** A model sensor is budgeted: run only after this many commits since its last reading. */
  readonly every?: { readonly commits: number };
  /** Closed vocabulary for model fingerprints; one outside it is prefixed `new:` and needs a second source to count. */
  readonly vocabulary?: ReadonlyArray<string>;
  /** A hung sensor is a failed reading, never a wedged host. Default ten minutes. */
  readonly timeoutMs?: number;
}

export interface ActuatorSpec {
  readonly id: string;
  /** argv, run with cwd at a worktree of the snapshot and `TIDE_BRIEF` set to a JSON file path. */
  readonly run: ReadonlyArray<string>;
  /** Default one hour: an actuator is an agent and the long step. */
  readonly timeoutMs?: number;
}

/** What a sensor prints. Decoded, never cast: a malformed reading is a failed one, and `failed` keeps it from reading as a zero. */
export const Sensed = Schema.Struct({
  findings: Schema.Array(Finding),
  analyzed: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  excluded: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  failed: Schema.Finite.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
  error: Schema.optionalKey(Schema.String),
});
export type Sensed = typeof Sensed.Type;
export const SensedJson = Schema.fromJsonString(Sensed);
export const failedSensed = (error: string): Sensed => ({
  findings: [],
  analyzed: 0,
  excluded: 0,
  failed: 1,
  error,
});

export interface Head {
  readonly snapshot: string;
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

export interface Brief {
  readonly loop: string;
  readonly fingerprint: string;
  readonly snapshot: string;
  readonly sources: ReadonlyArray<string>;
  readonly findings: ReadonlyArray<Finding>;
  /** The text of the last rejection of this fingerprint, so the next attempt reads it. */
  readonly feedback: string;
}

export interface Decision {
  readonly apply: string;
  readonly accept: boolean;
  readonly text: string;
  readonly cite: string;
}

/**
 * The plant as a service: snapshot, sense, act, propose, observe verdicts, apply. Git is one implementation;
 * the kernel never sees a branch. `apply` strings are target-typed and opaque above this seam.
 */
export interface PlantService {
  readonly head: (plant: PlantSpec, parent: string | undefined) => Effect.Effect<Head, PlantError>;
  readonly sense: (
    plant: PlantSpec,
    sensor: SensorSpec,
    snapshot: string,
  ) => Effect.Effect<Sensed, PlantError>;
  readonly act: (
    plant: PlantSpec,
    actuator: ActuatorSpec,
    brief: Brief,
  ) => Effect.Effect<Changes | null, PlantError>;
  /** `pr`: put the changes where the plant's people decide and return a cite there. `auto`: an apply string, no gate. */
  readonly propose: (
    plant: PlantSpec,
    changes: Changes,
    text: string,
    gate: "pr" | "auto",
  ) => Effect.Effect<{ readonly apply: string; readonly cite: string }, PlantError>;
  readonly decisions: (
    plant: PlantSpec,
    applies: ReadonlyArray<string>,
  ) => Effect.Effect<ReadonlyArray<Decision>, PlantError>;
  readonly apply: (plant: PlantSpec, apply: string) => Effect.Effect<string, PlantError>;
}

export class Plant extends Context.Service<Plant, PlantService>()("tide/Plant") {}

/** A scripted plant: a world the test writes, so failures are chosen rather than random. */
export const fakeWorld = () => ({
  heads: [] as Array<Head>,
  sensed: new Map<string, Sensed>(),
  acts: [] as Array<Brief>,
  changes: null as Changes | null,
  proposed: [] as Array<{ changes: Changes; text: string }>,
  decisions: [] as Array<Decision>,
  applied: [] as Array<string>,
  failNext: 0,
  headFails: false,
});
export type FakeWorld = ReturnType<typeof fakeWorld>;

export const fakePlant = (world: FakeWorld): Layer.Layer<Plant> =>
  Layer.succeed(Plant, {
    head: (plant) =>
      world.headFails
        ? Effect.fail(new PlantError({ op: `head ${plant.id}`, message: "remote is down" }))
        : Effect.sync(() => world.heads.at(-1) ?? { snapshot: "s0", commits: 0, churn: 0 }),
    sense: (_plant, sensor, snapshot) =>
      Effect.sync(
        () =>
          world.sensed.get(`${sensor.id}@${snapshot}`) ??
          failedSensed(`no scripted reading for ${sensor.id}@${snapshot}`),
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
      Effect.sync(() => {
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
  id: "tide::plant",
  service: Plant,
  live: Layer.effect(
    Plant,
    Effect.die(new Error("tide::plant live layer is provided by the host, per plant kind")),
  ),
  fake: fakePlant(shared),
  meta: meta(
    "Plant",
    "The thing a tide measures and changes: a repo, a deployment, a folder.",
    "git is one adapter; the core never assumes it",
  ),
});
