import { Port } from "@swell/kernel";
import { Context, Effect, Layer, Schema } from "effect";
import { meta, Signal } from "./facts.ts";
import type { ActuatorSpec, PlantSpec } from "./spec.ts";

/** Every plant failure, so a controller can skip one dead plant this tick and still sweep the others. */
export class PlantError extends Schema.TaggedError<PlantError>()("PlantError", {
  op: Schema.String,
  message: Schema.String,
}) {}

/** What one measure call runs: the instrument resolved to argv, a timeout and the environment it may see. */
export interface Instrument {
  readonly id: string;
  readonly run: ReadonlyArray<string>;
  readonly timeoutMs: number;
  readonly env: ReadonlyArray<string>;
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
export const Sampled = Schema.Struct({
  sample: Schema.String,
  parent: Schema.optionalKey(Schema.String),
  commits: Schema.Number,
  churn: Schema.Number,
});
export type Sampled = typeof Sampled.Type;

/** What an actuator left behind, before anyone has approved it. */
export const Changes = Schema.Struct({ ref: Schema.String, head: Schema.String, summary: Schema.String });
export type Changes = typeof Changes.Type;

/** The evidence handed to an actuator. Never the number: the actuator is told what was seen, not what to move. */
export const Brief = Schema.Struct({
  loop: Schema.String,
  signature: Schema.String,
  /** The move's identity: signature, evidence set and arming. A retry of this subject resumes its own branch. */
  subject: Schema.String,
  sample: Schema.String,
  sources: Schema.Array(Schema.String),
  signals: Schema.Array(Signal),
  /** The text of the last rejection of this signature, so the next attempt reads it. */
  feedback: Schema.String,
  /** Why the last attempt on this subject failed, or why the last arming's move could not land, or empty. */
  previous: Schema.String,
});
export type Brief = typeof Brief.Type;

/** A decision made where the plant keeps its operator, such as a merged or closed PR. */
export const Decision = Schema.Struct({
  apply: Schema.String,
  accept: Schema.Boolean,
  text: Schema.String,
  cite: Schema.String,
});
export type Decision = typeof Decision.Type;

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

/** The controller resolves this port to its plant layer; a test resolves it to `fakePlant` from `swell/testing`. */
export const PlantPort = Port.make({
  id: "control::plant",
  service: Plant,
  live: Layer.effect(
    Plant,
    Effect.die(new Error("control::plant live layer is provided by the controller, per plant kind")),
  ),
  fake: Layer.effect(
    Plant,
    Effect.die(new Error("control::plant fake is fakePlant(world) from swell/testing")),
  ),
  impl: "real",
  meta: meta(
    "Plant",
    "The thing a controller measures and moves: a repo, a deployment, a folder.",
    "git is the one adapter today; another kind is another adapter behind this port",
  ),
});
