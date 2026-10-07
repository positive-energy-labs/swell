import { Array as Arr, Schema } from "effect";
import { SignatureId, type Signature } from "./facts.ts";

/**
 * `swell.config.ts`, declared once as a Schema and decoded at load: a typo is an error naming its path, never a
 * silently dropped limit. The types are derived from the schemas, so the declaration and the check cannot drift.
 */
const Kebab = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]*$/, { expected: "a kebab-case id" }));
const Argv = Schema.Array(Schema.String).check(Schema.isMinLength(1, { expected: "a non-empty argv" }));
const Millis = Schema.Int.check(Schema.isGreaterThan(0));
/**
 * Extra environment an instrument or actuator receives, by name, beyond the base set (PATH, HOME and the platform's
 * own). Children never inherit the controller's environment, and the controller's own `SWELL_*` is never one of these.
 */
const EnvName = Schema.String.check(
  Schema.isPattern(/^[A-Za-z_][A-Za-z0-9_]*$/, { expected: "an environment variable name" }),
  Schema.makeFilter((s) => !/^SWELL_/i.test(s) || "SWELL_* is the controller's own and never passed down"),
);

/** Git is the one plant kind today; another kind is another member of this union, with its own adapter. */
export const GitPlantSpec = Schema.Struct({
  id: Kebab,
  kind: Schema.Literal("git"),
  /** Absolute after load: the tree the controller loaded this plant's config from. Never the controller's own tree. */
  root: Schema.String,
  ref: Schema.String,
  remote: Schema.optionalKey(Schema.String),
});
export const PlantSpec = Schema.Union([GitPlantSpec]);
export type PlantSpec = typeof PlantSpec.Type;

/** A deterministic instrument: argv run at a clean checkout of the sample, printing one JSON `Measured`. */
export const SensorSpec = Schema.Struct({
  id: Kebab,
  run: Argv,
  /** A hung sensor is a failed measurement, never a wedged controller. Default ten minutes. */
  timeoutMs: Schema.optionalKey(Millis),
  env: Schema.optionalKey(Schema.Array(EnvName)),
});
export type SensorSpec = typeof SensorSpec.Type;

/**
 * A model instrument: it estimates what no sensor can measure and distills it to cited signals. It costs money and
 * cannot be recomputed, so it is budgeted by `every`, and its signatures come from a closed `vocabulary`: one outside
 * it is `new:` and needs a second source to count.
 */
export const ObserverSpec = Schema.Struct({
  id: Kebab,
  run: Argv,
  vocabulary: Schema.Array(SignatureId).check(Schema.isMinLength(1, { expected: "a non-empty vocabulary" })),
  /** Run only after this many commits since its last estimate. */
  every: Schema.optionalKey(Schema.Struct({ commits: Schema.Int.check(Schema.isGreaterThan(0)) })),
  /** Default thirty minutes: an observer is an agent. */
  timeoutMs: Schema.optionalKey(Millis),
  env: Schema.optionalKey(Schema.Array(EnvName)),
});
export type ObserverSpec = typeof ObserverSpec.Type;

export const ActuatorSpec = Schema.Struct({
  id: Kebab,
  /** argv, run with cwd at a worktree of the move branch and `SWELL_BRIEF` set to a JSON file path. */
  run: Argv,
  /** Default one hour: an actuator is an agent and the long step. */
  timeoutMs: Schema.optionalKey(Millis),
  env: Schema.optionalKey(Schema.Array(EnvName)),
});
export type ActuatorSpec = typeof ActuatorSpec.Type;

const Threshold = Schema.declare(
  (u: unknown): u is (signature: Signature) => boolean => typeof u === "function",
  { expected: "a function of a Signature" },
);

export const Mode = Schema.Literals(["manual", "auto"]);
export type Mode = typeof Mode.Type;

export const LoopSpec = Schema.Struct({
  id: Kebab,
  /** Sensor and observer ids whose signatures this loop acts on. */
  inputs: Schema.Array(Kebab).check(Schema.isMinLength(1, { expected: "at least one input" })),
  /** Default: two sources agree, or a known signature is seen in every run of the window, over at least three runs. */
  threshold: Schema.optionalKey(Threshold),
  /** The actuator's rate limit: attempts per UTC day. */
  limit: Schema.optionalKey(Schema.Struct({ perDay: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })),
  actuator: Kebab,
  /** `manual`: the operator approves each move. `auto`: the move applies, for pre-approved classes. */
  mode: Mode,
  /** Who approves, and who the loop acts for: a name an operator token maps to. */
  operator: Kebab,
});
export type LoopSpec = typeof LoopSpec.Type;

const dupes = (ids: ReadonlyArray<string>) => ids.filter((id, i) => ids.indexOf(id) !== i);

/** One plant's instruments, actuators and loops: everything a controller needs to close its loops. */
export const ControlSpec = Schema.Struct({
  plant: PlantSpec,
  sensors: Schema.Array(SensorSpec),
  observers: Schema.optionalKey(Schema.Array(ObserverSpec)),
  actuators: Schema.Array(ActuatorSpec),
  loops: Schema.Array(LoopSpec),
}).check(
  Schema.makeFilter((s) => {
    const issues: Array<Schema.FilterIssue> = [];
    const instruments = [...s.sensors, ...(s.observers ?? [])];
    for (const id of dupes(instruments.map((i) => i.id)))
      issues.push({ path: ["sensors"], issue: `instrument '${id}' is declared twice` });
    for (const id of dupes(s.actuators.map((a) => a.id)))
      issues.push({ path: ["actuators"], issue: `actuator '${id}' is declared twice` });
    for (const id of dupes(s.loops.map((l) => l.id)))
      issues.push({ path: ["loops"], issue: `loop '${id}' is declared twice` });
    const known = new Set(instruments.map((i) => i.id));
    const actuators = new Set(s.actuators.map((a) => a.id));
    s.loops.forEach((l, i) => {
      for (const src of l.inputs)
        if (!known.has(src))
          issues.push({ path: ["loops", i, "inputs"], issue: `unknown instrument '${src}'` });
      if (!actuators.has(l.actuator))
        issues.push({ path: ["loops", i, "actuator"], issue: `unknown actuator '${l.actuator}'` });
      // Push-only apply: an auto move lands by pushing to the remote, never by touching the plant's own tree.
      if (l.mode === "auto" && s.plant.remote === undefined)
        issues.push({
          path: ["loops", i, "mode"],
          issue: `auto, and apply is push-only: plant '${s.plant.id}' has no remote`,
        });
    });
    return Arr.isArrayNonEmpty(issues) ? issues : undefined;
  }),
);
export type ControlSpec = typeof ControlSpec.Type;

const options = { errors: "all", onExcessProperty: "error" } as const;

/** Decode a config at load. A misspelled key is an error, not an ignored one. */
export const decodeControl = (input: unknown) => Schema.decodeUnknownEffect(ControlSpec)(input, options);

/** The same check, synchronously, for a config built in code or a test. */
export const defineControl = (spec: ControlSpec): ControlSpec => {
  try {
    return Schema.decodeUnknownSync(ControlSpec)(spec, options);
  } catch (e) {
    throw new Error(`swell: ${e instanceof Error ? e.message : String(e)}`);
  }
};
