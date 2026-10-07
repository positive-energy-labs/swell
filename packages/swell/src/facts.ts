import { Fact, type Meta, Projection, type Reader } from "@swell/kernel";
import { Array as Arr, DateTime, Effect, Number as Num, Order, Record as Rec, Schema } from "effect";

export const meta = (label: string, plain: string, src: string): Meta => ({
  label,
  plain,
  owner: "kai",
  audience: "dev",
  layer: "control",
  ruled: true,
  src,
});

/**
 * What signals group under. Printable ASCII without `|` and `~`: a signature is part of a tally key, and a
 * character past `~` would sort out of the range a window reads. An instrument that prints another is a failed
 * measurement, never a corrupted or invisible tally.
 */
export const SignatureId = Schema.String.check(
  Schema.isPattern(/^[\x21-\x7b\x7d]+$/, { expected: "a signature: printable ASCII without | or ~" }),
);

/**
 * One thing a sensor or observer saw. `signature` is the identity signals group under; an observer draws it
 * from a closed vocabulary.
 */
export const Signal = Schema.Struct({
  signature: SignatureId,
  mechanism: Schema.String,
  path: Schema.optionalKey(Schema.String),
  cite: Schema.optionalKey(Schema.String),
  evidence: Schema.optionalKey(Schema.String),
});
export type Signal = typeof Signal.Type;

/**
 * The plant sampled at one instant: a commit, a deployment at a time, a folder listing. Instruments measure a
 * sample, never a moving tree. `commits` and `churn` are since `parent`.
 */
export const Sample = Fact.make({
  id: "control::sample",
  class: "observed",
  fields: {
    plant: Schema.String,
    sample: Schema.String,
    parent: Schema.optionalKey(Schema.String),
    commits: Schema.Number,
    churn: Schema.Number,
  },
  key: ["plant", "sample"],
  unique: true,
  indexes: { by_plant: ["plant", "at"] },
  meta: meta("Sample", "The plant at one instant the controller saw it.", "an instrument measures a sample"),
});

/**
 * One instrument over one sample. Signals are never rows: they ride inside the measurement and the signatures
 * rollup folds them. A sensor's measurement is `measured`; an observer's is `estimated`, cost money, and cannot
 * be recomputed, so it is irreplaceable.
 */
export const Measurement = Fact.make({
  id: "control::measurement",
  class: "observed",
  fields: {
    plant: Schema.String,
    instrument: Schema.String,
    sample: Schema.String,
    kind: Schema.Literals(["measured", "estimated"]),
    signals: Schema.Array(Signal),
    analyzed: Schema.Number,
    excluded: Schema.Number,
    failed: Schema.Number,
    error: Schema.optionalKey(Schema.String),
  },
  key: ["plant", "instrument", "sample"],
  unique: true,
  indexes: { by_instrument: ["plant", "instrument", "at"] },
  invariants: ["a failed measurement carries `error` and no signals; it is never a zero, and never a run"],
  meta: meta(
    "Measurement",
    "What one instrument saw in one sample, with how much it looked at.",
    "measurements carry their denominator",
  ),
});

/** A pointer to something another system wrote, keyed by its own id. Peers are read this way, never copied. */
export const Cite = Fact.make({
  id: "control::cite",
  class: "observed",
  fields: {
    urn: Schema.String,
    source: Schema.String,
    excerpt: Schema.optionalKey(Schema.String),
    hash: Schema.optionalKey(Schema.String),
  },
  key: ["urn"],
  unique: true,
  meta: meta(
    "Cite",
    "Something that happened in another system, cited by its own id.",
    "identity is where it was written; everything else is a citation",
  ),
});

/**
 * One loop's proposed move for one signature. `apply` is target-typed and opaque to the kernel: a PR ref for
 * git, a command call for The Current, a port call for Drive. `sources` is the evidence set at proposal time,
 * so a dismissal is keyed to it and holds until the set grows: hysteresis. `arming` counts the moves on this
 * signature: once one is made (or can no longer be), the next is a new arming with no dismissals behind it.
 */
export const Proposal = Fact.make({
  id: "control::proposal",
  class: "inferred",
  fields: {
    plant: Schema.String,
    loop: Schema.String,
    subject: Schema.String,
    signature: Schema.String,
    arming: Schema.Int,
    operator: Schema.String,
    apply: Schema.String,
    text: Schema.String,
    cites: Schema.Array(Schema.String),
    sources: Schema.Array(Schema.String),
  },
  key: ["plant", "loop", "subject"],
  unique: true,
  indexes: {
    by_loop: ["plant", "loop", "at"],
    by_signature: ["plant", "loop", "signature", "at"],
    by_operator: ["operator", "at"],
  },
  invariants: [
    "a loop never moves a plant without a verdict; a yes applies exactly `apply`",
    "the subject is the signature, the evidence set and the arming, so new evidence is a new subject",
  ],
  meta: meta(
    "Proposal",
    "A move one loop wants made, waiting for its operator.",
    "operators approve the thing that will run",
  ),
});

/** The operator's yes or no, or auto mode's. `cite` names where it was decided when that was elsewhere, such as a PR. */
export const Verdict = Fact.make({
  id: "control::verdict",
  class: "event",
  fields: {
    plant: Schema.String,
    loop: Schema.String,
    subject: Schema.String,
    accept: Schema.Boolean,
    text: Schema.optionalKey(Schema.String),
    cite: Schema.optionalKey(Schema.String),
  },
  key: ["plant", "loop", "subject"],
  unique: true,
  indexes: { by_plant: ["plant", "at"], by_accept: ["plant", "accept", "at"] },
  meta: meta("Verdict", "The yes or no on a proposal.", "only a verdict turns a proposal into a move"),
});

export const ControlFacts = [Sample, Measurement, Cite, Proposal, Verdict] as const;

const utc = (at: number) => DateTime.makeUnsafe(at);

/** The tally week: the UTC Monday it starts on, as an ISO date. Sortable, so a window is a run of keys. */
export const week = (at: number): string =>
  DateTime.formatIsoDate(DateTime.startOf(utc(at), "week", { weekStartsOn: 1 }));

/** Midnight UTC of the day `now` falls in: where `limit.perDay` starts counting. */
export const dayStart = (now: number): number => DateTime.toEpochMillis(DateTime.startOf(utc(now), "day"));

const weeksBack = (at: number, n: number): ReadonlyArray<string> =>
  Arr.makeBy(n, (i) => week(DateTime.toEpochMillis(DateTime.subtract(utc(at), { weeks: i }))));

const ROLLUP = "control::signatures";

/**
 * Tally keys, week first so a window is one bounded read per week: `<plant>|<week>|run|<instrument>` counts the
 * measurements that ran (`runs`) and those that failed (`failed`, never a run that saw nothing);
 * `<plant>|<week>|sig|<signature>` counts `hits` (signals), `seen` (measurements that saw it, once however many
 * signals) and marks each source.
 */
export const signaturesRollup = Projection.rollup({
  id: ROLLUP,
  on: Measurement,
  apply: (row) =>
    Effect.sync(() => {
      const at = `${row.plant}|${week(row.at)}`;
      if (row.error !== undefined) return [[`${at}|run|${row.instrument}`, { failed: 1 }] as const];
      return [
        [`${at}|run|${row.instrument}`, { runs: 1 }] as const,
        ...Rec.collect(
          Arr.groupBy(row.signals, (s) => s.signature),
          (sig, xs) =>
            [`${at}|sig|${sig}`, { hits: xs.length, seen: 1, [`src:${row.instrument}`]: 1 }] as const,
        ),
      ];
    }),
});

export const Signature = Schema.Struct({
  plant: Schema.String,
  signature: Schema.String,
  /** Distinct instruments that saw it in the window. Strength is this, never a count. */
  sources: Schema.Array(Schema.String),
  /** Signals in the window: a measurement with two signals of one signature adds two. */
  hits: Schema.Number,
  /** Measurements in the window that saw it at least once, however many signals each held. */
  seen: Schema.Number,
  /** Measurements in the window by the sources that saw it. A failed measurement is not a run. */
  runs: Schema.Number,
  /** `seen / runs`, 0..1: the share of runs that saw it. */
  rate: Schema.Number,
});
/** A signature and its tally over the window. It is never a row: it is folded from the rollup on read. */
export type Signature = typeof Signature.Type;

const PER_WEEK = 2048;

/** One exact read per window week. A week that fills the limit is a defect, said out loud, never a silent cut. */
const weekRows = (db: Reader<never>, plant: string, w: string) =>
  db
    .tallies(ROLLUP, { gte: `${plant}|${w}|`, lt: `${plant}|${w}|~`, limit: PER_WEEK })
    .pipe(
      Effect.flatMap((rows) =>
        rows.length === PER_WEEK
          ? Effect.die(new Error(`swell: ${plant} week ${w} has ${PER_WEEK}+ tally rows; widen the read`))
          : Effect.succeed(rows.map((r) => ({ rest: r.key.slice(`${plant}|${w}|`.length), value: r.value }))),
      ),
    );

const sources = (value: Readonly<Record<string, number>>) =>
  Rec.keys(value)
    .filter((k) => k.startsWith("src:"))
    .map((k) => k.slice(4));

/**
 * Signatures of a plant over the last `weeks`, folded from the rollup. An observer's `new:X` joins `X` when another
 * source saw `X`: the outsider then has its second source.
 */
export const signaturesOf = Effect.fn("swell/signaturesOf")(function* (
  db: Reader<never>,
  plant: string,
  now: number,
  weeks = 4,
) {
  const rows = (yield* Effect.forEach(weeksBack(now, weeks), (w) => weekRows(db, plant, w))).flat();
  const runs = Rec.map(
    Arr.groupBy(
      rows.filter((r) => r.rest.startsWith("run|")),
      (r) => r.rest.slice(4),
    ),
    (xs) => Num.sumAll(xs.map((x) => x.value.runs ?? 0)),
  );
  const bySig = Arr.groupBy(
    rows.filter((r) => r.rest.startsWith("sig|")),
    (r) => r.rest.slice(4),
  );
  for (const sig of Rec.keys(bySig)) {
    const known = sig.startsWith("new:") ? sig.slice(4) : undefined;
    if (known !== undefined && bySig[known] !== undefined) {
      bySig[known] = [...bySig[known], ...bySig[sig]!];
      delete bySig[sig];
    }
  }
  const folded = Rec.collect(bySig, (signature, xs): Signature => {
    const srcs = Arr.sort(Arr.dedupe(xs.flatMap((x) => sources(x.value))), Order.String);
    const seen = Num.sumAll(xs.map((x) => x.value.seen ?? 0));
    const r = Num.sumAll(srcs.map((s) => runs[s] ?? 0));
    const hits = Num.sumAll(xs.map((x) => x.value.hits ?? 0));
    return { plant, signature, sources: srcs, hits, seen, runs: r, rate: r === 0 ? 0 : seen / r };
  });
  return Arr.sort(
    folded,
    Order.mapInput(Order.String, (s: Signature) => s.signature),
  );
});

export const Signatures = Projection.make({
  id: "control::signatures",
  args: { plant: Schema.String, now: Schema.Number },
  returns: Schema.Array(Signature),
  reads: [Measurement],
  shows: ["signatures"],
  rollups: [signaturesRollup],
  run: ({ plant, now }, db) => signaturesOf(db, plant, now),
  meta: meta(
    "Signatures",
    "What the instruments keep seeing in one plant, by signature, with how many sources agree.",
    "a signature is never its own row",
  ),
});
