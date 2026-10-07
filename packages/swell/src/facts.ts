import { Fact, type Meta, Projection, type Reader } from "@tc/kernel";
import { Effect, Schema } from "effect";

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
 * One thing a sensor or observer saw. `signature` is the identity signals group under; an observer draws it
 * from a closed vocabulary.
 */
export const Signal = Schema.Struct({
  signature: Schema.String,
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
  invariants: ["a failed measurement carries `error` and no signals; it is never a zero"],
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
 * so a dismissal is keyed to it and holds until the set grows: hysteresis.
 */
export const Proposal = Fact.make({
  id: "control::proposal",
  class: "inferred",
  fields: {
    plant: Schema.String,
    loop: Schema.String,
    subject: Schema.String,
    signature: Schema.String,
    operator: Schema.String,
    apply: Schema.String,
    text: Schema.String,
    cites: Schema.Array(Schema.String),
    sources: Schema.Array(Schema.String),
  },
  key: ["plant", "loop", "subject"],
  unique: true,
  indexes: { by_loop: ["plant", "loop", "at"], by_operator: ["operator", "at"] },
  invariants: [
    "a loop never moves a plant without a verdict; a yes applies exactly `apply`",
    "the subject is the signature plus the evidence set, so new evidence is a new subject",
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
  indexes: { by_plant: ["plant", "at"] },
  meta: meta("Verdict", "The yes or no on a proposal.", "only a verdict turns a proposal into a move"),
});

export const ControlFacts = [Sample, Measurement, Cite, Proposal, Verdict] as const;

/** ISO week key, so tallies stay bounded and a fixed signature decays out of the window. */
export const week = (at: number): string => {
  const d = new Date(at);
  // Midnight first: without it any instant past 12:00 UTC rounds into the next week.
  d.setUTCHours(0, 0, 0, 0);
  const day = (d.getUTCDay() + 6) % 7;
  d.setUTCDate(d.getUTCDate() - day + 3);
  const year = d.getUTCFullYear();
  const jan4 = Date.UTC(year, 0, 4);
  const n = 1 + Math.round((d.getTime() - jan4) / 604_800_000 + ((new Date(jan4).getUTCDay() + 6) % 7) / 7);
  return `${year}-W${String(n).padStart(2, "0")}`;
};

const weeksBack = (at: number, n: number): ReadonlyArray<string> =>
  Array.from({ length: n }, (_, i) => week(at - i * 604_800_000));

/** Tally keys: `<plant>|run|<instrument>|<week>` counts measurements; `<plant>|sig|<signature>|<week>` counts hits and marks each source. */
export const signaturesRollup = Projection.rollup({
  id: "control::signatures",
  on: Measurement,
  apply: (row) =>
    Effect.sync(() => {
      const w = week(row.at);
      const out: Array<readonly [string, Record<string, number>]> = [
        [`${row.plant}|run|${row.instrument}|${w}`, { runs: 1 }],
      ];
      const hits = new Map<string, number>();
      for (const s of row.signals) hits.set(s.signature, (hits.get(s.signature) ?? 0) + 1);
      for (const [sig, n] of hits)
        out.push([`${row.plant}|sig|${sig}|${w}`, { hits: n, [`src:${row.instrument}`]: 1 }]);
      return out;
    }),
});

/** A signature and its tally over the window. It is never a row: it is folded from the rollup on read. */
export interface Signature {
  readonly plant: string;
  readonly signature: string;
  /** Distinct instruments that saw it in the window. Strength is this, never a count. */
  readonly sources: ReadonlyArray<string>;
  readonly hits: number;
  /** Measurements in the window by the sources that saw it, so `rate` is hits per run. */
  readonly runs: number;
  readonly rate: number;
}

/** Signatures of a plant over the last `weeks`, folded from the rollup. One bounded read per key family. */
export const signaturesOf = (db: Reader<never>, plant: string, now: number, weeks = 4) =>
  Effect.gen(function* () {
    const window = new Set(weeksBack(now, weeks));
    const inWindow = (key: string) => window.has(key.slice(key.lastIndexOf("|") + 1));
    const runs = new Map<string, number>();
    for (const r of yield* db.tallies("control::signatures", {
      gte: `${plant}|run|`,
      lt: `${plant}|run|~`,
      limit: 2048,
    })) {
      if (!inWindow(r.key)) continue;
      const instrument = r.key.split("|")[2]!;
      runs.set(instrument, (runs.get(instrument) ?? 0) + (r.value.runs ?? 0));
    }
    const sigs = new Map<string, { hits: number; sources: Set<string> }>();
    for (const r of yield* db.tallies("control::signatures", {
      gte: `${plant}|sig|`,
      lt: `${plant}|sig|~`,
      limit: 2048,
    })) {
      if (!inWindow(r.key)) continue;
      const sig = r.key.slice(`${plant}|sig|`.length, r.key.lastIndexOf("|"));
      const cur = sigs.get(sig) ?? { hits: 0, sources: new Set<string>() };
      cur.hits += r.value.hits ?? 0;
      for (const k of Object.keys(r.value)) if (k.startsWith("src:")) cur.sources.add(k.slice(4));
      sigs.set(sig, cur);
    }
    return [...sigs].map(([signature, { hits, sources }]): Signature => {
      const srcs = [...sources].sort();
      const r = srcs.reduce((n, s) => n + (runs.get(s) ?? 0), 0);
      return { plant, signature, sources: srcs, hits, runs: r, rate: r === 0 ? 0 : hits / r };
    });
  });

export const SignatureSchema = Schema.Struct({
  plant: Schema.String,
  signature: Schema.String,
  sources: Schema.Array(Schema.String),
  hits: Schema.Number,
  runs: Schema.Number,
  rate: Schema.Number,
});

export const Signatures = Projection.make({
  id: "control::signatures",
  args: { plant: Schema.String, now: Schema.Number },
  returns: Schema.Array(SignatureSchema),
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
