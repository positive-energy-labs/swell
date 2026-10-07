import { Fact, type Meta, Projection, type Reader } from "@tc/kernel";
import { Effect, Schema } from "effect";

export const meta = (label: string, plain: string, src: string): Meta => ({
  label,
  plain,
  owner: "kai",
  audience: "dev",
  layer: "tide",
  ruled: true,
  src,
});

/** One thing a sensor saw. `fingerprint` is the identity signals group under; a model sensor draws it from a closed vocabulary. */
export const Finding = Schema.Struct({
  fingerprint: Schema.String,
  mechanism: Schema.String,
  path: Schema.optionalKey(Schema.String),
  cite: Schema.optionalKey(Schema.String),
  evidence: Schema.optionalKey(Schema.String),
});
export type Finding = typeof Finding.Type;

/** A plant state a sensor can read: a commit, a deployment at a time, a folder listing. `commits` and `churn` are since `parent`. */
export const Snapshot = Fact.make({
  id: "tide::snapshot",
  class: "observed",
  fields: {
    plant: Schema.String,
    snapshot: Schema.String,
    parent: Schema.optionalKey(Schema.String),
    commits: Schema.Number,
    churn: Schema.Number,
  },
  key: ["plant", "snapshot"],
  unique: true,
  indexes: { by_plant: ["plant", "at"] },
  meta: meta(
    "Snapshot",
    "The plant at one point the host saw it.",
    "a sensor reads a snapshot, never a moving tree",
  ),
});

/**
 * One sensor over one snapshot. Signals are never rows: they ride inside the reading and the issues rollup
 * folds them by fingerprint. A `model` reading cost money and cannot be recomputed, so it is irreplaceable.
 */
export const Reading = Fact.make({
  id: "tide::reading",
  class: "observed",
  fields: {
    plant: Schema.String,
    sensor: Schema.String,
    snapshot: Schema.String,
    kind: Schema.Literals(["measured", "model"]),
    findings: Schema.Array(Finding),
    analyzed: Schema.Number,
    excluded: Schema.Number,
    failed: Schema.Number,
    error: Schema.optionalKey(Schema.String),
  },
  key: ["plant", "sensor", "snapshot"],
  unique: true,
  indexes: { by_sensor: ["plant", "sensor", "at"] },
  invariants: ["a failed reading carries `error` and no findings; it is never a zero"],
  meta: meta(
    "Reading",
    "What one sensor saw in one snapshot, with how much it looked at.",
    "readings carry their denominator",
  ),
});

/** A pointer to something that happened elsewhere, keyed by its own id. Peers are read this way, never copied. */
export const Observed = Fact.make({
  id: "tide::observed",
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
    "Observed",
    "Something that happened in another system, cited by its own id.",
    "identity is where it was written; everything else is a citation",
  ),
});

/**
 * One loop's wave for one issue. `apply` is target-typed and opaque to the kernel: a PR ref for git, a
 * command call for The Current, a port call for Drive. `sources` is the evidence set at proposal time, so
 * a dismissal is keyed to it and the issue returns only when the set grows.
 */
export const Proposal = Fact.make({
  id: "tide::proposal",
  class: "inferred",
  fields: {
    plant: Schema.String,
    loop: Schema.String,
    subject: Schema.String,
    fingerprint: Schema.String,
    person: Schema.String,
    apply: Schema.String,
    text: Schema.String,
    cites: Schema.Array(Schema.String),
    sources: Schema.Array(Schema.String),
  },
  key: ["plant", "loop", "subject"],
  unique: true,
  indexes: { by_loop: ["plant", "loop", "at"], by_person: ["person", "at"] },
  invariants: [
    "a loop never changes a plant without a verdict; a yes applies exactly `apply`",
    "the subject is the fingerprint plus the evidence set, so new evidence is a new subject",
  ],
  meta: meta(
    "Proposal",
    "A change one loop wants made, waiting for its person.",
    "humans approve the thing that will run",
  ),
});

/** A person's yes or no, or a policy's for a pre-approved class. `cite` names where it was decided when that was elsewhere, such as a PR. */
export const Verdict = Fact.make({
  id: "tide::verdict",
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
  meta: meta("Verdict", "The yes or no on a proposal.", "only a verdict turns a proposal into a change"),
});

export const TideFacts = [Snapshot, Reading, Observed, Proposal, Verdict] as const;

/** ISO week key, so tallies stay bounded and a fixed issue decays out of the window. */
export const week = (at: number): string => {
  const d = new Date(at);
  const day = (d.getUTCDay() + 6) % 7;
  d.setUTCDate(d.getUTCDate() - day + 3);
  const year = d.getUTCFullYear();
  const jan4 = Date.UTC(year, 0, 4);
  const n = 1 + Math.round((d.getTime() - jan4) / 604_800_000 + ((new Date(jan4).getUTCDay() + 6) % 7) / 7);
  return `${year}-W${String(n).padStart(2, "0")}`;
};

const weeksBack = (at: number, n: number): ReadonlyArray<string> =>
  Array.from({ length: n }, (_, i) => week(at - i * 604_800_000));

/** Tally keys: `<plant>|run|<sensor>|<week>` counts readings; `<plant>|fp|<fingerprint>|<week>` counts hits and marks each source. */
export const issuesRollup = Projection.rollup({
  id: "tide::issues",
  on: Reading,
  apply: (row) =>
    Effect.sync(() => {
      const w = week(row.at);
      const out: Array<readonly [string, Record<string, number>]> = [
        [`${row.plant}|run|${row.sensor}|${w}`, { runs: 1 }],
      ];
      const hits = new Map<string, number>();
      for (const f of row.findings) hits.set(f.fingerprint, (hits.get(f.fingerprint) ?? 0) + 1);
      for (const [fp, n] of hits)
        out.push([`${row.plant}|fp|${fp}|${w}`, { hits: n, [`src:${row.sensor}`]: 1 }]);
      return out;
    }),
});

export interface Issue {
  readonly plant: string;
  readonly fingerprint: string;
  /** Distinct sensors that saw it in the window. Strength is this, never a count. */
  readonly sources: ReadonlyArray<string>;
  readonly hits: number;
  /** Readings in the window by the sources that saw it, so `rate` is hits per run. */
  readonly runs: number;
  readonly rate: number;
}

/** Issues of a plant over the last `weeks`, folded from the rollup. One bounded read per key family. */
export const issuesOf = (db: Reader<never>, plant: string, now: number, weeks = 4) =>
  Effect.gen(function* () {
    const window = new Set(weeksBack(now, weeks));
    const inWindow = (key: string) => window.has(key.slice(key.lastIndexOf("|") + 1));
    const runs = new Map<string, number>();
    for (const r of yield* db.tallies("tide::issues", {
      gte: `${plant}|run|`,
      lt: `${plant}|run|~`,
      limit: 2048,
    })) {
      if (!inWindow(r.key)) continue;
      const sensor = r.key.split("|")[2]!;
      runs.set(sensor, (runs.get(sensor) ?? 0) + (r.value.runs ?? 0));
    }
    const issues = new Map<string, { hits: number; sources: Set<string> }>();
    for (const r of yield* db.tallies("tide::issues", {
      gte: `${plant}|fp|`,
      lt: `${plant}|fp|~`,
      limit: 2048,
    })) {
      if (!inWindow(r.key)) continue;
      const fp = r.key.slice(`${plant}|fp|`.length, r.key.lastIndexOf("|"));
      const cur = issues.get(fp) ?? { hits: 0, sources: new Set<string>() };
      cur.hits += r.value.hits ?? 0;
      for (const k of Object.keys(r.value)) if (k.startsWith("src:")) cur.sources.add(k.slice(4));
      issues.set(fp, cur);
    }
    return [...issues].map(([fingerprint, { hits, sources }]): Issue => {
      const srcs = [...sources].sort();
      const r = srcs.reduce((n, s) => n + (runs.get(s) ?? 0), 0);
      return { plant, fingerprint, sources: srcs, hits, runs: r, rate: r === 0 ? 0 : hits / r };
    });
  });

export const IssueSchema = Schema.Struct({
  plant: Schema.String,
  fingerprint: Schema.String,
  sources: Schema.Array(Schema.String),
  hits: Schema.Number,
  runs: Schema.Number,
  rate: Schema.Number,
});

export const Issues = Projection.make({
  id: "tide::issues",
  args: { plant: Schema.String, now: Schema.Number },
  returns: Schema.Array(IssueSchema),
  reads: [Reading],
  shows: ["issues"],
  rollups: [issuesRollup],
  run: ({ plant, now }, db) => issuesOf(db, plant, now),
  meta: meta(
    "Issues",
    "What the sensors keep seeing in one plant, by fingerprint, with how many sources agree.",
    "an issue is never its own row",
  ),
});
